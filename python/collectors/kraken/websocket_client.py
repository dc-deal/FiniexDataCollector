"""
FiniexDataCollector - Kraken WebSocket Client
WebSocket client for Kraken v2 API with automatic reconnection.

Location: python/collectors/kraken/websocket_client.py
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import List, Optional, Callable

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK
)

from python.collectors.base import AbstractCollector
from python.collectors.kraken.message_parser import KrakenMessageParser
from python.collectors.kraken.symbols import to_kraken_format, normalize_symbol
from python.framework.types.tick_types import TickData
from python.framework.exceptions.collector_exceptions import (
    WebSocketConnectionError,
    WebSocketSubscriptionError,
    CollectorHealthReport
)
from python.utils.logging_setup import get_collector_logger


class KrakenWebSocketClient(AbstractCollector):
    """
    Kraken WebSocket v2 ticker collector.
    
    Features:
    - Automatic reconnection with exponential backoff
    - Heartbeat monitoring
    - Multi-symbol subscription
    - Graceful shutdown
    """
    
    DEFAULT_URL = "wss://ws.kraken.com/v2"
    
    def __init__(
        self,
        symbols: List[str],
        url: str = DEFAULT_URL,
        reconnect_initial_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        heartbeat_interval: float = 30.0
    ):
        """
        Initialize Kraken WebSocket client.
        
        Args:
            symbols: List of symbols to subscribe (e.g., ["BTC/USD", "ETH/USD"])
            url: WebSocket URL
            reconnect_initial_delay: Initial reconnect delay in seconds
            reconnect_max_delay: Maximum reconnect delay in seconds
            heartbeat_interval: Heartbeat check interval in seconds
        """
        super().__init__(name="kraken", symbols=symbols)
        
        self._url = url
        self._reconnect_initial_delay = reconnect_initial_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._heartbeat_interval = heartbeat_interval
        
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._parser = KrakenMessageParser()
        self._logger = get_collector_logger("kraken")
        
        self._connection_status = "disconnected"
        self._last_message_time: Optional[datetime] = None
        self._reconnect_attempt = 0
        self._should_reconnect = True
        
        # Tasks
        self._receive_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> bool:
        """
        Establish WebSocket connection.
        
        Returns:
            True if connection successful
        """
        try:
            self._logger.info(f"Connecting to {self._url}...")
            
            self._websocket = await websockets.connect(
                self._url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5
            )
            
            self._connection_status = "connected"
            self._reconnect_attempt = 0
            self._last_message_time = datetime.now(timezone.utc)
            
            self._logger.info("WebSocket connected successfully")
            return True
            
        except Exception as e:
            self._connection_status = "failed"
            self._logger.error(f"Connection failed: {e}")
            raise WebSocketConnectionError(
                message=str(e),
                url=self._url,
                attempt=self._reconnect_attempt
            )
    
    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        self._should_reconnect = False
        self._connection_status = "disconnecting"
        
        # Cancel tasks
        if self._receive_task:
            self._receive_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        # Close WebSocket
        if self._websocket:
            try:
                await self._websocket.close()
            except Exception as e:
                self._logger.warning(f"Error closing WebSocket: {e}")
        
        self._websocket = None
        self._connection_status = "disconnected"
        self._is_running = False
        self._logger.info("Disconnected from Kraken WebSocket")
    
    async def subscribe(self) -> bool:
        """
        Subscribe to ticker channel for configured symbols.
        
        Returns:
            True if subscription successful
        """
        if not self._websocket:
            return False
        
        # Convert symbols to Kraken format
        kraken_symbols = [to_kraken_format(s) for s in self._symbols]
        
        subscribe_msg = {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": kraken_symbols
            }
        }
        
        try:
            await self._websocket.send(json.dumps(subscribe_msg))
            self._logger.info(f"Subscription request sent for {kraken_symbols}")
            
            # Wait for confirmation (with timeout)
            try:
                response = await asyncio.wait_for(
                    self._websocket.recv(),
                    timeout=10.0
                )
                
                if self._parser.is_subscription_confirmation(response):
                    self._logger.info("Subscription confirmed")
                    return True
                
                error = self._parser.is_error_message(response)
                if error:
                    raise WebSocketSubscriptionError(
                        message=error,
                        channel="ticker",
                        symbols=kraken_symbols
                    )
                    
            except asyncio.TimeoutError:
                self._logger.warning("Subscription confirmation timeout")
            
            return True  # Continue anyway, might receive data
            
        except Exception as e:
            self._logger.error(f"Subscription failed: {e}")
            raise WebSocketSubscriptionError(
                message=str(e),
                channel="ticker",
                symbols=kraken_symbols
            )
    
    async def start(self) -> None:
        """Start tick collection."""
        self._is_running = True
        self._should_reconnect = True
        self._start_time = datetime.now(timezone.utc)
        
        self._logger.info(f"Starting Kraken collector for {len(self._symbols)} symbols")
        
        while self._should_reconnect:
            try:
                # Connect
                await self.connect()
                
                # Subscribe
                await self.subscribe()
                
                # Start receive loop
                self._receive_task = asyncio.create_task(self._receive_loop())
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                
                # Wait for tasks
                await asyncio.gather(
                    self._receive_task,
                    self._heartbeat_task,
                    return_exceptions=True
                )
                
            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as e:
                self._logger.warning(f"Connection closed: {e}")
                self._connection_status = "reconnecting"
                
            except WebSocketConnectionError as e:
                self._logger.error(f"Connection error: {e}")
                
            except Exception as e:
                self._logger.error(f"Unexpected error: {e}")
                self._errors_count += 1
            
            # Reconnect with backoff
            if self._should_reconnect:
                delay = self._get_reconnect_delay()
                self._logger.info(f"Reconnecting in {delay:.1f}s...")
                await asyncio.sleep(delay)
                self._reconnect_attempt += 1
    
    async def stop(self) -> None:
        """Stop tick collection gracefully."""
        self._logger.info("Stopping Kraken collector...")
        self._should_reconnect = False
        await self.disconnect()
    
    async def _receive_loop(self) -> None:
        """Main message receive loop."""
        if not self._websocket:
            return
        
        async for message in self._websocket:
            self._last_message_time = datetime.now(timezone.utc)
            
            # Skip heartbeats
            if self._parser.is_heartbeat(message):
                continue
            
            # Parse ticker messages
            try:
                ticks = self._parser.parse_message(message)
                if ticks:
                    for tick in ticks:
                        self._emit_tick(tick)
                        
            except Exception as e:
                self._logger.warning(f"Message parse error: {e}")
                self._errors_count += 1
    
    async def _heartbeat_loop(self) -> None:
        """Monitor connection health via heartbeat."""
        while self._is_running and self._websocket:
            await asyncio.sleep(self._heartbeat_interval)
            
            if not self._last_message_time:
                continue
            
            # Check for stale connection
            silence = (datetime.now(timezone.utc) - self._last_message_time).total_seconds()
            
            if silence > self._heartbeat_interval * 2:
                self._logger.warning(
                    f"No messages for {silence:.0f}s, connection may be stale"
                )
                
                # Force reconnect
                if silence > self._heartbeat_interval * 3:
                    self._logger.error("Connection appears dead, forcing reconnect")
                    if self._websocket:
                        await self._websocket.close()
                    break
    
    def _get_reconnect_delay(self) -> float:
        """
        Calculate reconnect delay with exponential backoff.
        
        Returns:
            Delay in seconds
        """
        delay = self._reconnect_initial_delay * (2 ** self._reconnect_attempt)
        return min(delay, self._reconnect_max_delay)
    
    def get_health_report(self) -> CollectorHealthReport:
        """
        Get current health status.
        
        Returns:
            CollectorHealthReport with current status
        """
        # Determine overall status
        if self._connection_status == "connected" and self._is_running:
            status = "healthy"
        elif self._connection_status == "reconnecting":
            status = "degraded"
        else:
            status = "failed"
        
        warnings = []
        if self._errors_count > 10:
            warnings.append(f"High error count: {self._errors_count}")
        if self._reconnect_attempt > 3:
            warnings.append(f"Multiple reconnect attempts: {self._reconnect_attempt}")
        
        last_tick = None
        if self._last_message_time:
            last_tick = self._last_message_time.isoformat()
        
        return CollectorHealthReport(
            collector_name=self._name,
            status=status,
            uptime_seconds=self._get_uptime_seconds(),
            ticks_collected=self._ticks_collected,
            errors_count=self._errors_count,
            last_tick_time=last_tick,
            connection_status=self._connection_status,
            symbols_active=[normalize_symbol(s) for s in self._symbols],
            warnings=warnings,
            details={
                "url": self._url,
                "reconnect_attempts": self._reconnect_attempt,
                "heartbeat_interval": self._heartbeat_interval
            }
        )
