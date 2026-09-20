"""
FiniexDataCollector - Kraken WebSocket Client
WebSocket client for Kraken v2 API with automatic reconnection.

Location: python/collectors/kraken/websocket_client.py
"""

import asyncio
import json
import ssl
import time
from datetime import datetime, timezone
from typing import Any, List, Optional, Callable

import certifi
import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK
)

from python.collectors.base import AbstractCollector
from python.collectors.kraken.message_parser import KrakenMessageParser
from python.types.broker_config_types import to_kraken_format, normalize_symbol
from python.types.tick_types import TickData
from python.exceptions.collector_exceptions import (
    WebSocketConnectionError,
    WebSocketSubscriptionError,
    CollectorHealthReport
)
from python.collectors.kraken.quote_cache import QuoteCache
from python.utils.collection_clock import CollectionClock
from python.utils.logging_setup import describe_exception, get_collector_logger


# What makes Kraken push a ticker update. The API default is "trades", which
# ties the quote to the trade stream: the cache then only refreshes when someone
# trades, and a trade tick reads a quote as old as the gap since the last one -
# measured at a median of 464 ms and over a second for a third of all ticks.
# "bbo" pushes whenever the best bid or offer moves, which is what the quote on a
# trade tick is supposed to describe. quote_age_ms is the instrument that shows
# the difference, so this is a measured setting, not a guessed one.
TICKER_EVENT_TRIGGER = "bbo"

# How often the feed's silence is judged. Kraken sends a heartbeat every second
# on a subscribed connection - measured 2026-09-19 on a quiet pair, the largest
# gap between any two messages 1.03 s over 45 s - so a live feed is never quiet
# for long, and checking every second costs nothing. Checking only every
# `stale_after` seconds, as before, added up to a whole interval to detection.
STALE_CHECK_INTERVAL = 1.0

# A check that wakes this much later than it asked to was not sleeping: the
# event loop was blocked - a file closing, the midnight cascade, 17.85 s
# measured on production 2026-09-19. The silence it would see is then ours, not
# the feed's, because the receive loop has not had its turn to read what
# arrived. Judged anyway, it would force a reconnect at every day cut.
LATE_WAKE_TOLERANCE = 1.0


class KrakenWebSocketClient(AbstractCollector):
    """
    Kraken WebSocket v2 ticker/trade collector.

    Features:
    - Automatic reconnection with exponential backoff
    - Heartbeat monitoring
    - Multi-symbol subscription
    - Configurable streams (ticker, trade)
    - Graceful shutdown
    """

    DEFAULT_URL = "wss://ws.kraken.com/v2"
    VALID_STREAMS = {"ticker", "trade"}

    def __init__(
        self,
        symbols: List[str],
        clock: CollectionClock,
        quote_cache: QuoteCache,
        streams: List[str] = None,
        url: str = DEFAULT_URL,
        reconnect_initial_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        stale_after: float = 10.0
    ):
        """
        Initialize Kraken WebSocket client.

        Args:
            symbols: List of symbols to subscribe (e.g., ["BTC/USD", "ETH/USD"])
            clock: Session clock handed to the parser, which stamps collected_msc
            quote_cache: Last bid/ask per symbol, filled from the ticker channel
                and stated on every trade tick
            streams: List of streams to subscribe (e.g., ["ticker"], ["trade"], ["ticker", "trade"])
            url: WebSocket URL
            reconnect_initial_delay: Initial reconnect delay in seconds
            reconnect_max_delay: Maximum reconnect delay in seconds
            stale_after: Seconds without any message, heartbeats included,
                after which the connection is closed and reopened
        """
        super().__init__(name="kraken", symbols=symbols)

        self._url = url
        self._streams = streams if streams else ["ticker"]
        self._reconnect_initial_delay = reconnect_initial_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._stale_after = stale_after

        # The time source and the sleep are attributes so a test can drive both
        # without patching the module-wide ones the event loop itself runs on.
        self._monotonic: Callable[[], float] = time.monotonic
        self._sleep: Callable[[float], Any] = asyncio.sleep

        # Validate streams
        for stream in self._streams:
            if stream not in self.VALID_STREAMS:
                raise ValueError(
                    f"Invalid stream '{stream}'. Valid: {self.VALID_STREAMS}")

        self._websocket: Optional[Any] = None
        self._parser = KrakenMessageParser(clock, quote_cache)
        self._logger = get_collector_logger("kraken")

        self._connection_status = "disconnected"
        # Wall clock for reporting when, monotonic for measuring how long: a
        # silence derived from two wall-clock readings turns every forward NTP
        # step into a dead connection.
        self._last_message_time: Optional[datetime] = None
        self._last_message_monotonic: Optional[float] = None
        self._reconnect_attempt = 0
        self._should_reconnect = True

        # SSL context with certifi certificates (cross-platform)
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())

        # Callbacks
        self._status_callback: Optional[Callable[[str], None]] = None

        # Tasks
        self._receive_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    def set_status_callback(self, callback: Callable[[str], None]) -> None:
        """
        Set callback for connection status changes.

        Args:
            callback: Function called with status string (connected, disconnected, reconnecting)
        """
        self._status_callback = callback

    def _set_status(self, status: str) -> None:
        """
        Update connection status and notify callback.

        Args:
            status: New status string
        """
        self._connection_status = status
        if self._status_callback:
            try:
                self._status_callback(status)
            except Exception:
                pass  # Don't crash on callback errors

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
                ssl=self._ssl_context,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5
            )

            self._set_status("connected")
            self._reconnect_attempt = 0
            self._last_message_time = datetime.now(timezone.utc)
            self._last_message_monotonic = self._monotonic()

            self._logger.info("WebSocket connected successfully")
            return True

        except Exception as e:
            self._set_status("failed")
            self._logger.error(f"Connection failed: {describe_exception(e)}")
            raise WebSocketConnectionError(
                message=str(e),
                url=self._url,
                attempt=self._reconnect_attempt
            )

    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        self._should_reconnect = False
        self._set_status("disconnecting")

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
                self._logger.warning(
                    f"Error closing WebSocket: {describe_exception(e)}")

        self._websocket = None
        self._set_status("disconnected")
        self._is_running = False
        self._logger.info("Disconnected from Kraken WebSocket")

    async def subscribe(self) -> bool:
        """
        Subscribe to configured channels for configured symbols.

        Returns:
            True if subscription successful
        """
        if not self._websocket:
            return False

        # Convert symbols to Kraken format
        kraken_symbols = [to_kraken_format(s) for s in self._symbols]

        # Subscribe to each configured stream
        for stream in self._streams:
            params = {
                "channel": stream,
                "symbol": kraken_symbols
            }

            if stream == "ticker":
                params["event_trigger"] = TICKER_EVENT_TRIGGER

            subscribe_msg = {
                "method": "subscribe",
                "params": params
            }

            try:
                await self._websocket.send(json.dumps(subscribe_msg))
                self._logger.info(
                    f"Subscription request sent: {stream} for {kraken_symbols}")

                # Wait for confirmation (with timeout)
                try:
                    response = await asyncio.wait_for(
                        self._websocket.recv(),
                        timeout=10.0
                    )

                    if self._parser.is_subscription_confirmation(response):
                        self._logger.info(f"Subscription confirmed: {stream}")
                    else:
                        error = self._parser.is_error_message(response)
                        if error:
                            raise WebSocketSubscriptionError(
                                message=error,
                                channel=stream,
                                symbols=kraken_symbols
                            )

                except asyncio.TimeoutError:
                    self._logger.warning(
                        f"Subscription confirmation timeout: {stream}")

            except Exception as e:
                self._logger.error(
                    f"Subscription failed for {stream}: {describe_exception(e)}")
                raise WebSocketSubscriptionError(
                    message=str(e),
                    channel=stream,
                    symbols=kraken_symbols
                )

        return True

    async def start(self) -> None:
        """Start tick collection."""
        self._is_running = True
        self._should_reconnect = True
        self._start_time = datetime.now(timezone.utc)

        self._logger.info(
            f"Starting Kraken collector for {len(self._symbols)} symbols, "
            f"streams: {self._streams}"
        )

        while self._should_reconnect:
            try:
                # Connect
                await self.connect()

                # Subscribe
                await self.subscribe()

                # Start receive loop
                self._receive_task = asyncio.create_task(self._receive_loop())
                self._heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop())

                # Wait for tasks
                await asyncio.gather(
                    self._receive_task,
                    self._heartbeat_task,
                    return_exceptions=True
                )

            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as e:
                self._logger.warning(
                    f"Connection closed: {describe_exception(e)}")
                self._set_status("reconnecting")

            except WebSocketConnectionError as e:
                self._logger.error(
                    f"Connection error: {describe_exception(e)}")

            except Exception as e:
                self._logger.error(
                    f"Unexpected error: {describe_exception(e)}")
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
            self._last_message_monotonic = self._monotonic()

            # Skip heartbeats
            if self._parser.is_heartbeat(message):
                continue

            # Parse ticker/trade messages
            try:
                ticks = self._parser.parse_message(message)
                if ticks:
                    for tick in ticks:
                        self._emit_tick(tick)

            except Exception as e:
                self._logger.warning(
                    f"Message parse error: {describe_exception(e)}")
                self._errors_count += 1

    async def _heartbeat_loop(self) -> None:
        """
        Close and reopen a connection that has gone silent.

        Detection is what a drop costs: the trades Kraken makes while a dead
        socket is still believed alive are never received. Measured on
        production 2026-09-18, four drops lost 537 trades at 41-51 s of silence
        each on the liquid pairs, of which the old check - every 10 s,
        reconnect at three times that - accounted for 30-40 s.
        """
        while self._is_running and self._websocket:
            asleep_since = self._monotonic()
            await self._sleep(STALE_CHECK_INTERVAL)
            now = self._monotonic()

            if now - asleep_since > STALE_CHECK_INTERVAL + LATE_WAKE_TOLERANCE:
                # The loop was blocked, so this silence is ours; the next round
                # sees what the receive loop reads in the meantime.
                continue

            if self._last_message_monotonic is None:
                continue

            silence = now - self._last_message_monotonic
            if silence > self._stale_after:
                self._logger.error(
                    f"No messages for {silence:.0f}s - connection appears "
                    f"dead, forcing reconnect")
                # Announce it before closing. close() is a CLEAN close, so
                # the receive loop ends without raising and the handler that
                # would set this status never runs - which left the status
                # at "connected" across every forced reconnect. Measured on
                # the production log: 173 forced reconnects, 0 recorded.
                self._set_status("reconnecting")
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
            warnings.append(
                f"Multiple reconnect attempts: {self._reconnect_attempt}")

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
                "streams": self._streams,
                "reconnect_attempts": self._reconnect_attempt,
                "stale_after": self._stale_after
            }
        )
