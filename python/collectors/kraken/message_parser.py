"""
FiniexDataCollector - Kraken Message Parser
Parses Kraken WebSocket v2 ticker messages into TickData format.

Location: python/collectors/kraken/message_parser.py
"""

import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from python.types.tick_types import TickData, KrakenTickerMessage
from python.types.broker_config_types import BrokerConfig, normalize_symbol
from python.exceptions.collector_exceptions import MessageParseError


class KrakenMessageParser:
    """
    Parses Kraken WebSocket v2 messages.

    Converts ticker updates to TickData format matching MT5 output.
    Uses BrokerConfig for digits/tick_size (loaded from API).
    """

    def __init__(self):
        """Initialize parser."""
        self._tick_counter: Dict[str, int] = {}  # Per-symbol tick counter

    def parse_message(self, raw_message: str) -> Optional[List[TickData]]:
        """
        Parse raw WebSocket message.

        Args:
            raw_message: JSON string from WebSocket

        Returns:
            List of TickData if ticker message, None for other messages

        Raises:
            MessageParseError: If message parsing fails
        """
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError as e:
            raise MessageParseError(
                f"Invalid JSON: {e}",
                raw_message=raw_message
            )

        # Skip non-ticker messages
        if not isinstance(data, dict):
            return None

        channel = data.get("channel")
        if channel != "ticker":
            return None

        msg_type = data.get("type")
        if msg_type not in ("snapshot", "update"):
            return None

        # Parse ticker data array
        ticker_data = data.get("data", [])
        if not ticker_data:
            return None

        receive_time_msc = int(time.time() * 1000)
        ticks = []

        for ticker in ticker_data:
            tick = self._parse_ticker_to_tick(ticker, receive_time_msc)
            if tick:
                ticks.append(tick)

        return ticks if ticks else None

    def _parse_ticker_to_tick(
        self,
        ticker: Dict[str, Any],
        receive_time_msc: int
    ) -> Optional[TickData]:
        """
        Convert single ticker message to TickData.

        Args:
            ticker: Ticker data dict from Kraken
            receive_time_msc: Local receive timestamp in milliseconds

        Returns:
            TickData instance or None if invalid
        """
        try:
            kraken_symbol = ticker.get("symbol", "")
            if not kraken_symbol:
                return None

            symbol = normalize_symbol(kraken_symbol)

            bid = float(ticker.get("bid", 0))
            ask = float(ticker.get("ask", 0))
            last = float(ticker.get("last", 0))
            volume = float(ticker.get("volume", 0))

            # Skip invalid ticks
            if bid <= 0 or ask <= 0:
                return None

            # Get tick_size and digits from BrokerConfig (API-sourced)
            tick_size = BrokerConfig.get_tick_size(symbol)
            digits = BrokerConfig.get_digits(symbol)

            # Calculate spread
            spread_raw = ask - bid
            spread_points = int(spread_raw / tick_size) if tick_size > 0 else 0
            spread_pct = (spread_raw / bid * 100) if bid > 0 else 0.0

            # Format timestamp
            dt_utc = datetime.now(timezone.utc)
            timestamp_str = dt_utc.strftime("%Y.%m.%d %H:%M:%S")
            server_time_str = timestamp_str

            # Increment tick counter for chart_tick_volume
            if symbol not in self._tick_counter:
                self._tick_counter[symbol] = 0
            self._tick_counter[symbol] += 1

            return TickData(
                symbol=symbol,
                timestamp=timestamp_str,
                time_msc=receive_time_msc,
                bid=round(bid, digits),
                ask=round(ask, digits),
                last=round(last, digits),
                tick_volume=0,
                real_volume=round(volume, 2),
                chart_tick_volume=self._tick_counter[symbol],
                spread_points=spread_points,
                spread_pct=round(spread_pct, 6),
                tick_flags="BID ASK",
                session="24h",
                server_time=server_time_str
            )

        except (KeyError, ValueError, TypeError) as e:
            raise MessageParseError(
                f"Failed to parse ticker: {e}",
                raw_message=str(ticker),
                symbol=ticker.get("symbol")
            )

    def parse_kraken_ticker(
        self,
        ticker: Dict[str, Any],
        receive_time_msc: int
    ) -> Optional[KrakenTickerMessage]:
        """
        Parse to intermediate KrakenTickerMessage format.

        Args:
            ticker: Raw ticker dict
            receive_time_msc: Local receive time

        Returns:
            KrakenTickerMessage or None
        """
        try:
            return KrakenTickerMessage(
                symbol=ticker.get("symbol", ""),
                bid=float(ticker.get("bid", 0)),
                bid_qty=float(ticker.get("bid_qty", 0)),
                ask=float(ticker.get("ask", 0)),
                ask_qty=float(ticker.get("ask_qty", 0)),
                last=float(ticker.get("last", 0)),
                volume=float(ticker.get("volume", 0)),
                vwap=float(ticker.get("vwap", 0)),
                low=float(ticker.get("low", 0)),
                high=float(ticker.get("high", 0)),
                change=float(ticker.get("change", 0)),
                change_pct=float(ticker.get("change_pct", 0)),
                received_at_msc=receive_time_msc
            )
        except (KeyError, ValueError, TypeError):
            return None

    def reset_tick_counter(self, symbol: Optional[str] = None) -> None:
        """
        Reset tick counter (e.g., on minute boundary).

        Args:
            symbol: Specific symbol to reset, or all if None
        """
        if symbol:
            self._tick_counter[symbol] = 0
        else:
            self._tick_counter.clear()

    def is_subscription_confirmation(self, raw_message: str) -> bool:
        """
        Check if message is subscription confirmation.

        Args:
            raw_message: JSON string

        Returns:
            True if subscription confirmation
        """
        try:
            data = json.loads(raw_message)
            return (
                isinstance(data, dict) and
                data.get("method") == "subscribe" and
                data.get("success") is True
            )
        except json.JSONDecodeError:
            return False

    def is_error_message(self, raw_message: str) -> Optional[str]:
        """
        Check if message is error and extract error text.

        Args:
            raw_message: JSON string

        Returns:
            Error message string or None if not error
        """
        try:
            data = json.loads(raw_message)
            if isinstance(data, dict) and data.get("success") is False:
                return data.get("error", "Unknown error")
            return None
        except json.JSONDecodeError:
            return None

    def is_heartbeat(self, raw_message: str) -> bool:
        """
        Check if message is heartbeat.

        Args:
            raw_message: JSON string

        Returns:
            True if heartbeat message
        """
        try:
            data = json.loads(raw_message)
            return (
                isinstance(data, dict) and
                data.get("channel") == "heartbeat"
            )
        except json.JSONDecodeError:
            return False
