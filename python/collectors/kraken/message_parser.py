"""
FiniexDataCollector - Kraken Message Parser
Parses Kraken WebSocket v2 ticker and trade messages into TickData format.

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

    Converts ticker and trade updates to TickData format matching MT5 output.
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
            List of TickData if ticker/trade message, None for other messages

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

        # Skip non-dict messages
        if not isinstance(data, dict):
            return None

        channel = data.get("channel")
        msg_type = data.get("type")

        # Handle ticker channel
        if channel == "ticker" and msg_type in ("snapshot", "update"):
            return self._parse_ticker_message(data)

        # Handle trade channel
        if channel == "trade" and msg_type in ("snapshot", "update"):
            return self._parse_trade_message(data)

        return None

    def _parse_ticker_message(self, data: Dict[str, Any]) -> Optional[List[TickData]]:
        """
        Parse ticker channel message.

        Args:
            data: Parsed JSON dict

        Returns:
            List of TickData or None
        """
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

    def _parse_trade_message(self, data: Dict[str, Any]) -> Optional[List[TickData]]:
        """
        Parse trade channel message.

        Args:
            data: Parsed JSON dict

        Returns:
            List of TickData or None
        """
        trade_data = data.get("data", [])
        if not trade_data:
            return None

        ticks = []

        for trade in trade_data:
            tick = self._parse_trade_to_tick(trade)
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

            # Get symbol config from BrokerConfig
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
                collected_msc=int(time.time() * 1000),
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

    def _parse_trade_to_tick(self, trade: Dict[str, Any]) -> Optional[TickData]:
        """
        Convert single trade message to TickData.

        Kraken trade format:
        {
            "symbol": "BTC/USD",
            "side": "buy" | "sell",
            "price": 92642.7,
            "qty": 0.01,
            "ord_type": "market",
            "trade_id": 12345,
            "timestamp": "2026-01-19T07:44:05.371000Z"
        }

        Args:
            trade: Trade data dict from Kraken

        Returns:
            TickData instance or None if invalid
        """
        try:
            kraken_symbol = trade.get("symbol", "")
            if not kraken_symbol:
                return None

            symbol = normalize_symbol(kraken_symbol)

            price = float(trade.get("price", 0))
            qty = float(trade.get("qty", 0))
            side = trade.get("side", "").upper()  # "BUY" or "SELL"

            # Skip invalid trades
            if price <= 0:
                return None

            # Get symbol config from BrokerConfig
            digits = BrokerConfig.get_digits(symbol)

            # Parse Kraken timestamp (ISO format)
            timestamp_str_kraken = trade.get("timestamp", "")
            if timestamp_str_kraken:
                try:
                    dt_utc = datetime.fromisoformat(
                        timestamp_str_kraken.replace("Z", "+00:00")
                    )
                    time_msc = int(dt_utc.timestamp() * 1000)
                except ValueError:
                    dt_utc = datetime.now(timezone.utc)
                    time_msc = int(time.time() * 1000)
            else:
                dt_utc = datetime.now(timezone.utc)
                time_msc = int(time.time() * 1000)

            timestamp_str = dt_utc.strftime("%Y.%m.%d %H:%M:%S")
            server_time_str = timestamp_str

            # Increment tick counter for chart_tick_volume
            if symbol not in self._tick_counter:
                self._tick_counter[symbol] = 0
            self._tick_counter[symbol] += 1

            # Trade price becomes bid/ask/last
            # No spread info in trades
            rounded_price = round(price, digits)

            return TickData(
                symbol=symbol,
                timestamp=timestamp_str,
                time_msc=time_msc,
                bid=rounded_price,
                ask=rounded_price,
                last=rounded_price,
                tick_volume=0,
                real_volume=round(qty, 8),
                chart_tick_volume=self._tick_counter[symbol],
                spread_points=0,
                spread_pct=0.0,
                collected_msc=int(time.time() * 1000),
                tick_flags=side if side else "TRADE",
                session="24h",
                server_time=server_time_str
            )

        except (KeyError, ValueError, TypeError) as e:
            raise MessageParseError(
                f"Failed to parse trade: {e}",
                raw_message=str(trade),
                symbol=trade.get("symbol")
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
