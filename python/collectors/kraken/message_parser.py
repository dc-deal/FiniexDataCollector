"""
FiniexDataCollector - Kraken Message Parser
Parses Kraken WebSocket v2 ticker and trade messages into TickData format.

Location: python/collectors/kraken/message_parser.py
"""

import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from python.types.tick_types import TickData, KrakenTickerMessage
from python.types.broker_config_types import BrokerConfig, normalize_symbol
from python.exceptions.collector_exceptions import MessageParseError
from python.collectors.kraken.quote_cache import QuoteCache
from python.utils.collection_clock import CollectionClock


class KrakenMessageParser:
    """
    Parses Kraken WebSocket v2 messages.

    Converts ticker and trade updates to TickData format matching MT5 output.
    """

    def __init__(self, clock: CollectionClock, quote_cache: QuoteCache):
        """
        Initialize parser.

        Args:
            clock: Session clock stamping collected_msc. Required rather than
                created here, because the writers report its counters and must
                read the same instance that issued the timestamps.
            quote_cache: Holds the last bid/ask per symbol. Ticker updates fill
                it and trade ticks read it - that is how an execution, which
                happens at one price, comes to carry the spread it executed
                against.
        """
        self._clock = clock
        self._quote_cache = quote_cache
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

        observed_msc = self._clock.next_msc()

        for ticker in ticker_data:
            kraken_symbol = ticker.get("symbol", "")
            if not kraken_symbol:
                continue

            self._quote_cache.update(
                symbol=normalize_symbol(kraken_symbol),
                bid=float(ticker.get("bid", 0)),
                ask=float(ticker.get("ask", 0)),
                observed_msc=observed_msc
            )

        # Deliberately no ticks: a ticker tick's time_msc is our local receive
        # time while a trade's is the exchange's event time, and interleaving
        # the two in one file steps time_msc backwards on nearly every channel
        # change - which the import pipeline rejects the whole file for.
        return None

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

            # Parse Kraken timestamp (ISO format). Falling back to our own clock
            # loses the exchange's event time, but a trade without a usable
            # stamp is still worth more than a dropped tick.
            timestamp_str_kraken = trade.get("timestamp", "")
            if timestamp_str_kraken:
                try:
                    time_msc = int(datetime.fromisoformat(
                        timestamp_str_kraken.replace("Z", "+00:00")
                    ).timestamp() * 1000)
                except ValueError:
                    time_msc = self._clock.next_msc()
            else:
                time_msc = self._clock.next_msc()

            timestamp_str = datetime.fromtimestamp(
                time_msc / 1000, tz=timezone.utc
            ).strftime("%Y.%m.%d %H:%M:%S")

            # Increment tick counter for chart_tick_volume
            if symbol not in self._tick_counter:
                self._tick_counter[symbol] = 0
            self._tick_counter[symbol] += 1

            rounded_price = round(price, digits)
            collected_msc = self._clock.next_msc()

            # The execution happened at one price; the quote it executed against
            # comes from the ticker channel. Without one - the first trades after
            # a start or reconnect - the trade price stands in for both sides, as
            # it always did, and quote_age_ms stays None to say so. Zero would
            # claim a quote observed in the same millisecond.
            quote = self._quote_cache.get(symbol)

            if quote:
                tick_size = BrokerConfig.get_tick_size(symbol)
                spread_raw = quote.ask - quote.bid

                bid = round(quote.bid, digits)
                ask = round(quote.ask, digits)
                # round, not int: bid and ask both sit on the tick grid, so the
                # quotient is an integer in exact arithmetic - but 53.53 - 53.52
                # is 0.009999999999997 in IEEE754, and truncating that reports a
                # genuine one-tick spread as zero. Measured on LTCUSD: 59 % of
                # ticks. A spread_points of 0 reads as "no spread", which is the
                # one thing this field must never say when there is one.
                spread_points = round(
                    spread_raw / tick_size) if tick_size > 0 else 0
                spread_pct = round(spread_raw / quote.bid * 100, 6)
                quote_age_ms = collected_msc - quote.observed_msc
            else:
                bid = rounded_price
                ask = rounded_price
                spread_points = 0
                spread_pct = 0.0
                quote_age_ms = None

            return TickData(
                symbol=symbol,
                timestamp=timestamp_str,
                time_msc=time_msc,
                bid=bid,
                ask=ask,
                last=rounded_price,
                tick_volume=0,
                real_volume=round(qty, 8),
                chart_tick_volume=self._tick_counter[symbol],
                spread_points=spread_points,
                spread_pct=spread_pct,
                collected_msc=collected_msc,
                tick_flags=side if side else "TRADE",
                session="24h",
                quote_age_ms=quote_age_ms,
                # Kraken's own id for the execution. Kept from 1.7.0 as the
                # deduplication key for the day two collectors capture one
                # symbol; None rather than invented when a message omits it.
                trade_id=trade.get("trade_id")
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
