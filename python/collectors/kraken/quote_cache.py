"""
FiniexDataCollector - Quote Cache
Last known bid/ask per symbol, observed on Kraken's ticker channel.

Kraken's trade channel reports executions, and an execution happens at exactly one
price - so a trade tick on its own carries bid == ask and a spread of zero. That is
honest but expensive downstream: a backtest that pays no spread produces a curve that
is too favourable, and a parameter sweep then optimises against a cost that does not
exist.

The ticker channel carries the quote the trade executed against. This cache holds the
last one per symbol so the writer can state it on the trade tick. Ticker updates never
become ticks of their own: their time_msc would be our local receive time while a
trade's is the exchange's event time, and interleaving the two time bases in one file
produces the backwards steps that make the import pipeline reject it.

A cached quote ages. How far is reported per tick as quote_age_ms rather than left for
a reader to guess - that field is what separates a measurement from an approximation.

Location: python/collectors/kraken/quote_cache.py
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class Quote:
    """
    One observed bid/ask pair.

    Attributes:
        bid: Best bid at observation time
        ask: Best ask at observation time
        observed_msc: Arrival time of the ticker update, from the session clock
    """
    bid: float
    ask: float
    observed_msc: int


class QuoteCache:
    """
    Holds the most recent quote per symbol.

    One instance per collector session, shared with the parser that fills it. Only the
    latest quote is kept: an older one has no reader, and keeping a history here would
    duplicate the archive the ticks themselves already form.
    """

    def __init__(self) -> None:
        """Initialize an empty cache."""
        self._quotes: Dict[str, Quote] = {}

    def update(self, symbol: str, bid: float, ask: float, observed_msc: int) -> None:
        """
        Record the quote most recently seen for a symbol.

        Crossed or non-positive quotes are dropped rather than stored: they would be
        written onto a trade tick as fact, and the import pipeline rejects a file whose
        ask is below its bid. Dropping keeps the previous quote, which ages visibly
        through quote_age_ms instead of failing invisibly.

        Args:
            symbol: Normalized symbol (e.g. "BTCUSD")
            bid: Best bid
            ask: Best ask
            observed_msc: Arrival time in epoch milliseconds UTC
        """
        if bid <= 0 or ask <= 0 or ask < bid:
            return

        self._quotes[symbol] = Quote(bid=bid, ask=ask, observed_msc=observed_msc)

    def get(self, symbol: str) -> Optional[Quote]:
        """
        Get the last known quote for a symbol.

        Args:
            symbol: Normalized symbol

        Returns:
            The most recent Quote, or None if none has been observed yet
        """
        return self._quotes.get(symbol)
