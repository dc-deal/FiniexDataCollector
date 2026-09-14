"""
FiniexDataCollector - Kraken Message Parser Tests

Two things happen in the parser and nowhere else: every local timestamp enters the
data here, and a trade meets the quote it executed against here. Both are tested
against real WebSocket payloads, with the clock driven by hand.

Location: tests/collectors/test_message_parser.py
"""

import json
from datetime import datetime, timezone
from typing import Callable, List, Tuple

import pytest

from python.collectors.kraken.message_parser import KrakenMessageParser
from python.collectors.kraken.quote_cache import QuoteCache
from python.utils.collection_clock import CollectionClock
from tests.conftest import FIRST_EVENT_MSC

TRADE_PRICE = 45005.0

# (parser, clock, setter for the OS reading in ms)
ParserFixture = Tuple[KrakenMessageParser, CollectionClock, Callable[[int], None]]


def ticker_message(
    bid: float = 45000.0,
    ask: float = 45010.0,
    symbol: str = "BTC/USD"
) -> str:
    """
    Build a Kraken v2 ticker payload.

    Args:
        bid: Best bid to report
        ask: Best ask to report
        symbol: Kraken-format pair name

    Returns:
        Raw JSON string as it arrives on the WebSocket
    """
    return json.dumps({
        "channel": "ticker",
        "type": "update",
        "data": [{
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "last": (bid + ask) / 2,
            "volume": 1.5
        }]
    })


def trade_message(
    timestamp: str = "2026-03-07T09:03:42.978000Z",
    side: str = "buy",
    price: float = TRADE_PRICE,
    symbol: str = "BTC/USD"
) -> str:
    """
    Build a Kraken v2 trade payload.

    Args:
        timestamp: ISO timestamp the exchange reports for the fill
        side: Taker side, "buy" or "sell"
        price: Execution price
        symbol: Kraken-format pair name

    Returns:
        Raw JSON string as it arrives on the WebSocket
    """
    return json.dumps({
        "channel": "trade",
        "type": "update",
        "data": [{
            "symbol": symbol,
            "side": side,
            "price": price,
            "qty": 0.01,
            "ord_type": "market",
            "trade_id": 12345,
            "timestamp": timestamp
        }]
    })


@pytest.fixture
def parser_with_clock(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> ParserFixture:
    """
    A parser wired the way the collector wires it.

    Args:
        steerable_clock: Clock plus a setter for its time source

    Returns:
        Tuple of parser, clock and the setter for the OS reading
    """
    clock, set_os_clock = steerable_clock
    return KrakenMessageParser(clock, QuoteCache()), clock, set_os_clock


# =============================================================================
# THE QUOTE A TRADE EXECUTED AGAINST
# =============================================================================

def test_ticker_updates_produce_no_ticks(parser_with_clock: ParserFixture) -> None:
    """
    The ticker channel feeds the cache and writes nothing of its own.

    Its time_msc would be our local receive time while a trade carries the
    exchange event time. Interleaved in one file, the two step time_msc backwards
    on nearly every channel change, and the importer rejects the file for it.
    """
    parser, _, set_os_clock = parser_with_clock
    set_os_clock(FIRST_EVENT_MSC)

    assert parser.parse_message(ticker_message()) is None


def test_trade_carries_the_quote_it_executed_against(
    parser_with_clock: ParserFixture
) -> None:
    """
    The whole point: a trade tick states a real spread instead of zero.

    The execution price stays in `last` - that is what was traded. Bid and ask
    describe the book it was traded against.
    """
    parser, _, set_os_clock = parser_with_clock
    set_os_clock(FIRST_EVENT_MSC)
    parser.parse_message(ticker_message(bid=45000.0, ask=45010.0))

    tick = parser.parse_message(trade_message())[0]

    assert (tick.bid, tick.ask) == (45000.0, 45010.0)
    assert tick.last == TRADE_PRICE
    assert tick.ask > tick.bid
    assert tick.spread_pct > 0


def test_trade_without_a_quote_says_so_instead_of_guessing(
    parser_with_clock: ParserFixture
) -> None:
    """
    The first trades after a start or reconnect have no quote yet.

    The old behaviour stands - the price fills both sides - because the importer
    rejects a file whose prices are not positive. What must not happen is a
    quote_age_ms of 0, which would claim a quote observed in that millisecond.
    """
    parser, _, set_os_clock = parser_with_clock
    set_os_clock(FIRST_EVENT_MSC)

    tick = parser.parse_message(trade_message())[0]

    assert tick.quote_age_ms is None
    assert tick.bid == tick.ask == TRADE_PRICE
    assert tick.spread_points == 0


def test_quote_age_is_measured_on_the_session_clock(
    parser_with_clock: ParserFixture
) -> None:
    """
    The age is what turns an enriched tick from an approximation into a
    measurement - a reader can tell a fresh spread from a stale one.
    """
    parser, _, set_os_clock = parser_with_clock

    set_os_clock(FIRST_EVENT_MSC)
    parser.parse_message(ticker_message())

    set_os_clock(FIRST_EVENT_MSC + 4_000)
    tick = parser.parse_message(trade_message())[0]

    assert tick.quote_age_ms == 4_000


def test_a_crossed_quote_never_reaches_a_trade_tick(
    parser_with_clock: ParserFixture
) -> None:
    """
    A quote whose ask sits below its bid would be written onto the tick as fact,
    and the importer rejects the file for it. The previous quote stands instead
    and ages visibly rather than failing invisibly.
    """
    parser, _, set_os_clock = parser_with_clock

    set_os_clock(FIRST_EVENT_MSC)
    parser.parse_message(ticker_message(bid=45000.0, ask=45010.0))

    set_os_clock(FIRST_EVENT_MSC + 100)
    parser.parse_message(ticker_message(bid=45020.0, ask=45010.0))

    tick = parser.parse_message(trade_message())[0]

    assert (tick.bid, tick.ask) == (45000.0, 45010.0)
    assert tick.ask >= tick.bid


def test_taker_side_survives_into_the_archive(
    parser_with_clock: ParserFixture
) -> None:
    """
    The side is what makes a later spread reconstruction tractable: a buy lifted
    the ask, a sell hit the bid, so only the width stays unknown. Every file
    since January carries it, and it has to keep carrying it.
    """
    parser, _, set_os_clock = parser_with_clock
    set_os_clock(FIRST_EVENT_MSC)

    buy = parser.parse_message(trade_message(side="buy"))[0]
    sell = parser.parse_message(trade_message(side="sell"))[0]

    assert buy.tick_flags.upper() == "BUY"
    assert sell.tick_flags.upper() == "SELL"


# =============================================================================
# TIME
# =============================================================================

def test_trade_keeps_the_exchange_event_time(
    parser_with_clock: ParserFixture
) -> None:
    """
    Kraken's own timestamp is the event time and is never clamped - only our
    arrival stamp comes from the clock.
    """
    parser, _, set_os_clock = parser_with_clock
    set_os_clock(FIRST_EVENT_MSC)

    tick = parser.parse_message(
        trade_message("2026-03-07T09:03:42.978000Z"))[0]

    expected = int(datetime(
        2026, 3, 7, 9, 3, 42, 978000, tzinfo=timezone.utc).timestamp() * 1000)

    assert tick.time_msc == expected
    assert tick.collected_msc == FIRST_EVENT_MSC


def test_trade_without_a_usable_timestamp_falls_back_to_the_clock(
    parser_with_clock: ParserFixture
) -> None:
    """A malformed exchange stamp costs the event time, not the tick."""
    parser, _, set_os_clock = parser_with_clock
    set_os_clock(FIRST_EVENT_MSC)

    tick = parser.parse_message(trade_message("not-a-timestamp"))[0]

    assert tick.time_msc == FIRST_EVENT_MSC


def test_a_clock_correction_never_reverses_arrival_times(
    parser_with_clock: ParserFixture
) -> None:
    """
    Three trades arrive in order, the OS clock is stepped back between the second
    and the third, and collected_msc still comes out non-decreasing.
    """
    parser, clock, set_os_clock = parser_with_clock

    arrivals: List[int] = []
    for offset in (0, 100):
        set_os_clock(FIRST_EVENT_MSC + offset)
        arrivals += [t.collected_msc
                     for t in parser.parse_message(trade_message())]

    set_os_clock(FIRST_EVENT_MSC + 100 - 300)
    arrivals += [t.collected_msc
                 for t in parser.parse_message(trade_message())]

    assert arrivals == sorted(arrivals)
    assert clock.resyncs > 0


def test_a_one_tick_spread_is_never_reported_as_zero(
    parser_with_clock: ParserFixture
) -> None:
    """
    Found in live data, not in review: 59 % of LTCUSD ticks claimed no spread.

    Bid and ask both sit on the tick grid, so the quotient is an integer in exact
    arithmetic. In IEEE754 it is not: 53.53 - 53.52 is 0.009999999999997, and
    truncating that quotient turns a genuine one-tick spread into zero - a field
    saying "no spread" where there is one.
    """
    parser, _, set_os_clock = parser_with_clock
    set_os_clock(FIRST_EVENT_MSC)

    parser.parse_message(ticker_message(
        bid=53.52, ask=53.53, symbol="LTC/USD"))
    tick = parser.parse_message(trade_message(
        price=53.53, symbol="LTC/USD"))[0]

    # The arithmetic this guards against, spelled out so the test explains itself.
    assert int((53.53 - 53.52) / 0.01) == 0

    assert tick.spread_points == 1
    assert tick.ask > tick.bid
