"""
FiniexDataCollector - Kraken Message Parser Tests

The parser is where every local timestamp enters the data, so this is where a
clock correction would do its damage. The cases below run real WebSocket
payloads through the parser while the clock is stepped backwards underneath it.

Location: tests/collectors/test_message_parser.py
"""

import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Tuple

import pytest

from python.collectors.kraken.message_parser import KrakenMessageParser
from python.utils.collection_clock import CollectionClock
from tests.conftest import FIRST_EVENT_MSC


def ticker_message(bid: float = 45000.0, ask: float = 45010.0) -> str:
    """
    Build a Kraken v2 ticker payload.

    Args:
        bid: Bid price to report
        ask: Ask price to report

    Returns:
        Raw JSON string as it arrives on the WebSocket
    """
    return json.dumps({
        "channel": "ticker",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD",
            "bid": bid,
            "ask": ask,
            "last": (bid + ask) / 2,
            "volume": 1.5
        }]
    })


def trade_message(timestamp: str = "2026.03.07T09:03:42.978000Z") -> str:
    """
    Build a Kraken v2 trade payload.

    Args:
        timestamp: ISO timestamp the exchange reports for the fill

    Returns:
        Raw JSON string as it arrives on the WebSocket
    """
    return json.dumps({
        "channel": "trade",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD",
            "side": "buy",
            "price": 45005.0,
            "qty": 0.01,
            "ord_type": "market",
            "trade_id": 12345,
            "timestamp": timestamp
        }]
    })


def test_stamps_collected_msc_from_the_session_clock(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    The parser must not read the OS clock on its own.

    A second, unclamped reading anywhere in the parser would reintroduce
    exactly the gap the clock was built to close.

    Args:
        steerable_clock: Clock plus a setter for its time source
    """
    clock, set_os_clock = steerable_clock
    set_os_clock(FIRST_EVENT_MSC)

    ticks = KrakenMessageParser(clock).parse_message(ticker_message())

    assert ticks is not None
    assert ticks[0].collected_msc == FIRST_EVENT_MSC
    assert ticks[0].time_msc == FIRST_EVENT_MSC


def test_a_clock_correction_never_reverses_a_tick_stream(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    The whole point, end to end.

    Three messages arrive in order, the OS clock is stepped back between the
    second and the third, and both time columns still come out non-decreasing -
    which is what the importer rejects a file for.

    Args:
        steerable_clock: Clock plus a setter for its time source
    """
    clock, set_os_clock = steerable_clock
    parser = KrakenMessageParser(clock)

    collected: List[int] = []
    events: List[int] = []

    for offset in (0, 100):
        set_os_clock(FIRST_EVENT_MSC + offset)
        for tick in parser.parse_message(ticker_message()):
            collected.append(tick.collected_msc)
            events.append(tick.time_msc)

    # NTP finds the machine 300 ms fast and corrects it hard.
    set_os_clock(FIRST_EVENT_MSC + 100 - 300)
    for tick in parser.parse_message(ticker_message()):
        collected.append(tick.collected_msc)
        events.append(tick.time_msc)

    assert collected == sorted(collected)
    assert events == sorted(events)
    assert clock.resyncs > 0


def test_timestamp_string_survives_a_correction_intact(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    The string is derived from time_msc, not read from the clock again.

    Were it a separate reading, it could land on the far side of a correction
    and disagree with time_msc by more than the tolerated second - losing the
    file to a different check than the one the clamp protects.

    Args:
        steerable_clock: Clock plus a setter for its time source
    """
    clock, set_os_clock = steerable_clock
    parser = KrakenMessageParser(clock)

    set_os_clock(FIRST_EVENT_MSC)
    parser.parse_message(ticker_message())

    # A correction larger than the timestamp tolerance.
    set_os_clock(FIRST_EVENT_MSC - 5_000)
    tick = parser.parse_message(ticker_message())[0]

    stamp = datetime.strptime(
        tick.timestamp, "%Y.%m.%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)

    assert abs(int(stamp.timestamp() * 1000) - tick.time_msc) <= 1_000


def test_trade_keeps_the_exchange_event_time(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    Kraken's own timestamp is the event time and must not be clamped.

    Only our arrival stamp comes from the clock; rewriting the exchange's time
    would corrupt the very thing the archive is collected for.

    Args:
        steerable_clock: Clock plus a setter for its time source
    """
    clock, set_os_clock = steerable_clock
    set_os_clock(FIRST_EVENT_MSC)

    ticks = KrakenMessageParser(clock).parse_message(
        trade_message("2026-03-07T09:03:42.978000Z"))

    assert ticks is not None
    expected = int(datetime(
        2026, 3, 7, 9, 3, 42, 978000, tzinfo=timezone.utc
    ).timestamp() * 1000)

    assert ticks[0].time_msc == expected
    assert ticks[0].collected_msc == FIRST_EVENT_MSC


def test_trade_without_a_usable_timestamp_falls_back_to_the_clock(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    A malformed exchange stamp costs the event time, not the tick.

    Args:
        steerable_clock: Clock plus a setter for its time source
    """
    clock, set_os_clock = steerable_clock
    set_os_clock(FIRST_EVENT_MSC)

    ticks = KrakenMessageParser(clock).parse_message(
        trade_message("not-a-timestamp"))

    assert ticks is not None
    assert ticks[0].time_msc == FIRST_EVENT_MSC
