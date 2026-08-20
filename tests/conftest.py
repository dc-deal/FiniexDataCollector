"""
FiniexDataCollector - Shared Test Fixtures

Initializes the global logger and broker config once per session and provides
synthetic tick builders plus a steerable clock, so no test depends on a live
WebSocket, on collected data, or on what the wall clock happens to do.

Location: tests/conftest.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Tuple

import pytest

from python.types.broker_config_types import BrokerConfig
from python.types.tick_types import TickData
from python.utils.collection_clock import CollectionClock
from python.utils.logging_setup import setup_logging

# Event time of the first synthetic tick, shared so the clock fixture and the
# tick builder describe the same moment.
FIRST_EVENT_MSC = 1_772_874_222_000


@pytest.fixture(scope="session", autouse=True)
def initialized_logging(tmp_path_factory) -> None:
    """
    Initialize the global logger before any collector object is built.

    JsonTickWriter resolves its logger in __init__ and raises when logging was
    never set up, so this has to run for the whole session.

    Args:
        tmp_path_factory: pytest factory for session-scoped temp dirs

    Returns:
        None
    """
    setup_logging(
        console_level="ERROR",
        file_level="ERROR",
        log_dir=tmp_path_factory.mktemp("logs")
    )


@pytest.fixture(scope="session", autouse=True)
def loaded_broker_config(tmp_path_factory) -> None:
    """
    Load a minimal broker config through the real loading path.

    The parser asks BrokerConfig for digits and tick size and gets a hard error
    when nothing was loaded, so this has to exist before any message is parsed.

    Args:
        tmp_path_factory: pytest factory for session-scoped temp dirs

    Returns:
        None
    """
    config_path = tmp_path_factory.mktemp("config") / "broker_config.json"
    config_path.write_text(json.dumps({
        "broker_type": "kraken_spot",
        "broker_info": {"broker_type": "kraken_spot", "server": "kraken_websocket"},
        "symbols": {
            "BTCUSD": {
                "digits": 1,
                "tick_size": 0.1,
                "point": 0.1,
                "base_currency": "BTC",
                "quote_currency": "USD"
            },
            "ETHUSD": {
                "digits": 2,
                "tick_size": 0.01,
                "point": 0.01,
                "base_currency": "ETH",
                "quote_currency": "USD"
            }
        }
    }), encoding="utf-8")

    BrokerConfig.load_from_file(config_path)


@pytest.fixture
def steerable_clock(
    monkeypatch: pytest.MonkeyPatch
) -> Tuple[CollectionClock, Callable[[int], None]]:
    """
    A CollectionClock whose time source the test drives by hand.

    Returns the clock together with a setter for the underlying OS reading, so
    a backwards step can be provoked deterministically instead of waiting for
    an NTP correction that may never come.

    Args:
        monkeypatch: pytest patching helper

    Returns:
        Tuple of (CollectionClock, callable setting the OS reading in ms)
    """
    now_ms = [FIRST_EVENT_MSC]

    # Half a millisecond past the requested value: the clock computes
    # int(seconds * 1000), and that float round trip can land just under a whole
    # millisecond and truncate to the one before it.
    monkeypatch.setattr(
        "python.utils.collection_clock.time.time",
        lambda: (now_ms[0] + 0.5) / 1000
    )

    def set_os_clock(value_ms: int) -> None:
        now_ms[0] = value_ms

    return CollectionClock(), set_os_clock


@pytest.fixture
def tick_series() -> List[TickData]:
    """
    A default synthetic stream for tests that do not care about its shape.

    Returns:
        List of TickData in arrival order
    """
    return build_ticks()


def build_ticks(
    count: int = 20,
    symbol: str = "BTCUSD",
    start_msc: int = FIRST_EVENT_MSC,
    interval_ms: int = 250,
    lag_ms: int = 7,
    burst_at: int = 5
) -> List[TickData]:
    """
    Build a synthetic tick series shaped like a real Kraken stream.

    Event times rise, collected_msc trails them by the measured median lag, and
    one burst repeats a millisecond — ties are legitimate on this feed and must
    not be mistaken for a defect.

    Args:
        count: Number of ticks to build
        symbol: Normalized symbol name
        start_msc: Event time of the first tick, in epoch milliseconds
        interval_ms: Spacing between consecutive event times
        lag_ms: Distance from event time to arrival time
        burst_at: Index that repeats its predecessor's timestamps

    Returns:
        List of TickData in arrival order
    """
    ticks: List[TickData] = []

    for i in range(count):
        step = i - 1 if i == burst_at else i
        time_msc = start_msc + step * interval_ms

        stamp = datetime.fromtimestamp(time_msc / 1000, tz=timezone.utc)

        ticks.append(TickData(
            symbol=symbol,
            timestamp=stamp.strftime("%Y.%m.%d %H:%M:%S"),
            time_msc=time_msc,
            bid=45000.0 + i,
            ask=45010.0 + i,
            last=45005.0 + i,
            real_volume=1.5,
            chart_tick_volume=i + 1,
            spread_points=100,
            spread_pct=0.022,
            collected_msc=time_msc + lag_ms,
            tick_flags="BID ASK",
            session="24h"
        ))

    return ticks
