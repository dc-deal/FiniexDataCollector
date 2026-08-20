"""
FiniexDataCollector - Shared Test Fixtures

Initializes the global logger once per session and provides synthetic tick
builders, so no test depends on a live WebSocket or on collected data.

Location: tests/conftest.py
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pytest

from python.types.tick_types import TickData
from python.utils.logging_setup import setup_logging


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
    start_msc: int = 1_772_874_222_000,
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
