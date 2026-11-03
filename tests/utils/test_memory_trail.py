"""
FiniexDataCollector - The memory trail

`/v1/status` says how much memory the process holds now, and a restart forgets it.
The trail writes one line per window that outlives the process, so a slope over
weeks can be read from the log. Every figure in it is a delta over its own window.

Location: tests/utils/test_memory_trail.py
"""

import asyncio
import json
import types
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

import python.utils.memory_trail as trail_module
from python.api.stats_serializer import process_resources
from python.types.collector_stats import (CollectorStats, ReconnectTotals,
                                          StallEvent)
from python.utils.memory_trail import MemoryTrail, read_memory_mb


class Process:
    """A psutil.Process with the figures a test sets."""

    def __init__(self, rss: int = 200 * 1024 * 1024, private: Any = 250 * 1024 * 1024) -> None:
        self.rss = rss
        self.private = private

    def memory_info(self) -> Any:
        info = types.SimpleNamespace(rss=self.rss)
        if self.private is not None:
            info.private = self.private
        return info

    def num_threads(self) -> int:
        return 7


def build(ticks: Dict[str, int] = None) -> tuple:
    """A trail over fresh statistics, with a fake process and a recording log."""
    stats = CollectorStats()
    counts = dict(ticks or {"BTCUSD": 0, "ETHUSD": 0})
    lines: List[str] = []
    trail = MemoryTrail(
        stats=stats, ticks_provider=lambda: counts, log=lines.append)
    trail._process = Process()
    return trail, stats, counts, lines


def stall(stats: CollectorStats, at: datetime, ms: float, cause: str) -> None:
    """Record a stall the way the lag monitor leaves it."""
    stats.stalls.append(StallEvent(at=at, ms=ms, gc_ms=0.0, gc_generation=-1, render_ms=0.0,
                                   exports_in_flight=0, cause=cause))


def test_the_first_reading_only_opens_a_window() -> None:
    trail, _, _, _ = build()

    assert trail.line() is None


def test_a_line_carries_the_deltas_of_its_window() -> None:
    trail, stats, counts, _ = build()
    stats.record_gc_pause(0, 2.0)
    stats.record_loop_lag(600.0)
    trail.line()

    counts["BTCUSD"] += 120
    counts["ETHUSD"] += 30
    stats.record_gc_pause(0, 3.0)
    stats.record_gc_pause(2, 41.5)
    stats.record_loop_lag(700.0)
    stats.record_loop_lag(800.0)
    line = trail.line()

    assert line["schema"] == 1
    assert line["ticks"] == {"BTCUSD": 120, "ETHUSD": 30}
    assert line["gc"]["collections"] == [1, 0, 1]
    assert line["gc"]["ms"] == 44.5
    assert line["gc"]["longest_ms"] == 41.5
    assert line["loop"]["over_500ms"] == 2
    assert line["rss_mb"] == 200.0
    assert line["private_mb"] == 250.0
    assert line["threads"] == 7
    assert len(line["gc"]["threshold"]) == 3


def test_the_longest_collection_is_per_window_not_since_the_start() -> None:
    trail, stats, _, _ = build()
    trail.line()
    stats.record_gc_pause(2, 90.0)
    assert trail.line()["gc"]["longest_ms"] == 90.0

    stats.record_gc_pause(0, 1.5)
    assert trail.line()["gc"]["longest_ms"] == 1.5


def test_a_symbol_new_in_the_window_counts_from_zero() -> None:
    trail, _, counts, _ = build({"BTCUSD": 10})
    trail.line()
    counts["DOTUSD"] = 4

    assert trail.line()["ticks"] == {"BTCUSD": 0, "DOTUSD": 4}


def test_the_worst_stall_is_taken_from_inside_the_window_only() -> None:
    trail, stats, _, _ = build()
    stall(stats, datetime.now(timezone.utc) -
          timedelta(hours=1), 9000.0, "before the window")
    trail.line()
    now = datetime.now(timezone.utc)
    stall(stats, now, 700.0, "handling 4 ticks")
    stall(stats, now, 1300.0, "garbage collection, generation 2")

    loop = trail.line()["loop"]

    assert loop["worst_stall_ms"] == 1300.0
    assert loop["worst_stall_cause"] == "garbage collection, generation 2"


def test_no_stall_is_null_not_zero() -> None:
    trail, _, _, _ = build()
    trail.line()

    loop = trail.line()["loop"]

    assert loop["worst_stall_ms"] is None
    assert loop["worst_stall_cause"] is None


def test_outages_are_null_until_the_first_one_and_a_delta_after() -> None:
    trail, stats, _, _ = build()
    trail.line()
    assert trail.line()["outages"] is None

    stats.reconnect_totals = ReconnectTotals(count=3)
    assert trail.line()["outages"] == 3
    stats.reconnect_totals.count = 5
    assert trail.line()["outages"] == 2


def test_private_memory_is_null_where_the_platform_does_not_report_it() -> None:
    """Zero would read as "nothing committed", which is a measurement."""
    assert read_memory_mb(Process(private=None)) == (200.0, None)
    assert read_memory_mb(Process()) == (200.0, 250.0)


def test_a_process_that_will_not_say_is_null_and_does_not_raise() -> None:
    class Refusing:
        def memory_info(self) -> Any:
            raise PermissionError("access denied")

    assert read_memory_mb(Refusing()) == (None, None)


def test_status_carries_private_memory_beside_rss() -> None:
    resources = process_resources()

    assert "private_mb" in resources
    assert "rss_mb" in resources


def test_it_logs_one_memory_line_per_interval_and_none_at_the_start(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    trail, _, counts, lines = build()
    sleeps: List[float] = []

    async def driven_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        counts["BTCUSD"] += 5
        if len(sleeps) == 3:
            raise asyncio.CancelledError()

    monkeypatch.setattr(trail_module, "asyncio",
                        types.SimpleNamespace(sleep=driven_sleep))

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(trail.run())

    assert sleeps == [600, 600, 600]
    assert len(lines) == 2
    assert all(line.startswith("[MEMORY] ") for line in lines)
    assert json.loads(lines[0][len("[MEMORY] "):])["ticks"]["BTCUSD"] == 5


def test_the_writer_counts_ticks_across_files(tmp_path: Any, tick_series: List[Any]) -> None:
    """The trail reads this, and it must not reset when a file closes."""
    from python.utils.collection_clock import CollectionClock
    from python.writers.json_tick_writer import JsonTickWriter

    writer = JsonTickWriter(output_dir=tmp_path, symbol="BTCUSD", clock=CollectionClock(),
                            broker="Kraken", server="kraken_websocket", broker_type="kraken_spot",
                            max_ticks_per_file=3, data_collector="kraken")
    for tick in tick_series[:5]:
        writer.write_tick(tick)

    assert writer.ticks_written == 5
