"""
FiniexDataCollector - Daily Close Tests

A file bounded only by tick count can span several UTC days. DASHUSD needs 24
days to reach 50,000 at its production rate, so "delete files older than N days"
has no well-defined subject, and nobody can say whether more will arrive for a
date already read.

A file that covers exactly one UTC day is final the moment that day ends.

Location: tests/writers/test_daily_close.py
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from python.types.tick_types import TickData
from python.utils.collection_clock import CollectionClock
from python.writers.json_tick_writer import JsonTickWriter

SYMBOL = "BTCUSD"
# 2026-09-14 23:59:58 UTC — two seconds before a day boundary.
BEFORE_MIDNIGHT = 1789430398000


def build_writer(output_dir: Path, max_ticks_per_file: int = 50000) -> JsonTickWriter:
    """
    Build a writer with a rotation threshold high enough to stay out of the way.

    Args:
        output_dir: Base output directory
        max_ticks_per_file: Rotation threshold

    Returns:
        Configured JsonTickWriter
    """
    return JsonTickWriter(
        output_dir=output_dir,
        symbol=SYMBOL,
        clock=CollectionClock(),
        broker="Kraken",
        server="kraken_websocket",
        broker_type="kraken_spot",
        max_ticks_per_file=max_ticks_per_file,
        data_collector="kraken"
    )


def tick_at(collected_msc: int) -> TickData:
    """
    Build a tick that arrived at a given instant.

    Args:
        collected_msc: Arrival time in epoch milliseconds UTC

    Returns:
        TickData with consistent event time and timestamp
    """
    event = collected_msc - 8
    return TickData(
        symbol=SYMBOL,
        timestamp=datetime.fromtimestamp(
            event / 1000, tz=timezone.utc).strftime("%Y.%m.%d %H:%M:%S"),
        time_msc=event,
        bid=79383.7,
        ask=79383.8,
        last=79383.8,
        collected_msc=collected_msc,
        spread_points=1,
        spread_pct=0.000126,
        quote_age_ms=42,
        tick_flags="BUY"
    )


def archive_files(output_dir: Path) -> List[Path]:
    """Finished archive files, oldest first."""
    return sorted((output_dir / "kraken").glob("*_ticks.json"))


def read_document(path: Path) -> Dict[str, Any]:
    """Parse one archive file."""
    return json.loads(path.read_text(encoding="utf-8"))


def days_covered(path: Path) -> set:
    """UTC days the arrival times in a file fall on."""
    return {
        datetime.fromtimestamp(t["collected_msc"] / 1000, tz=timezone.utc)
        .strftime("%Y-%m-%d")
        for t in read_document(path)["ticks"]
    }


def test_a_file_covers_exactly_one_utc_day(tmp_path: Path) -> None:
    """
    The property the whole mechanism exists for.

    Ticks spanning three days must not end up in one file, however far the tick
    count is from its threshold.
    """
    writer = build_writer(tmp_path)

    arrivals = [
        BEFORE_MIDNIGHT + day * 86_400_000 + second * 1000
        for day in range(3)
        for second in range(4)
    ]
    for msc in arrivals:
        writer.write_tick(tick_at(msc))
    writer.finalize()

    # Derived from the input rather than asserted as a literal: the series
    # starts two seconds before midnight, so each "day" of it straddles two.
    expected_days = {
        datetime.fromtimestamp(m / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        for m in arrivals
    }

    files = archive_files(tmp_path)
    assert len(files) == len(expected_days), "one file per day the ticks fell on"

    covered = set()
    for path in files:
        days = days_covered(path)
        assert len(days) == 1, f"{path.name} spans several days"
        covered |= days

    assert covered == expected_days, "every day present, none invented"


def test_the_first_tick_of_a_day_starts_the_new_file(tmp_path: Path) -> None:
    """
    The trap this is written against.

    The tick-count threshold is checked AFTER the tick is appended. Checked the
    same way, the first tick of the new day would land in the previous day's
    file and only then trigger the rotation — leaving the "closed" file with a
    tick that does not belong to it.
    """
    writer = build_writer(tmp_path)

    writer.write_tick(tick_at(BEFORE_MIDNIGHT))          # 23:59:58
    writer.write_tick(tick_at(BEFORE_MIDNIGHT + 1000))   # 23:59:59
    writer.write_tick(tick_at(BEFORE_MIDNIGHT + 3000))   # 00:00:01, next day
    writer.finalize()

    first, second = archive_files(tmp_path)

    assert days_covered(first) == {"2026-09-14"}
    assert days_covered(second) == {"2026-09-15"}
    assert len(read_document(first)["ticks"]) == 2
    assert len(read_document(second)["ticks"]) == 1


def test_the_tick_count_threshold_still_applies(tmp_path: Path) -> None:
    """The day boundary is an additional cut, not a replacement."""
    writer = build_writer(tmp_path, max_ticks_per_file=5)

    for second in range(12):
        writer.write_tick(tick_at(BEFORE_MIDNIGHT - 60_000 + second * 1000))
    writer.finalize()

    files = archive_files(tmp_path)
    assert len(files) == 3, "12 ticks at 5 per file"
    assert all(days_covered(p) == {"2026-09-14"} for p in files)


def test_a_day_with_no_ticks_produces_no_file(tmp_path: Path) -> None:
    """
    A quiet symbol skips days rather than emitting empty files. An archive file
    that covers a day with nothing in it is noise, not evidence of a gap — the
    gap is visible from the missing file.
    """
    writer = build_writer(tmp_path)

    writer.write_tick(tick_at(BEFORE_MIDNIGHT))
    writer.write_tick(tick_at(BEFORE_MIDNIGHT + 3 * 86_400_000))
    writer.finalize()

    files = archive_files(tmp_path)
    assert len(files) == 2, "two ticks three days apart, two files, no blanks"


def test_no_write_ahead_log_survives_a_day_roll(tmp_path: Path) -> None:
    """
    The day roll goes through the same finalize path as any rotation, so the
    durability guarantee is unchanged: the log of a closed file is gone, and the
    open file has exactly one.
    """
    writer = build_writer(tmp_path)

    writer.write_tick(tick_at(BEFORE_MIDNIGHT))
    writer.write_tick(tick_at(BEFORE_MIDNIGHT + 3000))

    logs = list((tmp_path / "kraken").glob("*.jsonl.part"))
    assert len(logs) == 1, "only the currently open file keeps a log"

    writer.finalize()
    assert not list((tmp_path / "kraken").glob("*.jsonl.part"))
