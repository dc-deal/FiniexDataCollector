"""
FiniexDataCollector - Write-Ahead Log Tests

Ticks are held in memory until a file rotates, which on a thin symbol takes
weeks - DASHUSD needs 24 days to reach 50,000. Every crash in between used to
cost the whole buffer; one machine shutdown cost 188,000 ticks across eight
symbols.

The invariant these tests defend: at every instant the ticks are in the archive
file, in the write-ahead log, or in both - never in neither.

Location: tests/writers/test_write_ahead_log.py
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from python.types.tick_types import TickData
from python.utils.collection_clock import CollectionClock
from python.writers.json_tick_writer import (
    JsonTickWriter,
    recover_orphaned_buffers
)

SYMBOL = "BTCUSD"


def build_writer(
    output_dir: Path,
    clock: CollectionClock,
    max_ticks_per_file: int = 50000
) -> JsonTickWriter:
    """
    Build a writer the way main.py does.

    Args:
        output_dir: Base output directory
        clock: Session clock
        max_ticks_per_file: Rotation threshold

    Returns:
        Configured JsonTickWriter
    """
    return JsonTickWriter(
        output_dir=output_dir,
        symbol=SYMBOL,
        clock=clock,
        broker="Kraken",
        server="kraken_websocket",
        broker_type="kraken_spot",
        max_ticks_per_file=max_ticks_per_file,
        data_collector="kraken"
    )


def wal_files(output_dir: Path) -> List[Path]:
    """Write-ahead logs currently on disk."""
    return sorted((output_dir / "kraken").glob("*_ticks.jsonl.part"))


def archive_files(output_dir: Path) -> List[Path]:
    """Finished archive files currently on disk."""
    return sorted((output_dir / "kraken").glob("*_ticks.json"))


def read_document(path: Path) -> Dict[str, Any]:
    """Parse one archive file."""
    return json.loads(path.read_text(encoding="utf-8"))


def simulate_process_death(writer: JsonTickWriter) -> None:
    """
    Release the write-ahead log the way a dying process does: the handle goes,
    the file stays.

    Recovery runs at startup, on logs left behind by a process that no longer
    exists. A test that calls it while this process still holds the handle open
    is testing something that cannot happen - and on Windows it cannot even be
    executed, because a file with an open handle refuses to be deleted. That
    divergence is invisible in the Linux dev container and fails on the
    production platform.

    Args:
        writer: The writer whose log should survive its owner
    """
    writer._close_wal(delete=False)


# =============================================================================
# THE LOG WHILE COLLECTING
# =============================================================================

def test_a_tick_reaches_disk_before_it_is_counted(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    The whole point: the buffer is no longer the only copy.

    Without the log, these ticks would exist solely in memory until rotation.
    """
    writer = build_writer(tmp_path, CollectionClock())

    for tick in tick_series[:5]:
        writer.write_tick(tick)

    logs = wal_files(tmp_path)
    assert len(logs) == 1
    assert not archive_files(tmp_path), "nothing should be rotated yet"

    lines = logs[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 6, "one metadata header plus five ticks"


def test_the_header_carries_what_a_crash_would_destroy(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    `start_time`, the device clock and the anchor counters exist only in the
    writer's memory. Written at open, they survive the process that held them.
    """
    writer = build_writer(tmp_path, CollectionClock())
    writer.write_tick(tick_series[0])

    header = json.loads(
        wal_files(tmp_path)[0].read_text(encoding="utf-8").splitlines()[0])

    assert header["symbol"] == SYMBOL
    assert header["start_time"]
    assert header["start_time_unix"] > 0
    assert header["data_format_version"] == "1.7.0"
    assert "anchor_resyncs" in header


# =============================================================================
# FLOW A/B — ROTATION AND REGULAR SHUTDOWN
# =============================================================================

def test_a_regular_finalize_removes_the_log(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    Flow B: after Ctrl+C the archive file stands and the log is gone.
    """
    writer = build_writer(tmp_path, CollectionClock())
    for tick in tick_series:
        writer.write_tick(tick)

    writer.finalize()

    assert len(archive_files(tmp_path)) == 1
    assert not wal_files(tmp_path), "the log outlived the file it protected"


def test_a_rotation_starts_a_fresh_log(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    Flow A: the old log goes with the file it belonged to, and the next file
    opens its own.
    """
    writer = build_writer(tmp_path, CollectionClock(), max_ticks_per_file=10)

    for tick in tick_series[:12]:
        writer.write_tick(tick)

    assert len(archive_files(tmp_path)) == 1
    logs = wal_files(tmp_path)
    assert len(logs) == 1, "exactly the log of the file still open"

    lines = logs[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3, "header plus the two ticks after the rotation"


# =============================================================================
# FLOW C — CRASH
# =============================================================================

def test_a_crashed_buffer_is_recovered_into_an_archive_file(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    Flow C, the case that cost 188,000 ticks: the process dies with a full
    buffer and no archive file.
    """
    writer = build_writer(tmp_path, CollectionClock())
    for tick in tick_series:
        writer.write_tick(tick)

    # The crash: the writer is abandoned without finalize().
    del writer
    assert not archive_files(tmp_path)

    recovered = recover_orphaned_buffers(tmp_path, "kraken")

    assert len(recovered) == 1
    assert not wal_files(tmp_path)

    document = read_document(recovered[0])
    assert len(document["ticks"]) == len(tick_series)
    assert document["summary"]["total_ticks"] == len(tick_series)
    assert document["metadata"]["data_format_version"] == "1.7.0"


def test_recovered_ticks_are_identical_to_what_was_written(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    Recovery must reproduce the ticks, not approximate them - a rebuilt file
    that differs from a rotated one would make the archive depend on how a run
    happened to end.
    """
    clock = CollectionClock()
    crashed = build_writer(tmp_path / "crashed", clock)
    for tick in tick_series:
        crashed.write_tick(tick)
    del crashed

    clean_dir = tmp_path / "clean"
    clean = build_writer(clean_dir, CollectionClock())
    for tick in tick_series:
        clean.write_tick(tick)
    clean.finalize()

    recovered = recover_orphaned_buffers(tmp_path / "crashed", "kraken")[0]

    assert (read_document(recovered)["ticks"]
            == read_document(archive_files(clean_dir)[0])["ticks"])


# =============================================================================
# FLOW D — THE DANGEROUS WINDOW
# =============================================================================

def test_an_existing_archive_file_is_never_overwritten(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    Flow D: the crash fell between writing the file and removing the log.

    A consumer may already have read that file. Recovery drops the log and
    leaves the file untouched.
    """
    writer = build_writer(tmp_path, CollectionClock())
    for tick in tick_series:
        writer.write_tick(tick)
    writer.finalize()

    archive = archive_files(tmp_path)[0]
    original = archive.read_text(encoding="utf-8")

    # Reconstruct the window: the file stands and a COMPLETE log beside it,
    # ticks included. A header-only log would hit the "no ticks" branch and
    # leave the file alone for a reason that has nothing to do with the guard.
    stale_log = archive.with_suffix(".jsonl.part")
    document = read_document(archive)
    stale_log.write_text(
        "\n".join([json.dumps(document["metadata"])]
                  + [json.dumps(t) for t in document["ticks"]]) + "\n",
        encoding="utf-8")

    assert recover_orphaned_buffers(tmp_path, "kraken") == []
    assert not stale_log.exists()
    assert archive.read_text(encoding="utf-8") == original


# =============================================================================
# FLOW E/F — TORN AND EMPTY LOGS
# =============================================================================

def test_a_torn_final_line_costs_one_tick_not_the_file(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """Flow E: the process died mid-write."""
    writer = build_writer(tmp_path, CollectionClock())
    for tick in tick_series[:6]:
        writer.write_tick(tick)
    del writer

    log = wal_files(tmp_path)[0]
    text = log.read_text(encoding="utf-8")
    log.write_text(text + '{"timestamp": "2026.09.15 08:0', encoding="utf-8")

    recovered = recover_orphaned_buffers(tmp_path, "kraken")

    assert len(recovered) == 1
    assert len(read_document(recovered[0])["ticks"]) == 6


def test_a_log_with_no_ticks_produces_no_file(tmp_path: Path) -> None:
    """
    Flow F: the crash landed between opening the file and the first tick.
    A zero-tick archive file is noise, not evidence.
    """
    writer = build_writer(tmp_path, CollectionClock())
    writer._start_new_file()

    assert len(wal_files(tmp_path)) == 1

    simulate_process_death(writer)

    assert recover_orphaned_buffers(tmp_path, "kraken") == []
    assert not wal_files(tmp_path)
    assert not archive_files(tmp_path)


def test_an_unreadable_header_is_kept_rather_than_invented(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    Without the header there is no `start_time` and no anchor state. Writing a
    file anyway would mean stating things nobody measured, so the log is set
    aside under a name that says so.
    """
    writer = build_writer(tmp_path, CollectionClock())
    for tick in tick_series[:3]:
        writer.write_tick(tick)
    del writer

    log = wal_files(tmp_path)[0]
    lines = log.read_text(encoding="utf-8").splitlines()
    log.write_text("{not json\n" + "\n".join(lines[1:]) + "\n", encoding="utf-8")

    assert recover_orphaned_buffers(tmp_path, "kraken") == []
    assert not archive_files(tmp_path)
    assert list((tmp_path / "kraken").glob("*.corrupt")), "the data is kept"


# =============================================================================
# THE CLOCK CHECKPOINT
# =============================================================================

def test_a_clock_correction_survives_into_the_recovered_summary(
    tmp_path: Path,
    tick_series: List[TickData],
    steerable_clock
) -> None:
    """
    `summary.anchor` states the clock's condition at close, which a crashed
    process cannot report. The log checkpoints it, so a recovered file does not
    repeat its opening counters and thereby claim nothing happened.
    """
    clock, set_os_clock = steerable_clock
    writer = build_writer(tmp_path, clock)

    set_os_clock(1_772_874_222_000)
    for tick in tick_series[:5]:
        clock.next_msc()
        writer.write_tick(tick)

    # The OS clock steps back, the clamp absorbs it, and more ticks arrive.
    set_os_clock(1_772_874_221_600)
    clock.next_msc()
    for tick in tick_series[5:10]:
        writer.write_tick(tick)

    del writer
    document = read_document(recover_orphaned_buffers(tmp_path, "kraken")[0])

    assert document["metadata"]["anchor_resyncs"] == 0
    assert document["summary"]["anchor"]["resyncs"] == 1
    assert document["summary"]["anchor"]["max_correction_ms"] == 400


def test_a_failed_write_leaves_the_log_in_place(
    tmp_path: Path,
    tick_series: List[TickData],
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Why the log is dropped after the archive file and never before.

    If the order were reversed, a write that fails here would leave the ticks
    in neither place. The happy path cannot show this - both orderings end
    identically when nothing goes wrong, which is what makes it easy to
    "simplify" later.
    """
    writer = build_writer(tmp_path, CollectionClock())
    for tick in tick_series:
        writer.write_tick(tick)

    def refuse(*args, **kwargs):
        raise OSError("disk full")

    # The rename lives in wal_archive since the writer and the recovery started
    # sharing one archive builder; this is still the same last step.
    monkeypatch.setattr("python.writers.wal_archive.os.replace", refuse)

    with pytest.raises(Exception):
        writer.finalize()

    assert not archive_files(tmp_path), "no file should exist after a failed write"
    assert wal_files(tmp_path), "the ticks must still be somewhere"

    monkeypatch.undo()
    simulate_process_death(writer)
    recovered = recover_orphaned_buffers(tmp_path, "kraken")

    assert len(recovered) == 1
    assert len(read_document(recovered[0])["ticks"]) == len(tick_series)


def test_one_undeletable_log_does_not_end_the_recovery(
    tmp_path: Path,
    tick_series: List[TickData],
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Recovery runs in main.py's startup path without a guard of its own, so an
    exception here means the collector does not start at all.

    On Windows a handle held by anything else - antivirus, a backup agent, a
    second instance the lock did not catch - makes a refused unlink reachable.
    One log that will not go must cost that log's cleanup, not the other
    symbols' recovery.
    """
    for symbol in ("BTCUSD", "ETHUSD"):
        writer = JsonTickWriter(
            output_dir=tmp_path, symbol=symbol, clock=CollectionClock(),
            broker="Kraken", server="kraken_websocket",
            broker_type="kraken_spot", max_ticks_per_file=50000,
            data_collector="kraken")
        for tick in tick_series:
            writer.write_tick(tick)
        del writer

    assert len(wal_files(tmp_path)) == 2

    real_unlink = Path.unlink

    def refuse_one(self, *args, **kwargs):
        if self.name.startswith("BTCUSD"):
            raise PermissionError(32, "held by another process")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", refuse_one)

    recovered = recover_orphaned_buffers(tmp_path, "kraken")

    assert len(recovered) == 2, "both symbols must still be rebuilt"
    assert len(wal_files(tmp_path)) == 1, "only the undeletable log remains"
    assert wal_files(tmp_path)[0].name.startswith("BTCUSD")


def test_a_shutdown_with_nothing_buffered_names_no_file(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    A file rotated moments before the stop leaves an empty buffer behind.

    `finalize()` used to hand back the path of the file it had just opened, and
    the shutdown then logged `Finalized: <name>` for a file nobody ever wrote.
    Measured on the production box 2026-09-17: the shutdown named thirteen files
    and twelve were on disk. That log is exactly where someone looks afterwards
    to decide whether a stop was clean, so a name with no file behind it sends
    them hunting at the worst possible moment.

    Args:
        tmp_path: pytest temp directory
        tick_series: A synthetic tick series
    """
    writer = build_writer(tmp_path, CollectionClock(), max_ticks_per_file=2)

    # Exactly the rotation threshold: the file closes and the next one opens
    # empty, which is the state a stop can arrive in.
    for tick in tick_series[:2]:
        writer.write_tick(tick)

    assert len(archive_files(tmp_path)) == 1, "the rotation itself did not happen"
    assert len(wal_files(tmp_path)) == 1, "no log was opened for the new file"

    assert writer.finalize() is None, "named a file it did not write"

    assert len(archive_files(tmp_path)) == 1, "an empty buffer produced a file"
    assert wal_files(tmp_path) == [], "the empty log was left behind"
