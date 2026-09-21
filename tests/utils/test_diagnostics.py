"""
FiniexDataCollector - Tests for the two diagnostics a remote session reads

Both exist because of one defect. Writing an archive file ran on the collector's
only event loop, and a blocked loop stamps `collected_msc` late - invisible in
the file itself until the lag passes the consuming importer's 30 s window and
costs the whole file. Measured on production 2026-09-20: 21 s of blocking at the
UTC day cut.

`loop_lag` measures the stall directly, so the next one is a number on
`/v1/status` rather than an inference from tick timestamps. `exports` counts the
subprocesses that write the files now, because a file that is owed and never
written would otherwise only show up as an absence in the archive.

Location: tests/utils/test_diagnostics.py
"""

import asyncio
import gc
import threading
import time

from python.main import FiniexDataCollector
from python.utils.gc_watcher import GcWatcher
from python.types.collector_stats import CollectorStats
from python.utils.config_loader import ConfigLoader
from python.utils.logging_setup import remove_log_listener


class FakeUsage:
    """What psutil.disk_usage returns, as much of it as the monitor reads."""
    total = 100 * 1024 ** 3
    used = 40 * 1024 ** 3
    free = 60 * 1024 ** 3


def run_one_round(collector, monitor) -> int:
    """
    Run one round of a monitoring task and stop it.

    Args:
        collector: The collector owning the task
        monitor: The bound coroutine function to run

    Returns:
        The identity of the thread the event loop ran on
    """
    collector._is_running = True

    async def round_trip() -> int:
        loop_thread = threading.get_ident()
        task = asyncio.create_task(monitor())
        await asyncio.sleep(0.3)
        collector._is_running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return loop_thread

    return asyncio.run(round_trip())


def build_collector():
    """A collector with its own stats and no task running."""
    return FiniexDataCollector(ConfigLoader().load(), show_display=False)


def test_the_folder_scan_runs_off_the_event_loop() -> None:
    """
    Counting files is file I/O, and on the production box file I/O is expensive.

    Measured 2026-09-21, while this still ran on the loop: stalls of up to 4.3 s
    about once a minute, which land in every tick's `collected_msc` as arrival
    lag that was ours rather than the venue's. The consuming project reads that
    field as latency.
    """
    collector = build_collector()
    ran_in = []

    def scan() -> float:
        ran_in.append(threading.get_ident())
        return 12.5

    try:
        collector._scan_folders = scan
        loop_thread = run_one_round(collector, collector._monitor_folders)

        assert ran_in, "the scan did not run at all"
        assert ran_in[0] != loop_thread, "the scan ran on the event loop"
        assert collector._stats.scans.folder_scan_last_ms == 12.5
    finally:
        remove_log_listener(collector._stats.record_logged)


def test_the_disk_check_runs_off_the_event_loop() -> None:
    """The same for the disk reading, which is a filesystem call too."""
    collector = build_collector()
    ran_in = []

    def read():
        ran_in.append(threading.get_ident())
        return FakeUsage(), 3.0

    try:
        collector._read_disk_usage = read
        loop_thread = run_one_round(collector, collector._monitor_disk_space)

        assert ran_in, "the disk check did not run at all"
        assert ran_in[0] != loop_thread, "the disk check ran on the event loop"
        assert collector._stats.scans.disk_check_last_ms == 3.0
        assert collector._stats.disk_space.free_bytes == FakeUsage.free
    finally:
        remove_log_listener(collector._stats.record_logged)


def test_a_collection_is_timed_by_the_interpreter_itself() -> None:
    """
    The pause is measured where it happens, not inferred afterwards.

    Nothing in Python reports what a collection cost, and the collector holds
    every tick of an open file in memory - tens of thousands of objects that a
    generation-2 walk touches, on the event loop, between two ticks.
    """
    stats = CollectorStats()
    watcher = GcWatcher(stats)
    watcher.start()
    try:
        gc.collect()
    finally:
        watcher.stop()

    assert stats.gc.collections[2] >= 1, "a full collection went unrecorded"
    assert stats.gc.last_generation == 2
    assert stats.gc.total_ms >= 0.0


def test_only_the_collections_inside_the_window_are_attributed() -> None:
    """
    A stall asks what happened in ITS window, not what happened at some point.

    Counting an older pause would explain a stall with something that was over
    before it began - which is the mistake the whole instrument exists to stop.
    """
    stats = CollectorStats()
    watcher = GcWatcher(stats)
    watcher.start()
    try:
        gc.collect()
        boundary = time.monotonic()
        gc.collect()
    finally:
        watcher.stop()

    inside, generation = watcher.time_spent_since(boundary)
    everything, _ = watcher.time_spent_since(0.0)

    assert generation == 2
    assert inside <= everything
    assert watcher.time_spent_since(time.monotonic()) == (0.0, -1)


def test_a_stall_names_what_accounts_for_it() -> None:
    """Half of the stall or more, or it says "unknown" rather than guessing."""
    stats = CollectorStats()

    stats.record_stall(1000.0, gc_ms=900.0, gc_generation=2,
                       render_ms=5.0, exports_in_flight=0)
    stats.record_stall(1000.0, gc_ms=10.0, gc_generation=0,
                       render_ms=800.0, exports_in_flight=0)
    stats.record_stall(1000.0, gc_ms=1.0, gc_generation=-1,
                       render_ms=2.0, exports_in_flight=1)
    stats.record_stall(1000.0, gc_ms=0.0, gc_generation=-1,
                       render_ms=0.0, exports_in_flight=0)

    causes = [s.cause for s in stats.stalls]

    assert causes[0] == "garbage collection, generation 2"
    assert causes[1] == "the live display"
    assert causes[2] == "an archive export was handed over"
    assert causes[3] == "unknown", "an unexplained stall must say so"


def test_the_stall_list_stays_short() -> None:
    """A diagnostic that grows without bound becomes the next defect."""
    stats = CollectorStats()
    for _ in range(stats.max_stalls + 5):
        stats.record_stall(300.0, 0.0, -1, 0.0, 0)

    assert len(stats.stalls) == stats.max_stalls


def test_a_real_stall_reaches_the_record() -> None:
    """
    The loop is blocked on purpose, because that is the failure being watched.

    Driving `record_stall` by hand would leave the one thing untested that
    matters: that the monitor notices at all, and asks the garbage collector
    about its own window while doing so.
    """
    collector = build_collector()
    collector._gc_watcher.start()

    async def block_the_loop() -> None:
        collector._is_running = True
        task = asyncio.create_task(collector._monitor_loop_lag())
        await asyncio.sleep(0.05)
        time.sleep(0.4)                      # the stall, deliberately
        await asyncio.sleep(0.3)
        collector._is_running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    try:
        asyncio.run(block_the_loop())
    finally:
        collector._gc_watcher.stop()
        remove_log_listener(collector._stats.record_logged)

    assert collector._stats.stalls, "a 400 ms stall was not written down"
    assert collector._stats.stalls[-1].ms >= 250
    assert collector._stats.loop_lag.max_ms >= 250


def test_the_worst_stall_is_kept_with_the_moment_it_happened() -> None:
    """A maximum without a time cannot be matched against anything else."""
    stats = CollectorStats()
    stats.record_loop_lag(12.0)
    stats.record_loop_lag(1800.0)
    stats.record_loop_lag(3.0)

    assert stats.loop_lag.samples == 3
    assert stats.loop_lag.max_ms == 1800.0
    assert stats.loop_lag.max_at is not None
    assert stats.loop_lag.last_ms == 3.0, "the last reading is the last one"
    assert stats.loop_lag.over_500ms == 1


def test_a_wake_up_that_was_early_counts_as_no_lag() -> None:
    """
    Sleep can return a hair early, and a negative lag is not a measurement.

    Clamped rather than recorded: a negative maximum would read as if the loop
    ran ahead of itself.
    """
    stats = CollectorStats()
    stats.record_loop_lag(-4.0)

    assert stats.loop_lag.max_ms == 0.0
    assert stats.loop_lag.last_ms == 0.0


def test_an_export_is_counted_from_handover_to_file() -> None:
    """In flight is what was handed over and not yet reported."""
    stats = CollectorStats()
    stats.record_export_started()
    stats.record_export_started()

    assert stats.exports.in_flight == 2

    stats.record_export_finished(
        "BTCUSD_20260920_000004_ticks.json", 50000, 900.0)

    assert stats.exports.in_flight == 1
    assert stats.exports.finished == 1
    assert stats.exports.last_ticks == 50000
    assert stats.exports.max_ms == 900.0


def test_a_failed_export_names_the_file_that_is_still_owed() -> None:
    """
    Its write-ahead log is still on disk and the next start recovers it - but
    only somebody who knows which file goes looking.
    """
    stats = CollectorStats()
    stats.record_export_started()
    stats.record_export_failed("ETHUSD_20260920_000005_ticks.json")

    assert stats.exports.failed == 1
    assert stats.exports.in_flight == 0
    assert stats.exports.last_failed_file == "ETHUSD_20260920_000005_ticks.json"
    assert stats.exports.last_failed_at is not None


def test_nothing_in_flight_can_go_below_zero() -> None:
    """A report without a handover must not make the gauge lie downwards."""
    stats = CollectorStats()
    stats.record_export_failed("orphan.json")

    assert stats.exports.in_flight == 0
