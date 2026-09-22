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

from tests.conftest import build_ticks

# Any fixed instant inside one UTC day: the tick contents are irrelevant
# here, only that handling them costs measurable time.
MIDNIGHT = 1789948800000


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


# =============================================================================
# WHAT THE TICK HANDLER COSTS THE LOOP (Befund 28)
# =============================================================================

def test_handling_ticks_is_named_when_it_accounts_for_the_stall() -> None:
    """
    The arm that was missing while ten production stalls said `unknown`.

    Measured 2026-09-21: the loop stalled up to 738 ms with the display off, no
    export running, and garbage collection accounting for under a tenth of it.
    Everything the attribution could name had been ruled out, which left a
    consumer nobody was measuring.
    """
    stats = CollectorStats()

    stats.record_stall(700.0, gc_ms=50.0, gc_generation=1, render_ms=0.0,
                       exports_in_flight=0, tick_ms=600.0, ticks=412)

    assert stats.stalls[-1].cause == "handling 412 ticks"
    assert stats.stalls[-1].tick_ms == 600.0
    assert stats.stalls[-1].ticks == 412


def test_a_measured_arm_outranks_the_presence_of_an_export() -> None:
    """
    `exports_in_flight` is a count of things running, not a duration.

    It is the weakest evidence in the record, and it must never outrank
    something that was actually timed - otherwise a stall that happens to
    coincide with a handover is attributed to the handover, which is how the
    folder scan was blamed and then measured at 15 ms.
    """
    stats = CollectorStats()

    stats.record_stall(700.0, gc_ms=0.0, gc_generation=-1, render_ms=0.0,
                       exports_in_flight=3, tick_ms=600.0, ticks=88)

    assert stats.stalls[-1].cause == "handling 88 ticks"


def test_nothing_measured_still_says_unknown() -> None:
    """
    A new arm must not turn `unknown` into a guess.

    The word is what made the previous attribution believable when it finally
    named the display: a cause is stated only when something accounts for half
    of the stall, and a tiny amount of tick work is not an explanation.
    """
    stats = CollectorStats()

    stats.record_stall(700.0, gc_ms=5.0, gc_generation=1, render_ms=0.0,
                       exports_in_flight=0, tick_ms=12.0, ticks=4)

    assert stats.stalls[-1].cause == "unknown"


def test_the_handler_is_timed_where_the_ticks_actually_arrive() -> None:
    """
    A measurement nobody takes explains nothing.

    The accumulator lives on the statistics and could be perfectly correct while
    no call site fills it, so this drives the real entry point rather than the
    recorder - which is the failure being guarded against.
    """
    collector = build_collector()
    try:
        for tick in build_ticks(count=3, start_msc=MIDNIGHT):
            collector._on_tick_received(tick)

        assert collector._stats.tick_work.ticks == 3, (
            "nothing measures what handling a tick costs")
        assert collector._stats.tick_work.total_ms > 0
    finally:
        remove_log_listener(collector._stats.record_logged)


def test_the_window_is_a_difference_and_not_a_running_total() -> None:
    """
    A stall must be explained by work done INSIDE it, not since startup.

    Taking the accumulator's total would blame the handler for every tick of the
    session the first time the loop hiccuped - a cause that is always true and
    therefore says nothing. The monitor reads before waiting and subtracts
    after; this drives that loop with work happening only before its window.
    """
    collector = build_collector()

    async def stall_after_the_work_is_done() -> None:
        # Work the handler did long before the window being measured.
        for tick in build_ticks(count=40, start_msc=MIDNIGHT):
            collector._on_tick_received(tick)

        collector._is_running = True
        task = asyncio.create_task(collector._monitor_loop_lag())
        await asyncio.sleep(0.05)
        time.sleep(0.4)                      # the stall, with no ticks in it
        await asyncio.sleep(0.3)
        collector._is_running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    try:
        asyncio.run(stall_after_the_work_is_done())
    finally:
        remove_log_listener(collector._stats.record_logged)

    assert collector._stats.stalls, "the stall was not recorded at all"
    stall = collector._stats.stalls[-1]
    assert stall.ticks == 0, (
        f"the window claimed {stall.ticks} ticks that arrived before it")
    assert stall.cause != "handling 0 ticks"


def test_when_two_measurements_overlap_the_more_specific_one_wins() -> None:
    """
    A collection triggered inside the tick handler is counted in both arms.

    That overlap is real and unavoidable: the allocation that tripped the
    collector happened while handling a tick, so the handler's elapsed time
    contains the pause. Both then exceed half the stall, and the answer has to
    be the one that sends somebody somewhere useful - "garbage collection,
    generation 2" names a thing to tune, "handling 412 ticks" names the normal
    work it happened during.
    """
    stats = CollectorStats()

    stats.record_stall(700.0, gc_ms=600.0, gc_generation=2, render_ms=0.0,
                       exports_in_flight=0, tick_ms=650.0, ticks=412)

    assert stats.stalls[-1].cause == "garbage collection, generation 2"
    assert stats.stalls[-1].tick_ms == 650.0, (
        "the overlapping measurement is still recorded, only not named")


def test_a_reconnect_inside_the_window_is_named() -> None:
    """
    Two stalls on 2026-09-22 said `unknown` because nothing connected them.

    The reconnect at 02:43:50 completed at 02:43:52 and produced stalls of
    252 ms and 271 ms in those two seconds. Everything the record could name had
    been ruled out, and the cause was sitting in the reconnect list the whole
    time with nothing joining the two.
    """
    stats = CollectorStats()

    stats.record_stall(260.0, gc_ms=0.0, gc_generation=-1, render_ms=0.0,
                       exports_in_flight=0, reconnected=True)

    assert stats.stalls[-1].cause == "the websocket reconnected"
    assert stats.stalls[-1].reconnected is True


def test_a_measured_cause_still_outranks_a_reconnect() -> None:
    """
    `reconnected` is presence, like `exports_in_flight`, and ranks with it.

    A collection that happens to land in the same second as a reconnect is
    explained by the collection; naming the socket there would send somebody to
    the network over something that was timed.
    """
    stats = CollectorStats()

    stats.record_stall(700.0, gc_ms=600.0, gc_generation=1, render_ms=0.0,
                       exports_in_flight=0, reconnected=True)

    assert stats.stalls[-1].cause == "garbage collection, generation 1"


def test_the_monitor_asks_whether_the_socket_came_back(monkeypatch) -> None:
    """
    The correlation lives at a call site, so the call site is what runs here.

    It is also the one place two clocks could be mixed: the reconnect list
    carries wall-clock stamps a human reads, and comparing a stall against those
    would mean subtracting two readings a clock correction can sit between.
    """
    collector = build_collector()
    asked = []
    collector._reconnected_between = lambda since: asked.append(since) or True

    async def one_stall() -> None:
        collector._is_running = True
        task = asyncio.create_task(collector._monitor_loop_lag())
        await asyncio.sleep(0.05)
        time.sleep(0.4)
        await asyncio.sleep(0.3)
        collector._is_running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    try:
        asyncio.run(one_stall())
    finally:
        remove_log_listener(collector._stats.record_logged)

    assert asked, "the stall record never asked about the socket"
    assert collector._stats.stalls[-1].reconnected is True


def test_a_reconnect_only_counts_for_the_window_it_happened_in() -> None:
    """
    Otherwise every stall for the rest of the session blames one reconnect.

    A cause that is always true says nothing, which is the failure mode of every
    attribution that is not bounded by its own window.
    """
    collector = build_collector()
    try:
        collector._last_reconnect_monotonic = time.monotonic()
        window_start = time.monotonic() + 1.0

        assert collector._reconnected_between(window_start) is False, (
            "a reconnect from before the window was counted inside it")
        assert collector._reconnected_between(window_start - 5.0) is True
    finally:
        remove_log_listener(collector._stats.record_logged)


def test_the_worst_collection_records_what_was_live_at_the_time() -> None:
    """
    The measurement that decides between a smaller archive and a smaller buffer.

    A single collection took 882 ms on production while every open file had been
    growing since midnight. Whether the pause TRACKS that number or merely
    happened near it is the whole question - and a running total cannot answer
    it, because it does not say what was live when the worst one hit.
    """
    stats = CollectorStats()
    stats.record_tick("BTCUSD", 1.0, 1.1, 0.1, 1.0)
    stats.symbols["BTCUSD"].current_file_ticks = 31000
    stats.record_tick("ETHUSD", 1.0, 1.1, 0.1, 1.0)
    stats.symbols["ETHUSD"].current_file_ticks = 12000

    stats.record_gc_pause(1, 882.2)

    assert stats.gc.live_ticks_at_max == 43000

    # A smaller pause must not overwrite the reading that belongs to the worst.
    stats.symbols["BTCUSD"].current_file_ticks = 1
    stats.record_gc_pause(1, 5.0)

    assert stats.gc.live_ticks_at_max == 43000, (
        "a later, shorter collection replaced the pairing that mattered")
