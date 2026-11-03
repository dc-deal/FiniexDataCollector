"""
FiniexDataCollector - Tests for the tick counters the operator reads

Two counters exist for one number. The writer counts what goes into the file;
the tick handler in main.py keeps a second one for the display, `/v1/status`,
the weekly report and the Telegram rotation notice. They disagreed by exactly
one tick at every UTC day cut, and the error was carried into the next file -
measured on production: "File rotated: ... (47,369 ticks)" for a file holding
47,368, and "(49,999)" for one holding 50,000.

The cause is a rule that cannot be guessed from outside the writer: the day cut
is checked BEFORE a tick is appended, so the tick that triggers it belongs to
the new file, while the count threshold fires after, so that trigger belongs to
the old one.

Nothing tested the handler before this file, which is why the drift ran for
weeks. The reconciliation below is the second half: it compares the two counters
while the collector runs, so the next such drift is a number on `/v1/status`
rather than something somebody eventually notices in a log.

Location: tests/utils/test_tick_counters.py
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pytest

from python.main import FiniexDataCollector
from python.types.tick_types import TickData
from python.utils.collection_clock import CollectionClock
from python.utils.config_loader import ConfigLoader
from python.utils.logging_setup import remove_log_listener
from python.writers.json_tick_writer import JsonTickWriter

from tests.conftest import build_ticks

SYMBOL = "BTCUSD"
MIDNIGHT = int(datetime(2026, 9, 21, tzinfo=timezone.utc).timestamp() * 1000)


@pytest.fixture
def collector():
    """A collector with its own stats, without any of its tasks running."""
    instance = FiniexDataCollector(ConfigLoader().load(), show_display=False)
    yield instance
    remove_log_listener(instance._stats.record_logged)


def attach_writer(collector: FiniexDataCollector, output_dir: Path,
                  max_ticks_per_file: int = 50000) -> JsonTickWriter:
    """Give the collector a writer that writes its files on the spot."""
    writer = JsonTickWriter(
        output_dir=output_dir,
        symbol=SYMBOL,
        clock=CollectionClock(),
        broker="Kraken",
        server="kraken_websocket",
        broker_type="kraken_spot",
        max_ticks_per_file=max_ticks_per_file,
        data_collector="kraken")
    collector._writers[SYMBOL] = writer
    return writer


def ticks_before_and_after_midnight() -> List[TickData]:
    """Four ticks on one UTC day, then one on the next."""
    return (build_ticks(count=4, start_msc=MIDNIGHT - 5000, interval_ms=1000)
            + build_ticks(count=1, start_msc=MIDNIGHT + 1000))


def written_file(output_dir: Path) -> dict:
    """The single archive file on disk."""
    files = sorted((output_dir / "kraken").glob("*_ticks.json"))
    assert len(files) == 1, f"expected one file, found {len(files)}"
    return json.loads(files[0].read_text(encoding="utf-8"))


def test_the_day_cut_reports_the_ticks_the_file_actually_holds(
    collector: FiniexDataCollector,
    tmp_path: Path
) -> None:
    """
    The tick that causes the cut belongs to the new file, and is reported there.

    Counted the other way, the closed file is reported one tick too large and
    the new one starts one too small - which is what production logged at every
    day cut, and what then turned into "49,999" at the next rotation.
    """
    attach_writer(collector, tmp_path)

    for tick in ticks_before_and_after_midnight():
        collector._on_tick_received(tick)

    document = written_file(tmp_path)

    assert document["summary"]["total_ticks"] == 4
    assert collector._stats.last_file.tick_count == 4, (
        "the closed file was reported with a count it does not have")
    assert collector._stats.symbols[SYMBOL].current_file_ticks == 1, (
        "the new file already holds the tick that caused the cut")


def test_a_full_file_reports_the_tick_that_filled_it(
    collector: FiniexDataCollector,
    tmp_path: Path
) -> None:
    """
    The other rule, opposite to the day cut: the count threshold fires after the
    tick was appended, so the trigger is in the file that just closed and the
    new one starts empty.
    """
    attach_writer(collector, tmp_path, max_ticks_per_file=5)

    for tick in build_ticks(count=5, start_msc=MIDNIGHT - 10000):
        collector._on_tick_received(tick)

    document = written_file(tmp_path)

    assert document["summary"]["total_ticks"] == 5
    assert collector._stats.last_file.tick_count == 5
    assert collector._stats.symbols[SYMBOL].current_file_ticks == 0


class FakeClock:
    """A session clock that has absorbed something."""
    resyncs = 3
    max_correction_ms = 17


def test_a_screen_elsewhere_can_read_what_the_clock_absorbed(
    collector: FiniexDataCollector,
    tmp_path: Path
) -> None:
    """
    The display read these off the live clock object, which only a program
    inside this process can do.

    The clock moves when a tick is stamped, so a tick is when the copy has to
    happen - a viewer showing zero corrections while the file headers count
    three would be the same defect this project refuses in its files.
    """
    attach_writer(collector, tmp_path)
    collector._clock = FakeClock()

    collector._on_tick_received(build_ticks(count=1, start_msc=MIDNIGHT)[0])

    assert collector._stats.clock.resyncs == 3
    assert collector._stats.clock.max_correction_ms == 17


def test_a_symbol_carries_its_decimal_places(
    collector: FiniexDataCollector
) -> None:
    """
    Without it a viewer prints two places for everything.

    That is not cosmetic: ADAUSD trades at 0.2103 against 0.2104, and two places
    render both as 0.21 - a screen stating a spread of zero where the book has
    one.
    """
    collector._prepare_symbol_stats(SYMBOL)

    assert collector._stats.symbols[SYMBOL].digits == 1, (
        "BTCUSD carries one decimal place in the test broker specification")


def test_an_unknown_symbol_says_it_does_not_know(
    collector: FiniexDataCollector
) -> None:
    """None, not 2. An invented precision is a claim nobody measured."""
    collector._prepare_symbol_stats("NOSUCHPAIR")

    assert collector._stats.symbols["NOSUCHPAIR"].digits is None


def test_the_comparison_actually_runs_on_its_own(
    collector: FiniexDataCollector,
    tmp_path: Path
) -> None:
    """
    A check nobody calls observes nothing.

    The comparison rides the folder monitor, which is a background task - so the
    test starts that task rather than calling the check, because the failure
    being guarded against is precisely that the call site disappears.
    """
    called: List[bool] = []
    collector._reconcile_tick_counters = lambda: called.append(True)
    collector._is_running = True

    async def run_one_round() -> None:
        task = asyncio.create_task(collector._monitor_folders())
        await asyncio.sleep(0.2)
        collector._is_running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run_one_round())

    assert called, "the folder monitor no longer compares the counters"


def test_the_counters_are_compared_while_the_collector_runs(
    collector: FiniexDataCollector,
    tmp_path: Path
) -> None:
    """
    The drift ran for weeks because nothing ever held the two numbers together.

    A clean comparison counts and says nothing; a disagreement is counted,
    named, and resolved in favour of the writer - what the writer holds is what
    the file will say.
    """
    attach_writer(collector, tmp_path)
    for tick in build_ticks(count=3, start_msc=MIDNIGHT - 10000):
        collector._on_tick_received(tick)

    collector._reconcile_tick_counters()

    assert collector._stats.counter_check.checks == 1
    assert collector._stats.counter_check.mismatches == 0

    collector._stats.symbols[SYMBOL].current_file_ticks = 99
    collector._reconcile_tick_counters()

    check = collector._stats.counter_check
    assert check.mismatches == 1
    assert check.last_mismatch_symbol == SYMBOL
    assert check.last_mismatch_counted == 99
    assert check.last_mismatch_written == 3
    assert check.last_mismatch_at is not None
    assert collector._stats.symbols[SYMBOL].current_file_ticks == 3
