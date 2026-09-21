"""
FiniexDataCollector - Tests for what a remote screen shows about itself

A viewer draws numbers it did not measure. The instant the collector stops
answering, all of them describe the past while looking exactly as current as they
did a second earlier - which is the defect this project refuses in its output
files, moved onto a screen. Issue #15 states the rule: the viewer must never
leave the last numbers standing as if they were current.

Two more guards here come from the first hand run against production, where the
screen was wrong in two ways that a suite of the time would not have noticed:

- It reported `2,406 / 1,000 (241%)` for a file on a collector configured for
  50,000, because the renderer read `max_ticks_per_file` out of the LOCAL config
  file on every frame. In the collector that was a disk read per symbol per
  second; in a viewer it is the wrong machine's configuration entirely.
- It printed ADAUSD as `0.24 / 0.24`, a stated spread of zero where the book had
  one, because an unknown precision fell back to two decimal places.

Location: tests/viewer/test_viewer_screen.py
"""

import asyncio
import io
from datetime import datetime, timedelta, timezone

from rich.console import Console

from python.types.collector_stats import CollectorStats
from python.types.feed_state import FeedState
from python.utils.live_display import LiveDisplay

SYMBOL = "ADAUSD"


def a_feed(source: str = "https://collector.example") -> FeedState:
    """A feed state that has never read anything."""
    return FeedState(source=source, interval_seconds=2.0)


def stats_with_a_symbol() -> CollectorStats:
    """A collector that has seen one tick of a four-decimal instrument."""
    stats = CollectorStats()
    stats.record_tick(SYMBOL, 0.2103, 0.2104, 0.0475, 12.5, quote_age_ms=7)
    stats.streams = ["ticker"]
    return stats


def drawn(display: LiveDisplay) -> str:
    """Render one frame into text."""
    console = Console(file=io.StringIO(), width=200, legacy_windows=False)
    console.print(display._render())
    return console.file.getvalue()


def test_a_screen_inside_the_collector_says_nothing_about_a_feed() -> None:
    """
    No feed means no wire, and a banner about one would be noise.

    This is also what keeps the change safe for the collector's own display: with
    `feed=None` the renderer behaves exactly as it did.
    """
    screen = drawn(LiveDisplay(stats_with_a_symbol()))

    assert "FiniexDataCollector Live" in screen
    assert "no answer" not in screen


def test_a_silent_collector_is_unmistakable_and_dated() -> None:
    """
    The frame changes, not one line inside it, and it carries a clock time.

    "Something is wrong" is not actionable; "nothing since 11:56:12, that is two
    minutes" tells the operator whether to look at the service or at the network.
    """
    feed = a_feed()
    feed.record_reading()
    feed.last_success = datetime.now(timezone.utc) - timedelta(seconds=137)
    feed.record_failure("unreachable: connection refused")

    screen = drawn(LiveDisplay(stats_with_a_symbol(), feed=feed))

    assert "no answer since" in screen
    assert feed.last_success.strftime("%H:%M:%S") in screen
    assert "2m 17s ago" in screen
    assert "not current" in screen
    assert "connection refused" in screen


def test_a_collector_that_never_answered_does_not_claim_an_outage_time() -> None:
    """
    Nothing has been read, so there is no "since" to state.

    The first seconds after the viewer starts are exactly this state, and a
    fabricated timestamp there would be the same invention the output contract
    forbids.
    """
    screen = drawn(LiveDisplay(CollectorStats(), feed=a_feed()))

    assert "never answered" in screen
    assert "no answer since" not in screen


def test_a_connected_screen_names_the_machine_it_is_drawing() -> None:
    """Two viewers open side by side are otherwise identical."""
    feed = a_feed("https://collector.finiex.example")
    feed.record_reading(skew_seconds=0.3)

    screen = drawn(LiveDisplay(stats_with_a_symbol(), feed=feed))

    assert "collector.finiex.example" in screen
    assert "every 2s" in screen
    assert "clock skew" not in screen


def test_two_machines_that_disagree_about_the_time_say_so() -> None:
    """
    Every duration on this screen is computed against the viewer's clock.

    A viewer running minutes off prints an uptime that never happened, and the
    only way anyone notices is if the two are compared and the result shown.
    """
    feed = a_feed()
    feed.record_reading(skew_seconds=-412.0)

    screen = drawn(LiveDisplay(stats_with_a_symbol(), feed=feed))

    assert "clock skew" in screen
    assert "-412s" in screen


def test_an_unknown_precision_prints_the_price_that_was_measured() -> None:
    """
    Two decimals rendered 0.2103 and 0.2104 as 0.21 and 0.21.

    A screen stating bid == ask where the book has a spread is a claim nobody
    measured - so an unstated precision shows the value as it arrived instead of
    rounding to a number the collector never sent.
    """
    stats = stats_with_a_symbol()
    assert stats.symbols[SYMBOL].digits is None, "precondition: nothing stated"

    screen = drawn(LiveDisplay(stats))

    assert "0.2103" in screen and "0.2104" in screen


def test_a_stated_precision_is_honoured() -> None:
    """The other half: when the collector says four places, four are drawn."""
    stats = stats_with_a_symbol()
    stats.symbols[SYMBOL].digits = 4

    screen = drawn(LiveDisplay(stats))

    assert "0.2103" in screen and "0.2104" in screen


def test_file_progress_is_measured_against_the_collectors_own_limit() -> None:
    """
    The limit belongs to the instance being watched, not to this machine.

    Read from a local config file it produced "2,406 / 1,000 (241%)" for a
    production file - a screen reporting a file 12 times over a boundary that
    instance does not have.
    """
    stats = stats_with_a_symbol()
    stats.symbols[SYMBOL].current_file_ticks = 2406
    stats.max_ticks_per_file = 50000

    screen = drawn(LiveDisplay(stats))

    assert "2,406 / 50,000 (5%)" in screen


def test_a_collector_that_states_no_limit_gets_no_denominator() -> None:
    """
    The count is a fact; a denominator would be invented.

    This is the build on the box right now, which predates the field - and it has
    to draw, rather than fall back on a number from somewhere else.
    """
    stats = stats_with_a_symbol()
    stats.symbols[SYMBOL].current_file_ticks = 2406

    screen = drawn(LiveDisplay(stats))

    assert "2,406" in screen
    assert "241%" not in screen and "/ 1,000" not in screen


def run_the_display(display: LiveDisplay) -> None:
    """
    Drive the real update loop for a few frames, off the terminal.

    The guard being tested lives in that loop and not in `_render`, so calling
    the renderer directly would pass against a broken implementation - which is
    the shape of test this project has been caught by before.
    """
    display._console = Console(file=io.StringIO(), width=200,
                               legacy_windows=False)
    display._update_interval = 0.01

    async def a_few_frames() -> None:
        await display.start()
        await asyncio.sleep(0.05)
        await display.stop()

    asyncio.run(a_few_frames())


def test_the_viewer_does_not_put_its_own_render_cost_on_the_collector() -> None:
    """
    `scans.render_ms` is the measurement that identified the stall on 2026-09-21.

    A viewer writing its own frame time into statistics that describe the
    collector would put a number from this machine under that machine's name -
    and that number is precisely the one a stall attribution is built from.
    """
    stats = stats_with_a_symbol()
    reported = 3858.0
    stats.record_render(reported)

    run_the_display(LiveDisplay(stats, feed=a_feed()))

    assert stats.scans.render_last_ms == reported, (
        "the viewer overwrote the collector's own render measurement")


def test_the_collectors_own_display_still_times_itself() -> None:
    """
    The other half, and the reason the check is on the feed rather than removed.

    In-process that measurement is the whole point: it is what named the live
    display as the cause of a 3.9 s stall instead of the folder scan.
    """
    stats = stats_with_a_symbol()

    run_the_display(LiveDisplay(stats))

    assert stats.scans.render_last_ms > 0, (
        "the in-process display stopped measuring its own frames")
