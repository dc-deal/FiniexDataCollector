"""
FiniexDataCollector - Collection Clock Tests

The clock exists for one rare event: the OS clock stepping backwards. Waiting
for a real NTP correction is not a test strategy, so every case here drives the
time source by hand through the steerable_clock fixture.

Location: tests/utils/test_collection_clock.py
"""

from datetime import datetime, timezone
from typing import Callable, Tuple

from python.utils.collection_clock import CollectionClock

# The correction the scenarios provoke, in milliseconds.
STEP_BACK_MS = 300


def test_passes_the_os_clock_through_while_it_behaves(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """A well-behaved clock must not be altered in any way."""
    clock, set_os_clock = steerable_clock

    readings = []
    for offset in (0, 100, 250, 900):
        set_os_clock(1_772_874_222_000 + offset)
        readings.append(clock.next_msc())

    assert readings == [
        1_772_874_222_000,
        1_772_874_222_100,
        1_772_874_222_250,
        1_772_874_222_900,
    ]


def test_clamps_a_backwards_step_to_the_last_value(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    The core case: two ticks arrive in order, the clock is corrected between
    them, and the second would otherwise carry the smaller stamp.

    Arrival order is never touched - only the numbers are held in place.
    """
    clock, set_os_clock = steerable_clock

    set_os_clock(1_772_874_222_300)
    first = clock.next_msc()

    set_os_clock(1_772_874_222_400)
    second = clock.next_msc()

    # NTP finds the clock 300 ms fast and steps it back.
    set_os_clock(1_772_874_222_400 - STEP_BACK_MS)
    third = clock.next_msc()

    assert [first, second, third] == [
        1_772_874_222_300,
        1_772_874_222_400,
        1_772_874_222_400,
    ]


def test_stays_non_decreasing_across_the_whole_correction_window(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    Every tick arriving before the clock catches up again is clamped, not just
    the first one after the step.
    """
    clock, set_os_clock = steerable_clock

    set_os_clock(1_772_874_222_400)

    # The reading before the step belongs in the series - it is the one every
    # later reading has to stay at or above.
    readings = [clock.next_msc()]
    for offset in (0, 100, 200, 400):
        set_os_clock(1_772_874_222_400 - STEP_BACK_MS + offset)
        readings.append(clock.next_msc())

    assert readings == sorted(readings)
    assert readings[-1] == 1_772_874_222_500  # clock overtook the clamp


def test_counts_every_clamped_stamp(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """
    The counter measures affected ticks, not clock jumps.

    One correction of 300 ms silently rewrites every stamp issued during those
    300 ms, and how many ticks that hit is the interesting number.
    """
    clock, set_os_clock = steerable_clock

    set_os_clock(1_772_874_222_400)
    clock.next_msc()

    for offset in (0, 100, 200):
        set_os_clock(1_772_874_222_400 - STEP_BACK_MS + offset)
        clock.next_msc()

    assert clock.resyncs == 3


def test_remembers_the_largest_correction(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """The worst step survives a later, smaller one."""
    clock, set_os_clock = steerable_clock

    set_os_clock(1_772_874_223_000)
    clock.next_msc()

    set_os_clock(1_772_874_222_500)   # 500 ms back
    clock.next_msc()

    set_os_clock(1_772_874_222_900)   # 100 ms back from the clamped value
    clock.next_msc()

    assert clock.max_correction_ms == 500


def test_reports_nothing_when_the_clock_never_stepped_back(
    steerable_clock: Tuple[CollectionClock, Callable[[int], None]]
) -> None:
    """Both counters stay at zero, which is what every healthy file declares."""
    clock, set_os_clock = steerable_clock

    for offset in (0, 250, 500):
        set_os_clock(1_772_874_222_000 + offset)
        clock.next_msc()

    assert clock.resyncs == 0
    assert clock.max_correction_ms == 0


def test_hands_out_epoch_milliseconds() -> None:
    """
    Not a monotonic counter.

    time.monotonic() would also never step backwards, but it has no epoch and
    could not be compared against the event time in time_msc. This test pins
    the property that rules it out as a substitute.
    """
    value = CollectionClock().next_msc()
    stamped = datetime.fromtimestamp(value / 1000, tz=timezone.utc)

    assert abs((datetime.now(timezone.utc) - stamped).total_seconds()) < 60
