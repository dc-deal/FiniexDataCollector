"""
FiniexDataCollector - Collection Clock
Monotonic UTC millisecond source for collected_msc.

The OS clock is read per tick, so it can neither drift nor overflow. Its one
weak spot is a backwards step from an NTP correction, which happens after
standby, in a migrated VM, or whenever the system time source finds a large
offset. Such a step would put a smaller collected_msc behind a larger one, and
the import pipeline rejects any file whose arrival times step backwards - the
whole file, up to 50,000 ticks, and irreversibly, because the importer never
repairs.

time.monotonic() cannot be used instead: it has no epoch, so its values cannot
be compared against the event time in time_msc. collected_msc has to be both
epoch-based and non-decreasing, which is exactly what this class produces.

Location: python/utils/collection_clock.py
"""

import time

from python.utils.logging_setup import get_collector_logger


class CollectionClock:
    """
    Hands out non-decreasing UTC timestamps and reports its own corrections.

    One instance serves the whole collection session. A globally non-decreasing
    series is non-decreasing in every subsequence, so a single clock keeps every
    symbol's file monotonic - and a clock step is counted once, not once per
    symbol, which is what makes the counters comparable to the MT5 collector's.

    Clamping alone would hide the broken clock it works around, so every
    correction is counted and logged.
    """

    def __init__(self) -> None:
        """Initialize clock with no history and no corrections."""
        self._last_msc = 0
        self._resyncs = 0
        self._max_correction_ms = 0
        self._logger = get_collector_logger("clock")

    @property
    def resyncs(self) -> int:
        """Number of ticks whose timestamp had to be clamped, cumulative."""
        return self._resyncs

    @property
    def max_correction_ms(self) -> int:
        """Largest backwards step absorbed so far, in milliseconds."""
        return self._max_correction_ms

    def next_msc(self) -> int:
        """
        Get the next collected_msc value.

        Reads the OS clock and clamps it to the previous value when it came back
        smaller. A clamped tick carries a stamp that is slightly too old - the
        alternative is losing the file it sits in.

        Returns:
            Epoch milliseconds UTC, never smaller than the previous return value
        """
        now_msc = int(time.time() * 1000)

        if now_msc < self._last_msc:
            correction_ms = self._last_msc - now_msc
            self._resyncs += 1
            self._max_correction_ms = max(
                self._max_correction_ms, correction_ms)

            self._logger.warning(
                f"System clock stepped back {correction_ms} ms - "
                f"collected_msc clamped to last value (resync #{self._resyncs})"
            )
            now_msc = self._last_msc

        self._last_msc = now_msc
        return now_msc
