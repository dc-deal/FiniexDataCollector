"""
FiniexDataCollector - Garbage collection, timed

The collector holds every tick of an open file in memory, which on a busy symbol
is tens of thousands of objects, and a generation-2 collection walks all of them.
That work happens wherever the interpreter happens to be - including in the
middle of the event loop, between two ticks - and nothing in Python reports it.

This is why it exists: on 2026-09-21 the loop stalled up to 4.2 s about four
times a minute on the production box, and the first attribution was wrong. The
folder scan was blamed and then measured at 15 ms. A stall has to name its own
cause, or the next one costs another day of guessing.

**Not in this module:** what to do about a pause (nothing here decides that) and
the loop lag itself, which `main.py` measures.

Location: python/utils/gc_watcher.py
"""

import gc
import time
from collections import deque
from typing import Any, Deque, Optional, Tuple


class GcWatcher:
    """
    Times every garbage collection and keeps the recent ones.

    A callback receives the start and the end of each collection, so the pause
    is measured rather than inferred. The recent pauses are kept with monotonic
    stamps so that a stall can ask "how much of me was garbage collection?"
    without comparing two wall-clock readings.
    """

    def __init__(self, stats: Any, keep: int = 64):
        """
        Initialize the watcher.

        Args:
            stats: CollectorStats to report pauses to
            keep: How many recent pauses to hold for attribution
        """
        self._stats = stats
        self._started_at: Optional[float] = None
        # (started, ended, generation, milliseconds)
        self._recent: Deque[Tuple[float, float,
                                  int, float]] = deque(maxlen=keep)
        self._installed = False

    def start(self) -> None:
        """Begin timing collections."""
        if not self._installed:
            gc.callbacks.append(self._on_collection)
            self._installed = True

    def stop(self) -> None:
        """Stop timing collections."""
        if self._installed:
            try:
                gc.callbacks.remove(self._on_collection)
            except ValueError:
                pass
            self._installed = False

    def _on_collection(self, phase: str, info: dict) -> None:
        """
        Record one collection, called by the interpreter itself.

        Args:
            phase: "start" or "stop"
            info: Carries the generation being collected
        """
        if phase == "start":
            self._started_at = time.monotonic()
            return

        if self._started_at is None:
            # Started before this watcher did; there is nothing to measure.
            return

        ended = time.monotonic()
        generation = int(info.get("generation", -1))
        duration_ms = (ended - self._started_at) * 1000

        self._recent.append((self._started_at, ended, generation, duration_ms))
        self._stats.record_gc_pause(generation, duration_ms)
        self._started_at = None

    def time_spent_since(self, since: float) -> Tuple[float, int]:
        """
        How much garbage collection happened in a window.

        Args:
            since: Monotonic timestamp the window starts at

        Returns:
            (milliseconds spent collecting, the highest generation involved);
            (0.0, -1) when nothing was collected in that window
        """
        total = 0.0
        worst = -1

        for started, ended, generation, duration_ms in self._recent:
            if ended <= since:
                continue
            total += duration_ms
            worst = max(worst, generation)

        return total, worst
