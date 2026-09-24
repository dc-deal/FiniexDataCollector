"""
FiniexDataCollector - Where the event loop was while it was not running
A sampling watchdog that observes a stall instead of reasoning about it.

Every attribution arm before this one names a suspect and times it: garbage
collection, the display, the tick handler, a thread. That works and it does not
scale - each arm answers for one candidate, and a stall caused by anything
nobody instrumented still reports `unknown`. Ten did on 2026-09-23 and ten more
on 2026-09-24, across two builds, with every existing arm reading zero.

This asks the loop directly. The lag watcher already wakes ten times a second,
so it can leave a mark each time; a plain thread then notices when the mark stops
moving and reads the loop thread's stack while it is still stuck. What comes back
is the file, line and function the loop was actually in - not a candidate that
happened to be measured.

Cost: one thread waking twenty-five times a second to compare two floats. A stack
is formatted only during a stall, and never more than once per stall.

What it is NOT: a profiler. One sample per stall says where the loop was at one
moment inside it, which is evidence and not proof - a long operation made of many
short ones can be sampled anywhere. Read it as the strongest available hint,
below anything that was actually timed, which is where the cause chain puts it.

Location: python/utils/stall_sampler.py
"""

import sys
import threading
import time
from pathlib import Path
from typing import Optional

from python.utils.logging_setup import describe_exception

# How long the loop has to be missing before its stack is worth reading. Below
# the stall threshold on purpose: a stall is only recorded once the loop comes
# back, and by then there is nothing left to look at.
CAPTURE_AFTER_SECONDS = 0.15

# The watchdog's own tick. Fast enough to catch the shortest stall that gets
# recorded, slow enough that the thread costs nothing measurable.
SAMPLE_INTERVAL_SECONDS = 0.04

# Innermost frames kept. Four reaches out of the library that is blocking and
# into the call in this project that led there, which is the pair that makes a
# sample actionable.
FRAMES_KEPT = 4


class StallSampler:
    """
    Reads the event loop's stack while the loop is too busy to report.

    Attributes:
        captures: How many stalls have been sampled this session
    """

    def __init__(self,
                 capture_after_seconds: float = CAPTURE_AFTER_SECONDS,
                 sample_interval_seconds: float = SAMPLE_INTERVAL_SECONDS):
        """
        Args:
            capture_after_seconds: Silence from the loop before a stack is read
            sample_interval_seconds: How often the watchdog checks
        """
        self._capture_after = capture_after_seconds
        self._interval = sample_interval_seconds

        self._loop_thread_id: Optional[int] = None
        self._alive_at = time.monotonic()

        # Written by the watchdog thread, read by the loop. Both are single
        # assignments of an immutable value, which the interpreter lock makes
        # atomic - no lock is needed and one would be held during a stall.
        self._stack = ""
        self._stack_at = 0.0

        self._sampling = False
        self._thread: Optional[threading.Thread] = None
        self.captures = 0

    def start(self) -> None:
        """
        Begin watching, and remember the calling thread as the loop's.

        Call this FROM the thread that runs the event loop. The sampler has no
        other way to know which stack to read, and reading the wrong one would
        produce a confident answer about an idle thread.
        """
        self._loop_thread_id = threading.get_ident()
        self._alive_at = time.monotonic()
        self._sampling = True
        self._thread = threading.Thread(
            target=self._watch, name="stall-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching. The thread is a daemon, so this is tidiness."""
        self._sampling = False

    def note_alive(self) -> None:
        """
        Mark the loop as having run just now.

        Called from the lag watcher's own cycle, which is already the thing that
        proves the loop is turning.
        """
        self._alive_at = time.monotonic()

    def blocked_in(self, since: float) -> str:
        """
        The stack read inside a window, if one was read.

        Args:
            since: Monotonic reading the window started at

        Returns:
            Innermost frames as text, or an empty string when the loop was never
            caught standing still inside that window
        """
        return self._stack if self._stack_at >= since else ""

    def _watch(self) -> None:
        """Compare the loop's last mark against the clock, forever."""
        while self._sampling:
            time.sleep(self._interval)

            silent_for = time.monotonic() - self._alive_at
            if silent_for < self._capture_after:
                continue

            # One sample per stall: the loop is still gone, and re-reading the
            # same stack every 40 ms would cost more than the stall does.
            if self._stack_at > self._alive_at:
                continue

            stack = self._read_loop_stack()
            if stack:
                self._stack = stack
                self._stack_at = time.monotonic()
                self.captures += 1

    def _read_loop_stack(self) -> str:
        """
        Format the innermost frames of the loop's thread.

        Returns:
            `file:line function < caller < ...`, or an empty string when the
            stack could not be read
        """
        if self._loop_thread_id is None:
            return ""

        try:
            frame = sys._current_frames().get(self._loop_thread_id)
            names = []
            while frame is not None and len(names) < FRAMES_KEPT:
                code = frame.f_code
                names.append(f"{Path(code.co_filename).name}:"
                             f"{frame.f_lineno} {code.co_name}")
                frame = frame.f_back
            return " < ".join(names)
        except Exception as error:
            # A diagnostic must never be the thing that takes the collector
            # down, and this one walks a stack another thread is mutating.
            return f"stack unreadable: {describe_exception(error)}"
