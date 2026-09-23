"""
FiniexDataCollector - What a remote screen knows about its own feed
The state a viewer has to show about itself, separate from what it shows about
the collector.

A viewer draws numbers it did not measure. The moment the collector stops
answering, every one of them becomes a statement about the past while looking
exactly as current as it did a second earlier - which is the defect this project
refuses in its output files, moved onto a screen.

So the viewer carries its own state beside the collector's, and shows it: when the
last reading came in, why the last attempt failed, and whether the two machines
even agree about the time. `age_seconds` is what makes a frozen screen readable as
frozen.

Location: python/types/feed_state.py
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# Beyond this the two machines disagree enough that every duration on the screen
# is wrong by a visible amount - uptime most of all, which is read off the
# collector's start time against the viewer's clock. Below it, NTP jitter.
SKEW_TOLERANCE_SECONDS = 5.0


@dataclass
class FeedState:
    """
    What the viewer knows about its own connection to the collector.

    Attributes:
        source: The base URL being polled, shown so two viewers are told apart
        interval_seconds: How often a reading is attempted
        connected: Whether the last attempt succeeded
        last_success: When the last reading arrived, by the viewer's clock
        last_error: Why the last attempt failed, in the words the viewer got
        readings: Successful readings this session
        failures: Consecutive failed attempts, reset by any success
        skew_seconds: Viewer clock minus collector clock, None until measured
        started_at: When this viewer began, so the wait for the first answer can
            be shown as a duration rather than as a silence
    """
    source: str
    interval_seconds: float
    connected: bool = False
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    readings: int = 0
    failures: int = 0
    skew_seconds: Optional[float] = None
    started_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))

    def record_reading(self, skew_seconds: Optional[float] = None) -> None:
        """
        Note that a reading arrived.

        Args:
            skew_seconds: Difference between the viewer's clock and the
                collector's, when it could be computed
        """
        self.connected = True
        self.last_success = datetime.now(timezone.utc)
        self.last_error = None
        self.readings += 1
        self.failures = 0
        self.skew_seconds = skew_seconds

    def record_failure(self, reason: str) -> None:
        """
        Note that an attempt failed, and why.

        The reason is kept rather than counted: "connection refused" and "token
        rejected" send the operator to different machines.

        Args:
            reason: What went wrong, already turned into text
        """
        self.connected = False
        self.last_error = reason
        self.failures += 1

    @property
    def age_seconds(self) -> Optional[float]:
        """
        How old the numbers on the screen are.

        Returns:
            Seconds since the last reading, or None when none has arrived
        """
        if self.last_success is None:
            return None
        return (datetime.now(timezone.utc) - self.last_success).total_seconds()

    @property
    def awaiting_first_answer(self) -> bool:
        """
        Whether the viewer has started and nothing has come back yet.

        Starting up and having lost contact look identical in `connected`, and
        they are not the same thing: one resolves itself in a second, the other
        wants somebody to look. Saying the second while the first is true is the
        screen asserting what it does not know - the defect this project refuses
        in its files. An attempt that FAILED is not this state: a failure is an
        answer about the collector, and it belongs on a red frame.

        Returns:
            True while no attempt has completed, either way
        """
        return self.readings == 0 and self.failures == 0

    @property
    def waiting_seconds(self) -> float:
        """
        How long the first answer has been outstanding.

        A refused connection reports in about two seconds, but a host that
        swallows the packets takes the whole request timeout - and without a
        number moving, a viewer waiting on one is indistinguishable from a
        viewer that has hung.

        Returns:
            Seconds since this viewer started
        """
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()

    @property
    def clock_disagrees(self) -> bool:
        """
        Whether the skew is large enough to make displayed durations wrong.

        Returns:
            True when the two clocks are further apart than NTP jitter explains
        """
        return (self.skew_seconds is not None
                and abs(self.skew_seconds) > SKEW_TOLERANCE_SECONDS)
