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

from dataclasses import dataclass
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
    """
    source: str
    interval_seconds: float
    connected: bool = False
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    readings: int = 0
    failures: int = 0
    skew_seconds: Optional[float] = None

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
    def clock_disagrees(self) -> bool:
        """
        Whether the skew is large enough to make displayed durations wrong.

        Returns:
            True when the two clocks are further apart than NTP jitter explains
        """
        return (self.skew_seconds is not None
                and abs(self.skew_seconds) > SKEW_TOLERANCE_SECONDS)
