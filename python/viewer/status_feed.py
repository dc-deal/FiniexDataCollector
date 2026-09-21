"""
FiniexDataCollector - Polling the collector for a screen
One HTTP reading of `/v1/status`, repeated, with every way it can fail named.

The collector is assumed to be down more often than a viewer is open - a reboot,
a service stop, an RDP session that ended. So an unreachable collector is a state
this program displays, never an error it exits on.

Every failure is turned into words the operator can act on before it reaches the
screen, because the four common ones send them to four different places: a refused
connection means the service is not running, a 401 means the token is wrong, a 403
means the token is right and lacks `status:detail`, and a payload mismatch means
this viewer is older or newer than that collector.

Read-only by construction: this module issues GETs and holds no session state the
collector could be affected by.

Location: python/viewer/status_feed.py
"""

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import aiohttp

from python.types.collector_stats import CollectorStats
from python.types.feed_state import FeedState
from python.utils.logging_setup import describe_exception
from python.viewer.stats_from_payload import (PayloadMismatch,
                                              stats_from_payload)

STATUS_PATH = "/v1/status"

# A reading that takes longer than this is not worth waiting for: the next one is
# already due, and a screen showing the age of the last answer is more honest than
# one that blocks hoping for a late one.
#
# The floor is not a round number picked for comfort. Measured on this Windows
# box on 2026-09-21: a REFUSED connection to a closed loopback port takes 2.04 s
# to report itself, because the stack retries the SYN before giving up. At a
# two-second timeout the refusal lost that race by forty milliseconds and the
# screen said "no answer within the timeout" for a service that was simply not
# running - the wrong sentence, sending the operator to the network instead of to
# the service. The age of the last reading is on screen the whole time either way,
# so waiting three more seconds for the right diagnosis costs nothing.
MIN_TIMEOUT_SECONDS = 5.0


class StatusFeed:
    """
    Repeatedly reads `/v1/status` and hands the result to a renderer.
    """

    def __init__(self, base_url: str, token: str, interval_seconds: float):
        """
        Initialize the feed.

        Args:
            base_url: Collector base URL, with or without a trailing slash
            token: Bearer token carrying `status:detail`
            interval_seconds: Seconds between readings
        """
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._interval = interval_seconds
        self.state = FeedState(source=self._base_url,
                               interval_seconds=interval_seconds)

    @property
    def url(self) -> str:
        """The full URL this feed reads."""
        return f"{self._base_url}{STATUS_PATH}"

    async def run(self, on_reading: Callable[[CollectorStats], None]) -> None:
        """
        Poll until cancelled, handing every successful reading on.

        A failure updates the feed state and nothing else: the previous statistics
        stay in place so the screen can keep showing them, marked with their age.
        Replacing them with an empty object would hide what the collector last
        said, which is usually the interesting part of an outage.

        Args:
            on_reading: Called with the statistics of each successful reading
        """
        timeout = aiohttp.ClientTimeout(
            total=max(MIN_TIMEOUT_SECONDS, self._interval))
        headers = {"Authorization": f"Bearer {self._token}"}

        async with aiohttp.ClientSession(timeout=timeout,
                                         headers=headers) as session:
            while True:
                try:
                    stats = await self._read_once(session)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.state.record_failure(_describe(error))
                else:
                    on_reading(stats)

                await asyncio.sleep(self._interval)

    async def _read_once(self, session: aiohttp.ClientSession) -> CollectorStats:
        """
        Take one reading and record what it cost.

        Args:
            session: The HTTP session to use

        Returns:
            The collector's statistics, rebuilt

        Raises:
            FeedError: The collector answered in a way that cannot be drawn
            Exception: Anything the transport raises, described by the caller
        """
        async with session.get(self.url) as response:
            if response.status != 200:
                raise FeedError(_describe_status(response.status))
            try:
                payload: Dict[str, Any] = await response.json()
            except Exception as error:
                raise FeedError(
                    f"unreadable answer: {describe_exception(error)}") from error

        stats = stats_from_payload(payload)
        self.state.record_reading(_skew_seconds(payload, stats))
        return stats


class FeedError(RuntimeError):
    """The collector answered, and the answer cannot be drawn."""


def _skew_seconds(payload: Dict[str, Any],
                  stats: CollectorStats) -> Optional[float]:
    """
    How far the viewer's clock sits from the collector's.

    The collector computes its own uptime before sending; the viewer computes the
    same figure from the reported start time against its own clock. The difference
    is the skew, and it matters because every duration on the screen is derived the
    second way. A viewer whose clock runs ten minutes fast reports ten minutes of
    uptime that never happened.

    Args:
        payload: The raw status payload, which carries the collector's own uptime
        stats: The rebuilt statistics, carrying the start time

    Returns:
        Viewer clock minus collector clock in seconds, or None when the collector
        did not state its uptime
    """
    reported = payload.get("uptime_seconds")
    if not isinstance(reported, (int, float)):
        return None

    here = (datetime.now(timezone.utc) - stats.start_time).total_seconds()
    return here - float(reported)


def _describe_status(status: int) -> str:
    """
    Turn an HTTP status into the sentence that names the next step.

    Args:
        status: The status code the collector answered with

    Returns:
        Text for the screen
    """
    if status == 401:
        return "401 - the token was not accepted"
    if status == 403:
        return "403 - the token is valid and lacks the status:detail grant"
    if status == 404:
        return f"404 - no {STATUS_PATH} there; is that a collector?"
    return f"HTTP {status}"


def _describe(error: Exception) -> str:
    """
    Turn an exception into something the screen can carry.

    `describe_exception` renders an exception carrying no message as its class
    name, which is right for a log file and useless on a screen: "TimeoutError"
    does not tell the operator whether to restart a service or check a token. The
    two failures that arrive empty-handed - a timeout and a refused connection -
    therefore get a sentence here, and everything else falls through to the
    project's own convention.

    Args:
        error: What went wrong

    Returns:
        A short, human-readable reason
    """
    if isinstance(error, FeedError):
        return str(error)
    if isinstance(error, (asyncio.TimeoutError, aiohttp.ServerTimeoutError)):
        return "no answer within the timeout"
    if isinstance(error, aiohttp.ClientConnectorError):
        return f"unreachable: {describe_exception(error)}"
    if isinstance(error, PayloadMismatch):
        return f"payload this viewer cannot read: {error}"

    return describe_exception(error)
