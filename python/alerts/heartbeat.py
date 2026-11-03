"""
FiniexDataCollector - Heartbeat to an outside dead man's switch
Tells a service outside this machine, once per interval, that the collector is
healthy.

Every alarm this process raises dies with it: on 2026-10-18 and 10-20 the host
went down twice and nothing here could say so. An outside check inverts that -
it alarms when the heartbeat STOPS, so a dead process, a frozen loop and a dead
machine all reach the operator's phone without any of them having to speak.

Which makes withholding the signal the whole mechanism. A heartbeat goes out
only while `/v1/health` says `ok`; anything else stays silent and the check
fires. That is why this asks the same provider the route serves, rather than
judging health a second time.

The address is a credential: whoever holds it can report this collector healthy.
It is never logged, and `/v1/configs` masks it by its key name.

Location: python/alerts/heartbeat.py
"""

import asyncio
import ssl
from typing import Any, Callable, Dict
from urllib.parse import urlsplit

import aiohttp
import certifi

from python.types.collector_stats import CollectorStats
from python.utils.logging_setup import describe_exception, get_logger

# Far below any sensible interval, so a hanging request costs one beat, never two.
REQUEST_TIMEOUT_SECONDS = 10


class Heartbeat:
    """
    Pings the outside check while the collector is healthy.

    Args:
        url: The check's ping address
        interval_seconds: Seconds between beats
        health_provider: Returns what `/v1/health` would answer right now
        stats: Live statistics, where each outcome is counted
    """

    def __init__(
        self,
        url: str,
        interval_seconds: int,
        health_provider: Callable[[], Dict[str, Any]],
        stats: CollectorStats
    ):
        self._url = url
        self._interval = interval_seconds
        self._health_provider = health_provider
        self._stats = stats
        self._logger = get_logger("FiniexDataCollector.heartbeat")
        self._host = urlsplit(url).hostname or "the configured host"
        self._secret_path = urlsplit(url).path
        # What was logged last. A beat every minute must not become a line every
        # minute: only a change of state is news.
        self._last_state = ""

        self._stats.heartbeat.enabled = True
        self._stats.heartbeat.interval_seconds = interval_seconds

    async def run(self) -> None:
        """
        Beat until cancelled.

        One session for the life of the process: FiniexRAGEngine measured what a
        fresh TLS context per poll costs in a long-running process - about 0.5 MB
        each, 46 MB a minute. It waits one interval first because the feed is not
        connected yet at startup, and a heartbeat withheld for that says nothing.
        """
        connector = aiohttp.TCPConnector(
            ssl=ssl.create_default_context(cafile=certifi.where()))
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(connector=connector,
                                         timeout=timeout) as session:
            while True:
                await asyncio.sleep(self._interval)
                await self.beat(session)

    async def beat(self, session: Any) -> None:
        """
        One beat: send it if healthy, withhold it if not. Never raises, because a
        heartbeat that cannot be sent must not disturb what it reports on.

        Args:
            session: An aiohttp client session
        """
        try:
            health = self._health_provider()
            if health["status"] != "ok":
                self._withhold(health.get("reasons") or [health["status"]])
                return

            async with session.get(self._url) as response:
                if response.status >= 300:
                    self._failed(f"the check answered {response.status}")
                    return
            self._sent()
        except Exception as e:
            self._failed(self._without_address(describe_exception(e)))

    def _sent(self) -> None:
        """A beat reached the check."""
        self._stats.heartbeat.record_sent()
        self._note("sent", self._logger.info,
                   f"Heartbeat reaching {self._host}")

    def _withhold(self, reasons: Any) -> None:
        """
        The collector is not healthy, so the check is left to fire.

        Args:
            reasons: What `/v1/health` named
        """
        text = ", ".join(reasons)
        self._stats.heartbeat.record_withheld(text)
        self._note(f"withheld {text}", self._logger.warning,
                   f"Heartbeat withheld ({text}) - the outside check will alarm "
                   f"if this lasts past its grace time")

    def _failed(self, text: str) -> None:
        """
        A healthy collector could not say so. The check will fire anyway, and that
        is a true statement: from outside, this machine cannot be heard.

        Args:
            text: What went wrong, without the address
        """
        self._stats.heartbeat.record_failed(text)
        self._note(f"failed {text}", self._logger.warning,
                   f"Heartbeat did not reach {self._host}: {text}")

    def _note(self, state: str, log: Callable[[str], None], line: str) -> None:
        """
        Log a line only when the state changed.

        Args:
            state: The state this beat ended in
            log: The logger method for it
            line: The line to write
        """
        if state != self._last_state:
            self._last_state = state
            log(line)

    def _without_address(self, text: str) -> str:
        """
        Remove the address from an error text; aiohttp puts it into some of them.

        Args:
            text: The described exception

        Returns:
            The text, safe for a log line and for `/v1/status`
        """
        text = text.replace(self._url, "<heartbeat url>")
        if self._secret_path.strip("/"):
            text = text.replace(self._secret_path, "/<redacted>")
        return text
