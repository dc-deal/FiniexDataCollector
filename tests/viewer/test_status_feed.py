"""
FiniexDataCollector - Tests for the viewer's feed

The collector is expected to be unreachable regularly - a reboot, a stopped
service, an RDP session that ended. So "cannot reach it" is a state the viewer
displays, never an error it exits on, and these tests hold that line.

The failure TEXT is tested rather than a failure count, because the four common
ones send the operator to four different places: a refused connection means the
service is not running, a 401 means the token is wrong, a 403 means the token is
right and the grant is missing, and a payload mismatch means the two builds
differ. A screen saying "error" would leave all four to be worked out by hand.

The other guard here is the clock. Uptime on the viewer is derived from the
collector's start time against the VIEWER's clock, so a laptop running ten
minutes fast prints ten minutes of uptime that never happened - visible only if
something compares the two, which is what the skew does.

Location: tests/viewer/test_status_feed.py
"""

import asyncio
from datetime import datetime, timedelta, timezone

import aiohttp
import pytest

from python.types.collector_stats import CollectorStats
from python.viewer.stats_from_payload import PayloadMismatch
from python.viewer.status_feed import (MIN_TIMEOUT_SECONDS, FeedError,
                                       StatusFeed, _describe, _describe_status,
                                       _skew_seconds)


def a_feed() -> StatusFeed:
    """A feed pointed at a collector that does not exist."""
    return StatusFeed("https://collector.example/", "not-a-real-token", 0.01)


def test_the_url_is_built_without_a_doubled_slash() -> None:
    """A base URL is copied from a config file by a human, trailing slash and all."""
    assert a_feed().url == "https://collector.example/v1/status"


def test_a_rejected_token_and_a_missing_grant_read_differently() -> None:
    """
    401 and 403 are one character apart and mean opposite things.

    401 says the credential is not accepted at all - wrong token, wrong instance.
    403 says it IS accepted and lacks `status:detail`, which is one line in a
    registry on the box. Collapsing them into "denied" costs the operator the trip
    that tells them apart.
    """
    assert "not accepted" in _describe_status(401)
    assert "status:detail" in _describe_status(403)
    assert "collector" in _describe_status(404)
    assert _describe_status(502) == "HTTP 502"


def test_a_reason_on_screen_is_a_sentence_and_not_a_class_name() -> None:
    """
    A timeout is the most common failure here and arrives carrying no message.

    `describe_exception` then renders it as `TimeoutError`, which is correct in a
    log file and useless on a screen whose whole job is to say whether to restart
    a service or check a token.
    """
    for error in (asyncio.TimeoutError(), aiohttp.ServerTimeoutError()):
        reason = _describe(error)

        assert " " in reason, f"this reads as a class name: {reason!r}"
        assert "timeout" in reason.lower()


def test_the_timeout_leaves_a_refusal_room_to_report_itself() -> None:
    """
    A closed port has to lose to the timeout, not the other way round.

    Measured on Windows on 2026-09-21: a refused connection to a closed loopback
    port takes 2.04 s to come back, because the stack retries the SYN first. At a
    two-second timeout the refusal lost by forty milliseconds, and the screen
    reported "no answer within the timeout" for a service that was simply not
    running - which sends the operator to the network instead of to the service.
    """
    assert MIN_TIMEOUT_SECONDS >= 3.0, (
        "a refused connection will be reported as a timeout again")


def test_a_build_mismatch_is_named_as_one() -> None:
    """Otherwise it reads as the collector being broken, which it is not."""
    described = _describe(PayloadMismatch("FileInfo: missing tick_count"))

    assert "viewer" in described.lower()
    assert "FileInfo" in described


def test_an_answer_that_cannot_be_drawn_keeps_its_own_words() -> None:
    """A FeedError already carries the sentence; wrapping it would bury it."""
    assert _describe(FeedError("403 - the grant is missing")) == (
        "403 - the grant is missing")


def test_the_skew_between_the_two_machines_is_measured() -> None:
    """
    The collector states its own uptime; the viewer recomputes it locally.

    The difference is the only signal either side has that the clocks disagree,
    and every duration on the screen is derived the local way.
    """
    stats = CollectorStats()
    stats.start_time = datetime.now(timezone.utc) - timedelta(seconds=3600)

    # The collector says it has been up an hour. The viewer agrees.
    assert abs(_skew_seconds({"uptime_seconds": 3600}, stats)) < 2

    # The collector says a quarter of an hour. This machine's clock is ahead.
    assert _skew_seconds({"uptime_seconds": 900}, stats) == pytest.approx(
        2700, abs=2)


def test_a_collector_that_does_not_state_its_uptime_produces_no_skew() -> None:
    """None, not zero. Zero would claim the clocks were compared and agreed."""
    assert _skew_seconds({}, CollectorStats()) is None


def test_a_failed_reading_leaves_the_last_one_standing() -> None:
    """
    The previous numbers are kept, and the feed stops calling itself connected.

    Blanking the screen during an outage throws away what the collector last
    said, which is usually the interesting part - and the banner that marks the
    numbers as old is what keeps that honest.
    """
    feed = a_feed()
    drawn = []

    async def one_failure_then_stop() -> None:
        async def always_fails(_session):
            raise FeedError("403 - the grant is missing")

        feed._read_once = always_fails
        task = asyncio.create_task(feed.run(drawn.append))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(one_failure_then_stop())

    assert not drawn, "a failed reading was handed to the renderer"
    assert feed.state.connected is False
    assert feed.state.failures >= 1
    assert feed.state.last_error == "403 - the grant is missing"


def test_a_reading_clears_the_failure_it_followed() -> None:
    """
    A recovered feed must not keep showing the reason it was down.

    A red banner that stays up after the collector came back is the same class of
    lie as a frozen number: it describes a moment that has passed.
    """
    feed = a_feed()
    feed.state.record_failure("unreachable")
    feed.state.record_reading(skew_seconds=0.2)

    assert feed.state.connected is True
    assert feed.state.last_error is None
    assert feed.state.failures == 0
    assert feed.state.age_seconds is not None
