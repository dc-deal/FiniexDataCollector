"""
FiniexDataCollector - Tests for dead-connection detection

A dropped connection costs the trades Kraken makes while the dead socket is
still believed alive. Checked every 10 s with a reconnect at three times that,
production lost 537 trades in four drops on 2026-09-18, 30-40 s of each spent
noticing. Kraken sends a heartbeat every second, so silence is now judged every
second against `stale_after`.

The dangerous half is the opposite error. While a file closes, the event loop
is blocked - 17.85 s at the 2026-09-19 midnight cut - and no message is read, so
a short threshold would see a dead feed at every day cut and force a reconnect
that costs real trades. These tests drive the time source and the sleep by hand
to make both happen on demand.

Since 2026-10-08 the watchdog REPORTS what it found and closes nothing: the end
of a connection is torn down in exactly one place, in start(), whichever
detector saw it first. So these tests read the detection the watchdog returns;
the teardown has its own tests in test_drop_detection.py, except one that needs
a frame no real far side here sends at that moment: a close frame without a
code, found by the teardown after a detector that did not carry it.

Location: tests/collectors/test_stale_detection.py
"""

import asyncio
from typing import Callable, List

from websockets.frames import Close

from python.collectors.kraken.outage_tracker import Detection
from python.collectors.kraken.quote_cache import QuoteCache
from python.collectors.kraken.websocket_client import KrakenWebSocketClient
from python.utils.collection_clock import CollectionClock

START = 1000.0
STALE_AFTER = 10.0


class FakeSocket:
    """A connection that never received a close frame."""

    close_rcvd = None


def run_monitor(
        on_sleep: Callable[[List[float], KrakenWebSocketClient, float], None]
) -> tuple:
    """
    Run the monitor against a hand-driven clock until it stops.

    Args:
        on_sleep: Called for every sleep with (clock cell, client, seconds);
            it advances the clock and decides what the feed did meanwhile

    Returns:
        (client, what the watchdog reported, seconds elapsed on the driven
        clock)
    """
    now = [START]
    client = KrakenWebSocketClient(
        symbols=["BTC/USD"], clock=CollectionClock(), quote_cache=QuoteCache(),
        streams=["trade"], stale_after=STALE_AFTER)
    socket = FakeSocket()

    async def sleep(seconds: float) -> None:
        on_sleep(now, client, seconds)
        await asyncio.sleep(0)

    client._monotonic = lambda: now[0]
    client._sleep = sleep
    client._is_running = True
    client._websocket = socket
    client._last_message_monotonic = START

    detection = asyncio.run(asyncio.wait_for(client._heartbeat_loop(),
                                             timeout=5))
    return client, detection, now[0] - START


def stop_at(limit: float, now: List[float],
            client: KrakenWebSocketClient) -> None:
    """End the run once the driven clock passes `limit` seconds."""
    if now[0] - START >= limit:
        client._is_running = False


def test_a_silent_feed_is_reopened_on_the_first_check_past_the_threshold() -> None:
    """Detected at 11 s, not at the 30-40 s the old check took."""
    def silent(now, client, seconds) -> None:
        now[0] += seconds

    _, detection, elapsed = run_monitor(silent)

    assert detection.kind == "silence_watchdog"
    assert elapsed == STALE_AFTER + 1


def test_a_feed_that_sends_its_heartbeat_is_left_alone() -> None:
    """A minute of one message per second - Kraken's measured cadence."""
    def heartbeating(now, client, seconds) -> None:
        now[0] += seconds
        client._last_message_monotonic = now[0]
        stop_at(60, now, client)

    _, detection, _ = run_monitor(heartbeating)

    assert detection is None


def test_a_blocked_event_loop_is_not_mistaken_for_a_dead_feed() -> None:
    """
    The midnight cut: the loop stands for 18 s and reads nothing meanwhile.

    The feed was alive the whole time; its messages were waiting in the socket
    and the receive loop reads them as soon as it gets its turn. Judging the
    silence the moment the loop comes back would force a reconnect at every
    day cut.
    """
    blocked = [False]

    def blocked_then_alive(now, client, seconds) -> None:
        if not blocked[0]:
            blocked[0] = True
            now[0] += 18.0
            return
        now[0] += seconds
        client._last_message_monotonic = now[0]
        stop_at(40, now, client)

    _, detection, _ = run_monitor(blocked_then_alive)

    assert detection is None


def test_a_feed_that_died_during_a_block_is_still_reopened() -> None:
    """The late-wake exemption skips one judgement; it never disables them."""
    blocked = [False]

    def blocked_then_silent(now, client, seconds) -> None:
        if not blocked[0]:
            blocked[0] = True
            now[0] += 18.0
            return
        now[0] += seconds

    _, detection, elapsed = run_monitor(blocked_then_silent)

    assert detection.kind == "silence_watchdog"
    assert elapsed == 18.0 + 1


class ClosedWithoutCode:
    """A connection whose far side sent a close frame that carried no code."""

    def __init__(self) -> None:
        self.close_rcvd = Close.parse(b"")


def test_a_frame_a_detector_did_not_carry_is_read_without_its_stand_in_code(
) -> None:
    """
    A detector without the frame - here the subscription deadline - fires
    after a close frame did arrive; the teardown names the end from the
    frame. That frame carried no code, which websockets reports as 1005, and
    the record must not: the code is the library's, not Kraken's.
    """
    client = KrakenWebSocketClient(
        symbols=["BTC/USD"], clock=CollectionClock(), quote_cache=QuoteCache(),
        streams=["trade"])
    client._websocket = ClosedWithoutCode()
    seen: List[Detection] = []
    client._tracker.connection_ended = (
        lambda detection, mono: seen.append(detection))

    client._end_connection(Detection("subscribe_unanswered"), 0.0)

    assert seen[0].kind == "far_side_close"
    assert seen[0].close_code is None
