"""
FiniexDataCollector - Tests for noticing a dropped connection, and what it cost

Measured on production over 19 days to 2026-10-08: 137 forced reconnects, and in
136 the socket was already closing when the 10 s silence watchdog found it. The
receive loop's end was handed back by gather(..., return_exceptions=True) and
ignored; a close frame that leaves the TCP connection open does not end recv()
at all until the next keepalive ping. Each drop cost about 12 s of trades, of
which about 10 were spent not knowing.

These tests run the REAL client against a real socket, which this project's
other tests avoid ("time is driven, not waited for"). The exception is
deliberate: the defect lived in the library's semantics - which ending raises
what, when recv() returns, that only one coroutine may call recv() - and a fake
reproduces those only as well as its author's assumptions. The review of this
change found exactly such an assumption. Every library-dependent case runs
against both websockets client implementations, because they differ precisely
here and production's dependency allows either. The check interval is shortened
so the whole file runs in seconds.

Location: tests/collectors/test_drop_detection.py
"""

import asyncio
import time
import warnings
from typing import Callable, List, Optional, Tuple

import pytest

from python.collectors.kraken import websocket_client
from python.collectors.kraken.quote_cache import QuoteCache
from python.collectors.kraken.websocket_client import KrakenWebSocketClient
from python.types.collector_stats import ReconnectEvent
from python.utils.collection_clock import CollectionClock
from python.utils.logging_setup import add_log_listener, remove_log_listener
from tests.collectors.far_side import FarSide

# Detection tests run with a 5 s silence threshold, so an end noticed by the
# receive loop or the close frame (well under a second) cannot be confused with
# one the watchdog found.
WATCHDOG_AFTER = 5.0
IMPLEMENTATIONS = ["asyncio", "legacy"]

Stages = List[Tuple[str, ReconnectEvent]]


@pytest.fixture(autouse=True)
def fast_checks(monkeypatch) -> None:
    """A tenth of a second between watchdog checks instead of one."""
    monkeypatch.setattr(websocket_client, "STALE_CHECK_INTERVAL", 0.1)


def opener(implementation: str,
           close_timeout: Optional[float] = None) -> Callable:
    """websockets.connect of one implementation, without TLS."""
    if implementation == "asyncio":
        from websockets.asyncio.client import connect
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from websockets.legacy.client import connect

    def open_connection(url, ssl=None, **kwargs):
        if close_timeout is not None:
            kwargs["close_timeout"] = close_timeout
        return connect(url, **kwargs)

    return open_connection


def client_for(far_side: FarSide, implementation: str = "asyncio",
               stale_after: float = WATCHDOG_AFTER,
               initial_delay: float = 0.05,
               max_delay: float = 2.0,
               close_timeout: Optional[float] = None) -> KrakenWebSocketClient:
    """The real client, pointed at the far side."""
    client = KrakenWebSocketClient(
        symbols=far_side.symbols, clock=CollectionClock(),
        quote_cache=QuoteCache(), streams=["trade", "ticker"],
        url=far_side.url, reconnect_initial_delay=initial_delay,
        reconnect_max_delay=max_delay, stale_after=stale_after)
    client._open = opener(implementation, close_timeout)
    return client


async def until_true(condition: Callable[[], bool],
                     timeout: float = 5.0) -> None:
    """Poll a condition; fail rather than hang when it never holds."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "the scenario never got there"
        await asyncio.sleep(0.02)


async def run(client: KrakenWebSocketClient, until: Callable[[Stages], bool],
              timeout: float = 10.0) -> Stages:
    """Collect until a condition on the outage stages holds, then stop."""
    stages: Stages = []
    client.set_outage_callback(
        lambda stage, event: stages.append((stage, event)))
    client.set_tick_callback(lambda tick: None)
    task = asyncio.create_task(client.start())
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline and not until(stages):
            await asyncio.sleep(0.02)
    finally:
        await asyncio.wait_for(client.stop(), timeout=5)
        await asyncio.wait_for(task, timeout=5)
    return stages


def resolved(stages: Stages) -> List[ReconnectEvent]:
    return [event for stage, event in stages if stage == "resolved"]


def restored(stages: Stages) -> List[ReconnectEvent]:
    return [event for stage, event in stages if stage == "restored"]


# ------------------------------------------------------------- detection


@pytest.mark.parametrize("implementation", IMPLEMENTATIONS)
@pytest.mark.parametrize("ending, reason", [
    ("abort", "connection_lost"),
    ("fin", "connection_lost"),
    ("clean_close", "far_side_close"),
])
def test_a_connection_the_far_side_ended_is_noticed_at_once(
        ending, reason, implementation) -> None:
    """
    Befund 44: the end of the receive loop starts the reconnect.

    A reset or a FIN carries no close frame, so before the fix only the silence
    watchdog could find it - 10-11 s later on production, 5 s here.
    """
    async def scenario():
        async with FarSide(ending=ending) as far_side:
            stages = await run(client_for(far_side, implementation),
                               lambda s: bool(restored(s)))
            return stages, far_side.connections

    stages, connections = asyncio.run(scenario())

    event = restored(stages)[0]
    assert event.reason == reason
    assert event.detected_after_ms < 1000, (
        f"noticed after {event.detected_after_ms} ms: the watchdog found it, "
        f"not the end of the connection")
    assert event.duration_seconds < WATCHDOG_AFTER
    assert connections >= 2
    if reason == "far_side_close":
        assert event.close_code == 1001
    else:
        assert event.close_code is None, "no frame arrived, so no code"


@pytest.mark.parametrize("implementation", IMPLEMENTATIONS)
def test_a_close_frame_on_a_connection_left_open_is_a_far_side_close(
        implementation) -> None:
    """
    The case that would have defeated a fix built on the receive loop alone.

    Kraken sends its close frame and keeps the TCP connection open. recv() then
    waits for the transport - until the next keepalive ping, 20 s later in one
    implementation and 10 s in the other - and the watchdog would have recorded
    'silence' with no code although the frame said exactly what happened.
    """
    async def scenario():
        async with FarSide(ending="lingering") as far_side:
            return await run(client_for(far_side, implementation),
                             lambda s: bool(restored(s)))

    event = restored(asyncio.run(scenario()))[0]

    assert event.reason == "far_side_close"
    assert event.close_code == 1001
    assert event.close_reason == "going away"
    assert event.detected_after_ms < 1000


@pytest.mark.parametrize("implementation", IMPLEMENTATIONS)
def test_a_silent_link_is_still_left_to_the_watchdog(implementation) -> None:
    """Nothing ended the connection, so silence is the only evidence there is."""
    async def scenario():
        async with FarSide(ending="silent") as far_side:
            return await run(client_for(far_side, implementation,
                                        stale_after=1.0),
                             lambda s: bool(restored(s)))

    event = restored(asyncio.run(scenario()))[0]

    assert event.reason == "silence_watchdog"
    assert event.close_code is None
    assert 1000 <= event.detected_after_ms < 2500
    assert event.teardown_ms < 500, (
        "a dead link is dropped, not closed politely for the close timeout")


@pytest.mark.parametrize("implementation", IMPLEMENTATIONS)
@pytest.mark.parametrize("ending", ["empty_close", "empty_lingering"])
def test_a_close_frame_without_a_code_records_no_code(
        ending, implementation) -> None:
    """
    websockets reports a close frame with no payload as code 1005, which RFC
    6455 forbids on the wire: the library's stand-in, like 1006 for a
    connection that ended without any frame. Recorded, it would assert a code
    Kraken never sent. The reason still says a frame came.

    Both ways the frame can arrive: with the TCP connection closed behind it,
    which the receive loop reports, and left open, which only the watchdog's
    reading of the connection's state finds - Kraken already does the latter
    with coded frames.
    """
    async def scenario():
        async with FarSide(ending=ending) as far_side:
            return await run(client_for(far_side, implementation),
                             lambda s: bool(restored(s)))

    event = restored(asyncio.run(scenario()))[0]

    assert event.reason == "far_side_close"
    assert event.close_code is None


# --------------------------------------------------------------- the record


def test_missing_ids_are_exactly_what_the_far_side_never_delivered() -> None:
    """
    first - last - 1 per symbol equals the ids produced while nobody listened.

    Checked against the far side's own ledger rather than against arithmetic on
    the same numbers the client saw.
    """
    async def scenario():
        async with FarSide(ending="abort") as far_side:
            stages = await run(client_for(far_side),
                               lambda s: bool(resolved(s)))
            return stages, far_side

    stages, far_side = asyncio.run(scenario())

    event = resolved(stages)[0]
    for kraken_symbol, symbol in (("BTC/USD", "BTCUSD"), ("ETH/USD", "ETHUSD")):
        gap = event.symbols[symbol]
        last = far_side.last_sent(kraken_symbol, 1)
        first = far_side.first_sent(kraken_symbol, 2)
        assert gap.last_trade_id == last
        assert gap.first_trade_id == first
        assert gap.missing_trade_ids == first - last - 1
    assert event.resolved_by == "all_symbols_traded"


def test_kraken_s_status_message_is_read_not_discarded() -> None:
    """
    The receive loop reads Kraken's status message and keeps what it says.

    Until 2026-10-08 subscribe() read one message per stream: Kraken's status
    message comes first, so the first read discarded it and the second labelled
    the first TRADE answer as the ticker's - 169 of 169 cycles on production.
    Both streams confirmed here proves little on its own - data confirms a pair
    as well as an answer does - which is why the refusal tests below exist.
    """
    async def scenario():
        async with FarSide(ending="abort") as far_side:
            return await run(client_for(far_side),
                             lambda s: bool(restored(s)))

    event = restored(asyncio.run(scenario()))[0]

    assert set(event.confirmed_after_ms) == {"trade", "ticker"}
    assert event.kraken_connection_id == str(900_000_000_000 + 1)
    assert event.kraken_system == "online"


def test_a_refused_pair_is_matched_by_req_id_and_named(monkeypatch) -> None:
    """
    A refusal names no channel - only the req_id ties it to its stream.

    Success answers carry result.channel and data confirms a pair anyway, so
    only a refusal shows whether answers are matched at all. Unmatched, the
    refused pair would wait out the deadline, be listed as unanswered instead
    of refused, and the 'Kraken refused' line would never be written.
    """
    monkeypatch.setattr(websocket_client, "SUBSCRIBE_DEADLINE_SECONDS", 3.0)

    async def scenario():
        async with FarSide(ending="abort",
                           refuse={(2, "trade", "ETH/USD")}) as far_side:
            stages = await run(client_for(far_side),
                               lambda s: bool(restored(s)), timeout=2.0)
            return stages, far_side.connections

    stages, connections = asyncio.run(scenario())

    assert restored(stages), "restored by the answers, not by the deadline"
    event = restored(stages)[0]
    assert event.rejected == [
        "trade ETHUSD: Currency pair not supported ETH/USD"]
    assert event.unconfirmed == []
    assert connections == 2


def test_a_stream_with_a_refusal_is_answered_not_dead(monkeypatch) -> None:
    """
    One ticker pair refused, the other silent, no ticker data at all: at the
    deadline the stream has answered - with a refusal - so the silent pair is
    listed and the feed carries on.

    A stream that produced nothing at all is reconnected; one Kraken answered
    must not be, or a refusal that lasts would cost a reconnect at every
    deadline. With every pair refused the stream is simply settled and this
    is never asked - which is why one pair here stays silent.
    """
    monkeypatch.setattr(websocket_client, "SUBSCRIBE_DEADLINE_SECONDS", 0.5)

    async def scenario():
        async with FarSide(ending="abort", end_after=1.0,
                           withhold_data={"ticker"},
                           silent_pairs={("ticker", "ETH/USD")},
                           refuse={(2, "ticker", "BTC/USD")}) as far_side:
            stages = await run(client_for(far_side),
                               lambda s: bool(restored(s)), timeout=5.0)
            return stages, far_side.connections

    stages, connections = asyncio.run(scenario())

    assert restored(stages), "restored at the deadline, not reconnected"
    event = restored(stages)[0]
    assert event.rejected == [
        "ticker BTCUSD: Currency pair not supported BTC/USD"]
    assert event.unconfirmed == ["ticker ETHUSD"]
    assert connections == 2


def test_data_on_a_pair_whose_answer_never_comes_confirms_it(
        monkeypatch) -> None:
    """
    Data on a pair is the stronger evidence that its subscription works.

    The ticker answers are withheld and the ticker data flows: the feed counts
    as restored at once, without waiting for the deadline.
    """
    monkeypatch.setattr(websocket_client, "SUBSCRIBE_DEADLINE_SECONDS", 0.5)

    async def scenario():
        async with FarSide(ending="abort", withhold_answers={"ticker"}) \
                as far_side:
            stages = await run(client_for(far_side),
                               lambda s: bool(restored(s)), timeout=4)
            return stages, far_side.connections

    stages, connections = asyncio.run(scenario())

    assert restored(stages), "the feed counts as restored by its data"
    assert connections == 2, "no reconnect over an answer the data replaced"


def test_one_silent_pair_is_listed_and_does_not_cost_the_connection(
        monkeypatch) -> None:
    """
    The deadline path: one pair neither answers nor trades while its stream
    lives.

    A thin pair or a slow answer from a degraded Kraken looks exactly like this
    at the deadline. Listing it and carrying on keeps the feed; reconnecting
    over it would kill every attempt for as long as Kraken stays slow - a
    working feed turned into a total outage. The record names the pair.
    """
    monkeypatch.setattr(websocket_client, "SUBSCRIBE_DEADLINE_SECONDS", 0.5)

    async def scenario():
        async with FarSide(ending="abort", end_after=1.0,
                           silent_pairs={("trade", "ETH/USD")}) as far_side:
            stages = await run(client_for(far_side),
                               lambda s: bool(restored(s)), timeout=6)
            return stages, far_side.connections

    stages, connections = asyncio.run(scenario())

    event = restored(stages)[0]
    assert event.unconfirmed == ["trade ETHUSD"]
    assert connections == 2, "one drop, one reconnect - none over the silent pair"


def test_a_stream_that_produces_nothing_at_all_is_reconnected(
        monkeypatch) -> None:
    """No answer, no refusal and no data: the subscription does not exist."""
    monkeypatch.setattr(websocket_client, "SUBSCRIBE_DEADLINE_SECONDS", 0.5)

    async def scenario():
        async with FarSide(withhold_answers={"ticker"},
                           withhold_data={"ticker"}) as far_side:
            await run(client_for(far_side),
                      lambda s: far_side.connections >= 2, timeout=4)
            return far_side.connections

    assert asyncio.run(scenario()) >= 2


def test_a_drop_during_resubscription_is_one_outage_with_two_attempts() -> None:
    """One interruption is one record, however many attempts it takes."""
    async def scenario():
        async with FarSide(ending="abort", fail_subscribe_on={2}) as far_side:
            return await run(client_for(far_side),
                             lambda s: bool(restored(s)))

    stages = asyncio.run(scenario())

    assert len([1 for stage, _ in stages if stage == "opened"]) == 1
    event = restored(stages)[0]
    assert event.attempts == 2
    assert "1011" in event.last_failure


# ----------------------------------------------------------------- backoff


def test_a_far_side_that_keeps_closing_is_backed_off() -> None:
    """
    Immediate detection must not become a reconnect loop.

    The backoff used to start over at every handshake. With the 10 s watchdog
    in front of it nobody noticed; with a closed socket found at once, a far
    side that accepts and closes again would be dialled every few seconds -
    about 150 times in ten minutes in a simulation with the 2.3 s handshake
    measured against Kraken, the limit Kraken documents.
    """
    async def scenario():
        async with FarSide(close_every_after=0.1) as far_side:
            client = client_for(far_side, initial_delay=0.02, max_delay=1.0)
            delays: List[float] = []
            original = client._get_reconnect_delay

            def recording() -> float:
                delays.append(original())
                return delays[-1]

            client._get_reconnect_delay = recording
            await run(client, lambda s: len(delays) >= 4, timeout=6)
            return delays

    delays = asyncio.run(scenario())

    assert delays[:4] == [0.02, 0.04, 0.08, 0.16], (
        f"the delay must double while connections stay short, got {delays}")


def test_a_stable_connection_starts_the_backoff_over() -> None:
    """
    Ten seconds fully subscribed, measured from restoration - the same anchor
    the record's connection age uses.

    Not sixty, as first built: a degraded Kraken dropping connections every
    20-50 s then climbed to the maximum delay and stayed there - 62 % offline
    in a simulation, against 9 % with ten seconds and 28 % with the old reset
    at every handshake. Ten keeps a far side that closes just past the window
    at 45 attempts in ten minutes, well inside the limit.
    """
    client = KrakenWebSocketClient(symbols=["BTC/USD"], clock=CollectionClock(),
                                   quote_cache=QuoteCache(), streams=["trade"])
    client._tracker.handshake(0.0)
    client._tracker.subscriptions_sent(0.0)
    client._tracker.subscription_answered("trade", "BTCUSD", None, 0.0)
    client._reconnect_attempt = 3

    client._reset_backoff_if_stable(9.9)
    assert client._reconnect_attempt == 3
    client._reset_backoff_if_stable(10.0)
    assert client._reconnect_attempt == 0


# ---------------------------------------------------------------- exits


def test_nothing_but_a_stop_ends_collection() -> None:
    """
    A failure anywhere in an iteration - the new teardown, the diagnostics - is
    followed by the next attempt, as any drop is. Before, an exception outside
    the old try ended start() and the process stayed up collecting nothing.
    """
    async def scenario():
        async with FarSide(ending="abort") as far_side:
            client = client_for(far_side)

            def broken(*args) -> None:
                raise RuntimeError("teardown broke")

            client._end_connection = broken
            await run(client, lambda s: far_side.connections >= 2, timeout=4)
            return far_side.connections

    assert asyncio.run(scenario()) >= 2


def test_a_stop_during_the_backoff_hands_over_the_open_outage() -> None:
    """
    The record is handed over by stop() itself, before anything is cancelled.

    At shutdown main never awaits start() - asyncio.run cancels it wherever it
    sleeps - so a record handed over on start()'s way out would never be
    written, and an outage still open at a deploy would leave no trace.
    """
    async def scenario():
        async with FarSide(ending="abort") as far_side:
            client = client_for(far_side, initial_delay=0.5)
            stages: Stages = []
            client.set_outage_callback(
                lambda stage, event: stages.append((stage, event)))
            client.set_tick_callback(lambda tick: None)
            task = asyncio.create_task(client.start())
            await until_true(lambda: bool(stages))
            await asyncio.wait_for(client.stop(), timeout=5)
            handed_over = resolved(stages)
            task.cancel()
            await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True), timeout=5)
            return handed_over

    handed_over = asyncio.run(scenario())

    assert len(handed_over) == 1, "handed over by stop(), not by start()"
    assert handed_over[0].resolved_by == "shutdown"
    assert handed_over[0].reconnected_at is None


def test_a_stop_during_a_pending_handshake_leaves_no_connection_behind() -> None:
    """The socket that opens after stop() belongs to nobody and is dropped."""
    async def scenario():
        async with FarSide(ending="abort",
                           delay_handshake_on={2: 0.6}) as far_side:
            client = client_for(far_side)
            stages: Stages = []
            client.set_outage_callback(
                lambda stage, event: stages.append((stage, event)))
            client.set_tick_callback(lambda tick: None)
            task = asyncio.create_task(client.start())
            await until_true(lambda: far_side.connections >= 2)
            await asyncio.wait_for(client.stop(), timeout=5)
            await asyncio.wait_for(task, timeout=5)
            await asyncio.sleep(0.3)
            return client, far_side

    client, far_side = asyncio.run(scenario())

    assert client._connection_status == "disconnected"
    assert client._websocket is None
    assert 2 in far_side.closed_by_client


def test_a_stop_while_a_send_waits_on_a_closing_connection_opens_nothing(
) -> None:
    """
    Kraken closes right after the handshake and leaves TCP open; the first
    subscription send then waits on the closing connection until the close
    timeout. A stop inside that wait hands the open record over as
    'shutdown' - and the send's failure must not then open a new one that
    nothing would ever close, with '- reconnecting' logged after the stop.

    The asyncio implementation only: the legacy one sends both requests while
    the connection is still open and never reaches this wait.
    """
    async def scenario():
        async with FarSide(ending="abort",
                           close_at_handshake_on={2}) as far_side:
            client = client_for(far_side, close_timeout=1.0)
            stages: Stages = []
            client.set_outage_callback(
                lambda stage, event: stages.append((stage, event)))
            client.set_tick_callback(lambda tick: None)
            task = asyncio.create_task(client.start())
            await until_true(lambda: far_side.connections >= 2)
            await asyncio.sleep(0.3)
            await asyncio.wait_for(client.stop(), timeout=5)
            await asyncio.wait_for(task, timeout=5)
            return client, stages

    client, stages = asyncio.run(scenario())

    assert [stage for stage, _ in stages] == ["opened", "resolved"]
    assert stages[-1][1].resolved_by == "shutdown"
    assert client._tracker.event is None
    assert client._connection_status == "disconnected"


@pytest.mark.parametrize("implementation", IMPLEMENTATIONS)
def test_a_message_that_cannot_be_handled_costs_only_that_message(
        implementation) -> None:
    """
    A status message whose data is not a list breaks its handler. Before the
    review it ended the connection - every connection, since Kraken sends one
    on each - and the backoff would have climbed to its maximum: a total
    outage over one format change. Now it costs that message.
    """
    async def scenario():
        ticks: List[object] = []
        async with FarSide(after_status=[{"channel": "status",
                                          "type": "update", "data": 5}]) \
                as far_side:
            client = client_for(far_side, implementation)
            client.set_outage_callback(lambda stage, event: None)
            client.set_tick_callback(ticks.append)
            task = asyncio.create_task(client.start())
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline and len(ticks) < 20:
                await asyncio.sleep(0.02)
            await asyncio.wait_for(client.stop(), timeout=5)
            await asyncio.wait_for(task, timeout=5)
            return len(ticks), far_side.connections

    ticks, connections = asyncio.run(scenario())

    assert ticks >= 20
    assert connections == 1, "the connection outlived the message"


def test_a_handshake_that_fails_after_a_stop_leaves_the_status_alone(
) -> None:
    """
    The stop sets 'disconnected'; a handshake still pending then fails. Its
    'failed' used to land last, so a stopped collector reported a failure -
    and its ERROR line counted one in total_errors.
    """
    async def scenario():
        async with FarSide(ending="abort",
                           drop_handshake_on={2: 0.4}) as far_side:
            client = client_for(far_side)
            client.set_outage_callback(lambda stage, event: None)
            client.set_tick_callback(lambda tick: None)
            statuses: List[str] = []
            client.set_status_callback(statuses.append)
            task = asyncio.create_task(client.start())
            await until_true(lambda: far_side.connections >= 2)
            errors: List[str] = []

            def listener(level, name, message) -> None:
                if level.name == "ERROR":
                    errors.append(message)

            add_log_listener(listener)
            try:
                await asyncio.wait_for(client.stop(), timeout=5)
                await asyncio.wait_for(task, timeout=5)
                await asyncio.sleep(0.6)
            finally:
                remove_log_listener(listener)
            return statuses, errors

    statuses, errors = asyncio.run(scenario())

    assert statuses[-1] == "disconnected", statuses
    assert errors == [], errors
