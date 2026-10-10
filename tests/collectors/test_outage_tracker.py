"""
FiniexDataCollector - Tests for the record each interruption of the feed leaves

Until 2026-10-08 a reconnect was recorded from the moment it was NOTICED to the
socket handshake, by subtracting two wall-clock readings: a drop recorded as
2.2 s had cost about 12 s of trades, its reason was always the same constant,
and nothing said which trades were missed. The OutageTracker replaces that with
one record per interruption, on the monotonic clock, from the last message
before it to the first trade per symbol after it.

Everything here is driven with plain numbers - the tracker holds no socket and
no event loop - so each test states exactly the timeline it means.

Location: tests/collectors/test_outage_tracker.py
"""

from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pytest
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.frames import Close

from python.collectors.kraken.outage_tracker import (RESOLVE_DEADLINE_SECONDS,
                                                     Detection, OutageTracker,
                                                     classify_close,
                                                     close_code_of)
from python.types.collector_stats import ReconnectEvent

SYMBOLS = ["BTCUSD", "ETHUSD"]
STREAMS = ["trade", "ticker"]
FAR_SIDE = Detection("far_side_close", 1001, "going away")


class Feed:
    """A tracker with a wall clock that can be stepped, and its callbacks."""

    def __init__(self, streams: List[str] = None) -> None:
        self.wall = [datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)]
        self.stages: List[Tuple[str, ReconnectEvent]] = []
        self.tracker = OutageTracker(SYMBOLS, streams or STREAMS,
                                     wall=lambda: self.wall[0])
        self.tracker.set_callback(
            lambda stage, event: self.stages.append((stage, event)))

    def subscribe_all(self, at: float) -> str:
        """Every pair acknowledged at `at`; returns the last answer's outcome."""
        self.tracker.subscriptions_sent(at)
        outcome = ""
        for stream in self.tracker._streams:
            for symbol in SYMBOLS:
                outcome = self.tracker.subscription_answered(
                    stream, symbol, None, at) or outcome
        return outcome

    def started(self) -> "Feed":
        """A process that got its feed going once, at t = 100."""
        self.tracker.handshake(100.0)
        assert self.subscribe_all(100.1) == "startup"
        return self

    def resolved(self) -> List[ReconnectEvent]:
        return [event for stage, event in self.stages if stage == "resolved"]


# --------------------------------------------------------------- classify


@pytest.mark.parametrize("error, kind, code", [
    (ConnectionClosedOK(Close(1001, "going away"), Close(1001, "going away"),
                        True), "far_side_close", 1001),
    (ConnectionClosedOK(Close(1000, ""), Close(1000, ""), True),
     "far_side_close", 1000),
    (ConnectionClosedError(Close(1011, "internal error"), None),
     "far_side_close", 1011),
    (ConnectionClosedError(None, None), "connection_lost", None),
    (ConnectionClosedError(None, Close(1011, "keepalive ping timeout")),
     "keepalive_timeout", None),
    (ConnectionClosedError(None, Close(1009, "message too big")),
     "library_failure", None),
])
def test_classify_close_names_every_kind(error, kind, code) -> None:
    """
    Sorted by the frames exchanged, never by the exception class.

    A close Kraken initiated arrives as ConnectionClosedOK or as
    ConnectionClosedError depending on its code; both are the far side closing.
    And a code is stated only when a frame carrying it arrived.
    """
    detection = classify_close(error)

    assert detection.kind == kind
    assert detection.close_code == code


def test_a_lost_connection_names_the_error_underneath() -> None:
    """The reset or the DNS failure is what an operator can act on."""
    error = ConnectionClosedError(None, None)
    error.__cause__ = ConnectionResetError("connection reset by peer")

    assert "ConnectionResetError" in classify_close(error).cause


def test_no_cause_is_an_empty_cause_not_the_text_of_none() -> None:
    """
    The asyncio implementation chains nothing for a plain FIN.

    describe_exception(None) would print 'NoneType: None' - a cause nobody had,
    in a field a session reads to decide what went wrong.
    """
    assert classify_close(ConnectionClosedError(None, None)).cause == ""


def test_a_close_frame_without_a_code_has_no_code() -> None:
    """
    websockets parses an empty close frame as code 1005, which RFC 6455
    forbids on the wire - the library's stand-in for 'no code', as 1006 is for
    'no frame'. Recorded, it would assert a code Kraken never sent.
    """
    empty = Close.parse(b"")
    detection = classify_close(ConnectionClosedOK(empty, empty, True))

    assert detection.kind == "far_side_close"
    assert detection.close_code is None
    assert close_code_of(Close(1001, "going away")) == 1001


# ----------------------------------------------------------------- timeline


def test_the_gap_runs_from_the_last_message_on_the_monotonic_clock() -> None:
    """
    Last message at 500.0, close at 500.4, handshake 502.6, answers 502.7/502.8.

    The wall clock is stepped back an hour in between. A duration derived from
    two wall-clock readings would come out at about minus an hour; one taken
    from detection would leave out the 400 ms before it.
    """
    feed = Feed().started()
    feed.tracker.message(500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)

    feed.wall[0] -= timedelta(hours=1)
    feed.tracker.attempt_started()
    feed.tracker.handshake(502.6)
    feed.tracker.subscriptions_sent(502.62)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("trade", symbol, None, 502.7)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.8)

    event = feed.tracker.last_restored
    assert event.detected_after_ms == 400.0
    assert event.handshake_after_ms == 2600.0
    assert event.confirmed_after_ms == {"trade": 2700.0, "ticker": 2800.0}
    assert event.duration_seconds == pytest.approx(2.8)
    assert event.reason == "far_side_close"
    assert event.close_code == 1001
    assert event.attempts == 1


def test_missing_ids_are_counted_per_symbol_across_the_drop() -> None:
    """
    first - last - 1 per symbol, with the ids from BEFORE the drop.

    Taking the last id at restoration instead would compare a symbol's first
    trade after the drop with itself.
    """
    feed = Feed().started()
    feed.tracker.message(500.0)
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(502.1)
    feed.tracker.trade("BTCUSD", 106, 2, 502.2)
    feed.tracker.trade("ETHUSD", 501, 2, 502.3)

    event = feed.resolved()[0]
    assert event.symbols["BTCUSD"].missing_trade_ids == 5
    assert event.symbols["ETHUSD"].missing_trade_ids == 0
    assert event.symbols["BTCUSD"].last_trade_id == 100
    assert event.symbols["BTCUSD"].first_trade_id == 106
    assert event.resolved_by == "all_symbols_traded"


def test_the_highest_id_counts_not_the_last_received() -> None:
    """A reordered delivery must not make the next gap look larger than it is."""
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("BTCUSD", 98, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)

    assert feed.tracker.event.symbols["BTCUSD"].last_trade_id == 100


def test_an_unknown_id_is_unknown_not_zero() -> None:
    """
    Zero would claim nothing was missed - a measurement nobody made.

    A trade without an id, and a symbol that had not traded since the process
    started, both leave the count unknown.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", None, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(502.1)
    feed.tracker.trade("BTCUSD", 106, 2, 502.2)
    feed.tracker.trade("ETHUSD", 501, 2, 502.3)

    event = feed.resolved()[0]
    assert event.symbols["BTCUSD"].missing_trade_ids is None
    assert event.symbols["ETHUSD"].last_trade_id is None
    assert event.symbols["ETHUSD"].missing_trade_ids is None


def test_ids_that_do_not_increase_are_flagged_not_counted() -> None:
    """A replay or a reordering is named; a negative 'missing' would hide it."""
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(502.1)

    warning = feed.tracker.trade("BTCUSD", 95, 2, 502.2)

    gap = feed.tracker.last_restored.symbols["BTCUSD"]
    assert gap.ids_not_increasing
    assert gap.missing_trade_ids is None
    assert warning and "not above" in warning


def test_a_failed_attempt_that_delivered_trades_does_not_end_the_count(
) -> None:
    """
    Attempt 2 delivers trades and fails before the feed is restored; attempt 3
    restores it.

    The ids Kraken published between the two attempts are missing too. Before
    the review the record closed at attempt 3's restoration on the strength of
    attempt 2's trades: 9 missing per symbol where 48 were never received, and
    a first trade the restored feed never delivered.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.message(500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)

    feed.tracker.attempt_started()
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("trade", symbol, None, 502.1)
    feed.tracker.trade("BTCUSD", 110, 2, 502.1)
    feed.tracker.trade("ETHUSD", 510, 2, 502.1)
    feed.tracker.connection_ended(
        Detection("far_side_close", 1011, "internal error"), 502.2)

    feed.tracker.attempt_started()
    feed.tracker.handshake(505.0)
    assert feed.subscribe_all(505.0) == "restored"
    assert not feed.resolved(), "nothing from the restored feed yet"
    feed.tracker.trade("BTCUSD", 150, 3, 505.5)
    feed.tracker.trade("ETHUSD", 550, 3, 505.5)

    event = feed.resolved()[0]
    assert event.resolved_by == "all_symbols_traded"
    for symbol, last, received, first in (("BTCUSD", 100, 110, 150),
                                          ("ETHUSD", 500, 510, 550)):
        gap = event.symbols[symbol]
        assert gap.last_trade_id == last
        assert gap.first_trade_id == first
        assert gap.missing_trade_ids == (received - last - 1) + \
            (first - received - 1)
    assert event.first_trade_after_ms > event.handshake_after_ms


def drop_and_fail_an_attempt(feed: Feed, btc_ids=(), eth_ids=()) -> None:
    """Last trades 100 and 500, a drop, and an attempt that delivers the
    given trades on its acknowledged trade stream and then fails."""
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.message(500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("trade", symbol, None, 502.1)
    for trade_id in btc_ids:
        feed.tracker.trade("BTCUSD", trade_id, 2, 502.1)
    for trade_id in eth_ids:
        feed.tracker.trade("ETHUSD", trade_id, 2, 502.1)
    feed.tracker.connection_ended(
        Detection("far_side_close", 1011, "internal error"), 502.2)
    feed.tracker.handshake(505.0)


def test_the_stretch_resumes_at_the_highest_id_the_failed_attempt_delivered(
) -> None:
    """
    One trade message carries several trades, so an attempt that delivers
    anything usually delivers a run of ids - and the next stretch starts after
    the last of them, not after the first.
    """
    feed = Feed().started()
    drop_and_fail_an_attempt(feed, btc_ids=(110, 111, 112), eth_ids=(510,))
    feed.subscribe_all(505.0)
    feed.tracker.trade("BTCUSD", 150, 3, 505.5)
    feed.tracker.trade("ETHUSD", 550, 3, 505.5)

    gap = feed.resolved()[0].symbols["BTCUSD"]
    assert gap.missing_trade_ids == (110 - 100 - 1) + (150 - 112 - 1)


def test_the_next_outage_counts_from_its_own_last_trade() -> None:
    """What a failed attempt of one outage left behind is no part of the next."""
    feed = Feed().started()
    drop_and_fail_an_attempt(feed, btc_ids=(110,), eth_ids=(510,))
    feed.subscribe_all(505.0)
    feed.tracker.trade("BTCUSD", 150, 3, 505.5)
    feed.tracker.trade("ETHUSD", 550, 3, 505.5)
    feed.tracker.trade("BTCUSD", 300, 3, 600.0)
    feed.tracker.trade("ETHUSD", 700, 3, 600.0)

    feed.tracker.connection_ended(FAR_SIDE, 600.4)
    feed.tracker.handshake(602.0)
    feed.subscribe_all(602.0)
    feed.tracker.trade("BTCUSD", 310, 4, 602.5)
    feed.tracker.trade("ETHUSD", 701, 4, 602.5)

    second = feed.resolved()[1]
    assert second.symbols["BTCUSD"].missing_trade_ids == 9
    assert second.symbols["ETHUSD"].missing_trade_ids == 0


def test_a_replay_on_a_failed_attempt_leaves_the_count_unknown_and_flagged(
) -> None:
    """
    The failed attempt's first BTCUSD trade is a replay; the restored feed's
    is fine. The count stays unknown and the flag says why: it describes the
    outage, not only the one trade the record names. And for ETHUSD, the
    warning names the id it was compared with - the failed attempt's highest.
    """
    feed = Feed().started()
    drop_and_fail_an_attempt(feed, btc_ids=(95,), eth_ids=(510,))
    feed.subscribe_all(505.0)
    later = feed.tracker.trade("ETHUSD", 505, 3, 505.5)
    feed.tracker.trade("BTCUSD", 150, 3, 505.5)

    event = feed.resolved()[0]
    btc, eth = event.symbols["BTCUSD"], event.symbols["ETHUSD"]
    assert btc.first_trade_id == 150
    assert btc.ids_not_increasing and btc.missing_trade_ids is None
    assert eth.ids_not_increasing and eth.missing_trade_ids is None
    assert "highest id received before it (id 510)" in later


# ------------------------------------------------------------ resolution


def test_a_quiet_symbol_is_closed_by_the_deadline_with_its_count_unknown() -> None:
    """A thin pair can take minutes; the record must not stay open forever."""
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.5)
    feed.tracker.handshake(502.0)
    # Whole seconds, so the boundary itself is what is tested, not rounding.
    feed.subscribe_all(600.0)
    feed.tracker.trade("BTCUSD", 106, 2, 600.0)

    feed.tracker.tick(600.0 + RESOLVE_DEADLINE_SECONDS - 1)
    assert not feed.resolved()
    feed.tracker.tick(600.0 + RESOLVE_DEADLINE_SECONDS)

    event = feed.resolved()[0]
    assert event.resolved_by == "deadline"
    assert event.symbols["ETHUSD"].first_trade_id is None


def test_a_symbol_quiet_across_two_drops_is_flagged_in_the_second() -> None:
    """
    The second record's gap for that symbol covers both drops.

    The total stays right; charging all of it to the second drop without a word
    would skew any per-drop comparison - the before/after this record exists for.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(502.1)
    feed.tracker.trade("BTCUSD", 106, 2, 502.2)

    feed.tracker.connection_ended(FAR_SIDE, 600.0)

    first = feed.resolved()[0]
    assert first.resolved_by == "next_drop"
    assert first.symbols["ETHUSD"].first_trade_id is None
    second = feed.tracker.event
    assert second.symbols["ETHUSD"].spans_previous_outage
    assert not second.symbols["BTCUSD"].spans_previous_outage


def test_a_symbol_the_deadline_left_unknown_is_flagged_in_the_next_drop(
) -> None:
    """
    The same span when the deadline, not the next drop, closed the record.

    Its next gap still starts at a trade from before the first outage; only
    the next-drop path set the flag until the review.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(503.0)
    feed.tracker.trade("BTCUSD", 106, 2, 503.5)
    feed.tracker.tick(503.0 + RESOLVE_DEADLINE_SECONDS)
    assert feed.resolved()[0].resolved_by == "deadline"

    feed.tracker.connection_ended(FAR_SIDE, 2000.0)

    second = feed.tracker.event
    assert second.symbols["ETHUSD"].spans_previous_outage
    assert second.symbols["ETHUSD"].last_trade_id == 500
    assert not second.symbols["BTCUSD"].spans_previous_outage

    feed.tracker.handshake(2002.0)
    feed.subscribe_all(2002.0)
    feed.tracker.trade("BTCUSD", 120, 3, 2003.0)
    feed.tracker.trade("ETHUSD", 600, 3, 2003.0)
    assert feed.resolved()[1].symbols["ETHUSD"].missing_trade_ids == \
        600 - 500 - 1, "the spanning gap is counted, from before both drops"


def test_a_pair_refused_on_the_restoring_connection_is_flagged_next() -> None:
    """
    A refused pair is kept out of the wait, so its first trade is still
    unknown when the record closes - and its next gap covers the whole
    connection it was not subscribed on, hours perhaps.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.1)
    feed.tracker.subscription_answered("trade", "BTCUSD", None, 502.2)
    feed.tracker.subscription_answered(
        "trade", "ETHUSD", "Currency pair not supported", 502.2)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.3)
    feed.tracker.trade("BTCUSD", 106, 2, 502.4)
    assert feed.resolved()[0].resolved_by == "all_symbols_traded"

    feed.tracker.connection_ended(FAR_SIDE, 900.0)

    assert feed.tracker.event.symbols["ETHUSD"].spans_previous_outage
    assert not feed.tracker.event.symbols["BTCUSD"].spans_previous_outage


@pytest.mark.parametrize("closer", ["deadline", "next_drop"])
def test_a_stretch_a_failed_attempt_counted_survives_a_record_closing_first(
        closer) -> None:
    """
    ETHUSD trades on a failed attempt - 510, nine missing since 500 - and then
    not again before its record closes. The next record's gap starts at 500
    and keeps the nine. Until the second review it started at 510 and the nine
    were counted in no record: 89 of 98 missing ids, and the flag's 'covers
    both' untrue.
    """
    feed = Feed().started()
    drop_and_fail_an_attempt(feed, eth_ids=(510,))
    feed.subscribe_all(505.0)
    feed.tracker.trade("BTCUSD", 150, 3, 505.5)
    if closer == "deadline":
        feed.tracker.tick(505.0 + RESOLVE_DEADLINE_SECONDS)
        next_drop = 2000.0
    else:
        next_drop = 600.0
    feed.tracker.connection_ended(FAR_SIDE, next_drop)
    first = feed.resolved()[0]
    assert first.resolved_by == closer
    assert first.symbols["ETHUSD"].missing_trade_ids is None

    feed.tracker.handshake(next_drop + 2)
    feed.subscribe_all(next_drop + 2)
    feed.tracker.trade("BTCUSD", 160, 4, next_drop + 3)
    feed.tracker.trade("ETHUSD", 600, 4, next_drop + 3)

    gap = feed.resolved()[1].symbols["ETHUSD"]
    assert gap.spans_previous_outage
    assert gap.last_trade_id == 500
    assert gap.missing_trade_ids == (510 - 500 - 1) + (600 - 510 - 1)


def test_a_symbol_that_trades_between_two_outages_is_not_flagged() -> None:
    """
    Unknown when the deadline closed its record, ETHUSD trades before the next
    drop - so its next gap starts at that trade and spans nothing.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(503.0)
    feed.tracker.trade("BTCUSD", 106, 2, 503.5)
    feed.tracker.tick(503.0 + RESOLVE_DEADLINE_SECONDS)
    feed.tracker.trade("ETHUSD", 520, 2, 1500.0)

    feed.tracker.connection_ended(FAR_SIDE, 2000.0)

    gap = feed.tracker.event.symbols["ETHUSD"]
    assert not gap.spans_previous_outage
    assert gap.last_trade_id == 520


def test_a_stop_during_an_attempt_that_never_restored_names_no_first_trade(
) -> None:
    """
    Trades arrive on an attempt whose ticker answers are still out, and the
    process stops. That attempt never restored the feed, so its trades are no
    first trade of a restored feed - the rule for a failed attempt.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("trade", symbol, None, 502.1)
    feed.tracker.trade("BTCUSD", 110, 2, 502.2)

    feed.tracker.stop()

    event = feed.resolved()[0]
    assert event.resolved_by == "shutdown"
    assert event.symbols["BTCUSD"].first_trade_id is None
    assert event.symbols["BTCUSD"].missing_trade_ids is None
    assert event.first_trade_after_ms is None


def test_a_stop_resolves_whatever_is_open_once() -> None:
    """Including an outage that never came back - it is still one the process saw."""
    feed = Feed().started()
    feed.tracker.connection_ended(FAR_SIDE, 500.4)

    feed.tracker.stop()
    feed.tracker.stop()

    resolved = feed.resolved()
    assert len(resolved) == 1
    assert resolved[0].resolved_by == "shutdown"
    assert resolved[0].reconnected_at is None


def test_a_connection_that_ends_before_restoration_is_the_same_outage() -> None:
    """One drop is one record however many attempts it takes."""
    feed = Feed().started()
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.attempt_started()
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.1)
    feed.tracker.connection_ended(
        Detection("far_side_close", 1011, "closed during subscribe"), 502.2)
    feed.tracker.attempt_started()
    feed.tracker.handshake(505.0)
    feed.subscribe_all(505.1)

    opened = [event for stage, event in feed.stages if stage == "opened"]
    assert len(opened) == 1
    assert feed.tracker.last_restored.attempts == 2
    assert "1011" in feed.tracker.last_restored.last_failure


def test_a_process_that_never_got_going_records_no_outage() -> None:
    """Startup failures stay log lines: there was no feed to interrupt."""
    feed = Feed()
    feed.tracker.handshake(100.0)
    feed.tracker.connection_ended(FAR_SIDE, 100.5)

    assert feed.stages == []


# -------------------------------------------------------- subscriptions


def test_data_on_a_pair_is_its_confirmation() -> None:
    """
    A slow answer must not hold a working feed hostage.

    Kraken answers in tens of milliseconds; when it does not, the data that
    arrives anyway is the stronger evidence that the subscription works.
    """
    feed = Feed().started()
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.1)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("trade", symbol, None, 502.2)

    assert feed.tracker.data_seen("ticker", SYMBOLS, 502.3) == "restored"


def test_stragglers_are_listed_not_waited_for() -> None:
    """
    Past the deadline the record is restored with the silent pairs named.

    Holding it open would keep the log line and the alert from ever being
    written while Kraken answers slowly.
    """
    feed = Feed().started()
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.1)
    feed.tracker.subscription_answered("trade", "BTCUSD", None, 502.2)
    feed.tracker.data_seen("ticker", ["BTCUSD", "ETHUSD"], 502.3)

    assert feed.tracker.overdue_subscriptions(510.0, 10.0) == []
    assert feed.tracker.overdue_subscriptions(512.2, 10.0) == ["trade ETHUSD"]
    assert feed.tracker.dead_streams() == []
    assert feed.tracker.give_up_waiting(512.2) == "restored"
    assert feed.tracker.last_restored.unconfirmed == ["trade ETHUSD"]


def test_a_stream_that_produced_nothing_is_dead() -> None:
    """No answer, no refusal, no data: that subscription does not exist."""
    feed = Feed().started()
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.1)
    feed.tracker.data_seen("ticker", SYMBOLS, 502.3)

    assert feed.tracker.dead_streams() == ["trade"]


def test_a_refused_pair_is_named_and_not_waited_for() -> None:
    """
    A pair Kraken will never send trades for must not hold the record for
    fifteen minutes - nor count as a pair that simply has not traded yet.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.1)
    feed.tracker.subscription_answered("trade", "BTCUSD", None, 502.2)
    feed.tracker.subscription_answered(
        "trade", "ETHUSD", "Currency pair not supported", 502.2)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.3)
    feed.tracker.trade("BTCUSD", 106, 2, 502.4)

    event = feed.resolved()[0]
    assert event.rejected == ["trade ETHUSD: Currency pair not supported"]
    assert event.resolved_by == "all_symbols_traded"


def test_a_refusal_after_the_deadline_is_still_a_refusal() -> None:
    """
    Kraken refuses a trade pair 11 s after the request - after the deadline
    had listed it as unanswered.

    Dropped, the record said nothing was refused while the log said Kraken
    had, and it stayed open for fifteen minutes waiting for that pair's trades.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    feed.tracker.subscription_answered("trade", "BTCUSD", None, 502.1)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.1)
    assert feed.tracker.give_up_waiting(512.5) == "restored"
    feed.tracker.trade("BTCUSD", 106, 2, 512.6)
    assert not feed.resolved(), "waiting for ETHUSD"

    feed.tracker.subscription_answered(
        "trade", "ETHUSD", "Currency pair not supported", 513.0)

    event = feed.resolved()[0]
    assert event.resolved_by == "all_symbols_traded"
    assert event.rejected == ["trade ETHUSD: Currency pair not supported"]
    assert event.unconfirmed == ["trade ETHUSD"], "true at the deadline too"


def test_a_refusal_for_an_acknowledged_pair_changes_nothing() -> None:
    """
    A refusal for a pair already acknowledged - a duplicate answer, say -
    contradicts what the record knows. Recorded, it named a live subscription
    as refused, and while the feed was still down it could close the record
    as 'all symbols traded' with no trade at all.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("trade", symbol, None, 502.1)
    feed.tracker.subscription_answered(
        "trade", "BTCUSD", "Already subscribed", 502.2)
    assert feed.tracker.event is not None and not feed.resolved()

    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.3)
    feed.tracker.subscription_answered(
        "trade", "ETHUSD", "Already subscribed", 502.4)
    assert not feed.resolved(), "waiting for both symbols' trades"
    feed.tracker.trade("BTCUSD", 106, 2, 502.5)
    feed.tracker.trade("ETHUSD", 506, 2, 502.5)

    event = feed.resolved()[0]
    assert event.rejected == []
    assert event.symbols["ETHUSD"].first_trade_id == 506


def test_a_named_late_refusal_refuses_that_pair_only() -> None:
    """Two trade pairs listed at the deadline; Kraken refuses one by name."""
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.1)
    assert feed.tracker.give_up_waiting(512.5) == "restored"

    feed.tracker.subscription_answered(
        "trade", "ETHUSD", "Currency pair not supported", 513.0)
    feed.tracker.trade("BTCUSD", 106, 2, 513.5)

    event = feed.resolved()[0]
    assert event.rejected == ["trade ETHUSD: Currency pair not supported"]
    assert event.resolved_by == "all_symbols_traded"


def test_a_late_refusal_without_a_symbol_spares_a_pair_that_delivered_data(
) -> None:
    """
    The deadline listed trade ETHUSD, its data came after all, and then a
    refusal naming no symbol. Pinned on ETHUSD it would name a pair that is
    trading as refused.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    feed.tracker.subscription_answered("trade", "BTCUSD", None, 502.1)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.1)
    feed.tracker.give_up_waiting(512.5)
    feed.tracker.data_seen("trade", ["ETHUSD"], 513.0)
    feed.tracker.trade("ETHUSD", 520, 2, 513.0)

    feed.tracker.subscription_answered(
        "trade", None, "Exceeded msg rate", 513.5)

    assert feed.tracker.last_restored.rejected == []


def test_a_stream_refused_outright_was_confirmed_at_no_time() -> None:
    """
    Every ticker pair refused: the record named the refusal's moment as the
    ticker's confirmation. Settled is not confirmed.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.message(500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("trade", symbol, None, 502.1)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered(
            "ticker", symbol, "Not available", 502.7)

    assert feed.tracker.last_restored.confirmed_after_ms == {"trade": 2100.0}
    assert feed.tracker.confirmation_text() == (
        "trade 2/2 after 100 ms, ticker 0/2 (2 refused)")


def test_the_confirmation_line_counts_a_refusal_as_refused() -> None:
    """
    'trade 2/2' was written while Kraken had refused one of the two - in the
    line a session reads after a deploy to see that the feed is subscribed.
    The time is the last confirmation's, 30 ms, not the refusal's 40.
    """
    refused = Feed()
    refused.tracker.handshake(100.0)
    refused.tracker.subscriptions_sent(100.0)
    refused.tracker.subscription_answered("trade", "BTCUSD", None, 100.03)
    refused.tracker.subscription_answered(
        "trade", "ETHUSD", "Currency pair not supported", 100.04)
    for symbol in SYMBOLS:
        refused.tracker.subscription_answered("ticker", symbol, None, 100.05)

    assert refused.tracker.confirmation_text() == (
        "trade 1/2 after 30 ms (1 refused), ticker 2/2 after 50 ms")

    unanswered = Feed()
    unanswered.tracker.handshake(100.0)
    unanswered.tracker.subscriptions_sent(100.0)
    unanswered.tracker.subscription_answered("trade", "BTCUSD", None, 100.03)
    unanswered.tracker.data_seen("ticker", SYMBOLS, 100.2)
    unanswered.tracker.give_up_waiting(110.0)

    assert unanswered.tracker.confirmation_text() == (
        "trade 1/2 (1 unanswered), ticker 2/2 after 200 ms")


def test_without_a_trade_stream_nothing_is_waited_for() -> None:
    """A ticker-only collector has no trades to wait for."""
    feed = Feed(streams=["ticker"])
    feed.tracker.handshake(100.0)
    feed.subscribe_all(100.1)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(502.1)

    assert feed.resolved()[0].resolved_by == "all_symbols_traded"

    feed.tracker.connection_ended(FAR_SIDE, 600.0)
    assert not any(gap.spans_previous_outage
                   for gap in feed.tracker.event.symbols.values()), (
        "no trade stream, no first trade expected - nothing spans")


def test_an_owner_that_fails_does_not_stop_the_record() -> None:
    """A diagnostic that throws must never be what ends the feed's bookkeeping."""
    feed = Feed().started()
    calls = []

    def failing(stage, event) -> None:
        calls.append(stage)
        raise RuntimeError("owner broke")

    feed.tracker.set_callback(failing)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.subscribe_all(502.1)

    assert calls[:2] == ["opened", "restored"]


def test_a_pair_acknowledged_after_the_deadline_is_not_refused_later(
) -> None:
    """
    The deadline listed trade ETHUSD; its acknowledgement came late, then a
    contradicting refusal. Like late data, a late acknowledgement means the
    pair is subscribed - the refusal must not name a live subscription.
    """
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    feed.tracker.subscription_answered("trade", "BTCUSD", None, 502.1)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.1)
    feed.tracker.give_up_waiting(512.5)
    feed.tracker.subscription_answered("trade", "ETHUSD", None, 513.0)

    feed.tracker.subscription_answered(
        "trade", "ETHUSD", "Already subscribed", 513.1)
    feed.tracker.subscription_answered("trade", None, "Already subscribed",
                                       513.2)
    feed.tracker.trade("BTCUSD", 106, 2, 513.5)

    assert not feed.resolved(), "still waiting for ETHUSD's first trade"
    assert feed.tracker.last_restored.rejected == []


def test_a_late_refusal_is_recorded_once() -> None:
    """A pair refused late is no longer listed: a second refusal adds nothing."""
    feed = Feed().started()
    feed.tracker.trade("BTCUSD", 100, 1, 500.0)
    feed.tracker.trade("ETHUSD", 500, 1, 500.0)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)
    feed.tracker.handshake(502.0)
    feed.tracker.subscriptions_sent(502.0)
    feed.tracker.subscription_answered("trade", "BTCUSD", None, 502.1)
    for symbol in SYMBOLS:
        feed.tracker.subscription_answered("ticker", symbol, None, 502.1)
    feed.tracker.give_up_waiting(512.5)

    feed.tracker.subscription_answered(
        "trade", "ETHUSD", "Currency pair not supported", 513.0)
    feed.tracker.subscription_answered(
        "trade", None, "Currency pair not supported", 513.1)

    assert feed.tracker.last_restored.rejected == [
        "trade ETHUSD: Currency pair not supported"]


def test_a_replay_carried_into_the_next_record_keeps_its_flag() -> None:
    """
    A replay on a failed attempt leaves ETHUSD's count unknown, and the record
    closes before ETHUSD trades again. The next record carries the unknown
    count - and the flag that explains it, or it would show a null between
    two known ids with no reason, while the record that had one left the ring.
    """
    feed = Feed().started()
    drop_and_fail_an_attempt(feed, eth_ids=(495,))
    feed.subscribe_all(505.0)
    feed.tracker.trade("BTCUSD", 150, 3, 505.5)
    feed.tracker.connection_ended(FAR_SIDE, 600.0)
    feed.tracker.handshake(602.0)
    feed.subscribe_all(602.0)
    feed.tracker.trade("BTCUSD", 160, 4, 603.0)
    feed.tracker.trade("ETHUSD", 600, 4, 603.0)

    gap = feed.resolved()[1].symbols["ETHUSD"]
    assert gap.spans_previous_outage
    assert gap.missing_trade_ids is None
    assert gap.ids_not_increasing


def test_a_failing_owner_is_contained_without_the_project_logging(
        monkeypatch) -> None:
    """
    The guard around the owner must not raise itself. Without the project's
    logging set up, fetching its logger raises - and the tracker raised from
    inside the guard meant to contain the owner's failure.
    """
    from python.collectors.kraken import outage_tracker

    def not_set_up(name):
        raise RuntimeError("Logging not initialized")

    monkeypatch.setattr(outage_tracker, "get_collector_logger", not_set_up)
    feed = Feed().started()

    def failing(stage, event) -> None:
        raise ValueError("owner broke")

    feed.tracker.set_callback(failing)
    feed.tracker.connection_ended(FAR_SIDE, 500.4)

    assert feed.tracker.event is not None
