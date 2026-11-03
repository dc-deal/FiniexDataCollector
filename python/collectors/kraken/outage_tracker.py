"""
FiniexDataCollector - What one interruption of the Kraken feed cost, measured
The timeline of an outage on one clock, from the last message before it to the
first trade per symbol after it.

Until 2026-10-08 a reconnect was recorded from the moment it was NOTICED to the
moment the socket handshake completed - before anything was subscribed again -
by subtracting two wall-clock readings. Kraken had usually ended the connection
long before: in 136 of the 137 drops between 2026-09-20 and 2026-10-08 the socket
was already closing when the 10 s silence watchdog found it, and a reconnect
recorded as 2.2 s had cost about 12 s of trades.

This module owns that timeline. It holds no socket and no event loop - the
client hands it explicit monotonic readings - which is what lets a test drive it
with plain numbers and what keeps a diagnostic from ever touching the feed.

What it does NOT do: decide when to reconnect, or count anything it did not
measure. A trade-id gap is an upper bound and is called one; a symbol that has
not traded again by the deadline keeps an unknown, not a zero.

Location: python/collectors/kraken/outage_tracker.py
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from websockets.exceptions import ConnectionClosed

from python.types.collector_stats import ReconnectEvent, TradeIdGap
from python.utils.logging_setup import describe_exception, get_collector_logger

# The reason websockets puts on the close frame it sends when a ping goes
# unanswered - the same text in the legacy and the asyncio implementation.
KEEPALIVE_REASON = "keepalive ping timeout"

# The code websockets reports for a close frame that carried none. RFC 6455
# reserves it for exactly that and forbids it on the wire - the library rejects
# a frame that sends it - so, like the 1006 of a connection that ended without
# any frame, it is the library's stand-in and never a code Kraken sent.
NO_STATUS_RCVD = 1005

# How long a restored outage waits for the first trade of every symbol. DASHUSD
# averages about 1.4 trades a minute, so a thin pair can take several minutes;
# past this the record is closed with that symbol's first trade unknown.
# Measured on the monotonic clock, like every duration here.
RESOLVE_DEADLINE_SECONDS = 15 * 60

OutageCallback = Callable[[str, ReconnectEvent], None]


@dataclass(frozen=True)
class Detection:
    """
    How one connection ended.

    Attributes:
        kind: far_side_close, connection_lost, keepalive_timeout,
            library_failure, silence_watchdog, subscribe_unanswered,
            subscribe_send_failed or unexpected
        close_code: Code of the close frame the far side sent; None if no frame
            arrived or it carried no code - never one the library made up, 1006
            for a missing frame or 1005 for a missing code
        close_reason: Text of that frame
        exception: What ended the connection, as text
        cause: The error chained under it, as text
    """
    kind: str
    close_code: Optional[int] = None
    close_reason: str = ""
    exception: str = ""
    cause: str = ""

    def describe(self) -> str:
        """
        One phrase for a log line.

        Returns:
            For instance "far_side_close (code 1001 'going away')"
        """
        if self.close_code is not None:
            reason = f" {self.close_reason!r}" if self.close_reason else ""
            return f"{self.kind} (code {self.close_code}{reason})"
        detail = self.cause or self.exception
        return f"{self.kind}: {detail}" if detail else self.kind


@dataclass(frozen=True)
class _Unfinished:
    """
    A symbol whose first trade after an outage was still unknown when the
    record closed, carried into the next record so that its gap covers both.

    Attributes:
        last_trade_id: Its last trade before the first outage the carry spans
        last_trade_time_msc: That trade's exchange time
        rearmed: A failed attempt in those outages had delivered trades for it
        resume_from: The highest id such an attempt delivered
        missing_before: What the stretches before it counted; None if unknown
        ids_not_increasing: A replay happened in those outages - the reason a
            count it carries is unknown
    """
    last_trade_id: Optional[int]
    last_trade_time_msc: Optional[int]
    rearmed: bool = False
    resume_from: Optional[int] = None
    missing_before: Optional[int] = None
    ids_not_increasing: bool = False


def close_code_of(frame: Any) -> Optional[int]:
    """
    The code a received close frame carried.

    Args:
        frame: A close frame as either websockets implementation parsed it

    Returns:
        The code, or None when the frame carried none - never the 1005 the
        library substitutes for a missing one
    """
    code = getattr(frame, "code", None)
    if code is None or code == NO_STATUS_RCVD:
        return None
    return int(code)


def classify_close(error: ConnectionClosed) -> Detection:
    """
    Say how a websocket connection ended, from the frames it exchanged.

    Sorted by the frames, never by the exception class: a close Kraken
    initiated arrives as ConnectionClosedOK or as ConnectionClosedError
    depending on its code, and both are the far side closing.

    Args:
        error: The ConnectionClosed the library raised

    Returns:
        The detection, with the close code and reason when a frame arrived
    """
    exception = describe_exception(error)
    # The asyncio implementation chains no cause for a plain FIN; the legacy
    # one chains an IncompleteReadError. describe_exception(None) would print
    # 'NoneType: None', which is a cause nobody had.
    cause = (describe_exception(error.__cause__)
             if error.__cause__ is not None else "")

    received = error.rcvd
    if received is not None:
        return Detection("far_side_close", close_code_of(received),
                         received.reason, exception, cause)

    sent = error.sent
    if sent is not None and sent.reason == KEEPALIVE_REASON:
        return Detection("keepalive_timeout", None, "", exception, cause)
    if sent is not None:
        return Detection("library_failure", None, "", exception, cause)

    return Detection("connection_lost", None, "", exception, cause)


class OutageTracker:
    """
    Follows the feed and turns each interruption into one ReconnectEvent.

    A record opens when a connection ends after the feed had been fully
    subscribed at least once - a process that never got going has no outage to
    measure, only log lines. It is 'restored' when every (stream, symbol) pair
    has been acknowledged, refused, or has delivered data, or when the
    subscription deadline passed with the stragglers listed. It is 'resolved'
    once nothing more will be added to it.
    """

    def __init__(self, symbols: List[str], streams: List[str],
                 wall: Optional[Callable[[], datetime]] = None):
        """
        Args:
            symbols: Normalized symbols, e.g. "BTCUSD"
            streams: Subscribed streams, e.g. ["trade", "ticker"]
            wall: Wall-clock source, injectable for tests
        """
        self._symbols = list(symbols)
        self._streams = list(streams)
        self._wall = wall or (lambda: datetime.now(timezone.utc))
        self._callback: Optional[OutageCallback] = None

        # The feed, across connections
        self._last_message_mono: Optional[float] = None
        self._last_message_wall: Optional[datetime] = None
        # The HIGHEST id seen per symbol, with its exchange time: a reordered
        # delivery must not make the next gap look larger than it is.
        self._last_trade: Dict[str, Tuple[Optional[int], Optional[int]]] = {}
        self._restored_once = False
        # Symbols whose first trade after an outage was still unknown when its
        # record closed - by the next drop, the deadline, or a refusal - and
        # that have not traded since. Their next gap starts at their last trade
        # before that outage and keeps what was counted in it, so it covers
        # both outages and nothing counted is lost between the two records.
        self._unfinished: Dict[str, _Unfinished] = {}

        # The current connection
        self._handshake_mono: Optional[float] = None
        self._sent_mono: Optional[float] = None
        self._pending: Dict[str, Set[str]] = {}
        self._alive_streams: Set[str] = set()
        self._confirmed_mono: Dict[str, float] = {}
        self._rejected: List[str] = []
        self._unconfirmed: List[str] = []
        self._trade_refused: Set[str] = set()
        # Pairs the deadline listed as unanswered and that have neither
        # delivered data nor been refused since - the only pairs a refusal
        # arriving after the deadline may still apply to.
        self._listed: Set[Tuple[str, str]] = set()
        self._restored_mono: Optional[float] = None
        self._connection_id = ""
        self._system = ""

        # The open outage
        self._event: Optional[ReconnectEvent] = None
        self._t0: Optional[float] = None
        self._phase = "idle"          # idle | down | awaiting_trades
        self._awaiting: Set[str] = set()
        self._last_restored: Optional[ReconnectEvent] = None
        # Per symbol a failed attempt delivered trades for - in this outage, or
        # in an earlier one whose record closed before the symbol traded again:
        # the highest id it delivered, where the next stretch of missing ids
        # starts, and what the stretches before it counted (None when one was
        # unknown). Set afresh whenever a record opens, read only while one is.
        self._resume_from: Dict[str, Optional[int]] = {}
        self._missing_before: Dict[str, Optional[int]] = {}

    def set_callback(self, callback: OutageCallback) -> None:
        """
        Register who hears about each stage.

        Args:
            callback: Called with ('opened' | 'restored' | 'resolved', event)
        """
        self._callback = callback

    # ------------------------------------------------------------------ feed

    def message(self, mono: float) -> None:
        """
        Note that a message of any kind arrived.

        Args:
            mono: Monotonic reading taken when it was read
        """
        self._last_message_mono = mono
        self._last_message_wall = self._wall()

    def status(self, connection_id: str, system: str) -> None:
        """
        Keep what Kraken's status message said about this connection.

        Args:
            connection_id: Kraken's id for the connection, as text
            system: The system state it announced (online, maintenance, ...)
        """
        if connection_id:
            self._connection_id = connection_id
        if system:
            self._system = system

    def data_seen(self, stream: str, symbols: List[str], mono: float) -> str:
        """
        Market data arrived on a stream; for a pair still waiting for its
        acknowledgement, the data is the confirmation.

        A slow acknowledgement must not turn a working feed into an outage, and
        data on a pair is stronger evidence than an answer about it.

        Args:
            stream: 'trade' or 'ticker'
            symbols: Normalized symbols the message carried
            mono: Monotonic reading of the message

        Returns:
            'restored' or 'startup' when this message completed the connection's
            subscriptions, else ''
        """
        self._alive_streams.add(stream)
        pending = self._pending.get(stream)
        if not pending:
            if self._listed:
                # Data on a pair the deadline listed: it is subscribed after all,
                # and a refusal without a symbol must not be pinned on it.
                self._listed -= {(stream, symbol) for symbol in symbols}
            return ""
        completed = ""
        for symbol in symbols:
            if symbol in pending:
                completed = self._answered(
                    stream, symbol, None, mono) or completed
        return completed

    def trade(self, symbol: str, trade_id: Optional[int],
              time_msc: Optional[int], mono: float) -> Optional[str]:
        """
        Note a trade, and fill the open record if it is the symbol's first one
        since the drop.

        Args:
            symbol: Normalized symbol
            trade_id: Kraken's id for the trade, None if it carried none
            time_msc: Its exchange time
            mono: Monotonic reading of the message it came in

        Returns:
            A warning to log when the ids did not increase across the drop,
            else None
        """
        warning = None
        event = self._event
        if event is not None and symbol in event.symbols:
            gap = event.symbols[symbol]
            if gap.first_trade_after_ms is None:
                warning = self._first_trade_after(event, symbol, gap,
                                                  trade_id, time_msc, mono)

        previous = self._last_trade.get(symbol, (None, None))[0]
        if trade_id is not None:
            if previous is None or trade_id > previous:
                self._last_trade[symbol] = (trade_id, time_msc)
        elif symbol not in self._last_trade:
            self._last_trade[symbol] = (None, time_msc)
        # Traded between two records: its next gap starts here. What the
        # earlier record left open for it is counted nowhere - that record has
        # been written, and this trade came after it closed.
        self._unfinished.pop(symbol, None)

        if self._phase == "awaiting_trades":
            self._resolve_if_complete()
        return warning

    def _first_trade_after(self, event: ReconnectEvent, symbol: str,
                           gap: TradeIdGap, trade_id: Optional[int],
                           time_msc: Optional[int], mono: float
                           ) -> Optional[str]:
        """Fill one symbol's first trade after the drop."""
        gap.first_trade_id = trade_id
        gap.first_trade_time_msc = time_msc
        gap.first_trade_after_ms = self._since_t0_ms(mono)
        if event.first_trade_after_ms is None:
            event.first_trade_after_ms = gap.first_trade_after_ms

        # The stretch this trade closes starts at the last trade before the
        # drop - or, after a failed attempt delivered trades, at the highest id
        # that attempt delivered, with the stretches before it already counted.
        start = (self._resume_from[symbol] if symbol in self._resume_from
                 else gap.last_trade_id)
        counted = self._missing_before.get(symbol, 0)
        if start is None or trade_id is None:
            return None
        if trade_id > start:
            if counted is not None:
                gap.missing_trade_ids = counted + trade_id - start - 1
            return None

        # Kept for the rest of the outage, also when a failed attempt is
        # measured again: a replay happened in it, and the count stays unknown.
        gap.ids_not_increasing = True
        return (f"{symbol}: the first trade after the reconnect (id {trade_id}) "
                f"is not above the highest id received before it (id {start})")

    # ------------------------------------------------------------ connection

    def connection_ended(self, detection: Detection, mono: float) -> None:
        """
        A connection is over: open an outage, or extend the one in progress.

        Args:
            detection: How it ended
            mono: Monotonic reading at detection
        """
        if self._phase == "down" and self._event is not None:
            # A connection that ended before the feed was restored is a failed
            # attempt of the same outage, not a new one.
            self._event.last_failure = detection.describe()
            self._measure_again_after_failed_attempt()
            self._reset_connection()
            return

        if self._phase == "awaiting_trades" and self._event is not None:
            self._resolve("next_drop")

        if not self._restored_once:
            self._reset_connection()
            return

        t0 = self._last_message_mono
        if t0 is None:
            t0 = self._handshake_mono if self._handshake_mono is not None else mono

        symbols = {}
        resume_from: Dict[str, Optional[int]] = {}
        missing_before: Dict[str, Optional[int]] = {}
        for symbol in self._symbols:
            unfinished = self._unfinished.get(symbol)
            if unfinished is None:
                last_id, last_time = self._last_trade.get(symbol, (None, None))
                symbols[symbol] = TradeIdGap(
                    last_trade_id=last_id, last_trade_time_msc=last_time)
                continue
            symbols[symbol] = TradeIdGap(
                last_trade_id=unfinished.last_trade_id,
                last_trade_time_msc=unfinished.last_trade_time_msc,
                ids_not_increasing=unfinished.ids_not_increasing,
                spans_previous_outage=True)
            if unfinished.rearmed:
                resume_from[symbol] = unfinished.resume_from
                missing_before[symbol] = unfinished.missing_before
        self._unfinished = {}

        event = ReconnectEvent(
            timestamp=self._last_message_wall or self._wall(),
            reconnected_at=None,
            duration_seconds=None,
            reason=detection.kind,
            close_code=detection.close_code,
            close_reason=detection.close_reason,
            exception=detection.exception,
            cause=detection.cause,
            detected_after_ms=round((mono - t0) * 1000, 1),
            connection_age_s=(round(mono - self._restored_mono, 1)
                              if self._restored_mono is not None else None),
            kraken_connection_id=self._connection_id,
            kraken_system=self._system,
            symbols=symbols)

        self._event = event
        self._t0 = t0
        self._phase = "down"
        self._resume_from = resume_from
        self._missing_before = missing_before
        self._reset_connection()
        self._notify("opened", event)

    def _measure_again_after_failed_attempt(self) -> None:
        """
        Re-open the first trade of every symbol a failed attempt traded on.

        Those trades are real, but the attempt did not restore the feed, and
        the ids Kraken published between its end and the next connection are
        missing too. Left as they were, the record closed at the restoration
        with that second stretch never counted - an 'upper bound' below the
        truth - and named as first trade one the restored feed never delivered.
        In the review's reproduction (2026-10-09) the record said 9 missing per
        symbol where 48 were never received.
        """
        event = self._event
        for symbol, gap in event.symbols.items():
            if gap.first_trade_after_ms is None:
                continue
            self._resume_from[symbol] = self._last_trade.get(
                symbol, (None, None))[0]
            self._missing_before[symbol] = gap.missing_trade_ids
            gap.first_trade_id = None
            gap.first_trade_time_msc = None
            gap.first_trade_after_ms = None
            gap.missing_trade_ids = None
            # In 'down' every first trade came from a failed attempt, so after
            # this none of the record's first trades stand.
            event.first_trade_after_ms = None

    def teardown(self, milliseconds: float) -> None:
        """
        Note how long dropping the old socket took, once per outage: about 0,
        because a connection given up on is aborted, not closed politely.

        Args:
            milliseconds: Time spent dropping it
        """
        if self._event is not None and self._event.teardown_ms is None:
            self._event.teardown_ms = round(milliseconds, 1)

    def backoff(self, seconds: float) -> None:
        """
        Note a pause before the next attempt, as measured around the sleep.

        Args:
            seconds: How long it was
        """
        if self._phase == "down" and self._event is not None:
            self._event.backoff_s = round(self._event.backoff_s + seconds, 3)

    def attempt_started(self) -> None:
        """Note that a connection attempt begins."""
        if self._phase == "down" and self._event is not None:
            self._event.attempts += 1

    def attempt_failed(self, description: str) -> None:
        """
        Note that an attempt failed before the feed was restored.

        Args:
            description: What went wrong, as text
        """
        if self._phase == "down" and self._event is not None:
            self._event.last_failure = description
        self._reset_connection()

    def handshake(self, mono: float) -> None:
        """
        A new connection is open.

        Args:
            mono: Monotonic reading at the completed handshake
        """
        self._reset_connection()
        self._handshake_mono = mono
        if self._phase == "down" and self._event is not None:
            self._event.handshake_after_ms = self._since_t0_ms(mono)

    def subscriptions_sent(self, mono: float) -> None:
        """
        Every subscription request on this connection has been sent.

        Args:
            mono: Monotonic reading after the last send
        """
        self._sent_mono = mono
        self._pending = {stream: set(self._symbols)
                         for stream in self._streams}
        self._alive_streams = set()
        self._confirmed_mono = {}
        self._rejected = []
        self._unconfirmed = []
        self._trade_refused = set()
        self._listed = set()

    def subscription_answered(self, stream: str, symbol: Optional[str],
                              error: Optional[str], mono: float) -> str:
        """
        Kraken answered a subscription for one pair, or refused a whole stream.

        Args:
            stream: The stream the request was for, matched by req_id
            symbol: The normalized symbol the answer names; None when the
                refusal names none, which refuses every pair still waiting
            error: Kraken's error text when it refused, None on success
            mono: Monotonic reading of the answer

        Returns:
            'restored' or 'startup' when this answer completed the connection's
            subscriptions, else ''
        """
        pending = self._pending.get(stream)
        if not pending:
            if error and self._sent_mono is not None:
                self._refused_after_deadline(stream, symbol, error)
            elif not error and symbol is not None:
                # Acknowledged after the deadline: subscribed after all, like a
                # pair whose data came late - a refusal must not be pinned on it.
                self._listed.discard((stream, symbol))
            return ""
        self._alive_streams.add(stream)
        targets = sorted(pending) if symbol is None else [symbol]
        completed = ""
        for target in targets:
            if target in pending:
                completed = self._answered(
                    stream, target, error, mono) or completed
        return completed

    def _answered(self, stream: str, symbol: str, error: Optional[str],
                  mono: float) -> str:
        """One pair is settled; restore when it was the last one."""
        pending = self._pending[stream]
        pending.discard(symbol)
        if error:
            self._rejected = self._rejected + [f"{stream} {symbol}: {error}"]
            if stream == "trade":
                self._trade_refused.add(symbol)
        else:
            # The latest confirmation, never a refusal: a stream Kraken refused
            # outright was confirmed at no time and is left out.
            self._confirmed_mono[stream] = mono
        if any(self._pending.get(name) for name in self._streams):
            return ""
        return self._restored(mono)

    def _refused_after_deadline(self, stream: str, symbol: Optional[str],
                                error: str) -> None:
        """
        A refusal that came after the deadline had listed the pair as unanswered.

        Still a refusal. Dropped, the record said nothing was refused while the
        log said Kraken had, and it held itself open for the full fifteen
        minutes waiting for trades Kraken had said it would not send.

        Only a pair the deadline left unanswered, and that has neither
        delivered data nor been refused since: a refusal for a pair that is
        acknowledged or trading contradicts what the record already knows, and
        recorded it would name a live subscription as refused. The client has
        logged it either way.

        Args:
            stream: The stream the refusal is for
            symbol: The pair it names; None refuses every pair of the stream
                still listed as unanswered
            error: Kraken's error text
        """
        listed = sorted(name for kind, name in self._listed if kind == stream)
        targets = listed if symbol is None else (
            [symbol] if symbol in listed else [])
        if not targets:
            return
        self._listed -= {(stream, target) for target in targets}
        self._rejected = self._rejected + [f"{stream} {target}: {error}"
                                           for target in targets]
        if stream == "trade":
            self._trade_refused.update(targets)

        # A listed pair exists only after the deadline restored the feed, so an
        # open record here is always one waiting for its first trades.
        event = self._event
        if event is None:
            return
        # Rebound, not appended: /v1/status reads the record from a worker
        # thread while this runs on the event loop.
        event.rejected = list(self._rejected)
        if stream == "trade":
            self._awaiting = self._awaiting - set(targets)
        self._resolve_if_complete()

    def overdue_subscriptions(self, mono: float, deadline: float) -> List[str]:
        """
        Pairs still unanswered past a deadline on this connection.

        Args:
            mono: Current monotonic reading
            deadline: Seconds after sending

        Returns:
            'stream SYMBOL' per pair that neither answered nor delivered data,
            or an empty list while within the deadline or once all settled
        """
        if (self._sent_mono is None or self._restored_mono is not None
                or mono - self._sent_mono < deadline):
            return []
        return [f"{stream} {symbol}"
                for stream in self._streams
                for symbol in sorted(self._pending.get(stream, set()))]

    def dead_streams(self) -> List[str]:
        """
        Streams on which nothing at all came back: no answer, no refusal, no data.

        Returns:
            Stream names; empty when every stream produced something
        """
        return [stream for stream in self._streams
                if stream not in self._alive_streams]

    def give_up_waiting(self, mono: float) -> str:
        """
        Take the feed as restored with the stragglers listed.

        Their data counts whenever it comes; holding the record open for them
        would keep the alert and the log line from ever being written while
        Kraken answers slowly.

        Args:
            mono: Current monotonic reading

        Returns:
            'restored' or 'startup', or '' when nothing was waiting
        """
        if self._restored_mono is not None or self._sent_mono is None:
            return ""
        self._unconfirmed = [f"{stream} {symbol}"
                             for stream in self._streams
                             for symbol in sorted(self._pending.get(stream, set()))]
        self._listed = {(stream, symbol) for stream in self._streams
                        for symbol in self._pending.get(stream, set())}
        for stream in self._streams:
            self._pending[stream] = set()
        return self._restored(mono)

    def _restored(self, mono: float) -> str:
        """Every pair on this connection is settled."""
        self._restored_mono = mono
        first_restoration = not self._restored_once
        self._restored_once = True
        if first_restoration or self._phase != "down" or self._event is None:
            return "startup" if first_restoration else ""

        event = self._event
        event.reconnected_at = self._wall()
        event.duration_seconds = round(mono - self._t0, 3)
        event.confirmed_after_ms = {stream: self._since_t0_ms(at)
                                    for stream, at in self._confirmed_mono.items()}
        event.rejected = list(self._rejected)
        event.unconfirmed = list(self._unconfirmed)
        self._awaiting = ({symbol for symbol in self._symbols
                           if symbol not in self._trade_refused}
                          if "trade" in self._streams else set())
        self._phase = "awaiting_trades"
        self._last_restored = event
        self._notify("restored", event)
        self._resolve_if_complete()
        return "restored"

    # ------------------------------------------------------------- questions

    def tick(self, mono: float) -> None:
        """
        Close a restored record whose slowest symbol has not traded in time.

        Args:
            mono: Current monotonic reading
        """
        if (self._phase == "awaiting_trades" and self._restored_mono is not None
                and mono - self._restored_mono >= RESOLVE_DEADLINE_SECONDS):
            self._resolve("deadline")

    def stop(self) -> None:
        """The process is stopping: hand over whatever is open."""
        if self._event is None:
            return
        if self._phase == "down":
            # The attempt in progress never restored the feed, so its trades
            # are no first trade of the restored feed - the same rule as for
            # an attempt that failed.
            self._measure_again_after_failed_attempt()
        self._resolve("shutdown")

    def restored_for(self, mono: float) -> Optional[float]:
        """
        How long the current connection has been fully subscribed.

        Args:
            mono: Current monotonic reading

        Returns:
            Seconds since restoration, None while not restored
        """
        if self._restored_mono is None:
            return None
        return mono - self._restored_mono

    def confirmation_text(self) -> str:
        """
        How the current connection's subscriptions were settled.

        Returns:
            For instance 'trade 9/9 after 45 ms, ticker 9/9 after 135 ms', or
            'trade 8/9 after 45 ms (1 refused)' - a refused pair is settled but
            not confirmed, and counting it as confirmed made the line read
            'trade 2/2' while Kraken had refused one of the two
        """
        parts = []
        total = len(self._symbols)
        refused_pairs = {tuple(entry.split(":", 1)[0].split(" ", 1))
                         for entry in self._rejected}
        unanswered_pairs = {tuple(entry.split(" ", 1))
                            for entry in self._unconfirmed} - refused_pairs
        for stream in self._streams:
            refused = sum(1 for name, _ in refused_pairs if name == stream)
            unanswered = sum(1 for name, _ in unanswered_pairs
                             if name == stream)
            at = self._confirmed_mono.get(stream)
            after = (f" after {(at - self._sent_mono) * 1000:.0f} ms"
                     if at is not None and self._sent_mono is not None
                     and not unanswered else "")
            notes = [f"{count} {label}" for count, label in
                     ((refused, "refused"), (unanswered, "unanswered")) if count]
            suffix = f" ({', '.join(notes)})" if notes else ""
            parts.append(f"{stream} {total - refused - unanswered}/{total}"
                         f"{after}{suffix}")
        return ", ".join(parts)

    @property
    def event(self) -> Optional[ReconnectEvent]:
        """The open outage record, if any."""
        return self._event

    @property
    def last_restored(self) -> Optional[ReconnectEvent]:
        """The record most recently restored, for the line that announces it."""
        return self._last_restored

    # -------------------------------------------------------------- internal

    def _resolve_if_complete(self) -> None:
        """Resolve once every symbol expected to trade has traded."""
        event = self._event
        if event is None:
            return
        if all(event.symbols[symbol].first_trade_after_ms is not None
               for symbol in self._awaiting):
            self._resolve("all_symbols_traded")

    def _since_t0_ms(self, mono: float) -> Optional[float]:
        """Milliseconds from the outage's last message to `mono`."""
        if self._t0 is None:
            return None
        return round((mono - self._t0) * 1000, 1)

    def _reset_connection(self) -> None:
        """Forget everything that belonged to the connection that ended."""
        self._handshake_mono = None
        self._sent_mono = None
        self._pending = {}
        self._alive_streams = set()
        self._confirmed_mono = {}
        self._rejected = []
        self._unconfirmed = []
        self._trade_refused = set()
        self._listed = set()
        self._restored_mono = None
        self._connection_id = ""
        self._system = ""

    def _resolve(self, resolved_by: str) -> None:
        """Close the open record and hand it over for the last time."""
        event = self._event
        if event is None:
            return
        event.resolved_by = resolved_by
        # Whatever closed the record - the next drop, the deadline, or a
        # refusal that kept a pair out of the wait - a symbol without its first
        # trade carries its last trade from before this outage, and what a
        # failed attempt of it counted, into the next record.
        # Without a trade stream no first trade is ever expected, so nothing
        # is unfinished: every symbol would be flagged in every later record.
        for symbol, gap in event.symbols.items():
            if gap.first_trade_after_ms is not None \
                    or "trade" not in self._streams:
                continue
            self._unfinished[symbol] = _Unfinished(
                gap.last_trade_id, gap.last_trade_time_msc,
                rearmed=symbol in self._resume_from,
                resume_from=self._resume_from.get(symbol),
                missing_before=self._missing_before.get(symbol),
                ids_not_increasing=gap.ids_not_increasing)
        self._event = None
        self._t0 = None
        self._phase = "idle"
        self._awaiting = set()
        self._notify("resolved", event)

    def _notify(self, stage: str, event: ReconnectEvent) -> None:
        """
        Tell the owner about a stage, never letting its failure end the feed.

        Args:
            stage: 'opened', 'restored' or 'resolved'
            event: The record
        """
        if self._callback is None:
            return
        try:
            self._callback(stage, event)
        except Exception as error:
            # Fetched here rather than held, and with a fallback: the module
            # stays usable without the project's logging set up, which raises
            # rather than returning a logger - and this guard must not raise.
            try:
                logger: Any = get_collector_logger("kraken")
            except RuntimeError:
                logger = logging.getLogger(__name__)
            logger.error(f"Outage record could not be handed over at "
                         f"'{stage}': {describe_exception(error)}")
