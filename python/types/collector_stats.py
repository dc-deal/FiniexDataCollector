"""
FiniexDataCollector - Collector Statistics Types
Type definitions for real-time collection monitoring.

Location: python/types/collector_stats.py
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict

from python.types.log_level import ERROR, LogLevel

# Dates the per-day outage count keeps - the most recent dates that had an
# outage, which on a feed dropping several times a day is two weeks.
RECONNECT_DAYS_KEPT = 14

# How far back the uncapped outage times reach: the week a report counts, and a
# day of margin for a report that runs late.
OUTAGE_TIMES_KEPT = timedelta(days=8)


@dataclass
class SymbolStats:
    """
    Real-time statistics for a single symbol.

    Attributes:
        symbol: Trading symbol (e.g., "BTCUSD")
        current_file_ticks: Ticks in current file (resets on rotation)
        last_bid: Last bid price
        last_ask: Last ask price
        last_spread_pct: Last spread as percentage
        last_quote_age_ms: Age of the quote the spread came from, None when no
            quote was known - a spread without its age cannot be judged
        last_volume: Last real volume
        last_tick_time: Timestamp of last tick
        errors_count: Errors for this symbol
        file_count: Number of files created this session
        folder_file_count: Total files in folder (all sessions)
        digits: Decimal places this instrument's prices carry, from the broker
            specification. Two fixed places once rendered ADAUSD's 0.2103 and
            0.2104 both as 0.21, a screen asserting a spread the book did not
            have - so a screen outside this process needs the real number
    """
    symbol: str
    current_file_ticks: int = 0
    last_bid: float = 0.0
    last_ask: float = 0.0
    last_spread_pct: float = 0.0
    last_quote_age_ms: Optional[int] = None
    last_volume: float = 0.0
    last_tick_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    errors_count: int = 0
    file_count: int = 0
    folder_file_count: int = 0
    digits: Optional[int] = None

    @property
    def is_active(self) -> bool:
        """Check if symbol received ticks recently (within 30s)."""
        if not self.last_tick_time:
            return False
        delta = (datetime.now(timezone.utc) -
                 self.last_tick_time).total_seconds()
        return delta < 30


@dataclass
class TradeIdGap:
    """
    What one symbol's trade ids say about one interruption of the feed.

    Kraken numbers trades per pair, and in this collector's 1.7.0 files those
    numbers have been dense: no gap outside a drop, consecutive files joining at
    last + 1. So the first id after a drop minus the last one before it says how
    many trades went by unseen - as an UPPER bound, because the venue may
    allocate ids it never publishes. Measured here, live, it does not depend on
    which file either trade ended up in.

    Attributes:
        last_trade_id: The symbol's last trade received before the drop - with
            spans_previous_outage, before the earliest outage the gap covers;
            None if there was none
        last_trade_time_msc: That trade's exchange time
        first_trade_id: The first trade after the drop on the connection that
            restored the feed; None until it arrives, and for good if the record
            was resolved before it did
        first_trade_time_msc: That trade's exchange time
        first_trade_after_ms: Offset of its arrival from the outage's last
            message, on the monotonic clock
        missing_trade_ids: Ids after the last trade before the drop and before
            the first one after it that never arrived - first - last - 1, less
            any trades an attempt that failed before the restoration delivered
            in between. An upper bound on trades missed; None when an end is
            unknown or the ids did not increase - never 0 by default, because 0
            would claim nothing was missed
        ids_not_increasing: Somewhere in this outage a first trade - on the
            restoring connection or on an attempt that failed - was not above
            the highest id received before it: a replay or a reordering,
            flagged rather than counted, and the count stays unknown
        spans_previous_outage: The symbol's first trade after the previous
            outage was still unknown when that record closed - by the next
            drop, the deadline, or a refusal - and it did not trade before
            this drop. This gap starts at its last trade before that outage
            and keeps whatever a failed attempt of it counted, so it covers
            both: the total stays right, the attribution to this one outage
            does not
    """
    last_trade_id: Optional[int] = None
    last_trade_time_msc: Optional[int] = None
    first_trade_id: Optional[int] = None
    first_trade_time_msc: Optional[int] = None
    first_trade_after_ms: Optional[float] = None
    missing_trade_ids: Optional[int] = None
    ids_not_increasing: bool = False
    spans_previous_outage: bool = False


@dataclass
class ReconnectEvent:
    """
    One interruption of the Kraken feed, from the last message before it to the
    moment every subscription was answered again.

    The four leading fields kept their names through 2026-10-08 and got back the
    meaning their documentation always claimed. Until then `timestamp` was the
    moment the drop was NOTICED - 10-11 s after the data stopped, because a
    socket Kraken had closed was only found by the silence watchdog - the event
    ended at the socket handshake, before anything was subscribed again, the
    duration was two wall-clock readings subtracted, and `reason` was the
    constant 'connection_restored'. A reconnect recorded as 2.2 s had cost about
    12 s of trades.

    Durations are measured on the monotonic clock; the two datetimes are wall
    clock readings for a human and are never subtracted from each other.

    Attributes:
        timestamp: Wall clock at the last message received before the drop -
            when data stopped, as far as this process can know
        reconnected_at: Wall clock when every (stream, symbol) subscription had
            been answered again - or when the 10 s subscription deadline gave
            up on the stragglers listed in `unconfirmed`; None while the
            outage is in progress
        duration_seconds: The data gap - last message to feed restored,
            monotonic; at a give-up it includes the wait for the deadline,
            while most pairs may have been back long before. None while in
            progress
        reason: How the drop was detected - far_side_close, connection_lost,
            keepalive_timeout, library_failure, silence_watchdog,
            subscribe_unanswered, subscribe_send_failed or unexpected
        close_code: Code of the close frame Kraken sent; None when no frame
            arrived or it carried no code - reason far_side_close says a frame
            came. Never 0, and never the 1006 or 1005 the library reports for a
            missing frame or a missing code
        close_reason: Text of that frame
        exception: What ended the connection, as text
        cause: The error chained under it (a reset, a DNS failure), as text
        detected_after_ms: Last message to detection
        teardown_ms: Time spent dropping the old socket: about 0, because a
            connection given up on is aborted rather than closed politely for
            its close timeout - it does not tell a closed link from a dead one
        attempts: Connection attempts in this outage, the successful one included
        backoff_s: Seconds slept between attempts in this outage
        last_failure: The most recent failed attempt, as text
        handshake_after_ms: Last message to the handshake that succeeded
        confirmed_after_ms: Per stream, last message to the latest moment a
            pair of it was acknowledged or confirmed by its data; a stream
            Kraken refused outright was confirmed at no time and is absent
        rejected: Pairs Kraken refused to subscribe, as 'stream SYMBOL: error'
        unconfirmed: Pairs that neither answered nor delivered data within the
            subscription deadline; the feed was taken as restored without them
        first_trade_after_ms: Last message to the first trade on any symbol
        connection_age_s: How long the dropped connection had been fully
            subscribed - the same anchor the backoff reset is measured from
        kraken_connection_id: Kraken's id for the dropped connection, a string
            because it is a 63-bit integer a JavaScript reader would round
        kraken_system: The last system state Kraken announced on it
        resolved_by: What closed the record - all_symbols_traded, deadline,
            next_drop or shutdown; '' while open
        symbols: Per symbol, what its trade ids say about this interruption
    """
    timestamp: datetime
    reconnected_at: Optional[datetime]
    duration_seconds: Optional[float]
    reason: str

    # Defaulted rather than required, as on StallEvent: a viewer runs against
    # whatever build the box carries, and an older collector sends only the four
    # fields above.
    close_code: Optional[int] = None
    close_reason: str = ""
    exception: str = ""
    cause: str = ""
    detected_after_ms: Optional[float] = None
    teardown_ms: Optional[float] = None
    attempts: int = 0
    backoff_s: float = 0.0
    last_failure: str = ""
    handshake_after_ms: Optional[float] = None
    confirmed_after_ms: Dict[str, float] = field(default_factory=dict)
    rejected: List[str] = field(default_factory=list)
    unconfirmed: List[str] = field(default_factory=list)
    first_trade_after_ms: Optional[float] = None
    connection_age_s: Optional[float] = None
    kraken_connection_id: str = ""
    kraken_system: str = ""
    resolved_by: str = ""
    symbols: Dict[str, TradeIdGap] = field(default_factory=dict)


@dataclass
class ReconnectTotals:
    """
    Every outage since the process started, beyond what the ring keeps.

    The detailed records are capped, because each one carries nine symbols and
    the status payload is serialised every second while a viewer is open. The
    totals are what a count or an average is computed from - the length of a
    capped list stops being a count the moment it fills.

    Attributes:
        count: Outages restored (or ended by a shutdown) since start
        by_reason: Count per detection kind
        by_day: Count per UTC date, for the fourteen most recent dates on which
            an outage occurred - not a calendar window: after a quiet stretch
            the oldest key can be weeks back, and the keys say how far
        gap_seconds_total: Sum of the data gaps
        gap_seconds_max: The longest data gap
        missing_trade_ids_total: Sum of the per-symbol counts the records came
            to. A symbol whose first trade after an outage arrived only after
            its record had closed, with no new outage before it, adds nothing,
            and neither does what a failed attempt counted for a symbol whose
            first trade had not come when the process stopped: those ids are
            counted in no record
    """
    count: int = 0
    by_reason: Dict[str, int] = field(default_factory=dict)
    by_day: Dict[str, int] = field(default_factory=dict)
    gap_seconds_total: float = 0.0
    gap_seconds_max: float = 0.0
    missing_trade_ids_total: int = 0


@dataclass
class LogEntry:
    """
    Single log entry for display.

    Attributes:
        timestamp: When the log occurred
        level: Log level (ERROR, WARNING, etc.)
        message: Log message text
    """
    timestamp: datetime
    level: str
    message: str


@dataclass
class FileInfo:
    """
    Information about a created file.

    Attributes:
        filename: Name of the file
        symbol: Symbol this file belongs to
        tick_count: Number of ticks in the file
        created_at: When the file was created
    """
    filename: str
    symbol: str
    tick_count: int
    created_at: datetime


@dataclass
class FolderStats:
    """
    Statistics for a monitored folder.

    Attributes:
        path: Folder path
        file_count: Number of files
        size_bytes: Total size in bytes
        last_scanned: Last scan timestamp
    """
    path: str
    file_count: int = 0
    size_bytes: int = 0
    last_scanned: Optional[datetime] = None

    @property
    def size_gb(self) -> float:
        """Get size in GB."""
        return self.size_bytes / (1024 ** 3)

    @property
    def size_mb(self) -> float:
        """Get size in MB."""
        return self.size_bytes / (1024 ** 2)


@dataclass
class DiskSpaceStats:
    """
    Disk space statistics.

    Attributes:
        total_bytes: Total disk space
        used_bytes: Used disk space
        free_bytes: Free disk space
        percent_used: Percentage used
        last_checked: Last check timestamp
    """
    total_bytes: int = 0
    used_bytes: int = 0
    free_bytes: int = 0
    percent_used: float = 0.0
    last_checked: Optional[datetime] = None

    @property
    def total_gb(self) -> float:
        """Get total space in GB."""
        return self.total_bytes / (1024 ** 3)

    @property
    def used_gb(self) -> float:
        """Get used space in GB."""
        return self.used_bytes / (1024 ** 3)

    @property
    def free_gb(self) -> float:
        """Get free space in GB."""
        return self.free_bytes / (1024 ** 3)

    @property
    def percent_free(self) -> float:
        """Get percentage free."""
        return 100.0 - self.percent_used

    @property
    def status(self) -> str:
        """Get status indicator (OK, WARNING, CRITICAL)."""
        if self.percent_free > 50:
            return "OK"
        elif self.percent_free > 30:
            return "WARNING"
        elif self.percent_free > 20:
            return "CRITICAL"
        else:
            return "EMERGENCY"


@dataclass
class LoopLag:
    """
    How late the collector's event loop runs, measured rather than assumed.

    Everything shares one loop, so a long piece of work anywhere delays the
    stamping of every tick that arrives meanwhile - and that delay is invisible
    in a tick file until it passes the consumer's 30 s window and costs the
    whole file.

    Attributes:
        samples: How many measurements the numbers rest on
        last_ms: The most recent lateness
        max_ms: The worst since this process started
        max_at: When that worst one happened
        over_500ms: How often the loop was late by more than half a second
    """
    samples: int = 0
    last_ms: float = 0.0
    max_ms: float = 0.0
    max_at: Optional[datetime] = None
    over_500ms: int = 0


@dataclass
class ClockState:
    """
    What the session clock has had to absorb, for whoever draws the screen.

    The display read these off the live `CollectionClock`, which only a program
    inside this process can do. They are here so a viewer somewhere else shows
    the same two numbers the file headers carry.

    Attributes:
        resyncs: Backwards steps the clock clamped, cumulative over the session
        max_correction_ms: The largest single correction it absorbed
    """
    resyncs: int = 0
    max_correction_ms: int = 0


@dataclass
class GcPauses:
    """
    What garbage collection costs this process.

    The collector holds every tick of an open file in memory, and a generation-2
    collection walks all of them - on the event loop, between two ticks, with
    nothing in Python reporting it.

    Attributes:
        collections: How many collections per generation, index 0 to 2
        max_ms: The longest pause per generation
        last_ms: The most recent pause
        last_generation: Which generation that was
        total_ms: Everything spent collecting since this process started
    """
    collections: List[int] = field(default_factory=lambda: [0, 0, 0])
    max_ms: List[float] = field(default_factory=lambda: [0.0, 0.0, 0.0])
    last_ms: float = 0.0
    last_generation: int = -1
    total_ms: float = 0.0

    # How many ticks were sitting in the open files when the worst pause
    # happened. The collector holds every tick of an open file in memory AND in
    # its write-ahead log, so a collection walks all of them - and whether the
    # pause tracks that number is the question that decides between a smaller
    # archive boundary and dropping the in-memory copy. One number, recorded at
    # the moment it matters, answers it in a night.
    live_ticks_at_max: int = 0


@dataclass
class TickWork:
    """
    How much of the loop's time the tick handler itself has consumed.

    A running total rather than a rate, because the reader is the stall
    attribution: it takes a snapshot before waiting and another after, and the
    difference is what the handler cost inside that one window. A rate computed
    here would have to guess the window somebody else is measuring.

    Attributes:
        ticks: Ticks handled since the session started
        total_ms: Milliseconds spent inside the handler, cumulative
    """
    ticks: int = 0
    total_ms: float = 0.0


@dataclass
class StallEvent:
    """
    One moment the event loop stood still, with what was going on.

    A stall costs stamping accuracy: `collected_msc` reads that much later than
    the tick arrived, and beyond the consumer's 30 s window a whole file is
    refused. The cause is recorded beside it because the first attribution made
    from reasoning alone was wrong - the folder scan was blamed and then
    measured at 15 ms.

    Attributes:
        at: When the stall was observed
        ms: How late the loop was
        gc_ms: How much of it was garbage collection
        gc_generation: The highest generation collected in that window, -1 for none
        render_ms: How long the live display's last redraw took
        exports_in_flight: Archive writers running at that moment
        cause: What the numbers above account for, or "unknown"
        blocked_in: Where the loop's stack was while it stood still, when the
            sampler caught it - an observation rather than a timing, so it ranks
            below every arm that measured a duration
    """
    at: datetime
    ms: float
    gc_ms: float
    gc_generation: int
    render_ms: float
    exports_in_flight: int
    cause: str

    # Defaulted rather than required, and that is a decision rather than a
    # convenience: a viewer runs against whatever build the box happens to
    # carry, and a required field missing from an older payload would make the
    # reading unreadable as a whole. A stall from before this existed reports
    # zero work measured, which is true - nobody measured it.
    tick_ms: float = 0.0
    ticks: int = 0

    # Work that runs in a thread but still contends for the interpreter lock, so
    # it delays the loop without appearing anywhere in the loop's own timings.
    # Measured 2026-09-23: nine stalls of 285-395 ms with no ticks in the window,
    # no collection and no export - against a folder scan whose worst reading
    # that day was 335 ms. A duration, not a presence, so it ranks with the rest
    # of the measured arms.
    offloop_ms: float = 0.0
    offloop_what: str = ""

    # Presence, not duration - a reconnect either happened inside this window or
    # it did not. Measured 2026-09-22: the reconnect at 02:43:50 produced two
    # stalls, 252 ms and 271 ms, and both said `unknown` because nothing
    # connected the socket to the loop that noticed.
    reconnected: bool = False

    # Where the loop actually was, read off its own stack while it was stuck.
    # Every arm above answers for one candidate that somebody thought to
    # instrument; twenty stalls across two nights said `unknown` with all of
    # them reading zero. This one has no candidate list.
    blocked_in: str = ""


@dataclass
class ScanTimings:
    """
    How long the background scans take, and therefore what they would cost.

    They run off the event loop, so these numbers no longer show up as stamping
    lag — which is exactly why they are worth reporting: measured 2026-09-21,
    while they still ran on the loop, they stalled it up to 4.3 s about once a
    minute. If `loop_lag` ever rises with these, they are back on the loop.

    Attributes:
        folder_scan_last_ms: The most recent walk over the archive, MT5 and logs
        folder_scan_max_ms: The worst one since this process started
        disk_check_last_ms: The most recent disk-space reading
        disk_check_max_ms: The worst one
        render_last_ms: The live display's most recent redraw, which unlike the
            two above does run on the event loop
        render_max_ms: The worst redraw
    """
    folder_scan_last_ms: float = 0.0
    folder_scan_max_ms: float = 0.0
    disk_check_last_ms: float = 0.0
    disk_check_max_ms: float = 0.0
    render_last_ms: float = 0.0
    render_max_ms: float = 0.0


@dataclass
class CounterCheck:
    """
    Whether the displayed tick count still matches what the writer wrote.

    Two counters for one number drift, and this pair drifted by exactly one tick
    at every UTC day cut until 2026-09-20 — for weeks, unnoticed, because
    nothing compared them. What is reported here is the comparison, not the
    repair: `mismatches` staying at zero is the evidence that the counts are
    sound, and any other number is the signal to go looking.

    Attributes:
        checks: Comparisons made since this process started
        mismatches: How many of them disagreed
        last_mismatch_symbol: The symbol of the most recent disagreement
        last_mismatch_counted: What the display had said
        last_mismatch_written: What the writer held, and what was taken
        last_mismatch_at: When that was
    """
    checks: int = 0
    mismatches: int = 0
    last_mismatch_symbol: Optional[str] = None
    last_mismatch_counted: int = 0
    last_mismatch_written: int = 0
    last_mismatch_at: Optional[datetime] = None


@dataclass
class ExportStats:
    """
    The archive writers that run as subprocesses.

    Attributes:
        started: Files handed over since this process started
        finished: Files the writer reported as written
        failed: Handovers that ended without a file; their logs stay on disk
        in_flight: Handed over and not yet reported
        last_file: The last file a writer finished
        last_ticks: How many ticks it held
        last_ms: How long that writer took, including process start
        max_ms: The slowest one so far
        last_failed_file: The last file that was not written - its log is still
            on disk and the next start recovers it
        last_failed_at: When that was
    """
    started: int = 0
    finished: int = 0
    failed: int = 0
    in_flight: int = 0
    last_file: Optional[str] = None
    last_ticks: int = 0
    last_ms: float = 0.0
    max_ms: float = 0.0
    last_failed_file: Optional[str] = None
    last_failed_at: Optional[datetime] = None

    # Spawning a child is not free and it happens ON the loop - `communicate()`
    # awaits, but `create_subprocess_exec` itself does not. At the UTC day cut
    # nine or ten go out within the same second, and on 2026-09-23 that cut cost
    # 3397 ms of loop lag against 856 ms the night before. Whether the cost is
    # the spawning or the writing is a question a total cannot answer.
    spawn_last_ms: float = 0.0
    spawn_max_ms: float = 0.0
    spawn_total_ms: float = 0.0


class CollectorStats:
    """
    Aggregated statistics for the entire collector.

    Central stats object updated by collector components.
    Read by LiveDisplay for rendering.

    Attributes:
        start_time: When collection started
        total_files: Total files created this session
        total_errors: Total errors across all components
        total_warnings: Total warnings
        websocket_status: Current WebSocket connection status
        symbols: Per-symbol statistics
        recent_logs: Recent error/warning log entries
        last_file: Most recently created file
        reconnect_events: List of reconnect events
        disk_space: Disk space statistics
        folders: Monitored folder statistics
    """

    def __init__(self):
        """Initialize stats."""
        self.start_time: datetime = datetime.now(timezone.utc)
        self.total_files: int = 0
        self.total_errors: int = 0
        self.total_warnings: int = 0
        self.websocket_status: str = "disconnected"
        self.symbols: Dict[str, SymbolStats] = {}
        self.recent_logs: List[LogEntry] = []
        self.last_file: Optional[FileInfo] = None
        self.reconnect_events: List[ReconnectEvent] = []
        self.last_reconnect: Optional[ReconnectEvent] = None

        # The outage whose feed is still down, until every subscription is
        # answered again or the deadline gave up on the stragglers. On
        # /v1/status this is the answer to 'is the feed down right now, and why'. A restored record still waiting for its first
        # trade per symbol is already the newest entry in reconnect_events,
        # with resolved_by '' until it is complete.
        self.reconnect_in_progress: Optional[ReconnectEvent] = None
        # None until the first outage, not a zero: a viewer reading a build
        # that does not send totals must not show a count nobody measured.
        self.reconnect_totals: Optional[ReconnectTotals] = None
        # When each outage of the last OUTAGE_TIMES_KEPT began, uncapped: an
        # hour's or a week's count read off the twenty-record ring stops at
        # twenty. Private, so /v1/status does not serialise it; the event loop
        # is its only reader and writer.
        self._outage_times: List[datetime] = []
        self.disk_space: DiskSpaceStats = DiskSpaceStats()
        self.folders: Dict[str, FolderStats] = {}
        self.loop_lag: LoopLag = LoopLag()
        self.exports: ExportStats = ExportStats()
        self.counter_check: CounterCheck = CounterCheck()
        self.scans: ScanTimings = ScanTimings()
        self.gc: GcPauses = GcPauses()
        self.tick_work: TickWork = TickWork()
        self.stalls: List[StallEvent] = []

        # The session's largest, kept apart from the latest. A ring of the most
        # recent alone discards the biggest first, which is backwards: the rare
        # stall is the one worth reading and the routine one is what evicts it.
        self.worst_stalls: List[StallEvent] = []

        # What a screen needs and the statistics did not carry: which streams
        # are subscribed, what the clock has absorbed, and how many decimals a
        # price is worth printing to. A display inside this process could read
        # all three from live objects; one in another process cannot, and a
        # second source for the same fact is how two screens start disagreeing.
        self.streams: List[str] = []
        self.clock: ClockState = ClockState()

        # The file boundary this instance was configured with. A screen shows a
        # file's progress against it, and the display used to read it out of the
        # local app_config on every frame - which on the collector is a file read
        # per symbol per second, and in a viewer is the wrong machine's config
        # entirely: a laptop set to 1,000 rendered a production file of 12,737
        # ticks as "1274 %" of a limit that instance does not have.
        self.max_ticks_per_file: int = 0

        # Config
        self.max_stalls: int = 10
        self.max_worst_stalls: int = 10
        self.max_recent_logs: int = 50
        # Twenty detailed records, with the totals carrying everything older:
        # one record with nine symbols is about 3 KB, and the payload is
        # serialised every second while a viewer is open.
        self.max_reconnect_history: int = 20

    def get_symbol_stats(self, symbol: str) -> SymbolStats:
        """
        Get or create stats for a symbol.

        Args:
            symbol: Symbol name

        Returns:
            SymbolStats instance
        """
        if symbol not in self.symbols:
            self.symbols[symbol] = SymbolStats(symbol=symbol)
        return self.symbols[symbol]

    def record_tick(self, symbol: str, bid: float, ask: float, spread_pct: float,
                    real_volume: float, quote_age_ms: Optional[int] = None) -> None:
        """
        Record a received tick.

        Args:
            symbol: Symbol name
            bid: Bid price
            ask: Ask price
            spread_pct: Spread percentage
            quote_age_ms: Age of the quote it was derived from
            real_volume: Real volume
        """
        stats = self.get_symbol_stats(symbol)
        stats.current_file_ticks += 1
        stats.last_bid = bid
        stats.last_ask = ask
        stats.last_spread_pct = spread_pct
        stats.last_quote_age_ms = quote_age_ms
        stats.last_volume = real_volume
        stats.last_tick_time = datetime.now(timezone.utc)
        if stats.start_time is None:
            stats.start_time = datetime.now(timezone.utc)

    def record_file_created(self, symbol: str, filename: str, tick_count: int) -> None:
        """
        Record a file creation (rotation).

        Args:
            symbol: Symbol name
            filename: Created filename
            tick_count: Ticks in the file
        """
        stats = self.get_symbol_stats(symbol)
        stats.file_count += 1
        stats.current_file_ticks = 0  # Reset counter
        self.total_files += 1

        self.last_file = FileInfo(
            filename=filename,
            symbol=symbol,
            tick_count=tick_count,
            created_at=datetime.now(timezone.utc)
        )

    def record_outage(self, stage: str, event: ReconnectEvent) -> None:
        """
        Follow one outage through its three stages.

        The record is the same object in every stage and on every list it is
        on, so what arrives later - the first trade per symbol - shows up on the
        status route without being copied anywhere.

        Args:
            stage: 'opened' when the drop is detected, 'restored' when every
                subscription is answered again, 'resolved' when nothing more
                will be added (all symbols traded, a deadline, the next drop,
                or a shutdown)
            event: The record
        """
        if stage == "opened":
            self.reconnect_in_progress = event
            return

        if stage == "restored":
            self._settle_outage(event)
            return

        if stage == "resolved":
            # A record a shutdown resolved was never restored; it is still an
            # outage the process saw, so it is counted once, here.
            never_restored = (event.reconnected_at is None
                              and self.reconnect_in_progress is event)
            if never_restored:
                self._settle_outage(event)
            if self.reconnect_totals is None:
                self.reconnect_totals = ReconnectTotals()
            self.reconnect_totals.missing_trade_ids_total += sum(
                gap.missing_trade_ids for gap in event.symbols.values()
                if gap.missing_trade_ids is not None)

    def _settle_outage(self, event: ReconnectEvent) -> None:
        """
        Move an outage from 'in progress' into the history and the totals.

        Args:
            event: The record, restored or ended by a shutdown
        """
        if self.reconnect_in_progress is event:
            self.reconnect_in_progress = None

        self.reconnect_events.append(event)
        self.last_reconnect = event
        history = self.max_reconnect_history
        if len(self.reconnect_events) > history:
            self.reconnect_events = self.reconnect_events[-history:]

        times = self._outage_times + [event.timestamp]
        newest = max(times)
        self._outage_times = [at for at in times
                              if at >= newest - OUTAGE_TIMES_KEPT]

        if self.reconnect_totals is None:
            self.reconnect_totals = ReconnectTotals()
        totals = self.reconnect_totals
        totals.count += 1

        # Rebound, never mutated: /v1/status serialises in a worker thread while
        # this runs on the event loop, and a dict gaining a key mid-iteration
        # raises 'dictionary changed size during iteration' - a sporadic 500.
        reason_count = totals.by_reason.get(event.reason, 0) + 1
        totals.by_reason = {**totals.by_reason, event.reason: reason_count}

        day = event.timestamp.astimezone(timezone.utc).date().isoformat()
        by_day = {**totals.by_day, day: totals.by_day.get(day, 0) + 1}
        kept = sorted(by_day)[-RECONNECT_DAYS_KEPT:]
        totals.by_day = {key: by_day[key] for key in kept}

        if event.duration_seconds is not None:
            totals.gap_seconds_total += event.duration_seconds
            totals.gap_seconds_max = max(totals.gap_seconds_max,
                                         event.duration_seconds)

    def outages_since(self, cutoff: datetime) -> int:
        """
        How many outages began at or after a moment, counted without a cap.

        Not off the ring, which holds the latest twenty: a cluster threshold
        above twenty could never fire, and a busy week read twenty. Not off
        by_day either, whose whole dates made a weekly count and the weekly
        list disagree about the part of a day seven days back.

        Args:
            cutoff: The earliest moment counted; at most OUTAGE_TIMES_KEPT back

        Returns:
            Outages whose last message before the drop came at or after cutoff
        """
        return sum(1 for at in self._outage_times if at >= cutoff)

    def record_error(self, message: str) -> None:
        """
        Record an error.

        Args:
            message: Error message
        """
        self.total_errors += 1
        self._add_log_entry("ERROR", message)

    def record_warning(self, message: str) -> None:
        """
        Record a warning.

        Args:
            message: Warning message
        """
        self.total_warnings += 1
        self._add_log_entry("WARNING", message)

    def record_loop_lag(self, lateness_ms: float) -> None:
        """
        Record one measurement of how late the event loop ran.

        Args:
            lateness_ms: Milliseconds the wake-up came after it was due
        """
        lateness_ms = max(0.0, lateness_ms)
        self.loop_lag.samples += 1
        self.loop_lag.last_ms = round(lateness_ms, 1)

        if lateness_ms > 500:
            self.loop_lag.over_500ms += 1

        if lateness_ms > self.loop_lag.max_ms:
            self.loop_lag.max_ms = round(lateness_ms, 1)
            self.loop_lag.max_at = datetime.now(timezone.utc)

    def record_clock(self, resyncs: int, max_correction_ms: int) -> None:
        """
        Copy the session clock's counters into the payload.

        Args:
            resyncs: Backwards steps the clock has clamped
            max_correction_ms: The largest correction it absorbed
        """
        self.clock.resyncs = resyncs
        self.clock.max_correction_ms = max_correction_ms

    def record_gc_pause(self, generation: int, duration_ms: float) -> None:
        """
        Record one garbage collection, as the interpreter reported it.

        Args:
            generation: Which generation was collected, 0 to 2
            duration_ms: How long the pause lasted
        """
        self.gc.last_ms = round(duration_ms, 1)
        self.gc.last_generation = generation
        self.gc.total_ms = round(self.gc.total_ms + duration_ms, 1)

        if 0 <= generation < len(self.gc.collections):
            self.gc.collections[generation] += 1
            if duration_ms > self.gc.max_ms[generation]:
                self.gc.max_ms[generation] = round(duration_ms, 1)
                # Only on a new worst: the question is whether the pause tracks
                # the buffer, and one paired reading answers that where a total
                # cannot. Nine entries to add up, a few hundred times a day.
                self.gc.live_ticks_at_max = sum(
                    entry.current_file_ticks for entry in self.symbols.values())

    def record_render(self, duration_ms: float) -> None:
        """
        Record how long the live display took to draw one frame.

        Args:
            duration_ms: Milliseconds spent rendering, on the event loop
        """
        self.scans.render_last_ms = round(duration_ms, 1)
        self.scans.render_max_ms = max(
            self.scans.render_max_ms, round(duration_ms, 1))

    def record_tick_work(self, duration_ms: float) -> None:
        """
        Add what handling one tick cost.

        Called for every tick, so it does two additions and nothing else. Any
        derived figure is computed by whoever reads it over a window they chose.

        Args:
            duration_ms: Milliseconds spent inside the tick handler
        """
        self.tick_work.ticks += 1
        self.tick_work.total_ms += duration_ms

    def record_stall(self, duration_ms: float, gc_ms: float,
                     gc_generation: int, render_ms: float,
                     exports_in_flight: int, tick_ms: float = 0.0,
                     ticks: int = 0, reconnected: bool = False,
                     offloop_ms: float = 0.0, offloop_what: str = "",
                     blocked_in: str = "") -> None:
        """
        Record a moment the event loop stood still, with what explains it.

        The cause is derived from what was measured in the same window, and says
        "unknown" when nothing accounts for at least half of it. An attribution
        made from reasoning rather than measurement was wrong once already.

        Args:
            duration_ms: How late the loop was
            gc_ms: Garbage collection time inside that window
            gc_generation: Highest generation collected, -1 for none
            render_ms: The display's most recent redraw
            exports_in_flight: Archive writers running at that moment
            tick_ms: Time the tick handler spent inside this window
            ticks: Ticks it handled in that time
            reconnected: Whether the socket was re-established inside it
            offloop_ms: Longest timed off-loop task that finished in the window
            offloop_what: Which one that was
            blocked_in: The loop's own stack, sampled while it was stuck
        """
        half = duration_ms / 2

        # Measured arms first, in order of how specific they are. They overlap
        # on purpose: a collection triggered by an allocation inside the tick
        # handler counts in both, and naming garbage collection is the more
        # useful answer of the two. `exports_in_flight` comes last because it is
        # a presence check rather than a duration - the weakest evidence here,
        # and it must never outrank something that was actually timed.
        if gc_ms >= half:
            cause = f"garbage collection, generation {gc_generation}"
        elif render_ms >= half:
            cause = "the live display"
        elif tick_ms >= half:
            cause = f"handling {ticks} ticks"
        elif offloop_ms >= half:
            cause = f"the {offloop_what}"
        elif blocked_in:
            # Observed rather than timed: one sample says where the loop was at
            # one moment inside the stall, which cannot be weighed against half
            # of it. It outranks the two presence checks below because it is a
            # reading of the loop itself rather than of something that happened
            # to be nearby.
            cause = f"the loop was in {blocked_in}"
        elif exports_in_flight:
            cause = "an archive export was handed over"
        elif reconnected:
            cause = "the websocket reconnected"
        else:
            cause = "unknown"

        event = StallEvent(
            at=datetime.now(timezone.utc),
            ms=round(duration_ms, 1),
            gc_ms=round(gc_ms, 1),
            gc_generation=gc_generation,
            render_ms=round(render_ms, 1),
            exports_in_flight=exports_in_flight,
            cause=cause,
            tick_ms=round(tick_ms, 1),
            ticks=ticks,
            reconnected=reconnected,
            offloop_ms=round(offloop_ms, 1),
            offloop_what=offloop_what,
            blocked_in=blocked_in)

        self.stalls.append(event)
        if len(self.stalls) > self.max_stalls:
            self.stalls = self.stalls[-self.max_stalls:]

        # The recent list alone loses the interesting one first. Measured
        # 2026-09-24: the UTC day cut closed nine files at 00:00 and its stall
        # was gone by 03:45, pushed out by ten routine 300 ms stalls - the one
        # event of the night anybody wanted to read, overwritten by the ones
        # nobody did. A session keeps its largest as well as its latest, and a
        # stall is normally in both.
        self.worst_stalls.append(event)
        self.worst_stalls.sort(key=lambda stall: stall.ms, reverse=True)
        if len(self.worst_stalls) > self.max_worst_stalls:
            self.worst_stalls = self.worst_stalls[:self.max_worst_stalls]

    def record_folder_scan(self, duration_ms: float) -> None:
        """
        Record how long the folder scan took.

        Args:
            duration_ms: Milliseconds spent counting and measuring folders
        """
        self.scans.folder_scan_last_ms = round(duration_ms, 1)
        self.scans.folder_scan_max_ms = max(
            self.scans.folder_scan_max_ms, round(duration_ms, 1))

    def record_disk_check(self, duration_ms: float) -> None:
        """
        Record how long reading the disk usage took.

        Args:
            duration_ms: Milliseconds spent on the reading
        """
        self.scans.disk_check_last_ms = round(duration_ms, 1)
        self.scans.disk_check_max_ms = max(
            self.scans.disk_check_max_ms, round(duration_ms, 1))

    def record_counter_check(self, symbol: str, counted: int,
                             written: int) -> None:
        """
        Record one comparison of the displayed count against the written one.

        Args:
            symbol: The symbol compared
            counted: What the display and the status API had
            written: What the writer holds, which is what the file says
        """
        self.counter_check.checks += 1

        if counted == written:
            return

        self.counter_check.mismatches += 1
        self.counter_check.last_mismatch_symbol = symbol
        self.counter_check.last_mismatch_counted = counted
        self.counter_check.last_mismatch_written = written
        self.counter_check.last_mismatch_at = datetime.now(timezone.utc)

    def record_export_started(self) -> None:
        """A closed file was handed to an archive writer."""
        self.exports.started += 1
        self.exports.in_flight += 1

    def record_export_spawn(self, duration_ms: float) -> None:
        """
        Record what starting one archive writer cost the event loop.

        Args:
            duration_ms: Milliseconds spent inside `create_subprocess_exec`
        """
        self.exports.spawn_last_ms = round(duration_ms, 1)
        self.exports.spawn_max_ms = round(
            max(self.exports.spawn_max_ms, duration_ms), 1)
        self.exports.spawn_total_ms = round(
            self.exports.spawn_total_ms + duration_ms, 1)

    def record_export_finished(self, filename: str, ticks: int,
                               duration_ms: float) -> None:
        """
        An archive writer reported a finished file.

        Args:
            filename: The archive file it wrote
            ticks: How many ticks it held
            duration_ms: How long it took, process start included
        """
        self.exports.finished += 1
        self.exports.in_flight = max(0, self.exports.in_flight - 1)
        self.exports.last_file = filename
        self.exports.last_ticks = ticks
        self.exports.last_ms = round(duration_ms, 1)
        self.exports.max_ms = max(self.exports.max_ms, round(duration_ms, 1))

    def record_export_failed(self, filename: str) -> None:
        """
        An archive writer produced no file; its log is still on disk.

        Args:
            filename: The archive file that is still owed
        """
        self.exports.failed += 1
        self.exports.in_flight = max(0, self.exports.in_flight - 1)
        self.exports.last_failed_file = filename
        self.exports.last_failed_at = datetime.now(timezone.utc)

    def record_logged(self, level: LogLevel, logger_name: str,
                      message: str) -> None:
        """
        Count one logged line at WARNING or above - the log listener.

        Registered with add_log_listener, so the counters follow the log
        instead of depending on each error path remembering to report itself.

        Args:
            level: Level of the line; ERROR and CRITICAL count as errors
            logger_name: The logger that wrote it - part of the listener
                signature, not stored, because the entry shape is shared with
                the display and the status API
            message: The logged text
        """
        if level >= ERROR:
            self.record_error(message)
        else:
            self.record_warning(message)

    def _add_log_entry(self, level: str, message: str) -> None:
        """Add log entry, maintaining max size."""
        entry = LogEntry(
            timestamp=datetime.now(timezone.utc),
            level=level,
            message=message
        )
        self.recent_logs.append(entry)

        # Trim if needed
        if len(self.recent_logs) > self.max_recent_logs:
            self.recent_logs = self.recent_logs[-self.max_recent_logs:]

    def set_websocket_status(self, status: str) -> None:
        """
        Update WebSocket status.

        Args:
            status: Status string (connected, disconnected, reconnecting)
        """
        self.websocket_status = status

    def update_folder_stats(self, folder_key: str, path: str, file_count: int, size_bytes: int = 0) -> None:
        """
        Update folder statistics.

        Args:
            folder_key: Folder identifier (e.g., "kraken", "mt5", "logs")
            path: Folder path
            file_count: Number of files
            size_bytes: Total size in bytes (optional, 0 if not calculated)
        """
        self.folders[folder_key] = FolderStats(
            path=path,
            file_count=file_count,
            size_bytes=size_bytes,
            last_scanned=datetime.now(timezone.utc)
        )

    def update_disk_space(self, total: int, used: int, free: int) -> None:
        """
        Update disk space statistics.

        Args:
            total: Total bytes
            used: Used bytes
            free: Free bytes
        """
        percent_used = (used / total * 100) if total > 0 else 0.0

        self.disk_space = DiskSpaceStats(
            total_bytes=total,
            used_bytes=used,
            free_bytes=free,
            percent_used=percent_used,
            last_checked=datetime.now(timezone.utc)
        )

    def get_uptime_seconds(self) -> float:
        """Get collector uptime in seconds."""
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()

    def get_uptime_hours(self) -> float:
        """Get collector uptime in hours."""
        return self.get_uptime_seconds() / 3600
