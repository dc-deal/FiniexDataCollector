"""
FiniexDataCollector - Collector Statistics Types
Type definitions for real-time collection monitoring.

Location: python/types/collector_stats.py
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Dict

from python.types.log_level import ERROR, LogLevel


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
class ReconnectEvent:
    """
    Single reconnect event.

    Attributes:
        timestamp: When disconnect occurred
        reconnected_at: When reconnection succeeded
        duration_seconds: Downtime duration
        reason: Disconnect reason
    """
    timestamp: datetime
    reconnected_at: Optional[datetime]
    duration_seconds: float
    reason: str


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
    """
    at: datetime
    ms: float
    gc_ms: float
    gc_generation: int
    render_ms: float
    exports_in_flight: int
    cause: str


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
        self.disk_space: DiskSpaceStats = DiskSpaceStats()
        self.folders: Dict[str, FolderStats] = {}
        self.loop_lag: LoopLag = LoopLag()
        self.exports: ExportStats = ExportStats()
        self.counter_check: CounterCheck = CounterCheck()
        self.scans: ScanTimings = ScanTimings()
        self.gc: GcPauses = GcPauses()
        self.stalls: List[StallEvent] = []

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
        self.max_recent_logs: int = 50
        self.max_reconnect_history: int = 100

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

    def record_reconnect(self, reason: str, duration_seconds: float = 0.0) -> None:
        """
        Record a reconnect event.

        Args:
            reason: Disconnect reason
            duration_seconds: Downtime duration
        """
        now = datetime.now(timezone.utc)
        disconnect_time = datetime.fromtimestamp(
            now.timestamp() - duration_seconds,
            tz=timezone.utc
        )

        event = ReconnectEvent(
            timestamp=disconnect_time,
            reconnected_at=now,
            duration_seconds=duration_seconds,
            reason=reason
        )

        self.reconnect_events.append(event)
        self.last_reconnect = event

        # Trim history
        if len(self.reconnect_events) > self.max_reconnect_history:
            self.reconnect_events = self.reconnect_events[-self.max_reconnect_history:]

        # Log for debugging
        from python.utils.logging_setup import get_logger
        logger = get_logger("FiniexDataCollector.stats")
        logger.debug(
            f"[STATS] Reconnect recorded: reason={reason}, "
            f"duration={duration_seconds:.1f}s, "
            f"disconnect_time={disconnect_time}, "
            f"reconnected_at={now}, "
            f"total_events={len(self.reconnect_events)}"
        )

    def get_reconnects_this_week(self) -> List[ReconnectEvent]:
        """
        Get reconnect events from last 7 days.

        Returns:
            List of recent reconnect events
        """
        now = datetime.now(timezone.utc)
        week_ago = datetime.fromtimestamp(
            now.timestamp() - (7 * 24 * 3600),
            tz=timezone.utc
        )

        return [
            event for event in self.reconnect_events
            if event.timestamp >= week_ago
        ]

    def reset_weekly_reconnects(self) -> None:
        """Reset reconnect history (called after weekly report)."""
        self.reconnect_events = []
        self.last_reconnect = None

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
            self.gc.max_ms[generation] = max(
                self.gc.max_ms[generation], round(duration_ms, 1))

    def record_render(self, duration_ms: float) -> None:
        """
        Record how long the live display took to draw one frame.

        Args:
            duration_ms: Milliseconds spent rendering, on the event loop
        """
        self.scans.render_last_ms = round(duration_ms, 1)
        self.scans.render_max_ms = max(
            self.scans.render_max_ms, round(duration_ms, 1))

    def record_stall(self, duration_ms: float, gc_ms: float,
                     gc_generation: int, render_ms: float,
                     exports_in_flight: int) -> None:
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
        """
        half = duration_ms / 2

        if gc_ms >= half:
            cause = f"garbage collection, generation {gc_generation}"
        elif render_ms >= half:
            cause = "the live display"
        elif exports_in_flight:
            cause = "an archive export was handed over"
        else:
            cause = "unknown"

        self.stalls.append(StallEvent(
            at=datetime.now(timezone.utc),
            ms=round(duration_ms, 1),
            gc_ms=round(gc_ms, 1),
            gc_generation=gc_generation,
            render_ms=round(render_ms, 1),
            exports_in_flight=exports_in_flight,
            cause=cause))

        if len(self.stalls) > self.max_stalls:
            self.stalls = self.stalls[-self.max_stalls:]

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
