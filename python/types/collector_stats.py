"""
FiniexDataCollector - Collector Statistics Types
Type definitions for real-time collection monitoring.

Location: python/types/collector_stats.py
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Dict


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
        last_volume: Last real volume
        last_tick_time: Timestamp of last tick
        errors_count: Errors for this symbol
        file_count: Number of files created this session
        folder_file_count: Total files in folder (all sessions)
    """
    symbol: str
    current_file_ticks: int = 0
    last_bid: float = 0.0
    last_ask: float = 0.0
    last_spread_pct: float = 0.0
    last_volume: float = 0.0
    last_tick_time: Optional[datetime] = None
    errors_count: int = 0
    file_count: int = 0
    folder_file_count: int = 0

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

        # Config
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

    def record_tick(self, symbol: str, bid: float, ask: float, spread_pct: float, real_volume: float) -> None:
        """
        Record a received tick.

        Args:
            symbol: Symbol name
            bid: Bid price
            ask: Ask price
            spread_pct: Spread percentage
            real_volume: Real volume
        """
        stats = self.get_symbol_stats(symbol)
        stats.current_file_ticks += 1
        stats.last_bid = bid
        stats.last_ask = ask
        stats.last_spread_pct = spread_pct
        stats.last_volume = real_volume
        stats.last_tick_time = datetime.now(timezone.utc)

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
