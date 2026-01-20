"""
FiniexDataCollector - Collector Statistics Types
Type definitions for real-time collection monitoring.

Location: python/types/collector_stats.py
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class SymbolStats:
    """
    Real-time statistics for a single symbol.

    Attributes:
        symbol: Trading symbol (e.g., "BTCUSD")
        ticks_count: Total ticks collected for this symbol
        ticks_per_minute: Current tick rate
        last_bid: Last bid price
        last_ask: Last ask price
        last_spread_pct: Last spread as percentage
        last_tick_time: Timestamp of last tick
        errors_count: Errors for this symbol
        file_count: Number of files created
        current_file_ticks: Ticks in current file
    """
    symbol: str
    ticks_count: int = 0
    ticks_per_minute: float = 0.0
    last_bid: float = 0.0
    last_ask: float = 0.0
    last_spread_pct: float = 0.0
    last_volume: float = 0.0
    last_tick_time: Optional[datetime] = None
    errors_count: int = 0
    file_count: int = 0
    current_file_ticks: int = 0

    @property
    def is_active(self) -> bool:
        """Check if symbol received ticks recently (within 30s)."""
        if not self.last_tick_time:
            return False
        delta = (datetime.utcnow() - self.last_tick_time).total_seconds()
        return delta < 30


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
class CollectorStats:
    """
    Aggregated statistics for the entire collector.

    Central stats object updated by collector components.
    Read by LiveDisplay for rendering.

    Attributes:
        start_time: When collection started
        total_ticks: Total ticks across all symbols
        total_files: Total files created
        total_errors: Total errors across all components
        total_warnings: Total warnings
        websocket_status: Current WebSocket connection status
        symbols: Per-symbol statistics
        recent_logs: Recent error/warning log entries
        last_file: Most recently created file
    """
    start_time: datetime = field(default_factory=datetime.utcnow)
    total_ticks: int = 0
    total_files: int = 0
    total_errors: int = 0
    total_warnings: int = 0
    websocket_status: str = "disconnected"
    symbols: dict = field(default_factory=dict)  # symbol -> SymbolStats
    recent_logs: List[LogEntry] = field(default_factory=list)
    last_file: Optional[FileInfo] = None

    # Config for log history
    max_recent_logs: int = 50

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
        """
        stats = self.get_symbol_stats(symbol)
        stats.ticks_count += 1
        stats.last_bid = bid
        stats.last_ask = ask
        stats.last_spread_pct = spread_pct
        stats.last_volume = real_volume
        stats.last_tick_time = datetime.utcnow()
        stats.current_file_ticks += 1
        self.total_ticks += 1

    def record_file_created(self, symbol: str, filename: str, tick_count: int) -> None:
        """
        Record a file creation.

        Args:
            symbol: Symbol name
            filename: Created filename
            tick_count: Ticks in the file
        """
        stats = self.get_symbol_stats(symbol)
        stats.file_count += 1
        stats.current_file_ticks = 0
        self.total_files += 1

        self.last_file = FileInfo(
            filename=filename,
            symbol=symbol,
            tick_count=tick_count,
            created_at=datetime.utcnow()
        )

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
            timestamp=datetime.utcnow(),
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

    def get_uptime_seconds(self) -> float:
        """Get collector uptime in seconds."""
        return (datetime.utcnow() - self.start_time).total_seconds()

    def calculate_ticks_per_minute(self) -> None:
        """Calculate ticks/minute for all symbols based on uptime."""
        uptime_minutes = self.get_uptime_seconds() / 60
        if uptime_minutes < 0.1:  # Less than 6 seconds
            return

        for stats in self.symbols.values():
            stats.ticks_per_minute = round(
                stats.ticks_count / uptime_minutes, 1)
