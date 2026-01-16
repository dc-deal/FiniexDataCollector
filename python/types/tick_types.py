"""
FiniexDataCollector - Tick Data Types
Core data structures for tick collection and processing.

Location: python/framework/types/tick_types.py
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any


@dataclass
class TickData:
    """
    Single tick data point - matches MT5 output format.

    All times are UTC with timezone awareness.
    """
    symbol: str                       # "BTCUSD" - normalized symbol for routing
    timestamp: str                    # "2025.01.13 14:30:45"
    time_msc: int                     # Unix milliseconds
    bid: float
    ask: float
    last: float
    tick_volume: int = 0
    real_volume: float = 0.0
    chart_tick_volume: int = 0
    spread_points: int = 0
    spread_pct: float = 0.0
    tick_flags: str = "BID ASK"
    session: str = "24h"              # "24h" for crypto, forex has sessions
    server_time: str = ""


@dataclass
class SymbolInfo:
    """Symbol metadata from broker config."""
    point_value: float
    digits: int
    tick_size: float
    tick_value: float = 0.0


@dataclass
class CollectionSettings:
    """Settings for tick collection behavior."""
    max_ticks_per_file: int = 50000
    max_errors_per_file: int = 1000
    include_real_volume: bool = True
    include_tick_flags: bool = True
    stop_on_fatal_errors: bool = False


@dataclass
class ErrorTracking:
    """Error tracking configuration."""
    enabled: bool = True
    log_negligible: bool = True
    log_serious: bool = True
    log_fatal: bool = True
    max_spread_percent: float = 5.0
    max_price_jump_percent: float = 10.0
    max_data_gap_seconds: int = 300


@dataclass
class TickFileMetadata:
    """
    Metadata header for tick JSON files.

    Mirrors MT5 TickCollector output format.
    """
    symbol: str
    broker: str
    server: str
    broker_utc_offset_hours: int = 0
    local_device_time: str = ""
    broker_server_time: str = ""
    start_time: str = ""
    start_time_unix: int = 0
    timeframe: str = "TICK"
    volume_timeframe: str = "PERIOD_M1"
    volume_timeframe_minutes: int = 1
    data_format_version: str = "1.0.5"
    data_collector: str = "kraken"
    collection_purpose: str = "backtesting"
    operator: str = "automated"
    symbol_info: Optional[SymbolInfo] = None
    collection_settings: Optional[CollectionSettings] = None
    error_tracking: Optional[ErrorTracking] = None


@dataclass
class ErrorSummary:
    """Error counts by severity."""
    negligible: int = 0
    serious: int = 0
    fatal: int = 0


@dataclass
class ErrorDetail:
    """Single error detail entry."""
    timestamp: str
    error_type: str
    severity: str
    message: str
    data: Optional[Dict[str, Any]] = None


@dataclass
class QualityMetrics:
    """Data quality metrics for tick file."""
    overall_quality_score: float = 1.0
    data_integrity_score: float = 1.0
    data_reliability_score: float = 1.0
    negligible_error_rate: float = 0.0
    serious_error_rate: float = 0.0
    fatal_error_rate: float = 0.0


@dataclass
class TimingSummary:
    """Timing information for tick file."""
    end_time: str = ""
    duration_minutes: float = 0.0
    avg_ticks_per_minute: float = 0.0


@dataclass
class TickFileSummary:
    """Summary section of tick JSON file."""
    total_ticks: int = 0
    total_errors: int = 0
    data_stream_status: str = "HEALTHY"
    quality_metrics: Optional[QualityMetrics] = None
    timing: Optional[TimingSummary] = None
    recommendations: str = ""


@dataclass
class TickFileContent:
    """
    Complete tick file structure.

    Matches MT5 TickCollector JSON output format exactly.
    """
    metadata: TickFileMetadata
    ticks: List[TickData] = field(default_factory=list)
    errors: Dict[str, Any] = field(default_factory=lambda: {
        "by_severity": {"negligible": 0, "serious": 0, "fatal": 0},
        "details": []
    })
    summary: Optional[TickFileSummary] = None


@dataclass
class KrakenTickerMessage:
    """
    Parsed Kraken WebSocket ticker message.

    Intermediate format before conversion to TickData.
    """
    symbol: str           # "BTC/USD"
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float
    last: float
    volume: float         # 24h volume
    vwap: float
    low: float            # 24h low
    high: float           # 24h high
    change: float
    change_pct: float
    received_at_msc: int  # Local receive timestamp in milliseconds
