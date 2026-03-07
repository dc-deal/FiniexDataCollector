"""
FiniexDataCollector - JSON Tick Writer
Writes ticks to JSON files matching MT5 output format.

Features:
- 50,000 tick file rotation
- Lock file protection for active files
- Atomic writes (temp file + rename)
- Quality metrics calculation

Location: python/writers/json_tick_writer.py
"""

import json
import os
import tempfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

from python.writers.base import AbstractTickWriter
from python.types.tick_types import (
    TickData,
    TickFileMetadata,
    TickFileContent,
    TickFileSummary,
    QualityMetrics,
    TimingSummary,
    SymbolInfo,
    CollectionSettings,
    ErrorTracking
)
from python.types.broker_config_types import BrokerConfig
from python.exceptions.collector_exceptions import (
    TickWriteError,
    FileRotationError
)
from python.utils.logging_setup import get_collector_logger


class JsonTickWriter(AbstractTickWriter):
    """
    Writes tick data to JSON files in MT5-compatible format.

    File naming: {SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.json
    Lock files: {SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.json.lock
    """

    def __init__(
        self,
        output_dir: Path,
        symbol: str,
        broker: str = "Kraken",
        server: str = "kraken_spot",
        broker_type: str = "",
        max_ticks_per_file: int = 50000,
        data_collector: str = "kraken"
    ):
        """
        Initialize JSON tick writer.

        Args:
            output_dir: Base output directory
            symbol: Trading symbol (normalized, e.g., "BTCUSD")
            broker: Broker name
            server: Server identifier
            broker_type: Broker type identifier (e.g., "kraken_spot")
            max_ticks_per_file: Maximum ticks before rotation
            data_collector: Data collector identifier
        """
        super().__init__(output_dir, symbol, max_ticks_per_file)

        self._broker = broker
        self._server = server
        self._broker_type = broker_type
        self._data_collector = data_collector
        self._logger = get_collector_logger(f"writer.{symbol}")

        # Current file state
        self._current_file: Optional[Path] = None
        self._current_lock: Optional[Path] = None
        self._ticks_buffer: List[TickData] = []
        self._file_start_time: Optional[datetime] = None
        self._errors: List[Dict[str, Any]] = []

        # Ensure output directory exists
        self._symbol_dir = self._output_dir / data_collector
        self._symbol_dir.mkdir(parents=True, exist_ok=True)

    def write_tick(self, tick: TickData) -> None:
        """
        Write single tick to buffer.

        Triggers rotation if buffer exceeds max_ticks_per_file.

        Args:
            tick: Tick data to write
        """
        # Initialize file if needed
        if self._current_file is None:
            self._start_new_file()

        # Add to buffer
        self._ticks_buffer.append(tick)
        self._current_tick_count += 1
        self._total_ticks_written += 1

        # Check rotation
        if self.needs_rotation():
            self.rotate_file()

    def rotate_file(self) -> Optional[Path]:
        """
        Close current file and start new one.

        Returns:
            Path to closed file
        """
        if not self._current_file:
            return None

        closed_file = self._finalize_current_file()
        self._start_new_file()

        self._logger.info(
            f"Rotated file: {closed_file.name} "
            f"({self._current_tick_count} ticks)"
        )

        return closed_file

    def finalize(self) -> Optional[Path]:
        """
        Finalize and close current file on shutdown.

        Returns:
            Path to finalized file
        """
        if not self._current_file:
            return None

        return self._finalize_current_file()

    def get_current_filepath(self) -> Optional[Path]:
        """Get path to current active file."""
        return self._current_file

    def get_lock_filepath(self) -> Optional[Path]:
        """Get path to lock file."""
        return self._current_lock

    def _start_new_file(self) -> None:
        """Initialize new tick file with lock."""
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        filename = f"{self._symbol}_{timestamp}_ticks.json"
        lock_filename = f"{filename}.lock"

        self._current_file = self._symbol_dir / filename
        self._current_lock = self._symbol_dir / lock_filename

        # Create lock file
        self._current_lock.touch()

        # Reset state
        self._file_start_time = now
        self._ticks_buffer = []
        self._current_tick_count = 0
        self._errors = []

        self._logger.debug(f"Started new file: {filename}")

    def _finalize_current_file(self) -> Path:
        """
        Write buffer to file and remove lock.

        Returns:
            Path to written file
        """
        if not self._current_file or not self._ticks_buffer:
            if self._current_lock and self._current_lock.exists():
                self._current_lock.unlink()
            return self._current_file

        # Build file content
        content = self._build_file_content()

        # Atomic write: temp file + rename
        try:
            self._atomic_write(content)
        except Exception as e:
            raise TickWriteError(
                message=str(e),
                filepath=str(self._current_file),
                tick_count=len(self._ticks_buffer)
            )

        # Remove lock file
        if self._current_lock and self._current_lock.exists():
            self._current_lock.unlink()

        self._files_created += 1
        completed_file = self._current_file

        # Reset state
        self._current_file = None
        self._current_lock = None
        self._ticks_buffer = []

        self._logger.info(
            f"Finalized: {completed_file.name} "
            f"({self._current_tick_count} ticks)"
        )

        return completed_file

    def _build_file_content(self) -> Dict[str, Any]:
        """
        Build complete file content structure.

        Returns:
            Dict matching MT5 JSON format
        """
        now = datetime.now(timezone.utc)

        # Metadata
        metadata = TickFileMetadata(
            symbol=self._symbol,
            broker=self._broker,
            server=self._server,
            broker_type=self._broker_type,
            broker_utc_offset_hours=0,
            local_device_time=now.strftime("%Y.%m.%d %H:%M:%S"),
            broker_server_time=now.strftime("%Y.%m.%d %H:%M:%S"),
            start_time=self._file_start_time.strftime(
                "%Y.%m.%d %H:%M:%S") if self._file_start_time else "",
            start_time_unix=int(self._file_start_time.timestamp()
                                ) if self._file_start_time else 0,
            timeframe="TICK",
            volume_timeframe="PERIOD_M1",
            volume_timeframe_minutes=1,
            data_format_version="1.3.0",
            data_collector=self._data_collector,
            collection_purpose="backtesting",
            operator="automated",
            symbol_info=self._get_symbol_info(),
            collection_settings=CollectionSettings(
                max_ticks_per_file=self._max_ticks_per_file
            ),
            error_tracking=ErrorTracking()
        )

        # Summary
        duration_minutes = 0.0
        if self._file_start_time:
            duration = (now - self._file_start_time).total_seconds()
            duration_minutes = round(duration / 60, 1)

        avg_ticks_per_minute = 0.0
        if duration_minutes > 0:
            avg_ticks_per_minute = round(
                len(self._ticks_buffer) / duration_minutes, 1)

        summary = TickFileSummary(
            total_ticks=len(self._ticks_buffer),
            total_errors=len(self._errors),
            data_stream_status="HEALTHY" if len(
                self._errors) == 0 else "DEGRADED",
            quality_metrics=QualityMetrics(
                overall_quality_score=self._calculate_quality_score(),
                data_integrity_score=1.0,
                data_reliability_score=1.0,
                negligible_error_rate=0.0,
                serious_error_rate=0.0,
                fatal_error_rate=0.0
            ),
            timing=TimingSummary(
                end_time=now.strftime("%Y.%m.%d %H:%M:%S"),
                duration_minutes=duration_minutes,
                avg_ticks_per_minute=avg_ticks_per_minute
            ),
            recommendations=self._get_recommendations()
        )

        # Build final structure
        return {
            "metadata": self._metadata_to_dict(metadata),
            "ticks": [self._tick_to_dict(t) for t in self._ticks_buffer],
            "errors": {
                "by_severity": {
                    "negligible": 0,
                    "serious": 0,
                    "fatal": 0
                },
                "details": self._errors
            },
            "summary": self._summary_to_dict(summary)
        }

    def _atomic_write(self, content: Dict[str, Any]) -> None:
        """
        Write content atomically using temp file + rename.

        Args:
            content: File content dict
        """
        # Create temp file in same directory
        fd, temp_path = tempfile.mkstemp(
            dir=self._symbol_dir,
            suffix=".tmp"
        )

        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(content, f, indent=2)

            # Atomic rename
            os.replace(temp_path, self._current_file)

        except Exception:
            # Clean up temp file on error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

    def _get_symbol_info(self) -> SymbolInfo:
        """
        Get symbol info from BrokerConfig.

        Uses API-sourced values for digits, tick_size, point.
        """
        try:
            config = BrokerConfig.get_symbol(self._symbol)
            return SymbolInfo(
                point_value=config.tick_size,
                digits=config.digits,
                tick_size=config.tick_size,
                tick_value=1.0
            )
        except Exception:
            # Fallback if BrokerConfig not loaded (should not happen)
            self._logger.warning(
                f"BrokerConfig not available for {self._symbol}, using defaults"
            )
            return SymbolInfo(
                point_value=0.00001,
                digits=5,
                tick_size=0.00001,
                tick_value=1.0
            )

    def _calculate_quality_score(self) -> float:
        """Calculate overall quality score."""
        if not self._ticks_buffer:
            return 1.0

        error_rate = len(self._errors) / len(self._ticks_buffer)
        return max(0.0, 1.0 - error_rate)

    def _get_recommendations(self) -> str:
        """Get recommendations based on data quality."""
        if len(self._errors) == 0:
            return "Data quality is excellent - no specific recommendations."
        elif len(self._errors) < 10:
            return "Minor data quality issues detected - review error details."
        else:
            return "Significant data quality issues - investigate connection stability."

    def _metadata_to_dict(self, metadata: TickFileMetadata) -> Dict[str, Any]:
        """Convert metadata dataclass to dict."""
        return {
            "symbol": metadata.symbol,
            "broker": metadata.broker,
            "server": metadata.server,
            "broker_type": metadata.broker_type,
            "broker_utc_offset_hours": metadata.broker_utc_offset_hours,
            "local_device_time": metadata.local_device_time,
            "broker_server_time": metadata.broker_server_time,
            "start_time": metadata.start_time,
            "start_time_unix": metadata.start_time_unix,
            "timeframe": metadata.timeframe,
            "volume_timeframe": metadata.volume_timeframe,
            "volume_timeframe_minutes": metadata.volume_timeframe_minutes,
            "data_format_version": metadata.data_format_version,
            "collection_purpose": metadata.collection_purpose,
            "operator": metadata.operator,
            "symbol_info": asdict(metadata.symbol_info) if metadata.symbol_info else {},
            "collection_settings": asdict(metadata.collection_settings) if metadata.collection_settings else {},
            "error_tracking": asdict(metadata.error_tracking) if metadata.error_tracking else {}
        }

    def _tick_to_dict(self, tick: TickData) -> Dict[str, Any]:
        """Convert tick dataclass to dict."""
        return {
            "timestamp": tick.timestamp,
            "time_msc": tick.time_msc,
            "collected_msc": tick.collected_msc,
            "bid": tick.bid,
            "ask": tick.ask,
            "last": tick.last,
            "tick_volume": tick.tick_volume,
            "real_volume": tick.real_volume,
            "chart_tick_volume": tick.chart_tick_volume,
            "spread_points": tick.spread_points,
            "spread_pct": tick.spread_pct,
            "tick_flags": tick.tick_flags,
            "session": tick.session,
            "server_time": tick.server_time
        }

    def _summary_to_dict(self, summary: TickFileSummary) -> Dict[str, Any]:
        """Convert summary dataclass to dict."""
        return {
            "total_ticks": summary.total_ticks,
            "total_errors": summary.total_errors,
            "data_stream_status": summary.data_stream_status,
            "quality_metrics": asdict(summary.quality_metrics) if summary.quality_metrics else {},
            "timing": asdict(summary.timing) if summary.timing else {},
            "recommendations": summary.recommendations
        }
