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
from typing import Optional, List, Dict, Any, TextIO

from python.writers.base import AbstractTickWriter
from python.types.tick_types import (
    COLLECTED_MSC_TIMEBASE,
    DATA_FORMAT_VERSION,
    TickData,
    TickFileMetadata,
    TickFileContent,
    TickFileSummary,
    AnchorSummary,
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
from python.utils.collection_clock import CollectionClock
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
        clock: CollectionClock,
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
            clock: The session clock that stamped the incoming ticks. Required
                without a default, because a writer that invented its own would
                report zero corrections for a clock it never read.
            broker: Broker name
            server: Server identifier
            broker_type: Broker type identifier (e.g., "kraken_spot")
            max_ticks_per_file: Maximum ticks before rotation
            data_collector: Data collector identifier
        """
        super().__init__(output_dir, symbol, max_ticks_per_file)

        self._clock = clock
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
        self._file_start_local_time: Optional[datetime] = None
        self._file_start_resyncs = 0
        self._file_start_max_correction_ms = 0
        self._errors: List[Dict[str, Any]] = []

        # Write-ahead log: every tick lands here the moment it arrives, so a
        # crash costs the last line rather than the whole buffer. Ticks are held
        # in memory until rotation, which on a thin symbol can be weeks - DASHUSD
        # needs 24 days to reach 50,000.
        self._wal: Optional[TextIO] = None
        self._wal_path: Optional[Path] = None
        self._wal_resyncs = 0

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

        # Write-ahead before counting it as collected
        self._append_to_wal(tick)

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

        # Reset state. The anchor counters are cumulative over the session, so
        # the opening state is captured here and the closing state is read at
        # finalize - a file whose two states differ contains a clamped tick.
        self._file_start_time = now
        self._file_start_local_time = datetime.now()
        self._file_start_resyncs = self._clock.resyncs
        self._file_start_max_correction_ms = self._clock.max_correction_ms
        self._ticks_buffer = []
        self._current_tick_count = 0
        self._errors = []

        self._open_wal()

        self._logger.debug(f"Started new file: {filename}")

    def _finalize_current_file(self) -> Path:
        """
        Write buffer to file and remove lock.

        Returns:
            Path to written file
        """
        if not self._current_file or not self._ticks_buffer:
            self._close_wal(delete=True)
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

        # Only now is the data in the archive file - drop the write-ahead log
        # after it, never before: a window with the data in two places is
        # recoverable, a window with it in neither is not.
        self._close_wal(delete=True)

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

    def _open_wal(self) -> None:
        """
        Open the write-ahead log for the current file and write its header.

        Line 1 carries the metadata as it stands at open - `start_time`, the
        device clock and the anchor counters. None of those can be recovered
        afterwards, which is why they go in first rather than at finalize.
        """
        if not self._current_file:
            return

        self._wal_path = self._current_file.with_suffix(".jsonl.part")
        self._wal_resyncs = self._clock.resyncs

        try:
            self._wal = self._wal_path.open("a", encoding="utf-8")
            self._wal.write(json.dumps(
                self._metadata_to_dict(self._build_metadata())) + "\n")
            self._wal.flush()
        except Exception as e:
            # Collection continues without the safety net rather than stopping:
            # losing a buffer on a crash is worse than never collecting at all.
            self._logger.error(f"Could not open write-ahead log: {e}")
            self._wal = None

    def _append_to_wal(self, tick: TickData) -> None:
        """
        Append one tick, and a checkpoint when the clock has been corrected.

        The checkpoint exists because `summary.anchor` describes the state at
        close, which a crashed process cannot report. Without it a recovered
        file would repeat its opening counters and thereby claim no correction
        happened inside it.

        Args:
            tick: The tick being collected
        """
        if not self._wal:
            return

        try:
            if self._clock.resyncs != self._wal_resyncs:
                self._wal.write(json.dumps({
                    "anchor_resyncs": self._clock.resyncs,
                    "anchor_max_correction_ms": self._clock.max_correction_ms
                }) + "\n")
                self._wal_resyncs = self._clock.resyncs

            self._wal.write(json.dumps(self._tick_to_dict(tick)) + "\n")
            self._wal.flush()
        except Exception as e:
            self._logger.error(f"Write-ahead log write failed: {e}")
            self._wal = None

    def _close_wal(self, delete: bool) -> None:
        """
        Close the write-ahead log, optionally removing it.

        Args:
            delete: Remove the file - only ever true after the archive file has
                been written successfully
        """
        if self._wal:
            try:
                self._wal.close()
            except Exception:
                pass
            self._wal = None

        if delete and self._wal_path and self._wal_path.exists():
            try:
                self._wal_path.unlink()
            except Exception as e:
                self._logger.warning(f"Could not remove write-ahead log: {e}")

        self._wal_path = None

    def _build_metadata(self) -> TickFileMetadata:
        """
        Build the metadata header.

        Split out of _build_file_content so the write-ahead log can write the
        same header at open - one source, so a recovered file and a rotated one
        cannot describe themselves differently.

        Returns:
            Metadata as of file open
        """
        return TickFileMetadata(
            symbol=self._symbol,
            broker=self._broker,
            server=self._server,
            broker_type=self._broker_type,
            local_device_time=self._file_start_local_time.strftime(
                "%Y.%m.%d %H:%M:%S") if self._file_start_local_time else "",
            broker_server_time=self._file_start_time.strftime(
                "%Y.%m.%d %H:%M:%S") if self._file_start_time else "",
            start_time=self._file_start_time.strftime(
                "%Y.%m.%d %H:%M:%S") if self._file_start_time else "",
            start_time_unix=int(self._file_start_time.timestamp()
                                ) if self._file_start_time else 0,
            timeframe="TICK",
            volume_timeframe="PERIOD_M1",
            volume_timeframe_minutes=1,
            data_format_version=DATA_FORMAT_VERSION,
            data_collector=self._data_collector,
            collected_msc_timebase=COLLECTED_MSC_TIMEBASE,
            anchor_resyncs=self._file_start_resyncs,
            anchor_max_correction_ms=self._file_start_max_correction_ms,
            collection_purpose="backtesting",
            operator="automated",
            symbol_info=self._get_symbol_info(),
            collection_settings=CollectionSettings(
                max_ticks_per_file=self._max_ticks_per_file
            ),
            error_tracking=ErrorTracking()
        )

    def _build_file_content(self) -> Dict[str, Any]:
        """
        Build complete file content structure.

        Returns:
            Dict matching MT5 JSON format
        """
        now = datetime.now(timezone.utc)
        metadata = self._build_metadata()

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
            anchor=AnchorSummary(
                resyncs=self._clock.resyncs,
                max_correction_ms=self._clock.max_correction_ms
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
            "local_device_time": metadata.local_device_time,
            "broker_server_time": metadata.broker_server_time,
            "start_time": metadata.start_time,
            "start_time_unix": metadata.start_time_unix,
            "timeframe": metadata.timeframe,
            "volume_timeframe": metadata.volume_timeframe,
            "volume_timeframe_minutes": metadata.volume_timeframe_minutes,
            "data_format_version": metadata.data_format_version,
            "data_collector": metadata.data_collector,
            "collected_msc_timebase": metadata.collected_msc_timebase,
            "anchor_resyncs": metadata.anchor_resyncs,
            "anchor_max_correction_ms": metadata.anchor_max_correction_ms,
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
            "quote_age_ms": tick.quote_age_ms,
            "tick_flags": tick.tick_flags,
            "session": tick.session
        }

    def _summary_to_dict(self, summary: TickFileSummary) -> Dict[str, Any]:
        """Convert summary dataclass to dict."""
        return {
            "total_ticks": summary.total_ticks,
            "total_errors": summary.total_errors,
            "data_stream_status": summary.data_stream_status,
            "quality_metrics": asdict(summary.quality_metrics) if summary.quality_metrics else {},
            "timing": asdict(summary.timing) if summary.timing else {},
            "anchor": asdict(summary.anchor) if summary.anchor else {},
            "recommendations": summary.recommendations
        }


def recover_orphaned_buffers(output_dir: Path, data_collector: str) -> List[Path]:
    """
    Turn write-ahead logs left by a crashed run into archive files.

    Runs once at startup, before collection begins. A `.jsonl.part` without its
    `*_ticks.json` means the process died between the first tick and the
    rotation; the data is in the log and nowhere else.

    Cases handled, in the order they matter:
      - archive file already exists: the crash fell between writing it and
        removing the log. Keep the file, drop the log. Never overwrite - the
        existing file is the one a consumer may already have read.
      - last line truncated: a crash mid-write. Skip it, keep the rest.
      - header unreadable: rename to `.corrupt` and leave it. Inventing metadata
        would produce a file that states things nobody measured.
      - no ticks, only a header: remove the log, write nothing. A file with zero
        ticks is noise, not an artifact.

    Args:
        output_dir: Base output directory
        data_collector: Collector name, the subdirectory files live in

    Returns:
        Paths of the archive files written
    """
    logger = get_collector_logger("recovery")
    target_dir = output_dir / data_collector

    if not target_dir.exists():
        return []

    recovered: List[Path] = []

    for wal_path in sorted(target_dir.glob("*_ticks.jsonl.part")):
        archive_path = wal_path.with_suffix("").with_suffix(".json")

        if archive_path.exists():
            logger.info(
                f"{archive_path.name} already written, dropping its log")
            wal_path.unlink()
            continue

        try:
            lines = wal_path.read_text(encoding="utf-8").splitlines()
        except Exception as e:
            logger.error(f"Cannot read {wal_path.name}: {e}")
            continue

        if not lines:
            wal_path.unlink()
            continue

        try:
            metadata = json.loads(lines[0])
        except json.JSONDecodeError:
            corrupt = wal_path.with_suffix(".corrupt")
            wal_path.rename(corrupt)
            logger.error(
                f"{wal_path.name} has an unreadable header, kept as "
                f"{corrupt.name} - not recovered")
            continue

        ticks: List[Dict[str, Any]] = []
        anchor = {
            "resyncs": metadata.get("anchor_resyncs", 0),
            "max_correction_ms": metadata.get("anchor_max_correction_ms", 0)
        }

        for line in lines[1:]:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # Only the final line can be torn; anything earlier would mean
                # the file is damaged in a way this routine should not paper over.
                if line is not lines[-1]:
                    logger.warning(
                        f"{wal_path.name}: unreadable line inside the log, skipped")
                continue

            if "time_msc" in record:
                ticks.append(record)
            elif "anchor_resyncs" in record:
                anchor = {
                    "resyncs": record["anchor_resyncs"],
                    "max_correction_ms": record["anchor_max_correction_ms"]
                }

        if not ticks:
            wal_path.unlink()
            logger.info(f"{wal_path.name} held no ticks, removed")
            continue

        last_event = ticks[-1]["time_msc"]
        start_unix = metadata.get("start_time_unix", 0)
        duration_minutes = round(
            (last_event / 1000 - start_unix) / 60, 1) if start_unix else 0.0

        content = {
            "metadata": metadata,
            "ticks": ticks,
            "errors": {
                "by_severity": {"negligible": 0, "serious": 0, "fatal": 0},
                "details": []
            },
            "summary": {
                "total_ticks": len(ticks),
                "total_errors": 0,
                "data_stream_status": "HEALTHY",
                "quality_metrics": {
                    "overall_quality_score": 1.0,
                    "data_integrity_score": 1.0,
                    "data_reliability_score": 1.0,
                    "negligible_error_rate": 0.0,
                    "serious_error_rate": 0.0,
                    "fatal_error_rate": 0.0
                },
                "timing": {
                    "end_time": datetime.fromtimestamp(
                        last_event / 1000, tz=timezone.utc
                    ).strftime("%Y.%m.%d %H:%M:%S"),
                    "duration_minutes": duration_minutes,
                    "avg_ticks_per_minute": round(
                        len(ticks) / duration_minutes, 1
                    ) if duration_minutes > 0 else 0.0
                },
                "anchor": anchor,
                "recommendations": (
                    "Recovered from a write-ahead log after an unclean stop. "
                    "The file is shorter than a rotation would have made it; "
                    "that is where the previous run ended."
                )
            }
        }

        # Same atomic path as a normal rotation: a consumer never sees a partial
        # file, recovered or not.
        fd, temp_path = tempfile.mkstemp(
            dir=str(target_dir), suffix=".tmp", text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(content, handle, indent=2, ensure_ascii=False)
            os.replace(temp_path, archive_path)
        except Exception as e:
            logger.error(f"Could not write {archive_path.name}: {e}")
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            continue

        wal_path.unlink()

        lock_path = archive_path.with_suffix(".json.lock")
        if lock_path.exists():
            lock_path.unlink()

        recovered.append(archive_path)
        logger.info(
            f"Recovered {archive_path.name} from its write-ahead log "
            f"({len(ticks):,} ticks)")

    return recovered
