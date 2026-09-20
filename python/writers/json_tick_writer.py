"""
FiniexDataCollector - JSON Tick Writer
Writes ticks to JSON files matching MT5 output format.

Features:
- Rotation at max_ticks_per_file and at the UTC day boundary
- A write-ahead log per file, which also marks the file as open
- Atomic writes (temp file + rename), here or in a subprocess
- Quality metrics calculation

Location: python/writers/json_tick_writer.py
"""

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional, List, Dict, Any, TextIO

from python.writers.base import AbstractTickWriter
from python.writers.wal_archive import (
    CLOSE_RECORD_KEY,
    WalContents,
    WalUnreadable,
    archive_path_for,
    build_archive_text,
    read_wal,
    write_archive_atomically,
)
from python.types.tick_types import (
    COLLECTED_MSC_TIMEBASE,
    DATA_FORMAT_VERSION,
    TickData,
    TickFileMetadata,
    TickFileContent,
    TickFileSummary,
    AnchorSummary,
    OriginBlock,
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
from python.utils.logging_setup import describe_exception, get_collector_logger


class JsonTickWriter(AbstractTickWriter):
    """
    Writes tick data to JSON files in MT5-compatible format.

    File naming: {SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.json
    Write-ahead log: {SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.jsonl.part
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
        data_collector: str = "kraken",
        origin: Optional[OriginBlock] = None,
        exporter: Optional[Callable[[Path, Path], None]] = None
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
            origin: Which instance produced these files. Absent only in tests;
                main.py always supplies it, and a collector that cannot
                establish its identity refuses to start.
            exporter: Called with (write-ahead log, archive file) when a file
                closes, and expected to write the archive and remove the log -
                main.py hands that to a subprocess. Without one the file is
                written here, inline, which is what a graceful stop does and
                what every test does unless it says otherwise.
        """
        super().__init__(output_dir, symbol, max_ticks_per_file)

        self._clock = clock
        self._broker = broker
        self._server = server
        self._broker_type = broker_type
        self._data_collector = data_collector
        self._origin = origin
        self._exporter = exporter
        self._logger = get_collector_logger(f"writer.{symbol}")

        # Current file state
        self._current_file: Optional[Path] = None
        self._ticks_buffer: List[TickData] = []
        self._file_start_time: Optional[datetime] = None
        self._file_start_local_time: Optional[datetime] = None
        self._file_start_resyncs = 0
        self._file_start_max_correction_ms = 0
        # UTC day this file covers, taken from its FIRST tick rather than from
        # the wall clock at open: both come from the same session clock in
        # production, but deriving it from the data keeps the file self
        # consistent whatever the clock is doing.
        self._file_day: Optional[str] = None
        self._errors: List[Dict[str, Any]] = []
        # How many ticks the file that closed last held. The caller cannot
        # derive it: at the day cut the triggering tick belongs to the NEW file,
        # at the count threshold to the old one.
        self._last_closed_tick_count = 0

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

        # The day boundary is checked BEFORE the tick is appended, unlike the
        # tick-count threshold below. Checked after, the first tick of the new
        # day would land in the previous day's file and only then trigger the
        # rotation - which is precisely the property a daily close exists to
        # provide.
        self._roll_to_new_day(tick)

        # Write-ahead before counting it as collected
        self._append_to_wal(tick)

        # Add to buffer
        self._ticks_buffer.append(tick)
        self._current_tick_count += 1
        self._total_ticks_written += 1

        # Check rotation
        if self.needs_rotation():
            self.rotate_file()

    def _roll_to_new_day(self, tick: TickData) -> None:
        """
        Close the current file when a tick belongs to a later UTC day.

        A file bounded only by tick count can span several days, which makes any
        age-based retention rule ambiguous and leaves the question open whether
        more will arrive for a date already read. A file that covers exactly one
        UTC day is final the moment that day ends.

        The day comes from the tick's arrival (`collected_msc`) and is fixed by
        the file's FIRST tick. Comparing against the wall clock at file open
        would mix two sources that agree in production and nowhere else.

        Args:
            tick: The tick about to be written
        """
        if self._current_file is None:
            return

        tick_day = datetime.fromtimestamp(
            tick.collected_msc / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

        if self._file_day is None:
            self._file_day = tick_day
            return

        if tick_day == self._file_day:
            return

        if self._ticks_buffer:
            self.rotate_file()
        else:
            # Nothing collected yet - reopen under the new day's name rather
            # than logging a rotation that produced no file.
            self._close_wal(delete=True)
            self._start_new_file()

    def rotate_file(self) -> Optional[Path]:
        """
        Close current file and start new one.

        Returns:
            Path to closed file
        """
        if not self._current_file:
            return None

        # Read before _start_new_file resets it - this line reported "0 ticks"
        # for every rotation the production log holds, because the counter was
        # read after the reset.
        tick_count = self._current_tick_count

        closed_file = self._finalize_current_file()
        self._start_new_file()

        self._logger.info(
            f"Rotated file: {closed_file.name} ({tick_count} ticks)")

        return closed_file

    def finalize(self) -> Optional[Path]:
        """
        Finalize and close the current file on shutdown.

        An empty buffer means the file was rotated moments ago and nothing has
        arrived since. There is a write-ahead log holding only its header;
        closing it is the whole job, and the answer is None - reporting a path
        would name a file that was never written, in the one log somebody reads
        afterwards to find out whether the stop was clean. Measured 2026-09-17
        on the production box: the shutdown named thirteen files and twelve were
        on disk.

        The check belongs here and not in `_finalize_current_file`, which the
        rotation path also calls and which reads `.name` off the result.

        The file is written here rather than handed to a subprocess: the loop
        this hands off to protect is ending anyway, and a child started now
        would outlive the process that is supposed to wait for it.

        Returns:
            Path to the finalized file, or None when there was nothing to write
        """
        if not self._current_file:
            return None

        if not self._ticks_buffer:
            self._close_wal(delete=True)
            self._current_file = None
            return None

        return self._finalize_current_file(use_exporter=False)

    def get_current_filepath(self) -> Optional[Path]:
        """Get path to current active file."""
        return self._current_file

    @property
    def last_closed_tick_count(self) -> int:
        """Ticks in the file that closed last, as it was written."""
        return self._last_closed_tick_count

    def _start_new_file(self) -> None:
        """Initialize new tick file and its write-ahead log."""
        now = datetime.now(timezone.utc)

        # The name has second resolution, so two rotations of one symbol inside
        # the same second would collide and the second file would overwrite the
        # first at finalize - silently. Implausible at 50,000 ticks per file and
        # reachable the moment a day boundary cuts one, so the stamp is advanced
        # until the name is free. It is an identifier, not data: the true open
        # time is in `start_time`, and the consuming importer orders files by
        # their tick bounds rather than by their names.
        stamp = now
        while True:
            filename = f"{self._symbol}_{stamp.strftime('%Y%m%d_%H%M%S')}_ticks.json"
            candidate = self._symbol_dir / filename
            if not candidate.exists() and not candidate.with_suffix(
                    ".jsonl.part").exists():
                break
            stamp += timedelta(seconds=1)

        self._current_file = candidate

        # Reset state. The anchor counters are cumulative over the session, so
        # the opening state is captured here and the closing state is read at
        # finalize - a file whose two states differ contains a clamped tick.
        self._file_start_time = now
        self._file_start_local_time = datetime.now()
        self._file_start_resyncs = self._clock.resyncs
        self._file_start_max_correction_ms = self._clock.max_correction_ms
        self._file_day = None
        self._ticks_buffer = []
        self._current_tick_count = 0
        self._errors = []

        self._open_wal()

        self._logger.debug(f"Started new file: {filename}")

    def _finalize_current_file(self, use_exporter: bool = True) -> Path:
        """
        Close the current file: hand it to the exporter, or write it here.

        Args:
            use_exporter: False writes the file on the spot even when an
                exporter is configured - what a graceful stop does, because
                there is no event loop left to protect and no child to wait for

        Returns:
            Path of the file, which the exporter may not have written yet
        """
        if not self._current_file or not self._ticks_buffer:
            self._close_wal(delete=True)
            return self._current_file

        completed_file = self._current_file
        tick_count = len(self._ticks_buffer)

        # The buffer is the file: whoever reports this count afterwards reports
        # what was written, not what a second counter believed.
        self._last_closed_tick_count = tick_count

        if self._exporter and use_exporter:
            return self._hand_over_to_exporter(completed_file, tick_count)

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

        self._files_created += 1

        # Reset state
        self._current_file = None
        self._ticks_buffer = []

        self._logger.info(
            f"Finalized: {completed_file.name} ({tick_count} ticks)")

        return completed_file

    def _hand_over_to_exporter(self, completed_file: Path,
                               tick_count: int) -> Path:
        """
        Give the finished log to whoever writes the archive file.

        The data is already on disk, line by line, so nothing is serialized
        here: the closing state goes into the log as its last record and the log
        is closed but **kept**. Whoever writes the archive removes it afterwards,
        which preserves the ordering the whole mechanism rests on - the log
        outlives the window in which the archive does not exist yet.

        Measured on production 2026-09-20: writing the file here cost about 1 s
        plus 66 us per tick, on the collector's only event loop, nine times in a
        row at the UTC day cut.

        Args:
            completed_file: The archive file that is now owed
            tick_count: How many ticks it will hold

        Returns:
            Path of the archive file, which does not exist yet
        """
        self._append_close_record()
        wal_path = self._wal_path
        self._close_wal(delete=False)

        if wal_path is None:
            # Nothing to hand over: opening the log failed earlier and was
            # reported then, so these ticks exist in memory only. They are
            # written here rather than handed to a reader of a file that does
            # not exist. State is untouched at this point, so the inline path
            # still finds its buffer.
            self._logger.error(
                f"{completed_file.name} has no write-ahead log to export from "
                f"- writing it inline instead")
            return self._finalize_current_file(use_exporter=False)

        self._files_created += 1
        self._current_file = None
        self._ticks_buffer = []

        self._logger.info(
            f"Closed: {completed_file.name} ({tick_count} ticks) - "
            f"handed to the archive writer")

        try:
            # Absolute, because the exporter does not share this process's
            # working directory: the subprocess runs from the repository root so
            # it can import the package, while `raw_data_dir` is relative in the
            # shipped configuration. Measured on the first live run: every
            # export failed with FileNotFoundError on a path that was correct
            # where it was built.
            self._exporter(wal_path.resolve(), completed_file.resolve())
        except Exception as e:
            # This runs inside write_tick, on the tick path. A handover that
            # throws must cost the file's export, which the next start repairs
            # from the log, and never the tick that happened to trigger it.
            self._logger.error(
                f"Could not hand {completed_file.name} to the archive writer: "
                f"{describe_exception(e)} - its log stays on disk and the next "
                f"start recovers it")

        return completed_file

    def _append_close_record(self) -> None:
        """
        Append the closing state as the log's last line.

        Everything a finished file needs that the log does not already hold:
        the summary as the writer computed it, including the anchor counters at
        close and the end time. Without it a file built from the log could only
        state what a reader can infer, and would describe itself as recovered.
        """
        if not self._wal:
            return

        try:
            self._wal.write(json.dumps(
                {CLOSE_RECORD_KEY: self._build_close_record()}) + "\n")
            self._wal.flush()
        except Exception as e:
            # The file can still be built without it; it would then read as a
            # recovered file, which is a true statement about a log that was
            # never closed properly.
            self._logger.error(
                f"Could not write the closing record: {describe_exception(e)}")

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
            self._logger.error(
                f"Could not open write-ahead log: {describe_exception(e)}")
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
            self._logger.error(
                f"Write-ahead log write failed: {describe_exception(e)}")
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
                self._logger.warning(
                    f"Could not remove write-ahead log: {describe_exception(e)}")

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
            origin=self._origin,
            collection_purpose="backtesting",
            operator="automated",
            symbol_info=self._get_symbol_info(),
            collection_settings=CollectionSettings(
                max_ticks_per_file=self._max_ticks_per_file
            ),
            error_tracking=ErrorTracking()
        )

    def _build_close_record(self) -> Dict[str, Any]:
        """
        Build the errors and summary blocks as they stand at close.

        Split out of _build_file_content so the same two blocks can be appended
        to the write-ahead log: a file finished by a subprocess is then finished
        from exactly what the writer knew, not from what a reader could infer.

        Returns:
            Dict with an "errors" and a "summary" block
        """
        now = datetime.now(timezone.utc)

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

        return {
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

    def _build_file_content(self) -> Dict[str, Any]:
        """
        Build complete file content structure.

        Returns:
            Dict matching MT5 JSON format
        """
        close = self._build_close_record()

        return {
            "metadata": self._metadata_to_dict(self._build_metadata()),
            "ticks": [self._tick_to_dict(t) for t in self._ticks_buffer],
            "errors": close["errors"],
            "summary": close["summary"]
        }

    def _atomic_write(self, content: Dict[str, Any]) -> None:
        """
        Write content atomically using temp file + rename.

        Args:
            content: File content dict
        """
        write_archive_atomically(
            build_archive_text(WalContents(
                metadata=content["metadata"],
                ticks=content["ticks"],
                close={"errors": content["errors"],
                       "summary": content["summary"]})),
            self._current_file)

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
            "origin": asdict(metadata.origin) if metadata.origin else None,
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
            "trade_id": tick.trade_id,
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


def _drop_log(wal_path: Path, logger) -> bool:
    """
    Remove a write-ahead log that is no longer needed, without letting one
    undeletable file end the recovery.

    A bare unlink() here aborts the whole startup, because main.py iterates this
    function's result directly. On Windows a handle held by another process -
    antivirus, a backup agent, or a second collector the instance lock did not
    catch - makes that outcome reachable. Skipping one log costs a duplicate on
    the next start, which the "archive already exists" branch above then drops.

    Args:
        wal_path: The log to remove
        logger: Where to report a refusal

    Returns:
        True if the file is gone
    """
    try:
        wal_path.unlink()
        return True
    except FileNotFoundError:
        return True
    except OSError as e:
        logger.warning(
            f"Could not remove {wal_path.name} ({describe_exception(e)}) - left in place, "
            f"the next start will find it again")
        return False


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
        archive_path = archive_path_for(wal_path)

        if archive_path.exists():
            logger.info(
                f"{archive_path.name} already written, dropping its log")
            _drop_log(wal_path, logger)
            continue

        try:
            contents = read_wal(wal_path)
        except WalUnreadable:
            if wal_path.stat().st_size == 0:
                _drop_log(wal_path, logger)
                continue
            corrupt = wal_path.with_suffix(".corrupt")
            wal_path.rename(corrupt)
            logger.error(
                f"{wal_path.name} has an unreadable header, kept as "
                f"{corrupt.name} - not recovered")
            continue
        except Exception as e:
            logger.error(
                f"Cannot read {wal_path.name}: {describe_exception(e)}")
            continue

        if contents.torn_lines > 1:
            # Only the final line can be torn by a crash mid-write; more than
            # one means damage this routine should not paper over silently.
            logger.warning(
                f"{wal_path.name}: {contents.torn_lines} unreadable lines, "
                f"skipped")

        if not contents.ticks:
            _drop_log(wal_path, logger)
            logger.info(f"{wal_path.name} held no ticks, removed")
            continue

        # The same builder and the same atomic path a rotation uses, so a
        # recovered file and a rotated one cannot differ in anything but what
        # the log actually held.
        try:
            write_archive_atomically(
                build_archive_text(contents), archive_path)
        except Exception as e:
            logger.error(
                f"Could not write {archive_path.name}: {describe_exception(e)}")
            continue

        _drop_log(wal_path, logger)
        recovered.append(archive_path)
        logger.info(
            f"Recovered {archive_path.name} from its write-ahead log "
            f"({len(contents.ticks):,} ticks)"
            + (" - closed by its writer, so it is a complete file"
               if contents.close else ""))

    return recovered
