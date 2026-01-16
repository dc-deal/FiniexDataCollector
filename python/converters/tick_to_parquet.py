"""
FiniexDataCollector - Tick to Parquet Converter
Converts JSON tick files to Parquet format.

Weekly job: Processes all completed JSON files (not .lock protected).

Location: python/converters/tick_to_parquet.py
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from python.exceptions.collector_exceptions import ParquetConversionError
from python.utils.logging_setup import get_logger


@dataclass
class ConversionResult:
    """Result of tick file conversion."""
    source_file: Path
    output_file: Optional[Path]
    tick_count: int
    success: bool
    error_message: Optional[str] = None


@dataclass
class BatchConversionResult:
    """Result of batch conversion job."""
    symbols_processed: int
    files_converted: int
    total_ticks: int
    errors: int
    duration_seconds: float
    results: List[ConversionResult]


class TickToParquetConverter:
    """
    Converts JSON tick files to Parquet format.

    Features:
    - Skips locked files (active collection)
    - Moves processed JSON to archive (optional)
    - Maintains metadata in parquet
    - Compatible with FiniexTestingIDE tick_importer
    """

    def __init__(
        self,
        raw_data_dir: Path,
        processed_data_dir: Path,
        archive_json: bool = False,
        delete_after_convert: bool = False
    ):
        """
        Initialize converter.

        Args:
            raw_data_dir: Source directory with JSON files
            processed_data_dir: Target directory for Parquet files
            archive_json: Move JSON to archive after conversion
            delete_after_convert: Delete JSON after successful conversion
        """
        self._raw_dir = Path(raw_data_dir)
        self._processed_dir = Path(processed_data_dir)
        self._archive_json = archive_json
        self._delete_after_convert = delete_after_convert
        self._logger = get_logger("FiniexDataCollector.converter")

        # Ensure directories exist
        self._processed_dir.mkdir(parents=True, exist_ok=True)

    def convert_all(self, data_collector: str = "kraken") -> BatchConversionResult:
        """
        Convert all eligible JSON files to Parquet.

        Skips files with active .lock files.

        Args:
            data_collector: Data collector identifier

        Returns:
            BatchConversionResult with statistics
        """
        start_time = time.time()
        results: List[ConversionResult] = []
        symbols_seen = set()

        self._logger.info("=" * 60)
        self._logger.info("Starting tick to parquet conversion")
        self._logger.info("=" * 60)

        # Find all JSON tick files
        source_dir = self._raw_dir / data_collector
        if not source_dir.exists():
            self._logger.warning(f"Source directory not found: {source_dir}")
            return BatchConversionResult(
                symbols_processed=0,
                files_converted=0,
                total_ticks=0,
                errors=0,
                duration_seconds=time.time() - start_time,
                results=[]
            )

        json_files = list(source_dir.glob("**/*_ticks.json"))
        self._logger.info(f"Found {len(json_files)} JSON tick files")

        for json_file in json_files:
            # Skip locked files
            lock_file = json_file.parent / f"{json_file.name}.lock"
            if lock_file.exists():
                self._logger.debug(f"Skipping locked file: {json_file.name}")
                continue

            # Convert file
            result = self._convert_file(json_file, data_collector)
            results.append(result)

            if result.success:
                # Extract symbol from path
                symbol = json_file.parent.name
                symbols_seen.add(symbol)

        duration = time.time() - start_time

        # Calculate totals
        files_converted = sum(1 for r in results if r.success)
        total_ticks = sum(r.tick_count for r in results if r.success)
        errors = sum(1 for r in results if not r.success)

        self._logger.info("=" * 60)
        self._logger.info(f"Conversion complete:")
        self._logger.info(f"  Symbols: {len(symbols_seen)}")
        self._logger.info(f"  Files: {files_converted}")
        self._logger.info(f"  Ticks: {total_ticks:,}")
        self._logger.info(f"  Errors: {errors}")
        self._logger.info(f"  Duration: {duration:.1f}s")
        self._logger.info("=" * 60)

        return BatchConversionResult(
            symbols_processed=len(symbols_seen),
            files_converted=files_converted,
            total_ticks=total_ticks,
            errors=errors,
            duration_seconds=duration,
            results=results
        )

    def _convert_file(
        self,
        json_file: Path,
        data_collector: str
    ) -> ConversionResult:
        """
        Convert single JSON file to Parquet.

        Args:
            json_file: Source JSON file
            data_collector: Collector identifier

        Returns:
            ConversionResult
        """
        try:
            # Load JSON
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            metadata = data.get("metadata", {})
            ticks = data.get("ticks", [])

            if not ticks:
                self._logger.warning(f"Empty tick file: {json_file.name}")
                return ConversionResult(
                    source_file=json_file,
                    output_file=None,
                    tick_count=0,
                    success=True  # Not an error, just empty
                )

            # Convert to DataFrame
            df = self._ticks_to_dataframe(ticks, metadata)

            # Build output path
            symbol = metadata.get("symbol", "UNKNOWN")
            output_dir = self._processed_dir / data_collector / "ticks" / symbol
            output_dir.mkdir(parents=True, exist_ok=True)

            # Output filename (same as source, different extension)
            output_name = json_file.stem + ".parquet"
            output_file = output_dir / output_name

            # Write parquet with metadata
            self._write_parquet(df, output_file, metadata, json_file.name)

            self._logger.info(
                f"Converted: {json_file.name} -> {output_name} "
                f"({len(ticks):,} ticks)"
            )

            # Handle source file
            if self._delete_after_convert:
                json_file.unlink()
            elif self._archive_json:
                self._archive_file(json_file, data_collector)

            return ConversionResult(
                source_file=json_file,
                output_file=output_file,
                tick_count=len(ticks),
                success=True
            )

        except Exception as e:
            self._logger.error(f"Conversion failed for {json_file.name}: {e}")
            return ConversionResult(
                source_file=json_file,
                output_file=None,
                tick_count=0,
                success=False,
                error_message=str(e)
            )

    def _ticks_to_dataframe(
        self,
        ticks: List[Dict[str, Any]],
        metadata: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Convert tick list to DataFrame.

        Args:
            ticks: List of tick dicts
            metadata: File metadata

        Returns:
            DataFrame with typed columns
        """
        df = pd.DataFrame(ticks)

        # Parse timestamp to datetime
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                format="%Y.%m.%d %H:%M:%S",
                utc=True
            )

        # Ensure correct types
        type_map = {
            "time_msc": "int64",
            "bid": "float64",
            "ask": "float64",
            "last": "float64",
            "tick_volume": "int32",
            "real_volume": "float64",
            "chart_tick_volume": "int32",
            "spread_points": "int32",
            "spread_pct": "float64",
        }

        for col, dtype in type_map.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        return df

    def _write_parquet(
        self,
        df: pd.DataFrame,
        output_file: Path,
        metadata: Dict[str, Any],
        source_file: str
    ) -> None:
        """
        Write DataFrame to Parquet with metadata.

        Args:
            df: Tick DataFrame
            output_file: Target file path
            metadata: Original JSON metadata
            source_file: Source filename
        """
        # Build parquet metadata
        pq_metadata = {
            "source_file": source_file,
            "symbol": metadata.get("symbol", "UNKNOWN"),
            "broker": metadata.get("broker", "Kraken"),
            "data_collector": metadata.get("data_collector", "kraken"),
            "data_format_version": metadata.get("data_format_version", "1.0.5"),
            "tick_count": str(len(df)),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

        # Convert to PyArrow table
        table = pa.Table.from_pandas(df)

        # Add metadata
        existing_meta = table.schema.metadata or {}
        combined_meta = {
            **{k.encode(): v.encode() for k, v in pq_metadata.items()},
            **existing_meta
        }
        table = table.replace_schema_metadata(combined_meta)

        # Write with snappy compression
        pq.write_table(
            table,
            output_file,
            compression="snappy"
        )

    def _archive_file(self, json_file: Path, data_collector: str) -> None:
        """
        Move JSON file to archive directory.

        Args:
            json_file: File to archive
            data_collector: Collector identifier
        """
        archive_dir = self._raw_dir / data_collector / "_archive" / json_file.parent.name
        archive_dir.mkdir(parents=True, exist_ok=True)

        archive_path = archive_dir / json_file.name
        json_file.rename(archive_path)

        self._logger.debug(f"Archived: {json_file.name}")


async def run_weekly_conversion(
    raw_dir: Path,
    processed_dir: Path,
    data_collector: str = "kraken"
) -> Dict[str, Any]:
    """
    Run weekly conversion job (async wrapper).

    Args:
        raw_dir: Raw data directory
        processed_dir: Processed data directory
        data_collector: Collector identifier

    Returns:
        Result dict for reporting
    """
    converter = TickToParquetConverter(
        raw_data_dir=raw_dir,
        processed_data_dir=processed_dir,
        archive_json=False,
        delete_after_convert=False  # Keep JSON for safety
    )

    result = converter.convert_all(data_collector)

    return {
        "symbols_processed": result.symbols_processed,
        "files_converted": result.files_converted,
        "total_ticks": result.total_ticks,
        "errors": result.errors,
        "duration_seconds": result.duration_seconds
    }
