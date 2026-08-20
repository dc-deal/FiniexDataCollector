"""
FiniexDataCollector - JSON Tick Writer Output Tests

Two groups:
  1. What the file declares about itself — the metadata header
  2. What the file has to satisfy to be imported at all

The second group mirrors the invariants of FiniexTestingIDE's
TickImportValidator. That validator stays the authority; this is the local
guard that catches a break before a file ever reaches it.

Location: tests/writers/test_json_tick_writer.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytest

from python.types.tick_types import (
    COLLECTED_MSC_TIMEBASE,
    DATA_FORMAT_VERSION,
    TickData
)
from python.writers.json_tick_writer import JsonTickWriter

# Kept in step with FiniexTestingIDE's tick_import_validator, which owns them.
PLAUSIBLE_LAG_WINDOW_MS = 30_000
TIMESTAMP_CONSISTENCY_TOLERANCE_MS = 1_000


@pytest.fixture
def written_file(tmp_path: Path, tick_series: List[TickData]) -> Dict[str, Any]:
    """
    Drive the writer through a full file and read back what it wrote.

    Args:
        tmp_path: pytest temp directory
        tick_series: Synthetic stream from the shared fixtures

    Returns:
        Parsed JSON document of the finalized file
    """
    writer = JsonTickWriter(
        output_dir=tmp_path,
        symbol="BTCUSD",
        broker="Kraken",
        server="kraken_websocket",
        broker_type="kraken_spot",
        max_ticks_per_file=50000,
        data_collector="kraken"
    )

    for tick in tick_series:
        writer.write_tick(tick)

    path = writer.finalize()
    return json.loads(Path(path).read_text(encoding="utf-8"))


# =============================================================================
# WHAT THE FILE DECLARES
# =============================================================================

def test_declares_collected_msc_timebase_as_utc(written_file: Dict[str, Any]) -> None:
    """The field the importer reads to tell a collector defect from legacy data."""
    assert written_file["metadata"]["collected_msc_timebase"] == "utc"


def test_declares_data_format_version_1_5_0(written_file: Dict[str, Any]) -> None:
    """1.5.0 is the version at which the schema states its time base."""
    assert written_file["metadata"]["data_format_version"] == "1.5.0"


def test_declared_values_come_from_the_module_constants(
    written_file: Dict[str, Any]
) -> None:
    """
    Both fields state what the code does, so they must not drift from it.

    A literal typed into the writer would pass the two tests above while saying
    something the constants no longer say.
    """
    metadata = written_file["metadata"]
    assert metadata["data_format_version"] == DATA_FORMAT_VERSION
    assert metadata["collected_msc_timebase"] == COLLECTED_MSC_TIMEBASE


def test_names_its_collector(written_file: Dict[str, Any]) -> None:
    """Both identifying fields are written; the importer accepts either."""
    metadata = written_file["metadata"]
    assert metadata["data_collector"] == "kraken"
    assert metadata["broker_type"] == "kraken_spot"


def test_no_longer_declares_a_broker_utc_offset(written_file: Dict[str, Any]) -> None:
    """
    Dropped in 1.5.0 for schema symmetry with the MT5 collector.

    Kraken's offset was a true 0, but no consumer may derive an offset from a
    collector-written field — the import registry decides it per broker_type.
    """
    assert "broker_utc_offset_hours" not in written_file["metadata"]


# =============================================================================
# WHAT THE IMPORTER ENFORCES
# =============================================================================

def test_row_count_matches_the_declared_total(written_file: Dict[str, Any]) -> None:
    """A mismatch rejects the file."""
    assert len(written_file["ticks"]) == written_file["summary"]["total_ticks"]


@pytest.mark.parametrize("column", ["time_msc", "collected_msc"])
def test_time_columns_never_step_backwards(
    written_file: Dict[str, Any],
    column: str
) -> None:
    """
    Non-decreasing, not strictly increasing.

    Two ticks can share a millisecond when a market order sweeps the book, and
    the writer must preserve that rather than reorder it.

    Args:
        written_file: Parsed file document
        column: Time column under test
    """
    values = [tick[column] for tick in written_file["ticks"]]
    assert values == sorted(values)


def test_collected_msc_stays_inside_the_plausibility_window(
    written_file: Dict[str, Any]
) -> None:
    """
    Arrival time has to sit close to the event time it claims to follow.

    The minimum lag is the honest estimator of the clock offset — the
    least-delayed sample is the one carrying the least queueing noise.
    """
    lags = [
        tick["collected_msc"] - tick["time_msc"]
        for tick in written_file["ticks"]
    ]
    assert abs(min(lags)) <= PLAUSIBLE_LAG_WINDOW_MS


def test_timestamp_string_agrees_with_time_msc(written_file: Dict[str, Any]) -> None:
    """The string is truncated to the second, so it may only lag by under one."""
    for tick in written_file["ticks"]:
        stamp = datetime.strptime(
            tick["timestamp"], "%Y.%m.%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        deviation = abs(int(stamp.timestamp() * 1000) - tick["time_msc"])
        assert deviation <= TIMESTAMP_CONSISTENCY_TOLERANCE_MS


def test_prices_are_positive_and_not_inverted(written_file: Dict[str, Any]) -> None:
    """Zero and crossed quotes reject the file."""
    for tick in written_file["ticks"]:
        assert tick["bid"] > 0
        assert tick["ask"] > 0
        assert tick["ask"] >= tick["bid"]
