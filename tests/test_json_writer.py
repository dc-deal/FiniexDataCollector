"""
FiniexDataCollector - JSON Writer Tests
Tests for JSON tick file writing.

Location: tests/test_json_writer.py
"""

import json
import pytest
import tempfile
from pathlib import Path

from python.writers.json_tick_writer import JsonTickWriter
from python.types.tick_types import TickData


class TestJsonTickWriter:
    """Tests for JsonTickWriter."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def writer(self, temp_dir):
        """Create writer instance."""
        return JsonTickWriter(
            output_dir=temp_dir,
            symbol="BTCUSD",
            broker="Kraken",
            server="kraken_spot",
            max_ticks_per_file=100,  # Low for testing
            data_collector="kraken"
        )

    @pytest.fixture
    def sample_tick(self):
        """Create sample tick data."""
        return TickData(
            symbol="BTCUSD",
            timestamp="2025.01.13 14:30:45",
            time_msc=1736775045123,
            bid=45000.0,
            ask=45010.0,
            last=45005.0,
            tick_volume=0,
            real_volume=1.5,
            chart_tick_volume=1,
            spread_points=100,
            spread_pct=0.022,
            tick_flags="BID ASK",
            session="24h",
            server_time="2025.01.13 14:30:45"
        )

    def test_write_single_tick(self, writer, sample_tick, temp_dir):
        """Test writing a single tick."""
        writer.write_tick(sample_tick)

        # File should be created
        assert writer.get_current_filepath() is not None
        assert writer.current_tick_count == 1
        assert writer.total_ticks_written == 1

    def test_lock_file_created(self, writer, sample_tick, temp_dir):
        """Test that lock file is created with active file."""
        writer.write_tick(sample_tick)

        lock_file = writer.get_lock_filepath()
        assert lock_file is not None
        assert lock_file.exists()

    def test_finalize_removes_lock(self, writer, sample_tick, temp_dir):
        """Test that finalize removes lock file."""
        writer.write_tick(sample_tick)
        lock_file = writer.get_lock_filepath()

        # Finalize
        final_path = writer.finalize()

        assert final_path is not None
        assert final_path.exists()
        assert not lock_file.exists()  # Lock should be removed

    def test_file_content_format(self, writer, sample_tick, temp_dir):
        """Test that output matches expected JSON format."""
        writer.write_tick(sample_tick)
        final_path = writer.finalize()

        with open(final_path, 'r') as f:
            content = json.load(f)

        # Check structure
        assert "metadata" in content
        assert "ticks" in content
        assert "errors" in content
        assert "summary" in content

        # Check metadata
        assert content["metadata"]["symbol"] == "BTCUSD"
        assert content["metadata"]["broker"] == "Kraken"
        assert content["metadata"]["data_collector"] == "kraken"
        assert content["metadata"]["data_format_version"] == "1.0.5"

        # Check ticks
        assert len(content["ticks"]) == 1
        tick = content["ticks"][0]
        assert tick["bid"] == 45000.0
        assert tick["ask"] == 45010.0
        assert tick["session"] == "24h"

    def test_rotation_at_max_ticks(self, writer, sample_tick, temp_dir):
        """Test file rotation when max ticks reached."""
        # Write 100 ticks (max_ticks_per_file)
        for i in range(100):
            tick = TickData(
                symbol="BTCUSD",
                timestamp=f"2025.01.13 14:{i:02d}:00",
                time_msc=1736775000000 + i * 1000,
                bid=45000.0 + i,
                ask=45010.0 + i,
                last=45005.0 + i,
                tick_volume=0,
                real_volume=0.1,
                chart_tick_volume=i + 1,
                spread_points=100,
                spread_pct=0.022,
                tick_flags="BID ASK",
                session="24h",
                server_time=f"2025.01.13 14:{i:02d}:00"
            )
            writer.write_tick(tick)

        # Should have triggered rotation
        assert writer.files_created >= 1

        # Finalize remaining
        writer.finalize()

        # Check files created
        symbol_dir = temp_dir / "kraken" / "BTCUSD"
        json_files = list(symbol_dir.glob("*_ticks.json"))
        assert len(json_files) >= 1

    def test_needs_rotation(self, writer, sample_tick):
        """Test needs_rotation logic."""
        assert writer.needs_rotation() is False

        # Write max - 1 ticks
        for _ in range(99):
            writer.write_tick(sample_tick)

        assert writer.needs_rotation() is False

        # Write one more
        writer.write_tick(sample_tick)
        assert writer.needs_rotation() is True

    def test_symbol_info_for_btc(self, writer, sample_tick, temp_dir):
        """Test symbol info is correct for BTC."""
        writer.write_tick(sample_tick)
        final_path = writer.finalize()

        with open(final_path, 'r') as f:
            content = json.load(f)

        symbol_info = content["metadata"]["symbol_info"]
        assert symbol_info["digits"] == 1
        assert symbol_info["tick_size"] == 0.1

    def test_quality_metrics(self, writer, sample_tick, temp_dir):
        """Test quality metrics in summary."""
        for _ in range(10):
            writer.write_tick(sample_tick)

        final_path = writer.finalize()

        with open(final_path, 'r') as f:
            content = json.load(f)

        summary = content["summary"]
        assert summary["total_ticks"] == 10
        assert summary["total_errors"] == 0
        assert summary["data_stream_status"] == "HEALTHY"
        assert summary["quality_metrics"]["overall_quality_score"] == 1.0


class TestWriterForDifferentSymbols:
    """Test writer with different symbol configurations."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_eth_symbol_info(self, temp_dir):
        """Test ETH symbol has correct tick size."""
        writer = JsonTickWriter(
            output_dir=temp_dir,
            symbol="ETHUSD",
            max_ticks_per_file=10
        )

        tick = TickData(
            symbol="ETHUSD",
            timestamp="2025.01.13 14:30:45",
            time_msc=1736775045123,
            bid=2500.0,
            ask=2501.0,
            last=2500.5,
            tick_volume=0,
            real_volume=1.0,
            chart_tick_volume=1,
            spread_points=100,
            spread_pct=0.04,
            tick_flags="BID ASK",
            session="24h",
            server_time="2025.01.13 14:30:45"
        )

        writer.write_tick(tick)
        final_path = writer.finalize()

        with open(final_path, 'r') as f:
            content = json.load(f)

        symbol_info = content["metadata"]["symbol_info"]
        assert symbol_info["digits"] == 2
        assert symbol_info["tick_size"] == 0.01
