"""
FiniexDataCollector - Tests for the two diagnostics a remote session reads

Both exist because of one defect. Writing an archive file ran on the collector's
only event loop, and a blocked loop stamps `collected_msc` late - invisible in
the file itself until the lag passes the consuming importer's 30 s window and
costs the whole file. Measured on production 2026-09-20: 21 s of blocking at the
UTC day cut.

`loop_lag` measures the stall directly, so the next one is a number on
`/v1/status` rather than an inference from tick timestamps. `exports` counts the
subprocesses that write the files now, because a file that is owed and never
written would otherwise only show up as an absence in the archive.

Location: tests/utils/test_diagnostics.py
"""

from python.types.collector_stats import CollectorStats


def test_the_worst_stall_is_kept_with_the_moment_it_happened() -> None:
    """A maximum without a time cannot be matched against anything else."""
    stats = CollectorStats()
    stats.record_loop_lag(12.0)
    stats.record_loop_lag(1800.0)
    stats.record_loop_lag(3.0)

    assert stats.loop_lag.samples == 3
    assert stats.loop_lag.max_ms == 1800.0
    assert stats.loop_lag.max_at is not None
    assert stats.loop_lag.last_ms == 3.0, "the last reading is the last one"
    assert stats.loop_lag.over_500ms == 1


def test_a_wake_up_that_was_early_counts_as_no_lag() -> None:
    """
    Sleep can return a hair early, and a negative lag is not a measurement.

    Clamped rather than recorded: a negative maximum would read as if the loop
    ran ahead of itself.
    """
    stats = CollectorStats()
    stats.record_loop_lag(-4.0)

    assert stats.loop_lag.max_ms == 0.0
    assert stats.loop_lag.last_ms == 0.0


def test_an_export_is_counted_from_handover_to_file() -> None:
    """In flight is what was handed over and not yet reported."""
    stats = CollectorStats()
    stats.record_export_started()
    stats.record_export_started()

    assert stats.exports.in_flight == 2

    stats.record_export_finished(
        "BTCUSD_20260920_000004_ticks.json", 50000, 900.0)

    assert stats.exports.in_flight == 1
    assert stats.exports.finished == 1
    assert stats.exports.last_ticks == 50000
    assert stats.exports.max_ms == 900.0


def test_a_failed_export_names_the_file_that_is_still_owed() -> None:
    """
    Its write-ahead log is still on disk and the next start recovers it - but
    only somebody who knows which file goes looking.
    """
    stats = CollectorStats()
    stats.record_export_started()
    stats.record_export_failed("ETHUSD_20260920_000005_ticks.json")

    assert stats.exports.failed == 1
    assert stats.exports.in_flight == 0
    assert stats.exports.last_failed_file == "ETHUSD_20260920_000005_ticks.json"
    assert stats.exports.last_failed_at is not None


def test_nothing_in_flight_can_go_below_zero() -> None:
    """A report without a handover must not make the gauge lie downwards."""
    stats = CollectorStats()
    stats.record_export_failed("orphan.json")

    assert stats.exports.in_flight == 0
