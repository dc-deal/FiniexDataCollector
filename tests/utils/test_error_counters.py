"""
FiniexDataCollector - Tests for the error and warning counters

Until 2026-09-19 `total_errors`, `total_warnings` and `recent_logs` were
initialised and never raised: nothing called record_error or record_warning.
The live display said "No errors or warnings", /v1/status said 0 and 0, and the
weekly report agreed - while the production log carried forced reconnects. The
counters now follow the log through a listener, so no error path can forget to
report itself. These tests pin the listener, the wiring in the collector, and
the display code that had never rendered a single entry before.

Location: tests/utils/test_error_counters.py
"""

from datetime import datetime, timezone
from pathlib import Path

from python.main import FiniexDataCollector
from python.types.collector_stats import CollectorStats, LogEntry
from python.utils.config_loader import ConfigLoader
from python.utils.live_display import LiveDisplay
from python.utils.logging_setup import (add_log_listener, get_logger,
                                        remove_log_listener)


def test_warnings_and_errors_are_counted_and_info_is_not() -> None:
    """ERROR and CRITICAL are errors, WARNING is a warning, INFO is neither."""
    stats = CollectorStats()
    add_log_listener(stats.record_logged)
    try:
        logger = get_logger("test.error_counters.levels")
        logger.info("an ordinary line")
        logger.warning("no messages for 26s")
        logger.error("connection appears dead")
        logger.critical("something worse")
    finally:
        remove_log_listener(stats.record_logged)

    assert stats.total_warnings == 1
    assert stats.total_errors == 2
    assert [e.message for e in stats.recent_logs] == [
        "no messages for 26s", "connection appears dead", "something worse"]
    assert [e.level for e in stats.recent_logs] == [
        "WARNING", "ERROR", "ERROR"]


def test_a_failing_listener_does_not_cost_the_line() -> None:
    """The log file is the record; a broken counter must not take it down."""
    def broken(level, name, message) -> None:
        raise RuntimeError("listener bug")

    logger = get_logger("test.error_counters.broken_listener")
    add_log_listener(broken)
    try:
        logger.error("this line must still reach the file")
    finally:
        remove_log_listener(broken)

    assert "this line must still reach the file" in Path(
        logger._log_file).read_text(encoding="utf-8")


def test_the_collector_counts_what_its_log_says() -> None:
    """
    The wiring itself: a collector's stats hear every logger in the process.

    Without the add_log_listener call in FiniexDataCollector.__init__ the
    listener above works and nothing is connected to it - which is the exact
    shape the original defect had.
    """
    collector = FiniexDataCollector(ConfigLoader().load(), show_display=False)
    stats = collector._stats
    try:
        get_logger("FiniexDataCollector.kraken").error(
            "Connection appears dead, forcing reconnect")
        get_logger("FiniexDataCollector.telegram").warning("slow poll")
    finally:
        remove_log_listener(stats.record_logged)

    assert stats.total_errors == 1
    assert stats.total_warnings == 1


def test_the_display_renders_a_logged_message_as_text_not_markup() -> None:
    """
    A message shaped like a closing tag must render, literally.

    This code never ran with data before the counters were connected, and
    Rich raises MarkupError on "[/red]" that closes nothing.
    """
    stats = CollectorStats()
    stats.recent_logs.append(LogEntry(
        timestamp=datetime(2026, 9, 19, 0, 0, 19, tzinfo=timezone.utc),
        level="ERROR",
        message="weird [/red] closing tag and [WinError 10054] reset"))

    footer = LiveDisplay(stats)._build_footer()

    assert "weird [/red] closing tag" in footer.plain
