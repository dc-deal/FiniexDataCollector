"""
FiniexDataCollector - Tests for the error and warning counters

Until 2026-09-19 `total_errors`, `total_warnings` and `recent_logs` were
initialised and never raised: nothing called record_error or record_warning.
The live display said "No errors or warnings", /v1/status said 0 and 0, and the
weekly report agreed - while the production log carried forced reconnects. The
counters now follow the log through a listener, so no error path can forget to
report itself. These tests pin the listener, the wiring in the collector, the
display code that had never rendered a single entry before - and, since the
counters count lines, how many lines the Kraken client writes for one event: a
drop is a warning, not an error, and a failed attempt is one error, not two.

Location: tests/utils/test_error_counters.py
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from python.collectors.kraken.outage_tracker import Detection
from python.collectors.kraken.quote_cache import QuoteCache
from python.collectors.kraken.websocket_client import KrakenWebSocketClient
from python.exceptions.collector_exceptions import WebSocketSubscriptionError
from python.main import FiniexDataCollector
from python.types.collector_stats import CollectorStats, LogEntry
from python.utils.collection_clock import CollectionClock
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


class SendRefused:
    """A connection whose every send raises the given error."""

    def __init__(self, error: Exception) -> None:
        self.error = error

    async def send(self, message: str) -> None:
        raise self.error


def kraken_client() -> KrakenWebSocketClient:
    return KrakenWebSocketClient(
        symbols=["BTC/USD"], clock=CollectionClock(), quote_cache=QuoteCache(),
        streams=["trade"])


def levels_logged(action) -> List[str]:
    """The level of every WARNING-or-above line written while action ran."""
    levels: List[str] = []

    def listener(level, name, message) -> None:
        levels.append(level.name)

    add_log_listener(listener)
    try:
        action()
    finally:
        remove_log_listener(listener)
    return levels


@pytest.mark.parametrize("error, kind, errors", [
    (ConnectionClosedError(Close(1011, "internal error"), None),
     "far_side_close", 0),
    (OSError("send buffer gone"), "subscribe_send_failed", 1),
])
def test_a_send_refused_by_a_closed_connection_is_a_drop_not_an_error(
        error, kind, errors) -> None:
    """
    A subscription send refused because the connection ended is that drop,
    reported by the 'Connection ended' warning with its code. Also logged as an
    ERROR, every drop during a resubscription counted in total_errors; only a
    send that failed on a live connection is an error of its own.
    """
    client = kraken_client()
    client._websocket = SendRefused(error)
    raised: List[Exception] = []

    def subscribe() -> None:
        try:
            asyncio.run(client.subscribe())
        except WebSocketSubscriptionError as failure:
            raised.append(failure)

    levels = levels_logged(subscribe)

    assert raised[0].detection.kind == kind
    assert levels.count("ERROR") == errors


def test_a_failed_connection_attempt_is_one_error_line() -> None:
    """
    connect() reports the failure; the second ERROR for the same attempt that
    the loop around it wrote counted every failed attempt twice.
    """
    client = kraken_client()

    async def unreachable(*args, **kwargs):
        raise OSError("network is unreachable")

    client._open = unreachable

    levels = levels_logged(lambda: asyncio.run(client._run_connection()))

    assert levels == ["ERROR"]


class ClosedByKraken:
    """A connection on which Kraken's close frame had already arrived."""

    def __init__(self) -> None:
        self.close_rcvd = Close(1001, "going away")


@pytest.mark.parametrize("detection, frame_arrived, level", [
    (Detection("far_side_close", 1001, "going away"), False, "WARNING"),
    (Detection("unexpected", exception="RuntimeError: detector broke"),
     False, "ERROR"),
    (Detection("unexpected", exception="RuntimeError: detector broke"),
     True, "ERROR"),
])
def test_a_drop_is_a_warning_and_a_failure_of_our_own_an_error(
        detection, frame_arrived, level) -> None:
    """
    A connection that ended is a drop and a warning. One the code ended itself
    - a detector that raised - is an error, and counts as one: also when
    Kraken's close frame had arrived meanwhile and names the end, which then
    carries the fault in its line rather than losing it.
    """
    client = kraken_client()
    if frame_arrived:
        client._websocket = ClosedByKraken()
    lines: List[str] = []

    def listener(lvl, name, message) -> None:
        lines.append(message)

    add_log_listener(listener)
    try:
        levels = levels_logged(
            lambda: client._end_connection(detection, client._monotonic()))
    finally:
        remove_log_listener(listener)

    assert levels == [level]
    if detection.kind == "unexpected":
        assert "detector broke" in lines[0]
