"""
FiniexDataCollector - Tests for where an outage record goes and what it is used for

The record of one interruption reaches three places, and each one used to lie a
little:

- /v1/status kept reconnects in memory, capped at 100 and EMPTIED after every
  weekly report and every Telegram /report - on production on 2026-09-26 and
  2026-10-03 among others - so the live history rarely reached back a week.
- The log carried no record at all, only a sentence per drop.
- The alert judged 'downtime' from the moment a drop was noticed to the socket
  handshake, leaving out the 10-11 s before it was noticed.

Now: the record lives on /v1/status with totals that a capped list cannot give,
it is written to the log as one '[OUTAGE]' line when the feed is restored and
again when the record is resolved, through the same converter /v1/status uses,
and the alert reads the data gap.

Location: tests/utils/test_reconnect_reporting.py
"""

import asyncio
import json
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pytest

import python.main as main_module
from python.api.stats_serializer import plain
from python.main import FiniexDataCollector
from python.types.collector_stats import (CollectorStats, ReconnectEvent,
                                          TradeIdGap)

NOW = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)


def an_outage(at: datetime = NOW, gap: float = 2.8,
              reason: str = "far_side_close") -> ReconnectEvent:
    """A restored outage with two symbols, one of them counted."""
    return ReconnectEvent(
        timestamp=at, reconnected_at=at + timedelta(seconds=gap),
        duration_seconds=gap, reason=reason, close_code=1001,
        close_reason="going away", attempts=1,
        symbols={"BTCUSD": TradeIdGap(last_trade_id=100, first_trade_id=106,
                                      missing_trade_ids=5),
                 "ETHUSD": TradeIdGap(last_trade_id=500)})


def through_all_stages(stats: CollectorStats, event: ReconnectEvent) -> None:
    for stage in ("opened", "restored", "resolved"):
        stats.record_outage(stage, event)


class Recorder:
    """A logger and a Telegram that keep what they are given."""

    def __init__(self) -> None:
        self.lines: List[str] = []
        self.sent: List[tuple] = []

    def info(self, message: str) -> None:
        self.lines.append(message)

    debug = warning = error = info

    async def send_warning(self, title: str, body: str) -> bool:
        self.sent.append((title, body))
        return True

    async def send_info(self, title: str, body: str) -> bool:
        self.sent.append((title, body))
        return True


def an_app(telegram=None, tmp_path: Path = None) -> FiniexDataCollector:
    """The real collector's methods on a minimal instance."""
    app = object.__new__(FiniexDataCollector)
    app._stats = CollectorStats()
    app._logger = Recorder()
    app._telegram = telegram
    app._last_reconnect_alert = None
    app._last_reconnect_monotonic = None
    folder = str(tmp_path) if tmp_path else "."
    app._config = types.SimpleNamespace(
        monitoring=types.SimpleNamespace(
            reconnect_alert_cooldown_minutes=30,
            reconnect_alert_min_seconds=30.0,
            reconnect_alert_cluster=4),
        paths=types.SimpleNamespace(raw_data_dir=folder, logs_dir=folder),
        mt5=types.SimpleNamespace(enabled=False, raw_data_path=""))
    return app


# ------------------------------------------------------------- statistics


def test_an_outage_moves_from_in_progress_into_history_and_totals() -> None:
    stats = CollectorStats()
    event = an_outage()

    stats.record_outage("opened", event)
    assert stats.reconnect_in_progress is event
    assert stats.reconnect_events == []

    stats.record_outage("restored", event)
    assert stats.reconnect_in_progress is None
    assert stats.reconnect_events == [event]
    assert stats.last_reconnect is event
    assert stats.reconnect_totals.count == 1
    assert stats.reconnect_totals.by_reason == {"far_side_close": 1}
    assert stats.reconnect_totals.gap_seconds_max == 2.8

    stats.record_outage("resolved", event)
    assert stats.reconnect_totals.missing_trade_ids_total == 5, (
        "the unknown count adds nothing; it is not a zero")
    assert stats.reconnect_totals.count == 1, "counted once, at restoration"


def test_an_outage_a_shutdown_ended_is_counted_once() -> None:
    """Never restored, but an outage the process saw all the same."""
    stats = CollectorStats()
    event = ReconnectEvent(timestamp=NOW, reconnected_at=None,
                           duration_seconds=None, reason="connection_lost",
                           resolved_by="shutdown")

    stats.record_outage("opened", event)
    stats.record_outage("resolved", event)

    assert stats.reconnect_in_progress is None
    assert stats.reconnect_events == [event]
    assert stats.reconnect_totals.count == 1


def test_the_history_is_capped_and_the_count_is_not() -> None:
    """The length of a capped list stops being a count the moment it fills."""
    stats = CollectorStats()
    for minute in range(stats.max_reconnect_history + 5):
        through_all_stages(stats, an_outage(
            at=NOW + timedelta(minutes=minute)))

    assert len(stats.reconnect_events) == stats.max_reconnect_history
    assert stats.reconnect_totals.count == stats.max_reconnect_history + 5


def test_the_daily_count_keeps_fourteen_dates_and_the_times_a_week() -> None:
    """
    by_day keeps the fourteen most recent dates that had an outage; the
    uncapped outage times reach a week back, and a day's margin.
    """
    stats = CollectorStats()
    for day in range(20):
        through_all_stages(stats, an_outage(at=NOW - timedelta(days=day)))

    assert len(stats.reconnect_totals.by_day) == 14
    assert min(stats.reconnect_totals.by_day) == \
        (NOW - timedelta(days=13)).date().isoformat()
    assert stats.outages_since(NOW - timedelta(days=7) + timedelta(seconds=1)) \
        == 7
    assert stats.outages_since(NOW - timedelta(days=30)) == 9, (
        "eight days kept, both ends included")


def test_the_counters_are_replaced_not_changed_in_place() -> None:
    """
    /v1/status serialises in a worker thread while the event loop updates.

    A dict gaining a key while another thread iterates it raises 'dictionary
    changed size during iteration' - a sporadic 500 on the status route. A new
    dict per update cannot be caught half-built.
    """
    stats = CollectorStats()
    through_all_stages(stats, an_outage())
    by_reason = stats.reconnect_totals.by_reason
    by_day = stats.reconnect_totals.by_day

    through_all_stages(stats, an_outage(at=NOW + timedelta(days=1),
                                        reason="silence_watchdog"))

    assert stats.reconnect_totals.by_reason is not by_reason
    assert stats.reconnect_totals.by_day is not by_day
    assert by_reason == {"far_side_close": 1}, "the old one was never touched"


def test_no_totals_is_no_count_rather_than_a_zero() -> None:
    """A collector that never had an outage - or never measured - says nothing."""
    stats = CollectorStats()

    assert stats.reconnect_totals is None
    assert stats.outages_since(NOW - timedelta(days=7)) == 0


# -------------------------------------------------------------------- log


def test_each_outage_is_written_when_restored_and_again_when_resolved() -> None:
    """
    The durable record: one line at each of the two stages, in /v1/status' shape.

    Written at restoration as well, so a kill before the first trade per symbol
    arrives - up to fifteen minutes on a thin pair - still leaves the line.
    """
    app = an_app()
    event = an_outage()

    async def scenario() -> None:
        app._on_outage("opened", event)
        app._on_outage("restored", event)
        await asyncio.sleep(0)
        app._on_outage("resolved", event)

    asyncio.run(scenario())

    lines = [
        line for line in app._logger.lines if line.startswith("[OUTAGE] ")]
    assert len(lines) == 2
    records = [json.loads(line[len("[OUTAGE] "):]) for line in lines]
    assert [record["stage"] for record in records] == ["restored", "resolved"]
    expected = json.loads(json.dumps({"schema": 1, "stage": "resolved",
                                      **plain(event)}, default=str))
    assert records[1] == expected
    assert records[1]["symbols"]["BTCUSD"]["missing_trade_ids"] == 5


def test_a_value_json_cannot_hold_still_leaves_the_line() -> None:
    """The line that outlives the process must not be the one that fails."""
    app = an_app()
    event = an_outage()
    event.exception = Path("unexpected") / "value"

    async def scenario() -> None:
        app._on_outage("opened", event)
        app._on_outage("restored", event)

    asyncio.run(scenario())

    assert any(line.startswith("[OUTAGE] ") for line in app._logger.lines)


# ------------------------------------------------------------------ alert


def test_the_alert_reads_the_data_gap() -> None:
    """
    35 s without data is an alert even if detection to handshake took 2.3 s.

    That shorter window is what the old alert judged; it left out the silence
    before a drop was noticed.
    """
    telegram = Recorder()
    app = an_app(telegram)
    event = an_outage(gap=35.0)
    event.detected_after_ms = 32_700.0
    event.handshake_after_ms = 35_000.0

    asyncio.run(app._send_reconnect_alert(event))

    assert len(telegram.sent) == 1
    body = telegram.sent[0][1]
    assert "35.0s without data" in body
    assert "far_side_close" in body


def test_a_cluster_above_twenty_fires_and_is_counted_whole() -> None:
    """
    The hour's count read the twenty-record ring: a cluster threshold above
    twenty - which the configuration accepts up to 100 - could never fire, and
    the alert text stopped at '20 outages in the last hour'.
    """
    telegram = Recorder()
    app = an_app(telegram)
    app._config.monitoring.reconnect_alert_cluster = 25
    now = datetime.now(timezone.utc)
    for minute in range(30):
        through_all_stages(app._stats, an_outage(
            at=now - timedelta(minutes=59 - minute), gap=2.0))

    asyncio.run(app._send_reconnect_alert(app._stats.reconnect_events[-1]))

    assert len(telegram.sent) == 1
    assert telegram.sent[0][1].startswith("30 outages in the last hour")


def test_a_short_gap_stays_off_the_phone_and_says_why_in_the_log() -> None:
    telegram = Recorder()
    app = an_app(telegram)

    asyncio.run(app._send_reconnect_alert(an_outage(gap=2.8)))

    assert telegram.sent == []
    assert any("data resumed 2.8s after the last message" in line
               for line in app._logger.lines)


# ---------------------------------------------------------- weekly report


def test_a_weekly_report_leaves_the_history_in_place(tmp_path) -> None:
    """
    The report used to empty the history after sending - every Saturday and at
    every /report - while it only ever needed the last seven days.
    """
    telegram = Recorder()
    app = an_app(telegram, tmp_path)
    app._stats.start_time = datetime.now(timezone.utc) - timedelta(days=8)
    for hour in range(3):
        through_all_stages(app._stats,
                           an_outage(at=datetime.now(timezone.utc)
                                     - timedelta(hours=hour)))

    assert asyncio.run(app._send_weekly_report())

    assert len(app._stats.reconnect_events) == 3
    assert app._stats.last_reconnect is not None
    assert app._stats.reconnect_totals.count == 3
    report = telegram.sent[0][1]
    assert "Reconnects This Week: 3" in report
    assert "2.8s without data" in report, "seconds, never '0m downtime'"


def test_the_weekly_count_and_its_list_cover_the_same_seven_days(
        tmp_path, monkeypatch) -> None:
    """
    Whole UTC dates for the count, a rolling week for the list: an outage on
    the evening seven days back was listed under 'Reconnects This Week: 0', and
    with the default Saturday 06:00 report about 18 hours a week reached no
    report's count at all.

    The report's clock is frozen at that Saturday 06:00. Relative to the real
    clock, the outage seven days back lands on the next date during the last
    minutes of every UTC day, and a count by whole dates then passes too -
    found that way, by a mutation check run at 23:57 UTC.
    """
    report_at = datetime(2026, 10, 10, 6, 0, tzinfo=timezone.utc)

    class ReportClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return report_at

    monkeypatch.setattr(main_module, "datetime", ReportClock)
    telegram = Recorder()
    app = an_app(telegram, tmp_path)
    app._stats.start_time = datetime.now(timezone.utc) - timedelta(days=8)
    through_all_stages(app._stats, an_outage(
        at=datetime(2026, 10, 3, 5, 0, tzinfo=timezone.utc)))
    through_all_stages(app._stats, an_outage(
        at=datetime(2026, 10, 3, 20, 0, tzinfo=timezone.utc)))

    assert asyncio.run(app._send_weekly_report())

    report = telegram.sent[0][1]
    assert "Reconnects This Week: 1" in report
    assert report.count("s without data") == 1, "the same one, listed"


def test_a_report_from_a_younger_process_says_what_it_counted(
        tmp_path) -> None:
    """
    The counts cover what this process saw. Two days after a deploy, 'This
    Week' would state a week's count the process does not hold.
    """
    telegram = Recorder()
    app = an_app(telegram, tmp_path)
    app._stats.start_time = datetime.now(timezone.utc) - timedelta(hours=52)
    through_all_stages(app._stats, an_outage(
        at=datetime.now(timezone.utc) - timedelta(hours=1)))

    assert asyncio.run(app._send_weekly_report())

    report = telegram.sent[0][1]
    assert "Reconnects Since Start (52 h): 1" in report
    assert "This Week" not in report


@pytest.mark.parametrize("hours, label", [
    (167, "Reconnects Since Start (167 h): 1"),
    (169, "Reconnects This Week: 1"),
])
def test_the_weekly_label_turns_at_seven_days(tmp_path, hours, label) -> None:
    """One hour either side of the week the report claims to count."""
    telegram = Recorder()
    app = an_app(telegram, tmp_path)
    app._stats.start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    through_all_stages(app._stats, an_outage(
        at=datetime.now(timezone.utc) - timedelta(hours=1)))

    assert asyncio.run(app._send_weekly_report())

    assert label in telegram.sent[0][1]
