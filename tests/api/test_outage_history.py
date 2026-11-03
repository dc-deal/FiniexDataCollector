"""
FiniexDataCollector - The outage history

A consumer comparing a month of its own feed against ours needs every interruption
in that month. Twenty records on /v1/status hold about two days; this route reads
the `[OUTAGE]` lines the log keeps instead.

Location: tests/api/test_outage_history.py
"""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from python.api import outage_history
from python.api.api_app import create_api
from python.api.build_info import BuildInfo
from python.api.outage_history import read_outages
from python.api.token_loader import load_token_registry
from python.types.tick_types import OriginBlock

HISTORIAN = {"token": "h" * 40, "grants": ["outages:history"]}
WATCHER = {"token": "w" * 40, "grants": ["status:detail"]}
ORIGIN = OriginBlock(instance_id="a3f8c21d9b04", collected_on="test-box",
                     producer="finiex-data-collector", producer_version="1.2.1")
BUILD = BuildInfo(version="1.2.1", data_format_version="1.7.0", commit="abc1234",
                  dirty=False, python_version="3.14.7", started_at="2026-10-31T09:34:16+00:00")
DAY = date(2026, 10, 20)


@pytest.fixture(autouse=True)
def fresh_cache() -> None:
    """Every test reads its files itself."""
    outage_history.clear_cache()


def at(day: date, hhmmss: str) -> datetime:
    """A UTC moment on a day."""
    h, m, s = (int(x) for x in hhmmss.split(":"))
    return datetime(day.year, day.month, day.day, h, m, s, tzinfo=timezone.utc)


def outage_line(logged: datetime, stage: str, start: datetime, **fields: Any) -> str:
    """One `[OUTAGE]` line as main.py writes it."""
    record = {"schema": 1, "stage": stage, "timestamp": start.isoformat(),
              "reason": "connection_lost", "duration_seconds": 2.4, **fields}
    return (f"{logged:%Y-%m-%d %H:%M:%S} UTC | INFO     | FiniexDataCollector | "
            f"[OUTAGE] {json.dumps(record)}")


def other_line(logged: datetime, message: str) -> str:
    """Any other log line."""
    return f"{logged:%Y-%m-%d %H:%M:%S} UTC | INFO     | FiniexDataCollector.kraken | {message}"


def write_day(log_dir: Path, day: date, lines: List[str]) -> Path:
    """One day's log file, named the way the logger names it."""
    path = log_dir / f"finiexdatacollector_{day.isoformat()}.log"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def a_day(log_dir: Path) -> Path:
    """Two outages: one restored and resolved, one only restored, plus noise."""
    first, second = at(DAY, "09:32:40"), at(DAY, "14:10:00")
    return write_day(log_dir, DAY, [
        other_line(at(DAY, "09:32:40"), "Connection ended: connection_lost"),
        outage_line(at(DAY, "09:32:44"), "restored", first),
        other_line(at(DAY, "09:33:00"),
                   "a line that mentions [OUTAGE] in passing"),
        outage_line(at(DAY, "09:34:18"), "resolved", first, resolved_by="all_symbols_traded",
                    symbols={"BTCUSD": {"missing_trade_ids": 4}}),
        outage_line(at(DAY, "14:10:03"), "restored", second),
    ])


def window(day: date = DAY) -> Dict[str, datetime]:
    """The whole day."""
    return {"since": at(day, "00:00:00"), "until": at(day, "23:59:59")}


def test_names_the_file_the_logger_writes(tmp_path: Path) -> None:
    """If the two ever disagree, the route reads nothing and says so politely."""
    from python.api.log_reader import log_path

    assert log_path(tmp_path, DAY) == a_day(tmp_path)


def test_each_outage_appears_once_at_its_furthest_stage(tmp_path: Path) -> None:
    a_day(tmp_path)

    history = read_outages(tmp_path, **window())

    assert [r["stage"] for r in history["records"]] == ["resolved", "restored"]
    assert history["records"][0]["resolved_by"] == "all_symbols_traded"
    assert history["records"][0]["symbols"]["BTCUSD"]["missing_trade_ids"] == 4
    assert history["records"][0]["logged_at"] == "2026-10-20T09:34:18+00:00"
    assert history["count"] == 2
    assert history["truncated"] is False


def test_a_restored_line_after_its_resolved_one_does_not_win(tmp_path: Path) -> None:
    start = at(DAY, "09:00:00")
    write_day(tmp_path, DAY, [
        outage_line(at(DAY, "09:02:00"), "resolved", start),
        outage_line(at(DAY, "09:03:00"), "restored", start),
    ])

    assert read_outages(tmp_path, **window()
                        )["records"][0]["stage"] == "resolved"


def test_the_window_is_on_the_outage_start(tmp_path: Path) -> None:
    a_day(tmp_path)

    history = read_outages(tmp_path, since=at(
        DAY, "10:00:00"), until=at(DAY, "23:00:00"))

    assert [r["timestamp"]
            for r in history["records"]] == ["2026-10-20T14:10:00+00:00"]


def test_an_outage_resolved_after_midnight_is_found(tmp_path: Path) -> None:
    """Its resolved line sits in the next day's file."""
    start = at(DAY, "23:58:00")
    nxt = DAY + timedelta(days=1)
    write_day(tmp_path, DAY, [outage_line(
        at(DAY, "23:58:03"), "restored", start)])
    write_day(tmp_path, nxt, [outage_line(
        at(nxt, "00:05:00"), "resolved", start)])

    history = read_outages(tmp_path, **window())

    assert [r["stage"] for r in history["records"]] == ["resolved"]
    assert history["days_read"] == [DAY.isoformat(), nxt.isoformat()]


def test_a_day_without_a_file_is_named_not_hidden(tmp_path: Path) -> None:
    """A gap in the files is not a day without outages."""
    a_day(tmp_path)

    history = read_outages(tmp_path, since=at(DAY - timedelta(days=1), "00:00:00"),
                           until=at(DAY, "23:00:00"))

    assert history["days_missing"][0] == (DAY - timedelta(days=1)).isoformat()
    assert DAY.isoformat() in history["days_read"]


def test_a_torn_line_is_counted_and_the_rest_kept(tmp_path: Path) -> None:
    """A hard stop can cut the last line of a file."""
    lines = a_day(tmp_path).read_text(encoding="utf-8").splitlines()
    lines.append(f"{at(DAY, '20:00:00'):%Y-%m-%d %H:%M:%S} UTC | INFO     | x | "
                 '[OUTAGE] {"schema": 1, "stage": "rest')
    write_day(tmp_path, DAY, lines)

    history = read_outages(tmp_path, **window())

    assert history["unreadable_lines"] == 1
    assert history["count"] == 2


def test_a_long_answer_is_cut_and_says_where_to_go_on(tmp_path: Path) -> None:
    starts = [at(DAY, f"1{i}:00:00") for i in range(5)]
    write_day(tmp_path, DAY, [outage_line(s, "resolved", s) for s in starts])

    history = read_outages(tmp_path, **window(), limit=2)

    assert history["count"] == 2
    assert history["truncated"] is True
    assert history["next_since"] == starts[2].isoformat()
    following = read_outages(tmp_path, since=datetime.fromisoformat(history["next_since"]),
                             until=window()["until"], limit=2)
    assert following["records"][0]["timestamp"] == starts[2].isoformat()


def test_without_a_window_it_is_the_last_seven_days(tmp_path: Path) -> None:
    today = datetime.now(timezone.utc)
    recent, old = today - timedelta(days=2), today - timedelta(days=9)
    write_day(tmp_path, recent.date(), [
              outage_line(recent, "resolved", recent)])
    write_day(tmp_path, old.date(), [outage_line(old, "resolved", old)])

    history = read_outages(tmp_path)

    assert [r["timestamp"] for r in history["records"]] == [recent.isoformat()]


def test_a_reversed_or_too_long_window_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        read_outages(tmp_path, since=at(DAY, "12:00:00"),
                     until=at(DAY, "11:00:00"))
    with pytest.raises(ValueError):
        read_outages(tmp_path, since=at(DAY, "00:00:00"),
                     until=at(DAY, "00:00:00") + timedelta(days=63))


def test_a_naive_time_is_taken_as_utc(tmp_path: Path) -> None:
    a_day(tmp_path)

    history = read_outages(tmp_path, since=datetime(
        2026, 10, 20, 10, 0), until=datetime(2026, 10, 20, 23, 0))

    assert history["since"] == "2026-10-20T10:00:00+00:00"
    assert history["count"] == 1


def test_a_finished_day_is_read_once_and_a_grown_file_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A month on every call would be a month of log on every call."""
    path = a_day(tmp_path)
    opened: List[Path] = []
    real_open = Path.open

    def counting_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        if self == path:
            opened.append(self)
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)

    read_outages(tmp_path, **window())
    read_outages(tmp_path, **window())
    assert len(opened) == 1, "an unchanged file was read again"

    later = at(DAY, "22:00:00")
    with real_open(path, "a", encoding="utf-8") as handle:
        handle.write(outage_line(later, "resolved", later) + "\n")
    history = read_outages(tmp_path, **window())

    assert len(opened) == 2, "a grown file was not read again"
    assert history["count"] == 3


def test_a_credential_in_a_record_is_masked(tmp_path: Path) -> None:
    """A record carries exception text, and a log is free text."""
    start = at(DAY, "09:00:00")
    secret = "Bearer " + "a1b2c3d4" * 5
    write_day(tmp_path, DAY, [outage_line(at(DAY, "09:00:05"), "resolved", start,
                                          exception=f"refused: Authorization: {secret}")])

    record = read_outages(tmp_path, **window())["records"][0]

    assert "a1b2c3d4a1b2c3d4" not in json.dumps(record)


# ------------------------------------------------------------------- the route


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    """The API with a log directory holding one day."""
    a_day(tmp_path)
    return TestClient(create_api(
        build=BUILD, health_provider=lambda: {"status": "ok"},
        detail_provider=lambda: {}, origin=ORIGIN,
        registry=load_token_registry(
            {"historian": HISTORIAN, "watcher": WATCHER}),
        log_dir=tmp_path))


def get(client: TestClient, token: Dict[str, Any], **params: str) -> Any:
    """GET /v1/outages with a token."""
    return client.get("/v1/outages", params=params,
                      headers={"Authorization": f"Bearer {token['token']}"})


def test_the_route_serves_the_history_to_its_grant(client: TestClient) -> None:
    response = get(client, HISTORIAN, since="2026-10-20T00:00:00Z",
                   until="2026-10-20T23:59:59Z")

    assert response.status_code == 200
    assert response.json()["count"] == 2


def test_a_status_token_does_not_reach_the_history(client: TestClient) -> None:
    """Holding the live status is not holding a month of it."""
    assert get(client, WATCHER).status_code == 403


def test_without_a_token_it_is_refused(client: TestClient) -> None:
    assert client.get("/v1/outages").status_code == 401


def test_a_bad_window_is_a_400_with_the_reason(client: TestClient) -> None:
    response = get(client, HISTORIAN, since="2026-10-21T00:00:00Z",
                   until="2026-10-20T00:00:00Z")

    assert response.status_code == 400
    assert "after" in response.json()["detail"]


def test_the_grant_parses(tmp_path: Path) -> None:
    """A surface the vocabulary does not know is refused at boot."""
    registry = load_token_registry({"historian": HISTORIAN})

    assert registry.may("historian", "outages:history") is True
