"""
FiniexDataCollector - Package schedule

Packages come from the schedule, never from a request. FiniexTestingIDE reads the
schedule to plan its pickups, and only the operator's token changes it - within
limits that keep packages from piling up and that respect the collector's day cut.
These tests hold the rules, the service's loop and the routes to that.

Location: tests/packages/test_schedule.py
"""

import asyncio
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from python.packages.__main__ import EXIT_CONFIGURATION, cmd_schedule
from python.packages.ledger import Ledger
from python.packages.package_api import create_package_api
from python.packages.package_service import PackageService
from python.packages.package_store import PackageStore
from python.packages.package_tokens import load_package_tokens
from python.packages.schedule import (Schedule, ScheduleRefused,
                                      in_quiet_minutes)
from python.packages.schedule_client import schedule_call
from python.utils.config_loader import PackagesConfig, PackageSourceConfig


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


# Tuesday. The weekly run is Sunday 20:00 UTC: 15, 22 and 29 November.
NOW = utc(2026, 11, 10, 12, 0)

OPERATOR = {"token": "o" * 40, "grants": ["packages:*", "schedule:*"]}
READER = {"token": "r" * 40, "grants": ["packages:*", "receipts:*"]}


@pytest.fixture
def config(tmp_path: Path) -> PackagesConfig:
    return PackagesConfig(
        output_dir=str(tmp_path / "packages"), originals_dir=str(tmp_path / "originals"),
        sources={"kraken": PackageSourceConfig(root=str(tmp_path / "k"), subdir="kraken"),
                 "mt5": PackageSourceConfig(root=str(tmp_path / "m"))})


@pytest.fixture
def plan(config: PackagesConfig) -> Schedule:
    return Schedule(Path(config.output_dir), config)


def refused(plan: Schedule, collector: str, cutoff: date, run_at: Optional[datetime] = None,
            now: datetime = NOW) -> str:
    with pytest.raises(ScheduleRefused) as caught:
        plan.add(collector, cutoff, now, "operator", run_at=run_at)
    return str(caught.value)


# ----------------------------------------------------------------- the rules


def test_a_special_run_packs_the_morning_after_its_cutoff_and_is_kept(
        plan: Schedule, config: PackagesConfig) -> None:
    special = plan.add("kraken", date(2026, 11, 18), NOW, "operator")

    assert special["cutoff"] == "2026-11-18T00:00:00+00:00"
    assert special["run_at"] == "2026-11-18T01:00:00+00:00"
    assert (special["state"], special["created_by"]) == ("planned", "operator")
    reread = Schedule(Path(config.output_dir), config).view(NOW)["special"]
    assert [s["id"] for s in reread] == [special["id"]]
    assert "created_by" not in reread[0]


def test_a_cutoff_whose_morning_has_passed_packs_in_a_few_minutes(plan: Schedule) -> None:
    special = plan.add("kraken", date(2026, 11, 10), NOW, "operator")

    assert special["run_at"] == "2026-11-10T12:05:00+00:00"


def test_a_special_run_waits_for_the_seal(plan: Schedule) -> None:
    reason = refused(plan, "kraken", date(2026, 11, 18),
                     run_at=utc(2026, 11, 18, 0, 29))

    assert "no earlier than 30 minutes after its cutoff" in reason
    assert plan.add("kraken", date(2026, 11, 18), NOW, "operator",
                    run_at=utc(2026, 11, 18, 0, 30))["run_at"] == "2026-11-18T00:30:00+00:00"


def test_the_quiet_minutes_stay_quiet(plan: Schedule) -> None:
    assert "stays quiet" in refused(plan, "kraken", date(2026, 11, 17),
                                    run_at=utc(2026, 11, 17, 23, 55))
    assert in_quiet_minutes(utc(2026, 11, 18, 0, 15))
    assert not in_quiet_minutes(utc(2026, 11, 17, 23, 54))
    assert not in_quiet_minutes(utc(2026, 11, 18, 0, 16))


def test_a_run_in_the_past_and_an_unknown_collector_are_refused(plan: Schedule) -> None:
    assert "in the past" in refused(plan, "kraken", date(2026, 11, 9),
                                    run_at=utc(2026, 11, 10, 11, 0))
    assert "no collector 'binance'" in refused(
        plan, "binance", date(2026, 11, 18))
    assert plan.add("all", date(2026, 11, 18), NOW,
                    "operator")["collector"] == "all"


def test_no_special_run_shortly_before_a_weekly_one(plan: Schedule) -> None:
    reason = refused(plan, "kraken", date(2026, 11, 14),
                     run_at=utc(2026, 11, 14, 20, 1))

    assert "less than 24 h after it" in reason
    # Saturday 01:00 is 43 hours before Sunday 20:00.
    assert plan.add("kraken", date(2026, 11, 14), NOW, "operator")


def test_no_special_run_to_the_weekly_runs_own_cutoff_before_or_after_it(plan: Schedule) -> None:
    before = refused(plan, "kraken", date(2026, 11, 15))
    after = refused(plan, "kraken", date(2026, 11, 15),
                    run_at=utc(2026, 11, 15, 21, 0))

    assert "less than 24 h" in before
    assert "already packs to this cutoff" in after


def test_a_weekly_run_set_aside_neither_blocks_nor_covers(plan: Schedule) -> None:
    plan.set_aside(NOW)

    assert plan.add("kraken", date(2026, 11, 15), NOW, "operator")
    assert "already packs" in refused(plan, "mt5", date(2026, 11, 22),
                                      run_at=utc(2026, 11, 22, 21, 0))


def test_one_run_per_collector_and_day_and_at_most_two_a_week(plan: Schedule) -> None:
    first = plan.add("kraken", date(2026, 11, 17), NOW, "operator")
    plan.add("kraken", date(2026, 11, 18), NOW, "operator")

    assert "already has a special run to this cutoff" in refused(
        plan, "kraken", date(2026, 11, 17), run_at=utc(2026, 11, 17, 6, 0))
    assert "at most 2" in refused(plan, "kraken", date(2026, 11, 19))
    assert "kraken already has 2" in refused(plan, "all", date(2026, 11, 20))
    assert plan.add("mt5", date(2026, 11, 19), NOW, "operator")

    plan.cancel(first["id"])
    assert plan.add("kraken", date(2026, 11, 19), NOW, "operator")


def test_a_run_for_all_counts_for_each_collector(plan: Schedule) -> None:
    plan.add("all", date(2026, 11, 17), NOW, "operator")

    assert "mt5 already has a special run to this cutoff" in refused(
        plan, "mt5", date(2026, 11, 17), run_at=utc(2026, 11, 17, 6, 0))


def test_a_run_that_is_done_still_counts(plan: Schedule) -> None:
    special = plan.add("kraken", date(2026, 11, 18), NOW, "operator")

    assert plan.due(utc(2026, 11, 18, 0, 59)) == []
    assert [s["id"] for s in plan.due(utc(2026, 11, 18, 1, 0))] == [
        special["id"]]
    plan.finish(special["id"], [{"source": "kraken", "result": "packed"}])

    assert plan.due(utc(2026, 11, 18, 2, 0)) == []
    assert plan.view(NOW)["special"][0]["state"] == "done"
    assert "already has" in refused(plan, "kraken", date(2026, 11, 18),
                                    run_at=utc(2026, 11, 18, 6, 0))


def test_cancelling_touches_only_a_planned_run(plan: Schedule) -> None:
    special = plan.add("kraken", date(2026, 11, 18), NOW, "operator")

    assert plan.cancel("deadbeef") is None
    assert plan.cancel(special["id"])["state"] == "cancelled"
    assert plan.cancel(special["id"]) is None
    assert plan.next_special_at() is None
    assert plan.due(utc(2026, 11, 18, 2, 0)) == []


def test_setting_aside_the_next_weekly_run_and_resuming(plan: Schedule) -> None:
    plan.set_aside(NOW)

    assert [r["set_aside"] for r in plan.view(NOW)["weekly"]["next_runs"]] == \
        [True, False, False, False]
    plan.resume()
    assert not any(r["set_aside"]
                   for r in plan.view(NOW)["weekly"]["next_runs"])


def test_setting_aside_every_weekly_run_until_a_date(plan: Schedule) -> None:
    plan.set_aside(NOW, until=date(2026, 12, 1))

    runs = plan.view(NOW)["weekly"]["next_runs"]
    assert [(r["run_at"][:10], r["set_aside"]) for r in runs] == [
        ("2026-11-15", True), ("2026-11-22", True), ("2026-11-29", True),
        ("2026-12-06", False)]
    with pytest.raises(ScheduleRefused):
        plan.set_aside(NOW, until=date(2026, 11, 10))


def test_a_special_default_in_the_quiet_minutes_is_a_configuration_error(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        PackagesConfig(output_dir=str(tmp_path), originals_dir=str(tmp_path),
                       special_hour=0, special_minute=10)


# ----------------------------------------------------------------- the service


@pytest.fixture
def service(config: PackagesConfig) -> Tuple[PackageService, List[Tuple[Any, ...]]]:
    """The service with packing, reporting and housekeeping recorded instead of done."""
    svc = PackageService(config, {})
    calls: List[Tuple[Any, ...]] = []

    async def run_once(only: Optional[str] = None,
                       cutoff: Optional[datetime] = None) -> List[Dict[str, Any]]:
        calls.append(("pack", only, cutoff))
        return [{"source": only or "kraken", "result": "nothing_new"}]

    async def report(results: List[Dict[str, Any]], title: str = "weekly run") -> bool:
        calls.append(("report", title))
        return True

    async def housekeeping() -> None:
        calls.append(("housekeeping",))

    svc.run_once, svc.report, svc.housekeeping = run_once, report, housekeeping
    svc._weekly_at = utc(2026, 11, 22, 20, 0)
    return svc, calls


def events(config: PackagesConfig, name: str) -> List[Dict[str, Any]]:
    return Ledger(Path(config.output_dir)).read(event=name)["events"]


def test_a_due_special_run_packs_its_collector_to_its_cutoff_once(
        service: Tuple[PackageService, List[Tuple[Any, ...]]], config: PackagesConfig) -> None:
    svc, calls = service
    special = svc.plan.add("kraken", date(2026, 11, 18), NOW, "operator")

    assert asyncio.run(svc.step(utc(2026, 11, 18, 0, 59))) == []
    assert asyncio.run(svc.step(utc(2026, 11, 18, 1, 0))) == [
        f"special {special['id']}"]
    assert asyncio.run(svc.step(utc(2026, 11, 18, 1, 1))) == []

    assert calls == [("pack", "kraken", utc(2026, 11, 18)),
                     ("report", "special run to 2026-11-18"), ("housekeeping",)]
    assert svc.plan.view(NOW)["special"][0]["state"] == "done"
    assert events(config, "special_ran")[0]["results"] == {
        "kraken": "nothing_new"}


def test_a_special_run_for_all_packs_every_collector(
        service: Tuple[PackageService, List[Tuple[Any, ...]]]) -> None:
    svc, calls = service
    svc.plan.add("all", date(2026, 11, 18), NOW, "operator")

    asyncio.run(svc.step(utc(2026, 11, 18, 1, 0)))

    assert calls[0] == ("pack", None, utc(2026, 11, 18))


def test_a_run_missed_while_down_waits_out_the_quiet_minutes(
        service: Tuple[PackageService, List[Tuple[Any, ...]]]) -> None:
    """A start at 23:58 finds a special run long due; the packer would refuse it there."""
    svc, calls = service
    special = svc.plan.add("kraken", date(2026, 11, 18), NOW, "operator")

    assert asyncio.run(svc.step(utc(2026, 11, 18, 23, 58))) == []
    assert asyncio.run(svc.step(utc(2026, 11, 19, 0, 15))) == []
    assert asyncio.run(svc.step(utc(2026, 11, 19, 0, 16))) == [
        f"special {special['id']}"]
    assert calls[0] == ("pack", "kraken", utc(2026, 11, 18))


def test_the_weekly_run_packs_reports_and_moves_on(
        service: Tuple[PackageService, List[Tuple[Any, ...]]]) -> None:
    svc, calls = service

    assert asyncio.run(svc.step(utc(2026, 11, 22, 19, 59))) == []
    assert asyncio.run(svc.step(utc(2026, 11, 22, 20, 0, 30))) == ["weekly"]

    assert calls == [("pack", None, None), ("report",
                                            "weekly run"), ("housekeeping",)]
    assert svc.status()["next_run"] == "2026-11-29T20:00:00+00:00"


def test_a_weekly_run_set_aside_packs_nothing_and_says_so(
        service: Tuple[PackageService, List[Tuple[Any, ...]]], config: PackagesConfig) -> None:
    svc, calls = service
    svc.plan.set_aside(NOW, until=date(2026, 11, 23))
    assert svc.status()["next_run_set_aside"] is True

    assert asyncio.run(svc.step(utc(2026, 11, 22, 20, 0, 30))) == [
        "weekly set aside"]

    assert calls == [("housekeeping",)]
    assert events(config, "weekly_set_aside")[
        0]["run_at"] == "2026-11-22T20:00:00+00:00"
    assert svc.status()["next_run"] == "2026-11-29T20:00:00+00:00"
    assert svc.status()["next_run_set_aside"] is False


# ----------------------------------------------------------------- the routes


@pytest.fixture
def api(config: PackagesConfig) -> Dict[str, Any]:
    store = PackageStore(Path(config.output_dir))
    ledger = Ledger(store.root)
    registry = load_package_tokens({"operator": OPERATOR, "ide": READER})
    app = create_package_api(store, registry, lambda: {},
                             ledger, config, clock=lambda: NOW)
    return {"client": TestClient(app), "ledger": ledger}


def bearer(token: Dict[str, Any]) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token['token']}"}


def test_the_schedule_is_open_and_names_no_token(api: Dict[str, Any]) -> None:
    client = api["client"]
    client.post("/v1/packages/schedule", headers=bearer(OPERATOR),
                json={"collector": "kraken", "cutoff": "2026-11-18"})

    answer = client.get("/v1/packages/schedule")

    assert answer.status_code == 200
    view = answer.json()
    assert view["weekly"]["next_runs"][0]["run_at"] == "2026-11-15T20:00:00+00:00"
    assert view["special"][0]["run_at"] == "2026-11-18T01:00:00+00:00"
    assert "operator" not in answer.text


def test_only_the_schedule_grant_changes_the_schedule(api: Dict[str, Any]) -> None:
    client = api["client"]
    body = {"collector": "kraken", "cutoff": "2026-11-18"}

    assert client.post("/v1/packages/schedule", json=body).status_code == 401
    assert client.post("/v1/packages/schedule", headers=bearer(READER),
                       json=body).status_code == 403
    assert client.post("/v1/packages/schedule/weekly", headers=bearer(READER),
                       json={"action": "resume"}).status_code == 403
    created = client.post("/v1/packages/schedule",
                          headers=bearer(OPERATOR), json=body)
    assert created.status_code == 201
    assert client.delete(f"/v1/packages/schedule/{created.json()['id']}",
                         headers=bearer(READER)).status_code == 403


def test_a_refusal_names_the_rule_and_every_change_is_booked(api: Dict[str, Any]) -> None:
    client, ledger = api["client"], api["ledger"]
    body = {"collector": "kraken", "cutoff": "2026-11-18"}
    special = client.post("/v1/packages/schedule",
                          headers=bearer(OPERATOR), json=body).json()

    again = client.post("/v1/packages/schedule",
                        headers=bearer(OPERATOR), json=body)
    cancelled = client.delete(
        f"/v1/packages/schedule/{special['id']}", headers=bearer(OPERATOR))

    assert again.status_code == 409
    assert "already has a special run to this cutoff" in again.json()["detail"]
    assert cancelled.json()["state"] == "cancelled"
    assert client.delete(f"/v1/packages/schedule/{special['id']}",
                         headers=bearer(OPERATOR)).status_code == 404
    booked = [(e["event"], e["consumer"]) for e in ledger.read()["events"]]
    assert booked == [("schedule_added", "operator"), ("schedule_refused", "operator"),
                      ("schedule_cancelled", "operator")]


@pytest.mark.parametrize("body", [
    {"cutoff": "2026-11-18"},
    {"collector": "kraken", "cutoff": "18.11.2026"},
    {"collector": "kraken", "cutoff": "2026-11-18", "run_at": "2026-11-18T01:00:00"},
])
def test_a_malformed_special_run_is_a_bad_request(api: Dict[str, Any], body: Dict[str, Any]) -> None:
    answer = api["client"].post(
        "/v1/packages/schedule", headers=bearer(OPERATOR), json=body)

    assert answer.status_code == 400


def test_the_weekly_run_is_set_aside_and_resumed_over_the_api(api: Dict[str, Any]) -> None:
    client, ledger = api["client"], api["ledger"]

    set_aside = client.post("/v1/packages/schedule/weekly", headers=bearer(OPERATOR),
                            json={"action": "set_aside", "until": "2026-11-23"}).json()
    resumed = client.post("/v1/packages/schedule/weekly", headers=bearer(OPERATOR),
                          json={"action": "resume"}).json()

    assert [r["set_aside"] for r in set_aside["weekly"]
            ["next_runs"]] == [True, True, False, False]
    assert not any(r["set_aside"] for r in resumed["weekly"]["next_runs"])
    assert client.post("/v1/packages/schedule/weekly", headers=bearer(OPERATOR),
                       json={"action": "pause"}).status_code == 400
    assert [e["event"] for e in ledger.read()["events"]] == [
        "schedule_weekly_set_aside", "schedule_weekly_resumed"]


# ----------------------------------------------------------------- the client


def test_the_client_reads_without_a_token_and_changes_with_one(api: Dict[str, Any]) -> None:
    client = api["client"]

    shown = schedule_call("http://testserver",
                          "not a token", "show", session=client)
    refused_ = schedule_call("http://testserver", "not a token", "add", collector="kraken",
                             cutoff=date(2026, 11, 18), session=client)
    status, special = schedule_call("http://testserver", OPERATOR["token"], "add",
                                    collector="kraken", cutoff=date(2026, 11, 18), session=client)

    assert shown[0] == 200 and refused_[0] == 401 and status == 201
    assert schedule_call("http://testserver", OPERATOR["token"], "cancel",
                         special_id=special["id"], session=client)[1]["state"] == "cancelled"
    assert schedule_call("http://testserver", OPERATOR["token"], "set_aside",
                         session=client)[1]["weekly"]["next_runs"][0]["set_aside"] is True
    assert schedule_call("http://testserver", OPERATOR["token"], "resume",
                         session=client)[0] == 200


def test_reading_the_schedule_sends_no_token() -> None:
    """The open route needs none, and a token that is not sent cannot leak."""
    sent: List[Dict[str, str]] = []

    class Answer:
        status_code = 200

        def json(self) -> Dict[str, Any]:
            return {}

    class Session:
        def request(self, method: str, url: str, headers: Dict[str, str],
                    **kwargs: Any) -> Answer:
            sent.append(headers)
            return Answer()

    schedule_call("https://collector.example",
                  OPERATOR["token"], "show", session=Session())
    schedule_call("https://collector.example",
                  OPERATOR["token"], "resume", session=Session())

    assert sent == [{}, {"Authorization": f"Bearer {OPERATOR['token']}"}]


def test_the_command_takes_one_change_at_a_time() -> None:
    assert cmd_schedule("live", "kraken", "2026-11-18", "", "abc12345",
                        False, "", False) == EXIT_CONFIGURATION
