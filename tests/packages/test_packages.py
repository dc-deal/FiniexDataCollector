"""
FiniexDataCollector - Tick packages

FiniexTestingIDE imports a week of ticks as one step: fetch a package, check it,
import it, file the zip as its raw source of truth, acknowledge it. These tests hold
the packer and the API to what that step relies on - every finished file in exactly
one package, the bytes unchanged, the manifest true to them, and nothing packed from
a folder the mint does not vouch for.

Location: tests/packages/test_packages.py
"""

import hashlib
import io
import json
import os
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil
import pytest
from fastapi.testclient import TestClient
from finiex_auth.route_walk import assert_no_identity_route_is_ungated
from pydantic import ValidationError

from python.api import outage_history
from python.packages import packer as packer_module
from python.packages import preflight as preflight_module
from python.packages.package_api import create_package_api
from python.packages.package_service import next_run
from python.packages.package_store import LOCK_NAME, PackageStore
from python.packages.package_tokens import load_package_tokens
from python.packages.packer import pack_source
from python.utils.config_loader import PackagesConfig, PackageSourceConfig

MINT = "ab6381efd498"
OTHER = "cac17e8c4d70"
CUTOFF = datetime(2026, 11, 9, tzinfo=timezone.utc)
NOW = CUTOFF + timedelta(hours=1)
WEEK = timedelta(days=7)


def msc(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def write_mint(root: Path, instance_id: str = MINT) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "instance.json").write_text(json.dumps(
        {"instance_id": instance_id}), encoding="utf-8")


def tick_file(folder: Path, name: str, first: datetime, last: datetime,
              instance_id: Optional[str] = MINT, event_offset: timedelta = timedelta(0),
              producer: str = "finiex-data-collector") -> Path:
    """A finished tick file shaped like both collectors write them."""
    folder.mkdir(parents=True, exist_ok=True)
    ticks = [{"time_msc": msc(moment + event_offset), "collected_msc": msc(moment),
              "bid": 100.0, "ask": 100.5}
             for moment in (first, first + (last - first) / 2, last)]
    origin = {"instance_id": instance_id,
              "producer": producer} if instance_id else None
    document = {"metadata": {"symbol": name.split("_")[0], "data_format_version": "1.7.0",
                             "origin": origin},
                "ticks": ticks, "summary": {"total_ticks": len(ticks)}}
    path = folder / name
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    return path


def day(n: int, hour: int = 12) -> datetime:
    """A moment in the week before CUTOFF; n = 2 is Monday 2 Nov."""
    return datetime(2026, 11, n, hour, tzinfo=timezone.utc)


@pytest.fixture
def kraken(tmp_path: Path) -> Dict[str, Any]:
    """A Kraken data root: the mint one level above the files, as in production."""
    root = tmp_path / "raw"
    write_mint(root)
    files = root / "kraken"
    files.mkdir()
    return {"root": root, "files": files,
            "config": PackageSourceConfig(root=str(root), subdir="kraken"),
            "store": PackageStore(tmp_path / "packages")}


def pack(source: Dict[str, Any], cutoff: datetime = CUTOFF, now: Optional[datetime] = None,
         name: str = "kraken", min_free: float = 0.0) -> Dict[str, Any]:
    return pack_source(name, source["config"], source["store"], min_free,
                       cutoff=cutoff, now=now or cutoff + timedelta(hours=1),
                       packer={"version": "test"})


def zip_members(store: PackageStore, package_id: str) -> Dict[str, bytes]:
    paths = store.find(package_id)
    with zipfile.ZipFile(paths["zip"]) as archive:
        return {info.filename: archive.read(info) for info in archive.infolist()}


# --------------------------------------------------------------------- the packer


def test_a_package_holds_the_files_unchanged_and_describes_them(kraken: Dict[str, Any]) -> None:
    a = tick_file(
        kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(2, 0), day(2, 23))
    b = tick_file(
        kraken["files"], "ETHUSD_20261103_000000_ticks.json", day(3, 0), day(3, 23))

    result = pack(kraken)

    assert result["result"] == "packed"
    package_id = result["package_id"]
    assert package_id == "kraken-0001-20261109"
    store = kraken["store"]
    assert zip_members(store, package_id) == {
        a.name: a.read_bytes(), b.name: b.read_bytes()}
    with zipfile.ZipFile(store.find(package_id)["zip"]) as archive:
        assert {i.compress_type for i in archive.infolist()} == {
            zipfile.ZIP_DEFLATED}

    manifest = store.manifest(package_id)
    assert manifest["instance_id"] == MINT
    assert manifest["previous_package_id"] is None
    assert manifest["cutoff"] == CUTOFF.isoformat()
    assert manifest["member_count"] == 2 and manifest["tick_count"] == 6
    member = manifest["members"][0]
    assert member["sha256"] == hashlib.sha256(a.read_bytes()).hexdigest()
    assert member["last_collected_msc"] == msc(day(2, 23))
    zip_path = store.find(package_id)["zip"]
    assert manifest["zip"]["sha256"] == hashlib.sha256(
        zip_path.read_bytes()).hexdigest()
    assert manifest["zip"]["size_bytes"] == zip_path.stat().st_size


def test_the_next_package_takes_only_new_files_and_names_its_predecessor(
        kraken: Dict[str, Any]) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    first = pack(kraken)["package_id"]
    newer = tick_file(kraken["files"], "BTCUSD_20261110_000000_ticks.json",
                      day(10, 0), day(10, 23))

    second = pack(kraken, cutoff=CUTOFF + WEEK)

    manifest = kraken["store"].manifest(second["package_id"])
    assert manifest["previous_package_id"] == first
    assert [m["name"] for m in manifest["members"]] == [newer.name]
    assert manifest["span"]["from"].startswith("2026-11-09T00:00:00")


def test_a_file_reaching_past_the_cutoff_waits_for_the_next_package(
        kraken: Dict[str, Any]) -> None:
    """A 50,000-tick file closed on Tuesday still holds Monday's ticks; it waits whole."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    late = tick_file(kraken["files"], "DOTUSD_20261108_000000_ticks.json",
                     day(8, 20), CUTOFF + timedelta(minutes=30))

    first = pack(kraken)
    assert first["held_for_next"] == 1
    assert late.name not in zip_members(kraken["store"], first["package_id"])

    second = pack(kraken, cutoff=CUTOFF + WEEK)
    assert late.name in zip_members(kraken["store"], second["package_id"])


def test_a_file_that_appears_late_is_taken_by_the_next_run_not_skipped(
        kraken: Dict[str, Any]) -> None:
    """The cutoff bounds a package from above only: a recovered file is never lost."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    pack(kraken)
    recovered = tick_file(kraken["files"], "ADAUSD_20261104_000000_ticks.json",
                          day(4, 0), day(4, 23))

    second = pack(kraken, cutoff=CUTOFF + WEEK)

    assert list(zip_members(kraken["store"], second["package_id"])) == [
        recovered.name]


def test_no_file_is_in_two_packages(kraken: Dict[str, Any]) -> None:
    for n in range(2, 8):
        tick_file(kraken["files"], f"BTCUSD_202611{n:02d}_000000_ticks.json", day(
            n, 0), day(n, 23))
    pack(kraken)
    for n in range(9, 15):
        tick_file(kraken["files"], f"BTCUSD_202611{n:02d}_000000_ticks.json", day(
            n, 0), day(n, 23))
    pack(kraken, cutoff=CUTOFF + WEEK)

    names = [m["name"] for manifest in kraken["store"].manifests("kraken")
             for m in manifest["members"]]
    assert len(names) == len(set(names)) == 12


def test_no_mint_no_package(kraken: Dict[str, Any]) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    (kraken["root"] / "instance.json").unlink()

    result = pack(kraken)

    assert result["result"] == "refused"
    assert "instance.json" in result["reason"]
    assert kraken["store"].manifests("kraken") == []
    assert kraken["store"].last_runs()["kraken"]["result"] == "refused"


def test_a_member_with_another_identity_stays_out_and_is_named(kraken: Dict[str, Any]) -> None:
    """A copied file or another terminal's: same folder, different mint."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    stranger = tick_file(kraken["files"], "ETHUSD_20261102_000000_ticks.json",
                         day(2, 0), day(2, 23), instance_id=OTHER)
    unattributed = tick_file(kraken["files"], "XRPUSD_20261102_000000_ticks.json",
                             day(2, 0), day(2, 23), instance_id=None)

    manifest = kraken["store"].manifest(pack(kraken)["package_id"])

    assert [m["name"] for m in manifest["members"]] == [
        "BTCUSD_20261102_000000_ticks.json"]
    excluded = {e["name"]: e["reason"] for e in manifest["excluded"]}
    assert OTHER in excluded[stranger.name]
    assert unattributed.name in excluded


def test_open_files_are_named_and_never_packed(kraken: Dict[str, Any]) -> None:
    """
    Including an orphan whose content is complete: MT5 writes the footer and then
    renames, so a failed rename leaves a whole, valid file under the `.part` name.
    It is recoverable by hand, and it is not the packer's to decide that it is done.
    """
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    (kraken["files"] / "ETHUSD_20261108_000000_ticks.jsonl.part").write_text("{}\n")
    orphan = tick_file(
        kraken["files"], "XRPUSD_20261103_000000_ticks.json", day(3, 0), day(3, 23))
    orphan.rename(orphan.with_name(orphan.name + ".part"))

    package_id = pack(kraken)["package_id"]

    manifest = kraken["store"].manifest(package_id)
    assert [f["name"] for f in manifest["open_files"]] == [
        "ETHUSD_20261108_000000_ticks.jsonl.part", "XRPUSD_20261103_000000_ticks.json.part"]
    assert list(zip_members(kraken["store"], package_id)) == [
        "BTCUSD_20261102_000000_ticks.json"]
    assert manifest["excluded"] == []


def test_an_unparseable_file_is_named_not_packed(kraken: Dict[str, Any]) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    (kraken["files"] /
     "ETHUSD_20261102_000000_ticks.json").write_text('{"metadata": {', "utf-8")

    manifest = kraken["store"].manifest(pack(kraken)["package_id"])

    assert manifest["excluded"][0]["name"] == "ETHUSD_20261102_000000_ticks.json"
    assert manifest["member_count"] == 1


def test_nothing_new_makes_no_package(kraken: Dict[str, Any]) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    pack(kraken)

    again = pack(kraken, cutoff=CUTOFF + WEEK)

    assert again["result"] == "nothing_new"
    assert len(kraken["store"].manifests("kraken")) == 1
    assert not list(kraken["store"].folder("kraken").glob("*.part"))


def test_a_run_that_died_is_removed_and_built_again(kraken: Dict[str, Any]) -> None:
    """A zip without its manifest was never a package."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    folder = kraken["store"].folder("kraken")
    folder.mkdir(parents=True)
    (folder / "kraken-0001-20261109.zip").write_bytes(b"half")
    (folder / "kraken-0001-20261109.manifest.json.part").write_bytes(b"{")

    result = pack(kraken)

    assert result["result"] == "packed"
    assert sorted(result["removed_incomplete"]) == [
        "kraken-0001-20261109.manifest.json.part", "kraken-0001-20261109.zip"]
    assert list(zip_members(kraken["store"], result["package_id"])) == [
        "BTCUSD_20261102_000000_ticks.json"]


def test_a_package_folder_that_cannot_be_written_refuses(kraken: Dict[str, Any], tmp_path: Path) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    blocker = tmp_path / "not_a_folder"
    blocker.write_text("x")
    kraken["store"] = PackageStore(blocker)

    result = pack(kraken)

    assert result["result"] in ("refused", "failed")
    assert not list(kraken["files"].parent.rglob("*.zip"))


def test_the_preflight_proves_the_package_folder_takes_a_file(
        kraken: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """The probe is the check, not the folder's existence: a write can fail where mkdir did not."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    real_open = Path.open

    def refusing_open(self: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if self.name.startswith(".probe-") and "w" in mode:
            raise PermissionError("access denied")
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", refusing_open)

    check = preflight_module.check_source(
        "kraken", kraken["config"], kraken["store"], 0.0)

    assert check.ok is False
    assert "not writable" in check.reason
    assert not list(kraken["store"].folder("kraken").glob(".probe-*"))


def test_too_little_disk_is_refused(kraken: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """The collector calls 20 % free disk_low; a package must not be what gets it there."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))

    class Usage:
        total, free = 1000 * 10**6, 250 * 10**6

    monkeypatch.setattr(preflight_module.shutil,
                        "disk_usage", lambda path: Usage)

    result = pack(kraken, min_free=30.0)

    assert result["result"] == "refused"
    assert "below the 30 %" in result["reason"]


def test_the_quiet_minutes_and_a_future_cutoff_are_refused(kraken: Dict[str, Any]) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))

    assert pack(kraken, now=CUTOFF + timedelta(minutes=10)
                )["result"] == "refused"
    assert pack(kraken, now=CUTOFF - timedelta(minutes=2)
                )["result"] == "refused"
    assert pack(kraken, cutoff=CUTOFF + WEEK, now=NOW)["result"] == "refused"
    assert kraken["store"].manifests("kraken") == []


def test_a_second_packer_is_refused_while_one_holds_the_lock(kraken: Dict[str, Any]) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    folder = kraken["store"].folder("kraken")
    folder.mkdir(parents=True)
    parent = psutil.Process().parent()
    (folder / LOCK_NAME).write_text(
        json.dumps({"pid": parent.pid, "create_time": parent.create_time()}))

    result = pack(kraken)

    assert result["result"] == "refused"
    assert "another packer" in result["reason"]


def test_a_stale_lock_is_taken_over(kraken: Dict[str, Any]) -> None:
    """A crash must not stop every run after it."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    folder = kraken["store"].folder("kraken")
    folder.mkdir(parents=True)
    (folder /
     LOCK_NAME).write_text(json.dumps({"pid": 999999, "create_time": 1.0}))

    assert pack(kraken)["result"] == "packed"
    assert not (folder / LOCK_NAME).exists()


def test_mt5_is_cut_on_collected_time_not_on_broker_time(tmp_path: Path) -> None:
    """MT5 names and `time_msc` carry the broker's server time, two or three hours ahead."""
    root = tmp_path / "MQL5" / "Files"
    write_mint(root)
    mt5 = {"config": PackageSourceConfig(
        root=str(root)), "store": PackageStore(tmp_path / "out")}
    broker = timedelta(hours=2)
    inside = tick_file(root, "EURUSD_20261108_230000_ticks.json", day(8, 20),
                       CUTOFF - timedelta(hours=1), event_offset=broker,
                       producer="finiex-mt5-collector")
    outside = tick_file(root, "GBPUSD_20261109_010000_ticks.json", day(8, 22),
                        CUTOFF + timedelta(minutes=1), event_offset=broker,
                        producer="finiex-mt5-collector")
    (root / "USDJPY_20261109_020000_ticks.json.part").write_text("{")

    result = pack(mt5, name="mt5")

    manifest = mt5["store"].manifest(result["package_id"])
    assert [m["name"] for m in manifest["members"]] == [inside.name]
    assert manifest["held_for_next"] == [outside.name]
    assert manifest["members"][0]["last_time_msc"] >= msc(CUTOFF)
    assert manifest["producer"] == "finiex-mt5-collector"
    assert manifest["outages"] is None and manifest["collector_starts"] is None


def test_outages_and_starts_ride_along_from_the_collectors_log(
        kraken: Dict[str, Any], tmp_path: Path) -> None:
    """A restart is the one gap no outage record covers."""
    outage_history.clear_cache()
    logs = tmp_path / "logs"
    logs.mkdir()

    def line(moment: datetime, message: str) -> str:
        return f"{moment:%Y-%m-%d %H:%M:%S} UTC | INFO     | FiniexDataCollector | {message}"

    start = day(4, 9)
    record = {"schema": 1, "stage": "resolved", "timestamp": start.isoformat(),
              "reason": "connection_lost", "duration_seconds": 2.0}
    (logs / "finiexdatacollector_2026-11-04.log").write_text("\n".join([
        line(day(4, 8), "FiniexDataCollector Starting"),
        line(start, "[OUTAGE] " + json.dumps(record)),
    ]) + "\n", encoding="utf-8")
    kraken["config"] = PackageSourceConfig(root=str(kraken["root"]), subdir="kraken",
                                           log_dir=str(logs))
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(8, 23))

    manifest = kraken["store"].manifest(pack(kraken)["package_id"])

    assert manifest["outages"]["count"] == 1
    assert manifest["outages"]["records"][0]["timestamp"] == start.isoformat()
    assert manifest["collector_starts"]["starts"] == [day(4, 8).isoformat()]
    assert "2026-11-03" in manifest["collector_starts"]["days_missing"]


# --------------------------------------------------------------------- configuration


def test_a_subdir_may_not_climb_out_of_the_minted_folder() -> None:
    with pytest.raises(ValidationError):
        PackageSourceConfig(root="C:/raw", subdir="../elsewhere")


def test_the_weekly_run_stays_out_of_the_quiet_minutes() -> None:
    with pytest.raises(ValidationError):
        PackagesConfig(hour=0, minute=5)
    with pytest.raises(ValidationError):
        PackagesConfig(hour=23, minute=58)
    assert PackagesConfig(hour=1, minute=0).hour == 1


def test_the_next_run_is_the_coming_monday_at_one() -> None:
    assert next_run(datetime(2026, 11, 5, 13, 0, tzinfo=timezone.utc), "monday", 1, 0) == \
        datetime(2026, 11, 9, 1, 0, tzinfo=timezone.utc)
    assert next_run(datetime(2026, 11, 9, 1, 0, tzinfo=timezone.utc), "monday", 1, 0) == \
        datetime(2026, 11, 16, 1, 0, tzinfo=timezone.utc)
    assert next_run(datetime(2026, 11, 9, 0, 30, tzinfo=timezone.utc), "monday", 1, 0) == \
        datetime(2026, 11, 9, 1, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------- the API

READER = {"token": "r" * 40, "grants": ["packages:*", "receipts:*"]}
FETCHER = {"token": "f" * 40, "grants": ["packages:*"]}
NOBODY = {"token": "n" * 40, "grants": []}


def bearer(token: Dict[str, Any]) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token['token']}"}


@pytest.fixture
def served(kraken: Dict[str, Any]) -> Dict[str, Any]:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    package_id = pack(kraken)["package_id"]
    registry = load_package_tokens(
        {"ide": READER, "fetcher": FETCHER, "nobody": NOBODY})
    app = create_package_api(
        kraken["store"], registry, lambda: {"next_run": None})
    return {"client": TestClient(app), "app": app, "id": package_id, "store": kraken["store"]}


def test_the_list_needs_a_token_and_the_grant(served: Dict[str, Any]) -> None:
    client = served["client"]

    assert client.get("/v1/packages").status_code == 401
    assert client.get("/v1/packages", headers=bearer(NOBODY)
                      ).status_code == 403
    listing = client.get("/v1/packages", headers=bearer(FETCHER)).json()
    assert [p["package_id"] for p in listing["packages"]] == [served["id"]]
    assert listing["packages"][0]["received_at"] is None
    assert listing["last_runs"]["kraken"]["result"] == "packed"


def test_every_identity_route_refuses_a_token_holding_nothing(served: Dict[str, Any]) -> None:
    walked = assert_no_identity_route_is_ungated(
        served["app"], served["client"], bearer(NOBODY),
        required=[("/v1/packages/{package_id}", "get"),
                  ("/v1/packages/{package_id}/zip", "get"),
                  ("/v1/packages/{package_id}/receipt", "post")])
    assert len(walked) == 3


def test_the_manifest_and_the_zip_are_served(served: Dict[str, Any]) -> None:
    client, store, package_id = served["client"], served["store"], served["id"]

    manifest = client.get(
        f"/v1/packages/{package_id}", headers=bearer(FETCHER)).json()
    body = client.get(
        f"/v1/packages/{package_id}/zip", headers=bearer(FETCHER))

    assert manifest["package_id"] == package_id and manifest["receipt"] is None
    assert body.status_code == 200
    assert body.content == store.find(package_id)["zip"].read_bytes()
    assert hashlib.sha256(
        body.content).hexdigest() == manifest["zip"]["sha256"]


def test_a_download_can_be_resumed(served: Dict[str, Any]) -> None:
    client, package_id = served["client"], served["id"]
    whole = served["store"].find(package_id)["zip"].read_bytes()

    tail = client.get(f"/v1/packages/{package_id}/zip",
                      headers={**bearer(FETCHER), "Range": "bytes=100-"})

    assert tail.status_code == 206
    assert tail.content == whole[100:]


def test_an_unknown_or_crafted_id_is_not_found(served: Dict[str, Any]) -> None:
    client = served["client"]
    for package_id in ("kraken-0099-20261109", "..%2F..%2Fsecrets", "kraken"):
        assert client.get(
            f"/v1/packages/{package_id}", headers=bearer(READER)).status_code == 404


def test_a_receipt_marks_once_and_deletes_nothing(served: Dict[str, Any]) -> None:
    client, package_id = served["client"], served["id"]
    url = f"/v1/packages/{package_id}/receipt"

    first = client.post(url, headers=bearer(READER),
                        json={"note": "imported 1 file"})
    again = client.post(url, headers=bearer(READER))

    assert first.status_code == 201 and first.json()["consumer"] == "ide"
    assert again.status_code == 200
    assert again.json()["received_at"] == first.json()["received_at"]
    assert served["store"].find(package_id)["zip"].exists()
    listing = client.get("/v1/packages", headers=bearer(READER),
                         params={"unacknowledged": True})
    assert listing.json()["packages"] == []


def test_fetching_is_not_acknowledging(served: Dict[str, Any]) -> None:
    """A token that may download may not mark a package received."""
    response = served["client"].post(f"/v1/packages/{served['id']}/receipt",
                                     headers=bearer(FETCHER))

    assert response.status_code == 403
    assert served["store"].receipt(served["id"]) is None


def test_health_is_open_and_names_no_content(served: Dict[str, Any]) -> None:
    body = served["client"].get("/v1/packages/health").json()

    assert body["status"] == "ok"
    assert body["last_runs"]["kraken"]["result"] == "packed"
    assert "packages" not in body


def test_package_grants_are_their_own_vocabulary() -> None:
    """A collector grant here is a typo, refused at load."""
    with pytest.raises(ValidationError):
        load_package_tokens(
            {"x": {"token": "x" * 40, "grants": ["status:detail"]}})
