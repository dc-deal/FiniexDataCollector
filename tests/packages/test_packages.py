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
from python.packages import lifecycle
from python.packages import packer as packer_module
from python.packages import preflight as preflight_module
from python.packages.ledger import Ledger
from python.packages.package_api import (LedgeredFileResponse,
                                         create_package_api)
from python.packages.package_store import LOCK_NAME, PackageStore
from python.packages.package_tokens import load_package_tokens
from python.packages.packer import pack_source
from python.packages.pickup import pickup
from python.packages.schedule import next_run
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
    source = PackageSourceConfig(root=str(root), subdir="kraken")
    return {"root": root, "files": files, "config": source,
            "store": PackageStore(tmp_path / "packages"),
            "originals": tmp_path / "originals",
            "packages_config": PackagesConfig(
                output_dir=str(tmp_path / "packages"), originals_dir=str(tmp_path / "originals"),
                min_free_percent=0.0, sources={"kraken": source},
                dev_consumers=["operator"])}


def pack(source: Dict[str, Any], cutoff: datetime = CUTOFF, now: Optional[datetime] = None,
         name: str = "kraken", min_free: float = 0.0, stage: bool = False) -> Dict[str, Any]:
    """A packing run; `stage` moves the originals on to stage A, as the service does."""
    return pack_source(name, source["config"], source["store"], min_free,
                       cutoff=cutoff, now=now or cutoff + timedelta(hours=1),
                       packer={"version": "test"},
                       originals_root=source["originals"] if stage else None)


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
    folder = kraken["store"].ready("kraken")
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
    assert not list(kraken["store"].ready("kraken").glob(".probe-*"))


def test_staging_needs_the_right_to_change_the_source_folder(
        kraken: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Found out after the zip, the package would exist with its originals unable to follow."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    real_probe = preflight_module._probe_write

    def refusing_probe(folder: Path) -> None:
        if folder == kraken["files"]:
            raise PermissionError("access denied")
        real_probe(folder)

    monkeypatch.setattr(preflight_module, "_probe_write", refusing_probe)

    without = preflight_module.check_source(
        "kraken", kraken["config"], kraken["store"], 0.0)
    staging = preflight_module.check_source("kraken", kraken["config"], kraken["store"], 0.0,
                                            kraken["originals"])

    assert without.ok is True
    assert staging.ok is False and "source folder" in staging.reason


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
    # 00:15 is the last quiet minute, for a run by hand as for the schedule.
    assert pack(kraken, now=CUTOFF + timedelta(minutes=15)
                )["result"] == "refused"
    assert pack(kraken, cutoff=CUTOFF + WEEK, now=NOW)["result"] == "refused"
    assert kraken["store"].manifests("kraken") == []
    assert pack(kraken, now=CUTOFF + timedelta(minutes=16)
                )["result"] == "packed"


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
    # The name every Finiex package service uses, beside ours (2026-11-12).
    assert manifest["process_starts"] == manifest["collector_starts"]


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
OPERATOR = {"token": "o" * 40, "grants": [
    "packages:*", "receipts:*", "purge:originals"]}


def bearer(token: Dict[str, Any]) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token['token']}"}


@pytest.fixture
def served(kraken: Dict[str, Any]) -> Dict[str, Any]:
    """The API over one packed and staged package, settling after a receipt as the service does."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    tick_file(kraken["files"], "ETHUSD_20261103_000000_ticks.json", day(
        3, 0), day(3, 23))
    package_id = pack(kraken, stage=True)["package_id"]
    registry = load_package_tokens(
        {"ide": READER, "fetcher": FETCHER, "nobody": NOBODY, "operator": OPERATOR})
    ledger = Ledger(kraken["store"].root)
    config = kraken["packages_config"]

    def settle_after_receipt(collector: str) -> None:
        lifecycle.settle_source(collector, config.sources[collector], kraken["store"],
                                kraken["originals"], ledger)

    app = create_package_api(
        kraken["store"], registry, lambda: {"next_run": None}, ledger, config,
        on_receipt=settle_after_receipt)
    return {"client": TestClient(app), "app": app, "id": package_id, "store": kraken["store"],
            "ledger": ledger, "kraken": kraken, "config": config}


def test_the_shared_names_stand_beside_ours(served: Dict[str, Any]) -> None:
    """
    `source` is the name every Finiex package service uses, so FiniexTestingIDE's one
    client reads the collector's packages and the RAG engine's alike (2026-11-12).
    Beside `collector`, never instead - and for packages built before it too.
    """
    client, store = served["client"], served["store"]
    manifest = store.manifest(served["id"])
    assert manifest["source"] == manifest["collector"] == "kraken"

    def listed(**params: str) -> List[Dict[str, Any]]:
        return client.get("/v1/packages", headers=bearer(FETCHER), params=params).json()["packages"]

    assert [(p["source"], p["collector"])
            for p in listed()] == [("kraken", "kraken")]
    assert [p["package_id"] for p in listed(source="kraken")] == [served["id"]]
    assert listed(source="mt5") == []

    path = store.find(served["id"])["manifest"]
    older = json.loads(path.read_text(encoding="utf-8"))
    del older["source"]
    path.write_text(json.dumps(older), encoding="utf-8")
    assert listed()[0]["source"] == "kraken"

    named = [e for e in served["ledger"].read()["events"] if "collector" in e]
    assert {e["event"] for e in named} >= {"packed", "staged"}
    assert all(e["source"] == e["collector"] for e in named)


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
                  ("/v1/packages/{package_id}/receipt", "post"),
                  ("/v1/packages/schedule/{special_id}", "delete")])
    assert len(walked) == 4


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


def fetch(served: Dict[str, Any], token: Dict[str, Any] = None, byte_range: str = "") -> Any:
    """Download the served package's zip, as the consumer does before it acknowledges."""
    headers = bearer(token or READER)
    if byte_range:
        headers["Range"] = byte_range
    return served["client"].get(f"/v1/packages/{served['id']}/zip", headers=headers)


def test_a_receipt_marks_once_and_deletes_nothing(served: Dict[str, Any]) -> None:
    client, package_id = served["client"], served["id"]
    url = f"/v1/packages/{package_id}/receipt"
    fetch(served)

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
    """Codes only: a refusal's reason names paths on the machine, and that is behind the token."""
    body = served["client"].get("/v1/packages/health").json()

    assert body["status"] == "ok" and body["reasons"] == []
    run = body["last_runs"]["kraken"]
    assert set(run) == {"result", "at", "duration_seconds"}
    assert run["result"] == "packed" and run["duration_seconds"] >= 0
    assert "packages" not in body and "storage" not in body


def test_no_route_can_pack_and_every_route_is_on_this_list(served: Dict[str, Any]) -> None:
    """
    Packing happens on the schedule and by a command on the machine - never because
    a request asked for it. The schedule routes plan a run, under the operator's
    grant and the limits; none starts one. A route added later fails here until it
    is looked at and written into this list.
    """
    routes = sorted((route.path, method) for route in served["app"].routes
                    for method in getattr(route, "methods", None) or []
                    if route.path.startswith("/v1/"))

    assert routes == sorted([
        ("/v1/packages/health", "GET"),
        ("/v1/packages", "GET"),
        ("/v1/packages/ledger", "GET"),
        ("/v1/packages/purge", "GET"),
        ("/v1/packages/purge", "POST"),
        ("/v1/packages/{package_id}", "GET"),
        ("/v1/packages/{package_id}/zip", "GET"),
        ("/v1/packages/{package_id}/receipt", "POST"),
        ("/v1/packages/schedule", "GET"),
        ("/v1/packages/schedule", "POST"),
        ("/v1/packages/schedule/weekly", "POST"),
        ("/v1/packages/schedule/{special_id}", "DELETE"),
    ])


def test_package_grants_are_their_own_vocabulary() -> None:
    """A collector grant here is a typo, refused at load."""
    with pytest.raises(ValidationError):
        load_package_tokens(
            {"x": {"token": "x" * 40, "grants": ["status:detail"]}})


# --------------------------------------------------------------------- the ledger


def events(ledger: Ledger, kind: Optional[str] = None) -> List[Dict[str, Any]]:
    return ledger.read(event=kind)["events"]


def test_a_committed_package_is_booked_with_its_members(served: Dict[str, Any]) -> None:
    """A second record of the contents, independent of the manifest file."""
    packed = events(served["ledger"], "packed")
    manifest = served["store"].manifest(served["id"])

    assert [e["package_id"] for e in packed] == [served["id"]]
    assert packed[0]["zip"]["sha256"] == manifest["zip"]["sha256"]
    assert packed[0]["members"] == [{"name": m["name"], "sha256": m["sha256"],
                                     "tick_count": m["tick_count"]} for m in manifest["members"]]
    assert "entered_late" not in packed[0]


def test_packages_older_than_the_ledger_are_entered_late_and_once(kraken: Dict[str, Any]) -> None:
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    package_id = pack(kraken)["package_id"]
    ledger = Ledger(kraken["store"].root)
    ledger.path.unlink()

    assert ledger.seed(kraken["store"].manifests()) == [package_id]
    assert ledger.seed(kraken["store"].manifests()) == []
    packed = events(ledger, "packed")
    assert len(packed) == 1 and packed[0]["entered_late"] is True


def test_a_whole_download_is_booked_complete(served: Dict[str, Any]) -> None:
    size = served["store"].find(served["id"])["zip"].stat().st_size

    served["client"].get(
        f"/v1/packages/{served['id']}/zip", headers=bearer(FETCHER))

    booked = events(served["ledger"], "served")
    assert len(booked) == 1
    assert booked[0]["consumer"] == "fetcher" and booked[0]["range"] is None
    assert booked[0]["bytes_sent"] == size and booked[0]["complete"] is True


def test_a_resumed_download_is_booked_with_its_range(served: Dict[str, Any]) -> None:
    size = served["store"].find(served["id"])["zip"].stat().st_size

    served["client"].get(f"/v1/packages/{served['id']}/zip",
                         headers={**bearer(FETCHER), "Range": "bytes=100-"})

    booked = events(served["ledger"], "served")[0]
    assert booked["range"] == "bytes=100-" and booked["status"] == 206
    assert booked["bytes_sent"] == size - 100 and booked["complete"] is True


def test_a_download_the_client_abandons_is_booked_incomplete(tmp_path: Path) -> None:
    """A gone client: the server would go on accepting bytes into the void."""
    import anyio

    path = tmp_path / "big.zip"
    path.write_bytes(os.urandom(3 * 64 * 1024))
    outcomes: List[Dict[str, Any]] = []
    response = LedgeredFileResponse(path, media_type="application/zip",
                                    on_finish=outcomes.append)
    sent: List[Dict[str, Any]] = []
    gone = anyio.Event()

    async def receive() -> Dict[str, Any]:
        await gone.wait()
        return {"type": "http.disconnect"}

    async def send(message: Dict[str, Any]) -> None:
        sent.append(message)
        if message["type"] == "http.response.body":
            gone.set()
            await anyio.sleep(0.05)

    scope = {"type": "http", "method": "GET", "headers": [], "extensions": {}}
    anyio.run(response, scope, receive, send)

    assert outcomes[0]["disconnected"] is True
    assert outcomes[0]["complete"] is False
    assert outcomes[0]["bytes_sent"] < path.stat().st_size


def test_a_receipt_with_the_right_hash_is_verified(served: Dict[str, Any]) -> None:
    sha = served["store"].manifest(served["id"])["zip"]["sha256"]
    fetch(served)

    response = served["client"].post(f"/v1/packages/{served['id']}/receipt",
                                     headers=bearer(READER), json={"zip_sha256": sha.upper()})

    assert response.status_code == 201 and response.json()["zip_sha256"] == sha
    received = events(served["ledger"], "received")
    assert len(received) == 1 and received[0]["verified"] is True


def test_a_receipt_with_another_hash_is_refused_and_booked(served: Dict[str, Any]) -> None:
    """
    The consumer holds other bytes than the package: it must not count as received -
    even after a complete download, so that it is the hash check that refuses.
    """
    fetch(served)
    response = served["client"].post(f"/v1/packages/{served['id']}/receipt",
                                     headers=bearer(READER), json={"zip_sha256": "0" * 64})

    assert response.status_code == 409 and "is not this package" in response.json()[
        "detail"]
    assert served["store"].receipt(served["id"]) is None
    refused = events(served["ledger"], "receipt_refused")
    assert len(refused) == 1 and refused[0]["reported_sha256"] == "0" * 64
    assert refused[0]["expected_sha256"] == served["store"].manifest(served["id"])[
        "zip"]["sha256"]
    assert events(served["ledger"], "received") == []


def test_a_receipt_without_a_hash_is_kept_unverified(served: Dict[str, Any]) -> None:
    fetch(served)
    served["client"].post(
        f"/v1/packages/{served['id']}/receipt", headers=bearer(READER))
    served["client"].post(
        f"/v1/packages/{served['id']}/receipt", headers=bearer(READER))

    received = events(served["ledger"], "received")
    assert len(received) == 1, "a repeated acknowledgement is not a second receipt"
    assert received[0]["verified"] is False and received[0]["zip_sha256"] is None


def test_the_ledger_route_needs_the_grant_and_filters(kraken: Dict[str, Any]) -> None:
    """A token entitled to one package sees that package's lines, and no other's."""
    tick_file(kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(
        2, 0), day(2, 23))
    mine = pack(kraken)["package_id"]
    tick_file(kraken["files"], "BTCUSD_20261110_000000_ticks.json", day(
        10, 0), day(10, 23))
    other = pack(kraken, cutoff=CUTOFF + WEEK)["package_id"]
    single = {"token": "s" * 40, "grants": [f"packages:{mine}"]}
    registry = load_package_tokens(
        {"fetcher": FETCHER, "single": single, "nobody": NOBODY})
    client = TestClient(create_package_api(kraken["store"], registry, lambda: {},
                                           Ledger(kraken["store"].root),
                                           kraken["packages_config"]))
    client.get(f"/v1/packages/{other}/zip", headers=bearer(FETCHER))

    assert client.get("/v1/packages/ledger",
                      headers=bearer(NOBODY)).status_code == 403
    everything = client.get("/v1/packages/ledger",
                            headers=bearer(FETCHER)).json()
    assert {e["package_id"] for e in everything["events"]} == {mine, other}
    assert client.get("/v1/packages/ledger", headers=bearer(FETCHER),
                      params={"event": "served"}).json()["count"] == 1
    narrow = client.get("/v1/packages/ledger", headers=bearer(single)).json()
    assert {e["package_id"] for e in narrow["events"]} == {mine}
    listed = client.get(
        "/v1/packages", headers=bearer(single)).json()["packages"]
    assert [p["package_id"] for p in listed] == [mine]


def test_bytes_are_counted_where_a_server_offers_to_send_the_path(tmp_path: Path) -> None:
    """With `pathsend` the body never passes through, and nothing would be counted."""
    import anyio

    path = tmp_path / "small.zip"
    path.write_bytes(os.urandom(100_000))
    outcomes: List[Dict[str, Any]] = []
    response = LedgeredFileResponse(path, media_type="application/zip",
                                    on_finish=outcomes.append)

    async def receive() -> Dict[str, Any]:
        await anyio.sleep_forever()

    async def send(message: Dict[str, Any]) -> None:
        assert message["type"] != "http.response.pathsend"

    scope = {"type": "http", "method": "GET", "headers": [],
             "extensions": {"http.response.pathsend": {}}}
    anyio.run(response, scope, receive, send)

    assert outcomes[0]["bytes_sent"] == 100_000 and outcomes[0]["complete"] is True


def test_a_torn_line_is_counted_and_the_next_event_starts_a_line_of_its_own(tmp_path: Path) -> None:
    """A crash mid-write leaves a line without its newline."""
    ledger = Ledger(tmp_path)
    ledger.append("packed", package_id="kraken-0001-20261109")
    with ledger.path.open("a", encoding="utf-8") as handle:
        handle.write('{"event": "served", "package_id": "kra')

    ledger.append("received", package_id="kraken-0001-20261109")

    book = ledger.read()
    assert book["unreadable_lines"] == 1
    assert [e["event"] for e in book["events"]] == ["packed", "received"]


# --------------------------------------------------------------------- the lifecycle


def awaiting(kraken: Dict[str, Any], package_id: str) -> Path:
    return kraken["originals"] / "awaiting" / "kraken" / package_id


def disposable(kraken: Dict[str, Any], package_id: str) -> Path:
    return kraken["originals"] / "disposable" / "kraken" / package_id


def receive(served: Dict[str, Any], token: Dict[str, Any] = READER, sha: Optional[str] = "",
            download: bool = True) -> Any:
    """Download the served package, then acknowledge it with its own hash unless told otherwise."""
    if download:
        fetch(served, token)
    body = {} if sha is None else {
        "zip_sha256": sha or served["store"].manifest(served["id"])["zip"]["sha256"]}
    return served["client"].post(f"/v1/packages/{served['id']}/receipt",
                                 headers=bearer(token), json=body)


def test_a_receipt_needs_a_complete_download_by_the_same_token(served: Dict[str, Any]) -> None:
    """The list publishes the hash; knowing it is not holding the bytes."""
    response = receive(served, download=False)

    assert response.status_code == 409 and "no complete download" in response.json()[
        "detail"]
    assert served["store"].receipt(served["id"]) is None
    refused = served["ledger"].read(event="receipt_refused")["events"]
    assert refused[0]["reason"] == "no complete download by this token"

    fetch(served, FETCHER)
    assert receive(served, download=False).status_code == 409, \
        "another token's download is not this token's"

    fetch(served)
    assert receive(served, download=False).status_code == 201


def test_a_resumed_download_counts_when_its_ranges_cover_the_zip(served: Dict[str, Any]) -> None:
    size = served["store"].find(served["id"])["zip"].stat().st_size

    fetch(served, byte_range="bytes=100-")
    assert receive(
        served, download=False).status_code == 409, "the first 100 bytes are missing"

    fetch(served, byte_range="bytes=0-99")
    assert receive(served, download=False).status_code == 201
    assert size > 100


def test_an_abandoned_download_and_its_resumption_count_together(served: Dict[str, Any]) -> None:
    """
    A resumed download's first attempt was abandoned by definition. It counts for
    the bytes it handed over; the rest comes from the resumption; the hash in the
    receipt proves the client holds them all.
    """
    size = served["store"].find(served["id"])["zip"].stat().st_size
    half = size // 2
    served["ledger"].append("served", package_id=served["id"], consumer="ide", range=None,
                            status=200, content_length=size, bytes_sent=half,
                            complete=False, disconnected=True)

    assert receive(
        served, download=False).status_code == 409, "half is not the whole"

    fetch(served, byte_range=f"bytes={half + 10}-")
    assert receive(
        served, download=False).status_code == 409, "ten bytes never left"

    fetch(served, byte_range=f"bytes={half}-")
    assert receive(served, download=False).status_code == 201


def test_stage_a_moves_the_packed_originals_out_and_keeps_two_copies(
        kraken: Dict[str, Any]) -> None:
    a = tick_file(
        kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(2, 0), day(2, 23))
    original = a.read_bytes()
    late = tick_file(kraken["files"], "DOTUSD_20261108_000000_ticks.json",
                     day(8, 20), CUTOFF + timedelta(minutes=30))

    result = pack(kraken, stage=True)

    moved = awaiting(kraken, result["package_id"]) / a.name
    assert result["staged"] == 1 and not a.exists()
    assert moved.read_bytes() == original
    assert zip_members(kraken["store"], result["package_id"])[
        a.name] == original
    assert late.exists(), "a file held for the next package stays where it is"
    staged = Ledger(kraken["store"].root).read(event="staged")["events"]
    assert staged[0]["moved"] == [a.name]


def test_a_zip_that_gives_back_other_bytes_is_no_package(
        kraken: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """The originals leave their folder on the strength of the self-check, so it must bite."""
    a = tick_file(
        kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(2, 0), day(2, 23))
    real_writestr = zipfile.ZipFile.writestr

    def corrupting(self: Any, info: Any, data: Any, *args: Any, **kwargs: Any) -> None:
        real_writestr(self, info, data[:-1] + b"X", *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "writestr", corrupting)

    result = pack(kraken, stage=True)

    assert result["result"] == "failed" and "other bytes" in result["reason"]
    assert kraken["store"].manifests("kraken") == []
    assert a.exists(), "nothing leaves the folder for a zip that failed its check"


def test_a_file_changed_since_packing_is_not_staged(kraken: Dict[str, Any]) -> None:
    a = tick_file(
        kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(2, 0), day(2, 23))
    package_id = pack(kraken)["package_id"]
    a.write_text(a.read_text(encoding="utf-8") + " ", encoding="utf-8")

    staged = lifecycle.stage_package("kraken", kraken["config"],
                                     kraken["store"].manifest(
                                         package_id), kraken["originals"],
                                     Ledger(kraken["store"].root))

    assert staged["moved"] == [
    ] and staged["skipped"][0]["reason"] == "changed since packing"
    assert a.exists()


def test_a_file_another_process_holds_stays_and_is_tried_again(
        kraken: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Windows refuses to move a file someone has open; the next settle tries again."""
    a = tick_file(
        kraken["files"], "BTCUSD_20261102_000000_ticks.json", day(2, 0), day(2, 23))
    real_replace = os.replace

    def busy(source: Any, target: Any) -> None:
        if Path(source) == a:
            raise PermissionError("in use by another process")
        real_replace(source, target)

    monkeypatch.setattr(lifecycle.os, "replace", busy)
    package_id = pack(kraken, stage=True)["package_id"]
    assert a.exists()

    monkeypatch.setattr(lifecycle.os, "replace", real_replace)
    lifecycle.settle(kraken["packages_config"],
                     kraken["store"], Ledger(kraken["store"].root))

    assert not a.exists() and (awaiting(kraken, package_id) / a.name).exists()


def test_a_verified_receipt_delivers_the_package_and_makes_its_originals_disposable(
        served: Dict[str, Any]) -> None:
    kraken, package_id = served["kraken"], served["id"]

    assert receive(served).status_code == 201

    paths = served["store"].find(package_id)
    assert paths["state"] == "delivered"
    assert {p.parent.name for p in (
        paths["zip"], paths["manifest"], paths["receipt"])} == {"delivered"}
    assert not awaiting(kraken, package_id).exists()
    assert len(list(disposable(kraken, package_id).iterdir())) == 2
    kinds = [e["event"]
             for e in served["ledger"].read(package_id=package_id)["events"]]
    assert kinds[-3:] == ["received", "delivered", "disposable"]
    listed = served["client"].get(
        "/v1/packages", headers=bearer(READER)).json()
    assert listed["packages"][0]["state"] == "delivered"


def test_a_receipt_without_the_hash_moves_nothing_on(served: Dict[str, Any]) -> None:
    """Received is not proven: the originals wait - also when the housekeeping comes by."""
    receive(served, sha=None)
    lifecycle.settle(served["config"], served["store"], served["ledger"])

    assert served["store"].find(served["id"])["state"] == "ready"
    assert awaiting(served["kraken"], served["id"]).exists()
    assert not disposable(served["kraken"], served["id"]).exists()
    created = datetime.fromisoformat(
        served["store"].manifest(served["id"])["created_at"])
    assert lifecycle.health(served["config"], served["store"],
                            now=created + timedelta(days=15))["reasons"] == ["receipt_overdue"]


def test_a_move_cut_short_is_finished_by_the_next_settle(served: Dict[str, Any]) -> None:
    """Zip moved, manifest not: the package is still found, and settle finishes the move."""
    store, package_id = served["store"], served["id"]
    receive(served, sha=None)
    paths = store.find(package_id)
    store.delivered("kraken").mkdir(parents=True, exist_ok=True)
    os.replace(paths["zip"], store.delivered("kraken") / paths["zip"].name)
    sha = store.manifest(package_id)["zip"]["sha256"]
    receipt = json.loads(paths["receipt"].read_text(encoding="utf-8"))
    paths["receipt"].write_text(json.dumps(
        {**receipt, "zip_sha256": sha}), encoding="utf-8")

    mid = store.find(package_id)
    assert mid["state"] == "ready" and mid["zip"].parent.name == "delivered"

    lifecycle.settle(served["config"], store, served["ledger"])

    assert store.find(package_id)["state"] == "delivered"
    assert disposable(served["kraken"], package_id).exists()


def fake_disk(monkeypatch: pytest.MonkeyPatch, free: int, total: int = 1000 * 10**6) -> None:
    class Usage:
        pass
    Usage.free, Usage.total = free, total
    monkeypatch.setattr(lifecycle.shutil, "disk_usage", lambda path: Usage)


def test_purge_by_default_stops_at_the_target_and_touches_nothing_above_it(
        served: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    receive(served)
    store, ledger, config = served["store"], served["ledger"], served["config"]

    fake_disk(monkeypatch, free=600 * 10**6)
    above = lifecycle.purge(config, store, ledger, apply=True)
    assert above["packages"] == [] and disposable(
        served["kraken"], served["id"]).exists()

    fake_disk(monkeypatch, free=100 * 10**6)
    dry = lifecycle.purge(config, store, ledger, apply=False)
    assert dry["packages"][0]["ok"] is True and dry["would_free_bytes"] > 0
    assert disposable(served["kraken"], served["id"]
                      ).exists(), "a dry run deletes nothing"

    done = lifecycle.purge(config, store, ledger, apply=True)
    assert done["packages"][0]["deleted"] is True
    assert not disposable(served["kraken"], served["id"]).exists()
    assert store.find(served["id"])["zip"].exists(), "our zip stays"
    purged = ledger.read(event="purged")["events"][0]
    assert {f["name"] for f in purged["files"]} == {
        "BTCUSD_20261102_000000_ticks.json", "ETHUSD_20261103_000000_ticks.json"}
    assert all(len(f["sha256"]) == 64 for f in purged["files"])


def test_a_full_purge_ignores_the_target(served: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    receive(served)
    fake_disk(monkeypatch, free=900 * 10**6)

    done = lifecycle.purge(served["config"], served["store"], served["ledger"],
                           apply=True, full=True)

    assert done["packages"][0]["deleted"] is True


@pytest.mark.parametrize("damage, expected", [
    ("extra", "does not name"),
    ("missing", "are not here"),
    ("size", "bytes, the manifest says"),
    ("content", "not the manifest's SHA-256"),
    ("zip", "our zip no longer"),
])
def test_purge_proves_every_file_again_and_keeps_a_package_it_cannot(
        served: Dict[str, Any], monkeypatch: pytest.MonkeyPatch, damage: str, expected: str) -> None:
    receive(served)
    folder = disposable(served["kraken"], served["id"])
    first = sorted(folder.iterdir())[0]
    if damage == "extra":
        (folder / "stray.json").write_text("{}")
    elif damage == "missing":
        first.unlink()
    elif damage == "size":
        first.write_bytes(first.read_bytes() + b" ")
    elif damage == "content":
        data = bytearray(first.read_bytes())
        data[10] = (data[10] + 1) % 256
        first.write_bytes(bytes(data))
    elif damage == "zip":
        zip_path = served["store"].find(served["id"])["zip"]
        zip_path.write_bytes(zip_path.read_bytes() + b"\0")

    done = lifecycle.purge(served["config"], served["store"], served["ledger"],
                           apply=True, full=True)

    verdict = done["packages"][0]
    assert verdict["ok"] is False and verdict["deleted"] is False
    assert any(expected in problem for problem in verdict["problems"])
    assert folder.exists()


def test_a_package_put_into_disposable_without_a_verified_receipt_is_kept(
        served: Dict[str, Any]) -> None:
    """Only a receipt with the zip's hash makes originals disposable - moving them by hand does not."""
    source = awaiting(served["kraken"], served["id"])
    target = disposable(served["kraken"], served["id"])
    target.parent.mkdir(parents=True)
    os.replace(source, target)

    done = lifecycle.purge(served["config"], served["store"], served["ledger"],
                           apply=True, full=True)

    assert "no receipt with the zip's hash" in done["packages"][0]["problems"]
    assert target.exists()


def test_remote_purge_is_its_own_grant_and_never_full(
        served: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    client = served["client"]
    receive(served)
    fake_disk(monkeypatch, free=100 * 10**6)

    assert client.get("/v1/packages/purge",
                      headers=bearer(READER)).status_code == 403
    assert client.post("/v1/packages/purge",
                       headers=bearer(READER)).status_code == 403
    preview = client.get("/v1/packages/purge", headers=bearer(OPERATOR)).json()
    assert preview["applied"] is False and preview["packages"][0]["ok"] is True
    assert client.post("/v1/packages/purge", headers=bearer(OPERATOR),
                       json={"target_free_percent": 200}).status_code == 400
    done = client.post("/v1/packages/purge", headers=bearer(OPERATOR),
                       json={"target_free_percent": 5}).json()
    assert done["packages"] == [], "10 % free is above a 5 % target"
    done = client.post("/v1/packages/purge", headers=bearer(OPERATOR)).json()
    assert done["full"] is False and done["packages"][0]["deleted"] is True
    assert served["ledger"].read(event="purged")[
        "events"][0]["by"] == "operator"


def test_health_names_each_reason(served: Dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    store, config = served["store"], served["config"]
    created = datetime.fromisoformat(
        store.manifest(served["id"])["created_at"])

    assert lifecycle.health(config, store, now=created +
                            timedelta(days=1))["status"] == "ok"
    late = lifecycle.health(config, store, now=created + timedelta(days=15))
    assert late["reasons"] == [
        "receipt_overdue"] and late["overdue"] == [served["id"]]

    receive(served)
    assert lifecycle.health(config, store, now=created +
                            timedelta(days=15))["status"] == "ok"

    tight = config.model_copy(
        update={"disposable_warn_gb": 0.000001, "min_free_percent": 50.0})
    fake_disk(monkeypatch, free=100 * 10**6)
    assert lifecycle.health(tight, store)["reasons"] == [
        "disk_low", "disposable_large"]

    store.write_last_run(
        "kraken", {"result": "refused", "reason": "no instance.json"})
    assert "last_run_refused" in lifecycle.health(config, store)["reasons"]


def test_the_operator_is_warned_once_a_day_per_reason(tmp_path: Path) -> None:
    import asyncio

    from python.packages.package_service import PackageService

    service = PackageService(PackagesConfig(output_dir=str(tmp_path / "p"),
                                            originals_dir=str(tmp_path / "o")), {})
    sent: List[str] = []

    class Telegram:
        async def send_alert(self, alert: Any) -> bool:
            sent.append(alert.title)
            return True

    service._telegram = Telegram()
    degraded = {"status": "degraded", "reasons": ["disk_low"], "overdue": []}

    asyncio.run(service._warn(degraded))
    asyncio.run(service._warn(degraded))
    assert sent == ["Tick packages: disk_low"]

    asyncio.run(service._warn({"status": "ok", "reasons": [], "overdue": []}))
    asyncio.run(service._warn(degraded))
    assert len(sent) == 2, "a reason that cleared and came back is news again"


# --------------------------------------------------------------------- the pickup client


def take(served: Dict[str, Any], token: Dict[str, Any], out: Path, **kwargs: Any) -> Dict[str, Any]:
    """Run the pickup client against the served API, as a consumer would."""
    results = pickup("http://testserver", token["token"], out, session=served["client"],
                     log=lambda line: None, **kwargs)
    assert len(results) == 1
    return results[0]


def test_the_pickup_client_rehearses_with_a_dev_token(served: Dict[str, Any], tmp_path: Path) -> None:
    """Every check runs on both sides; nothing is marked, nothing moves, the real receipt stays open."""
    result = take(served, OPERATOR, tmp_path / "pickup")

    assert result["ok"] is True and result["files"] == 2
    assert result["receipt_status"] == 200 and result["receipt"]["dev"] is True
    assert served["store"].receipt(served["id"]) is None
    assert served["store"].find(served["id"])["state"] == "ready"
    assert awaiting(served["kraken"], served["id"]).exists()
    assert len(served["ledger"].read(event="dev_received")["events"]) == 1
    assert served["ledger"].read(event="received")["events"] == []

    real = take(served, READER, tmp_path / "ide")
    assert real["receipt_status"] == 201
    assert served["store"].find(served["id"])["state"] == "delivered"


def test_a_dev_receipt_still_needs_the_download_and_the_hash(served: Dict[str, Any]) -> None:
    assert receive(served, OPERATOR, download=False).status_code == 409
    assert receive(served, OPERATOR, sha="0" * 64).status_code == 409
    assert receive(served, OPERATOR).status_code == 200


def test_a_dev_receipt_stands_on_nothing_not_even_a_real_one(
        served: Dict[str, Any], tmp_path: Path) -> None:
    """A repeated real receipt passes on its original; a rehearsal is checked every time."""
    assert receive(served).status_code == 201
    other = tmp_path / "fresh-ledger"
    served["ledger"]._path = other / "ledger.jsonl"
    served["ledger"]._lock_path = other / ".ledger.lock"

    assert receive(served, OPERATOR, download=False).status_code == 409


def test_the_client_downloads_through_a_requests_session(served: Dict[str, Any], tmp_path: Path) -> None:
    """
    The client runs on `requests` outside the tests, and a requests.Session carries
    a `stream` attribute that is a flag, not a method. Found by the first run against
    production; the tests had only ever driven the httpx client.
    """
    import requests

    from python.packages.pickup import download

    whole = served["store"].find(served["id"])["zip"].read_bytes()
    client = served["client"]

    class Response:
        def __init__(self, status: int, body: bytes) -> None:
            self.status_code, self._body = status, body

        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *exc: Any) -> None:
            pass

        def iter_content(self, size: int) -> Any:
            for at in range(0, len(self._body), size):
                yield self._body[at:at + size]

    class Session(requests.Session):
        def get(self, url: str, **kwargs: Any) -> Any:
            answer = client.get(url.replace("http://testserver", ""),
                                headers=kwargs.get("headers"))
            return Response(answer.status_code, answer.content)

    target = tmp_path / "x.zip"
    download(Session(), f"http://testserver/v1/packages/{served['id']}/zip",
             bearer(READER), target, len(whole))

    assert target.read_bytes() == whole


def test_the_client_checks_every_file_not_only_the_zip(served: Dict[str, Any], tmp_path: Path) -> None:
    """A zip whose own hash is right but whose manifest names other bytes is not taken."""
    from python.packages.pickup import PickupError, verify

    manifest = json.loads(json.dumps(served["store"].manifest(served["id"])))
    manifest["members"][0]["sha256"] = "0" * 64

    with pytest.raises(PickupError, match="not the manifest's SHA-256"):
        verify(served["store"].find(served["id"])["zip"], manifest)


def test_the_pickup_client_takes_a_package_for_real(served: Dict[str, Any], tmp_path: Path) -> None:
    result = take(served, READER, tmp_path / "ide")

    kept = tmp_path / "ide" / \
        served["store"].manifest(served["id"])["zip"]["name"]
    assert result["ok"] is True and result["receipt_status"] == 201
    assert hashlib.sha256(
        kept.read_bytes()).hexdigest() == result["zip_sha256"]
    assert disposable(served["kraken"], served["id"]).exists()


def test_the_pickup_client_resumes_a_cut_download(served: Dict[str, Any], tmp_path: Path) -> None:
    """The first attempt left half the zip; the next run asks for the rest and is accepted."""
    whole = served["store"].find(served["id"])["zip"].read_bytes()
    name = served["store"].manifest(served["id"])["zip"]["name"]
    out = tmp_path / "ide"
    out.mkdir()
    half = len(whole) // 2
    (out / f"{name}.part").write_bytes(whole[:half])
    served["ledger"].append("served", package_id=served["id"], consumer="ide", range=None,
                            status=200, content_length=len(whole), bytes_sent=half,
                            complete=False, disconnected=True)

    result = take(served, READER, out)

    assert result["resumed_from"] == half
    assert result["ok"] is True and result["receipt_status"] == 201
    assert (out / name).read_bytes() == whole
    asked = served["ledger"].read(event="served")["events"][-1]
    assert asked["range"] == f"bytes={half}-" and asked["status"] == 206


def test_the_pickup_client_does_not_acknowledge_what_does_not_match(
        served: Dict[str, Any], tmp_path: Path) -> None:
    """A part file that was not this package's beginning makes a zip with another hash."""
    whole = served["store"].find(served["id"])["zip"].read_bytes()
    name = served["store"].manifest(served["id"])["zip"]["name"]
    out = tmp_path / "ide"
    out.mkdir()
    (out / f"{name}.part").write_bytes(b"\0" * (len(whole) // 2))

    result = take(served, READER, out)

    assert result["ok"] is False and "SHA-256" in result["error"]
    assert "receipt_status" not in result
    assert served["store"].receipt(served["id"]) is None


# --------------------------------------------------------------------- the weekly report


SPACE = {"awaiting": {"bytes": 300_000_000}, "disposable": {"bytes": 2_860_000_000},
         "disk": {"free_percent": 60.8}}


def test_the_weekly_report_says_what_each_run_came_to() -> None:
    from python.alerts.base import AlertLevel
    from python.packages.package_service import run_report

    report = run_report([
        {"source": "kraken", "result": "packed", "package_id": "kraken-0002-20261108",
         "member_count": 22, "zip_size_bytes": 18_300_000, "staged": 22, "not_staged": [],
         "held_for_next": 1},
        {"source": "mt5", "result": "nothing_new"},
    ], SPACE)

    assert report.level == AlertLevel.INFO
    assert "kraken-0002-20261108: 22 files, 18.3 MB zip, 22 originals to awaiting, " \
           "1 held for next week" in report.message
    assert "mt5: nothing new" in report.message
    assert "disk 60.8 % free" in report.message


def test_a_run_that_went_wrong_is_reported_as_a_warning_with_its_reason() -> None:
    from python.alerts.base import AlertLevel
    from python.packages.package_service import run_report

    report = run_report([
        {"source": "kraken", "result": "packed", "package_id": "kraken-0002-20261108",
         "member_count": 22, "zip_size_bytes": 18_300_000, "staged": 21,
         "not_staged": [{"name": "x", "reason": "in use"}], "held_for_next": 0},
        {"source": "mt5", "result": "refused",
            "reason": "no instance.json in the folder"},
    ], SPACE)

    assert report.level == AlertLevel.WARNING
    assert "1 NOT moved" in report.message
    assert "mt5: refused - no instance.json in the folder" in report.message


def test_the_service_reports_every_run_and_says_so_when_there_was_none(tmp_path: Path) -> None:
    """Every week, not only on failure: silence is never the only sign of a run."""
    import asyncio

    from python.packages.package_service import PackageService

    service = PackageService(PackagesConfig(output_dir=str(tmp_path / "p"),
                                            originals_dir=str(tmp_path / "o")), {})
    assert asyncio.run(service.report(
        [{"source": "mt5", "result": "nothing_new"}])) is False

    sent: List[Any] = []

    class Telegram:
        async def send_alert(self, alert: Any) -> bool:
            sent.append(alert)
            return True

    service._telegram = Telegram()
    assert asyncio.run(service.report(
        [{"source": "mt5", "result": "nothing_new"}])) is True
    assert asyncio.run(service.report([])) is True

    assert "mt5: nothing new" in sent[0].message
    assert "no result at all" in sent[1].message
