"""
FiniexDataCollector - Package Lifecycle
Where the original tick files go after packing, and when they may go for good.

    <originals_dir>/awaiting/<collector>/<package_id>/     stage A: packed and checked by
                                                           us, waiting for the consumer
    <originals_dir>/disposable/<collector>/<package_id>/   stage B: the consumer confirmed
                                                           the zip's hash; may be purged

Stage A happens right after a package is committed: the zip was read back and
every member matched, so the originals leave the collector's folder. They are
moved, never copied, and never deleted here - until the consumer confirms, the zip
and the originals are two copies on our side.

Stage B happens when the consumer acknowledges a package with the zip's SHA-256:
the package moves from `ready/` to `delivered/`, and its originals from `awaiting/`
to `disposable/`. Its bytes then exist on two machines.

`purge` is the only thing that deletes, and only from `disposable/`, and only a
package it can prove again on the spot: the receipt verified, our zip intact, and
every file present, none extra, each with the manifest's name, size and SHA-256.

`settle` brings the folders in line with what the manifests and receipts say. It
runs after every packing run, after a receipt, every few minutes in the service
and at its start - so a move a crash cut short is finished by the next one.

Location: python/packages/lifecycle.py
"""

import hashlib
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from python.packages.ledger import Ledger
from python.packages.package_store import (READY, PackageStore, PackLock,
                                           PackLockHeld)
from python.utils.config_loader import PackagesConfig, PackageSourceConfig
from python.utils.logging_setup import describe_exception

AWAITING = "awaiting"
DISPOSABLE = "disposable"
HASH_BLOCK = 1024 * 1024


def sha256_of(path: Path) -> str:
    """A file's SHA-256, read in blocks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(HASH_BLOCK), b""):
            digest.update(block)
    return digest.hexdigest()


def files_dir(config: PackageSourceConfig) -> Path:
    """The folder a source's tick files lie in."""
    root = Path(config.root)
    return root / config.subdir if config.subdir else root


def verified(manifest: Dict[str, Any], receipt: Optional[Dict[str, Any]]) -> bool:
    """A receipt that names the zip's own hash - "these exact bytes"."""
    return bool(receipt) and receipt.get("zip_sha256") == manifest["zip"]["sha256"]


def _book(ledger: Ledger, event: str, **fields: Any) -> None:
    """A record, not a gate."""
    try:
        ledger.append(event, **fields)
    except OSError:
        pass


def stage_package(name: str, config: PackageSourceConfig, manifest: Dict[str, Any],
                  originals_root: Path, ledger: Ledger) -> Dict[str, Any]:
    """
    Stage A: move a committed package's originals out of the collector's folder.

    Only a file that is still the bytes the manifest describes moves; one that
    changed since packing stays and is named. A file not in the folder any more
    is not looked for - it was moved already, or someone took it, and `purge`
    would find either.

    Returns:
        `moved` names and `skipped` entries with their reason
    """
    package_id = manifest["package_id"]
    target = originals_root / AWAITING / name / package_id
    source = files_dir(config)
    moved: List[str] = []
    skipped: List[Dict[str, str]] = []
    for member in manifest["members"]:
        path = source / member["name"]
        if not path.exists():
            continue
        try:
            if sha256_of(path) != member["sha256"]:
                skipped.append(
                    {"name": member["name"], "reason": "changed since packing"})
                continue
            target.mkdir(parents=True, exist_ok=True)
            if (target / member["name"]).exists():
                skipped.append(
                    {"name": member["name"], "reason": "already in awaiting"})
                continue
            os.replace(path, target / member["name"])
            moved.append(member["name"])
        except OSError as e:
            # Windows refuses to move a file another process holds open. It stays,
            # and the next settle tries again.
            skipped.append(
                {"name": member["name"], "reason": describe_exception(e)})
    if moved or skipped:
        _book(ledger, "staged", package_id=package_id, collector=name,
              moved=moved, skipped=skipped)
    return {"moved": moved, "skipped": skipped}


def promote(name: str, package_id: str, originals_root: Path, ledger: Ledger) -> List[str]:
    """
    Stage B: a confirmed package's originals move from `awaiting/` to `disposable/`.

    File by file, so that a move cut short is finished by the next call.

    Returns:
        The names moved
    """
    source = originals_root / AWAITING / name / package_id
    if not source.exists():
        return []
    target = originals_root / DISPOSABLE / name / package_id
    target.mkdir(parents=True, exist_ok=True)
    moved = []
    for path in sorted(source.iterdir()):
        os.replace(path, target / path.name)
        moved.append(path.name)
    try:
        source.rmdir()
    except OSError:
        pass
    if moved:
        _book(ledger, "disposable", package_id=package_id,
              collector=name, files=moved)
    return moved


def settle_source(name: str, config: PackageSourceConfig, store: PackageStore,
                  originals_root: Path, ledger: Ledger,
                  log: Callable[[str], None] = lambda message: None) -> Dict[str, Any]:
    """
    Bring one collector's folders in line with its manifests and receipts.

    Takes the collector's packing lock; a collector whose lock is held is left to
    the run that holds it.

    Returns:
        What it did: `staged`, `delivered`, `disposable`, or `skipped` with the reason
    """
    done: Dict[str, Any] = {"staged": {}, "delivered": [], "disposable": {}}
    try:
        with PackLock(store.folder(name)):
            source = files_dir(config)
            for manifest in store.manifests(name):
                package_id = manifest["package_id"]
                paths = store.find(package_id)
                if paths is None:
                    continue
                if paths["state"] == READY and any(
                        (source / m["name"]).exists() for m in manifest["members"]):
                    staged = stage_package(
                        name, config, manifest, originals_root, ledger)
                    if staged["moved"]:
                        done["staged"][package_id] = len(staged["moved"])
                if not verified(manifest, store.receipt(package_id)):
                    continue
                if store.deliver(package_id):
                    _book(ledger, "delivered",
                          package_id=package_id, collector=name)
                    done["delivered"].append(package_id)
                moved = promote(name, package_id, originals_root, ledger)
                if moved:
                    done["disposable"][package_id] = len(moved)
    except PackLockHeld as e:
        return {"skipped": str(e)}
    except OSError as e:
        log(f"[LIFECYCLE] {name}: settle stopped: {describe_exception(e)}")
        done["error"] = describe_exception(e)
    if done["staged"] or done["delivered"] or done["disposable"]:
        log(f"[LIFECYCLE] {name}: staged {done['staged']}, delivered {done['delivered']}, "
            f"disposable {done['disposable']}")
    return done


def settle(config: PackagesConfig, store: PackageStore, ledger: Ledger,
           log: Callable[[str], None] = lambda message: None) -> Dict[str, Any]:
    """`settle_source` for every configured collector."""
    root = Path(config.originals_dir)
    return {name: settle_source(name, source, store, root, ledger, log)
            for name, source in config.sources.items()}


# ---------------------------------------------------------------------- storage


def _folder_size(folder: Path) -> Dict[str, int]:
    """Bytes, files and package folders below one stage folder."""
    total = files = packages = 0
    if folder.exists():
        for collector in folder.iterdir():
            if not collector.is_dir():
                continue
            for package in collector.iterdir():
                if not package.is_dir():
                    continue
                packages += 1
                for path in package.iterdir():
                    if path.is_file():
                        files += 1
                        total += path.stat().st_size
    return {"bytes": total, "files": files, "packages": packages}


def storage(config: PackagesConfig) -> Dict[str, Any]:
    """What the originals take up, per stage, and how full the disk is."""
    root = Path(config.originals_dir)
    usage_at = root if root.exists() else Path(config.output_dir)
    try:
        usage = shutil.disk_usage(usage_at if usage_at.exists() else Path("."))
        disk = {"free_bytes": usage.free, "total_bytes": usage.total,
                "free_percent": round(usage.free / usage.total * 100, 1)}
    except OSError:
        disk = {"free_bytes": None, "total_bytes": None, "free_percent": None}
    return {"awaiting": _folder_size(root / AWAITING),
            "disposable": _folder_size(root / DISPOSABLE),
            "disk": disk}


def health(config: PackagesConfig, store: PackageStore,
           now: Optional[datetime] = None) -> Dict[str, Any]:
    """
    The service's verdict, with reasons as codes.

    `disk_low`          less than `min_free_percent` of the disk is free
    `receipt_overdue`   a package has waited longer than `receipt_overdue_days`
                        for a verified receipt
    `disposable_large`  confirmed originals exceed `disposable_warn_gb`: purge
    `last_run_failed`   a collector's latest run failed
    `last_run_refused`  a collector's latest run was refused - a missing mint, an
                        unwritable folder, too little disk

    Returns:
        `status` (`ok` or `degraded`) and `reasons`; `overdue` names the packages
    """
    now = now or datetime.now(timezone.utc)
    reasons: List[str] = []
    space = storage(config)
    free = space["disk"]["free_percent"]
    if free is not None and free < config.min_free_percent:
        reasons.append("disk_low")
    overdue = []
    for manifest in store.manifests():
        paths = store.find(manifest["package_id"])
        if paths is None or paths["state"] != READY:
            continue
        created = datetime.fromisoformat(manifest["created_at"])
        if (now - created > timedelta(days=config.receipt_overdue_days)
                and not verified(manifest, store.receipt(manifest["package_id"]))):
            overdue.append(manifest["package_id"])
    if overdue:
        reasons.append("receipt_overdue")
    if space["disposable"]["bytes"] > config.disposable_warn_gb * 1e9:
        reasons.append("disposable_large")
    results = {run.get("result") for name, run in store.last_runs().items()
               if name in config.sources}
    if "failed" in results:
        reasons.append("last_run_failed")
    if "refused" in results:
        reasons.append("last_run_refused")
    return {"status": "degraded" if reasons else "ok", "reasons": reasons, "overdue": overdue}


# ---------------------------------------------------------------------- purge


@dataclass
class PurgeVerdict:
    """What `purge` found for one package in `disposable/`."""
    package_id: str
    collector: str
    files: int = 0
    bytes: int = 0
    received_at: str = ""
    problems: List[str] = field(default_factory=list)
    deleted: bool = False

    def summary(self) -> Dict[str, Any]:
        return {"package_id": self.package_id, "collector": self.collector,
                "files": self.files, "bytes": self.bytes, "received_at": self.received_at,
                "ok": not self.problems, "problems": self.problems, "deleted": self.deleted}


def _verify_for_purge(collector: str, folder: Path, store: PackageStore) -> PurgeVerdict:
    """Prove again, on the spot, that a package's originals may go."""
    verdict = PurgeVerdict(package_id=folder.name, collector=collector)
    manifest = store.manifest(folder.name)
    if manifest is None:
        verdict.problems.append("no committed package of this name")
        return verdict
    receipt = store.receipt(folder.name)
    if not verified(manifest, receipt):
        verdict.problems.append("no receipt with the zip's hash")
    else:
        verdict.received_at = receipt["received_at"]
    paths = store.find(folder.name)
    if paths is None or not paths["zip"].exists():
        verdict.problems.append("our zip is missing")
    elif sha256_of(paths["zip"]) != manifest["zip"]["sha256"]:
        verdict.problems.append("our zip no longer has the manifest's hash")

    members = {m["name"]: m for m in manifest["members"]}
    present = {p.name: p for p in folder.iterdir()}
    extra = sorted(set(present) - set(members))
    if extra:
        verdict.problems.append(
            f"files the manifest does not name: {', '.join(extra)}")
    missing = sorted(set(members) - set(present))
    if missing:
        verdict.problems.append(f"{len(missing)} of {len(members)} files are not here, "
                                f"e.g. {missing[0]}")
    for name, path in sorted(present.items()):
        member = members.get(name)
        if member is None or not path.is_file():
            continue
        size = path.stat().st_size
        verdict.files += 1
        verdict.bytes += size
        if size != member["size_bytes"]:
            verdict.problems.append(
                f"{name}: {size} bytes, the manifest says {member['size_bytes']}")
        elif sha256_of(path) != member["sha256"]:
            verdict.problems.append(f"{name}: not the manifest's SHA-256")
    return verdict


def purge(config: PackagesConfig, store: PackageStore, ledger: Ledger, apply: bool = False,
          full: bool = False, target_free_percent: Optional[float] = None,
          by: str = "operator") -> Dict[str, Any]:
    """
    Delete confirmed originals from `disposable/`, oldest receipt first.

    Without `full`, only until `target_free_percent` of the disk is free - nothing
    at all when it already is. Without `apply`, nothing is deleted and the answer
    says what would be.

    Returns:
        The disk before, the target, and a verdict per package considered
    """
    root = Path(config.originals_dir)
    target = config.purge_target_free_percent if target_free_percent is None else target_free_percent
    before = storage(config)["disk"]
    total, free = before["total_bytes"], before["free_bytes"]

    candidates = []
    base = root / DISPOSABLE
    if base.exists():
        for collector in sorted(p for p in base.iterdir() if p.is_dir()):
            for folder in sorted(p for p in collector.iterdir() if p.is_dir()):
                receipt = store.receipt(folder.name) or {}
                candidates.append(
                    (receipt.get("received_at", ""), collector.name, folder))
    candidates.sort()

    verdicts: List[Dict[str, Any]] = []
    freed = 0
    for _, collector, folder in candidates:
        if not full and total and (free + freed) / total * 100 >= target:
            break
        try:
            with PackLock(store.folder(collector)):
                verdict = _verify_for_purge(collector, folder, store)
                if not verdict.problems:
                    freed += verdict.bytes
                    if apply:
                        # Verified a moment ago: what is here is what the manifest
                        # names, so the manifest's figures are the record.
                        files = [{"name": m["name"], "size_bytes": m["size_bytes"],
                                  "sha256": m["sha256"]}
                                 for m in store.manifest(folder.name)["members"]]
                        for path in folder.iterdir():
                            path.unlink()
                        folder.rmdir()
                        verdict.deleted = True
                        _book(ledger, "purged", package_id=folder.name, collector=collector,
                              by=by, files=files, bytes=verdict.bytes)
        except PackLockHeld as e:
            verdict = PurgeVerdict(package_id=folder.name, collector=collector,
                                   problems=[str(e)])
        verdicts.append(verdict.summary())
    return {"applied": apply, "full": full, "target_free_percent": None if full else target,
            "disk_before": before, "freed_bytes": freed if apply else 0,
            "would_free_bytes": 0 if apply else freed, "packages": verdicts}
