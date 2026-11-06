"""
FiniexDataCollector - Package Store
Where packages live, and the only record of which file went into which.

One folder per collector under the output directory:

    <output_dir>/<collector>/ready/<id>.zip                waiting for the consumer
                                  /<id>.manifest.json      the package's commit point
                            /delivered/<id>.zip            confirmed with the right hash
                                      /<id>.manifest.json
                                      /<id>.receipt.json   the consumer's acknowledgement
                            /last_run.json                 what the latest run did, and why
                            /.pack.lock

A package moves from `ready/` to `delivered/` once - zip first, manifest last, so
that a move cut short is found and finished from either side.

**The manifests are the ledger.** A file is packed when a manifest names it, and
nowhere else is that recorded - so the record cannot disagree with the packages it
describes. A zip without a manifest is a run that died before it finished; it is
not a package, and the next run removes it and builds again.

**Nothing listed ever changes.** A zip and its manifest are written under a
temporary name and renamed into place, and nothing rewrites either afterwards. A
receipt is a file of its own for exactly that reason.

Location: python/packages/package_store.py
"""

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import psutil

PACKAGE_ID = re.compile(
    r"^(?P<collector>[a-z0-9]+)-(?P<seq>\d{4,})-(?P<cutoff>\d{8})$")

MANIFEST_SUFFIX = ".manifest.json"
RECEIPT_SUFFIX = ".receipt.json"
ZIP_SUFFIX = ".zip"
PART_SUFFIX = ".part"
LAST_RUN = "last_run.json"
LOCK_NAME = ".pack.lock"
READY = "ready"
DELIVERED = "delivered"

# psutil's creation time crosses a JSON round trip; see instance_lock.py for why the
# window is this narrow.
CREATE_TIME_TOLERANCE_S = 0.01


class PackLockHeld(Exception):
    """Another live process is packing this collector."""


def write_atomically(path: Path, data: bytes) -> None:
    """
    Write a file so that its name never exists before its content is complete.

    Args:
        path: The final path
        data: The whole content
    """
    temporary = path.with_name(path.name + PART_SUFFIX)
    with temporary.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_json_atomically(path: Path, document: Dict[str, Any]) -> None:
    """Write a JSON document atomically, indented for a reader."""
    write_atomically(
        path, (json.dumps(document, indent=2) + "\n").encode("utf-8"))


class PackLock:
    """
    One packer per collector folder, held for the length of a run.

    The service packs on its schedule and an operator may run `pack` by hand; two
    runs at once would both take the same unpacked files. The lock is a file
    naming the process and its creation time, so a lock left behind by a crash is
    recognised as stale rather than refusing every run after it.
    """

    def __init__(self, folder: Path):
        """
        Args:
            folder: The collector's package folder
        """
        self._path = Path(folder) / LOCK_NAME
        self._held = False

    def __enter__(self) -> "PackLock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        holder = self._live_holder()
        if holder is not None:
            raise PackLockHeld(
                f"another packer (pid {holder}) holds {self._path}")
        record = {"pid": os.getpid(), "create_time": psutil.Process().create_time()}
        self._path.write_text(json.dumps(record), encoding="utf-8")
        self._held = True
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._held:
            try:
                self._path.unlink()
            except FileNotFoundError:
                pass
            self._held = False

    def _live_holder(self) -> Optional[int]:
        """The pid holding the lock, or None when it is free or stale."""
        try:
            record = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        pid, created = record.get("pid"), record.get("create_time")
        if not isinstance(pid, int) or not isinstance(created, (int, float)):
            return None
        if pid == os.getpid():
            return None
        try:
            if abs(psutil.Process(pid).create_time() - created) <= CREATE_TIME_TOLERANCE_S:
                return pid
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        return None


class PackageStore:
    """
    Reads and writes the package folder.

    Manifests are read on every listing, so each is cached against its size and
    modification time - a year of weekly packages is a hundred files, and a list
    request should not parse all of them every time.
    """

    def __init__(self, output_dir: Path):
        """
        Args:
            output_dir: The packages' root folder
        """
        self._root = Path(output_dir)
        self._cache: Dict[Path, Tuple[Tuple[int, int], Dict[str, Any]]] = {}

    @property
    def root(self) -> Path:
        """The packages' root folder."""
        return self._root

    def folder(self, collector: str) -> Path:
        """The package folder of one collector."""
        return self._root / collector

    def ready(self, collector: str) -> Path:
        """Where a collector's packages wait for the consumer."""
        return self.folder(collector) / READY

    def delivered(self, collector: str) -> Path:
        """Where a collector's confirmed packages are kept."""
        return self.folder(collector) / DELIVERED

    # ------------------------------------------------------------------ reading

    def manifests(self, collector: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Every committed package, oldest first.

        Args:
            collector: Restrict to one collector

        Returns:
            The manifests, ordered by collector and sequence
        """
        collectors = [self.folder(collector)] if collector else (
            sorted(p for p in self._root.iterdir() if p.is_dir()) if self._root.exists() else [])
        found: Dict[str, Dict[str, Any]] = {}
        for folder in collectors:
            for state in (READY, DELIVERED):
                if not (folder / state).exists():
                    continue
                for path in (folder / state).glob(f"*{MANIFEST_SUFFIX}"):
                    if PACKAGE_ID.match(path.name[:-len(MANIFEST_SUFFIX)]):
                        manifest = self._read_manifest(path)
                        found[manifest["package_id"]] = manifest
        return sorted(found.values(), key=lambda m: (m["collector"], m["sequence"]))

    def _read_manifest(self, path: Path) -> Dict[str, Any]:
        """One manifest, from the cache while the file is unchanged."""
        stat = path.stat()
        key = (stat.st_size, stat.st_mtime_ns)
        cached = self._cache.get(path)
        if cached is not None and cached[0] == key:
            return cached[1]
        document = json.loads(path.read_text(encoding="utf-8"))
        self._cache[path] = (key, document)
        return document

    def packed_names(self, collector: str) -> Set[str]:
        """Every file name any package of this collector holds."""
        return {member["name"]
                for manifest in self.manifests(collector)
                for member in manifest["members"]}

    def find(self, package_id: str) -> Optional[Dict[str, Path]]:
        """
        The files of one committed package.

        Args:
            package_id: e.g. `kraken-0003-20261116`

        Returns:
            Paths of its zip, manifest and receipt, and its state (`ready` or
            `delivered`, wherever the manifest is), or None when there is no such
            committed package - including any id that is not one, so a crafted
            name never becomes a path. Mid-move, a zip or receipt already on the
            other side is found there.
        """
        match = PACKAGE_ID.match(package_id or "")
        if match is None:
            return None
        collector = match["collector"]
        folders = {READY: self.ready(
            collector), DELIVERED: self.delivered(collector)}
        for state, folder in folders.items():
            if not (folder / f"{package_id}{MANIFEST_SUFFIX}").exists():
                continue
            other = folders[DELIVERED if state == READY else READY]
            paths: Dict[str, Any] = {"state": state,
                                     "manifest": folder / f"{package_id}{MANIFEST_SUFFIX}"}
            for key, suffix in (("zip", ZIP_SUFFIX), ("receipt", RECEIPT_SUFFIX)):
                here, there = folder / \
                    f"{package_id}{suffix}", other / f"{package_id}{suffix}"
                paths[key] = there if not here.exists(
                ) and there.exists() else here
            if not paths["zip"].exists():
                return None
            return paths
        return None

    def deliver(self, package_id: str) -> bool:
        """
        Move a package from `ready/` to `delivered/`: zip, receipt, manifest last.

        Idempotent, and it finishes a move a crash cut short.

        Returns:
            True when anything moved
        """
        match = PACKAGE_ID.match(package_id or "")
        if match is None:
            return False
        source, target = self.ready(
            match["collector"]), self.delivered(match["collector"])
        target.mkdir(parents=True, exist_ok=True)
        moved = False
        for suffix in (ZIP_SUFFIX, RECEIPT_SUFFIX, MANIFEST_SUFFIX):
            path = source / f"{package_id}{suffix}"
            if path.exists():
                os.replace(path, target / path.name)
                moved = True
        return moved

    def manifest(self, package_id: str) -> Optional[Dict[str, Any]]:
        """The manifest of one committed package, or None."""
        paths = self.find(package_id)
        return self._read_manifest(paths["manifest"]) if paths else None

    def receipt(self, package_id: str) -> Optional[Dict[str, Any]]:
        """The consumer's acknowledgement of a package, or None."""
        paths = self.find(package_id)
        if paths is None or not paths["receipt"].exists():
            return None
        return json.loads(paths["receipt"].read_text(encoding="utf-8"))

    def last_runs(self) -> Dict[str, Any]:
        """What the latest run of each collector did."""
        runs: Dict[str, Any] = {}
        if not self._root.exists():
            return runs
        for folder in sorted(p for p in self._root.iterdir() if p.is_dir()):
            path = folder / LAST_RUN
            if path.exists():
                try:
                    runs[folder.name] = json.loads(
                        path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    runs[folder.name] = {"result": "unreadable"}
        return runs

    # ------------------------------------------------------------------ writing

    def next_id(self, collector: str, cutoff: datetime) -> Tuple[str, Optional[str], int]:
        """
        The id the next package of a collector gets.

        Args:
            collector: Source name
            cutoff: The package's cutoff, UTC

        Returns:
            (package id, the previous package's id or None, sequence number)
        """
        existing = self.manifests(collector)
        sequence = existing[-1]["sequence"] + 1 if existing else 1
        previous = existing[-1]["package_id"] if existing else None
        return f"{collector}-{sequence:04d}-{cutoff:%Y%m%d}", previous, sequence

    def clean_incomplete(self, collector: str) -> List[str]:
        """
        Remove what a run that died left behind.

        A `.part` is a write that never finished; a zip without its manifest is a
        package that was never committed. Both are rebuilt from the source files,
        which nothing has marked as packed.

        Returns:
            Names removed
        """
        folder = self.ready(collector)
        removed: List[str] = []
        if not folder.exists():
            return removed
        for path in folder.glob(f"*{PART_SUFFIX}"):
            path.unlink()
            removed.append(path.name)
        for path in folder.glob(f"*{ZIP_SUFFIX}"):
            stem = path.name[:-len(ZIP_SUFFIX)]
            # A manifest on either side makes it a package, mid-move or not.
            if not any((side / f"{stem}{MANIFEST_SUFFIX}").exists()
                       for side in (folder, self.delivered(collector))):
                path.unlink()
                removed.append(path.name)
        return removed

    def write_last_run(self, collector: str, record: Dict[str, Any]) -> None:
        """Record what this run did, for the listing."""
        folder = self.folder(collector)
        folder.mkdir(parents=True, exist_ok=True)
        write_json_atomically(folder / LAST_RUN, record)

    def write_receipt(
        self, package_id: str, consumer: str, note: str = "",
        zip_sha256: Optional[str] = None
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        """
        Mark a package received. The first acknowledgement stands.

        A second one changes nothing and says so: a consumer retrying after a lost
        answer must not move the date it first confirmed.

        Args:
            package_id: The package
            consumer: The token name that acknowledged
            note: Free text from the consumer, kept as given
            zip_sha256: The hash the consumer reported, already checked against the
                manifest; None when it reported none

        Returns:
            (the receipt, True when this call created it), or (None, False) when
            there is no such package
        """
        paths = self.find(package_id)
        if paths is None:
            return None, False
        record = {
            "package_id": package_id,
            "received_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "consumer": consumer,
            "note": note,
            "zip_sha256": zip_sha256,
        }
        # Written in full under a name of its own, then linked into place. A link
        # refuses an existing target on every platform, so two acknowledgements
        # racing cannot both win, and a crash never leaves an empty receipt that
        # reads as one.
        temporary = paths["receipt"].with_name(
            f"{paths['receipt'].name}.{uuid.uuid4().hex}{PART_SUFFIX}")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(record, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, paths["receipt"])
        except FileExistsError:
            return self.receipt(package_id), False
        finally:
            temporary.unlink()
        return record, True
