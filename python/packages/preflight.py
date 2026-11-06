"""
FiniexDataCollector - Package Preflight
Prove the access before the first byte is zipped.

The collectors' folders sit wherever their programs put them - MT5's inside a
terminal's AppData - and the package service runs as a different program, possibly
as a different account. So a run first checks, and refuses with the reason when
anything fails:

  1. the source folder exists and holds `instance.json` with a usable id. The mint
     is the proof that this is the collector's folder; without it nothing is packed.
     (A stale or copied folder with a genuine mint is beyond what any check here can
     tell. The id in every member is the second half of the proof.)
  2. the files can be listed, and each candidate can be opened for reading. Reading
     is all the packer does to a source; it never writes there, so a write probe in
     a source folder would test a permission nothing needs.
  3. the package folder takes a probe file: written, flushed, renamed, read back,
     deleted. That is every operation a package needs.
  4. writing the candidates' full size would still leave `min_free_percent` free.
     The collector shares the disk and calls 20 % free `disk_low`; a package must
     never be what pushes it there.

Location: python/packages/preflight.py
"""

import json
import os
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from python.packages.package_store import PackageStore
from python.utils.config_loader import PackageSourceConfig
from python.utils.instance_identity import IDENTITY_FILENAME, INSTANCE_ID
from python.utils.logging_setup import describe_exception

FINISHED_PATTERN = "*_ticks.json"
OPEN_PATTERN = "*.part"


@dataclass
class SourceCheck:
    """What a preflight found for one source."""
    source: str
    ok: bool = False
    reason: str = ""
    instance_id: Optional[str] = None
    mint: Dict[str, Any] = field(default_factory=dict)
    files_dir: Optional[Path] = None
    candidates: List[Path] = field(default_factory=list)
    candidate_bytes: int = 0
    unreadable: Dict[str, str] = field(default_factory=dict)
    open_files: List[Dict[str, Any]] = field(default_factory=list)
    already_packed: int = 0
    free_bytes: Optional[int] = None
    total_bytes: Optional[int] = None

    def summary(self) -> Dict[str, Any]:
        """The check as a record for the listing and the console."""
        return {
            "source": self.source,
            "ok": self.ok,
            "reason": self.reason,
            "instance_id": self.instance_id,
            "files_dir": str(self.files_dir) if self.files_dir else None,
            "candidates": len(self.candidates),
            "candidate_bytes": self.candidate_bytes,
            "unreadable": self.unreadable,
            "open_files": len(self.open_files),
            "already_packed": self.already_packed,
            "free_bytes": self.free_bytes,
            "total_bytes": self.total_bytes,
        }


def _refuse(check: SourceCheck, reason: str) -> SourceCheck:
    check.ok = False
    check.reason = reason
    return check


def read_mint(root: Path) -> Dict[str, Any]:
    """
    Read a collector folder's `instance.json`.

    Args:
        root: The folder that must hold it

    Returns:
        The mint record

    Raises:
        ValueError: With the reason, when it is missing, unreadable or carries no
            usable id
    """
    path = root / IDENTITY_FILENAME
    if not path.exists():
        raise ValueError(
            f"no {IDENTITY_FILENAME} in {root}: not proven to be the collector's "
            f"folder, so nothing is packed")
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise ValueError(f"cannot read {path}: {describe_exception(e)}")
    instance_id = record.get("instance_id") if isinstance(
        record, dict) else None
    if not isinstance(instance_id, str) or not INSTANCE_ID.match(instance_id):
        raise ValueError(
            f"{path} carries no usable instance_id ({instance_id!r})")
    return record


def _probe_write(folder: Path) -> None:
    """Every operation a package needs, once, on a throwaway file."""
    folder.mkdir(parents=True, exist_ok=True)
    payload = uuid.uuid4().bytes
    first = folder / f".probe-{uuid.uuid4().hex}"
    second = first.with_name(first.name + ".renamed")
    try:
        with first.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(first, second)
        if second.read_bytes() != payload:
            raise OSError("the probe file read back different bytes")
    finally:
        for path in (first, second):
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def check_source(
    name: str,
    config: PackageSourceConfig,
    store: PackageStore,
    min_free_percent: float
) -> SourceCheck:
    """
    Check one source and the package folder before a run.

    Args:
        name: The source's name, e.g. `kraken`
        config: Where its files and its mint are
        store: The package store, for what is already packed and where to write
        min_free_percent: Free share of the disk that must remain

    Returns:
        The check; `ok` False with a reason when anything failed
    """
    check = SourceCheck(source=name)
    root = Path(config.root) if config.root else None

    if root is None or not root.is_dir():
        return _refuse(check, f"source folder {config.root!r} is not a folder")

    try:
        check.mint = read_mint(root)
    except ValueError as e:
        return _refuse(check, str(e))
    check.instance_id = check.mint["instance_id"]

    files_dir = root / config.subdir if config.subdir else root
    check.files_dir = files_dir
    if not files_dir.is_dir():
        return _refuse(check, f"files folder {files_dir} is not a folder")

    try:
        finished = sorted(p for p in files_dir.glob(
            FINISHED_PATTERN) if p.is_file())
        open_paths = sorted(p for p in files_dir.glob(
            OPEN_PATTERN) if p.is_file())
    except OSError as e:
        return _refuse(check, f"cannot list {files_dir}: {describe_exception(e)}")

    for path in open_paths:
        try:
            stat = path.stat()
            check.open_files.append({
                "name": path.name,
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(
                    stat.st_mtime, timezone.utc).isoformat(timespec="seconds"),
            })
        except OSError:
            # Renamed into its finished name between the listing and here.
            continue

    try:
        packed = store.packed_names(name)
    except (OSError, ValueError, KeyError) as e:
        return _refuse(check, f"cannot read the existing manifests: {describe_exception(e)}")

    for path in finished:
        if path.name in packed:
            check.already_packed += 1
            continue
        try:
            with path.open("rb") as handle:
                handle.read(1)
            check.candidates.append(path)
            check.candidate_bytes += path.stat().st_size
        except OSError as e:
            check.unreadable[path.name] = describe_exception(e)

    if check.unreadable and not check.candidates:
        first = next(iter(check.unreadable.items()))
        return _refuse(check, f"no new file is readable, e.g. {first[0]}: {first[1]}")

    folder = store.folder(name)
    try:
        _probe_write(folder)
    except OSError as e:
        return _refuse(check, f"package folder {folder} is not writable: {describe_exception(e)}")

    try:
        usage = shutil.disk_usage(folder)
    except OSError as e:
        return _refuse(check, f"cannot read the free space at {folder}: {describe_exception(e)}")
    check.free_bytes, check.total_bytes = usage.free, usage.total

    # The candidates' full size is the bound: a Deflate zip of JSON is a twentieth
    # of it, but the bound is what the check can promise.
    left = (usage.free - check.candidate_bytes) / usage.total * 100
    if left < min_free_percent:
        return _refuse(
            check,
            f"writing up to {check.candidate_bytes / 1e9:.2f} GB would leave "
            f"{left:.1f} % free, below the {min_free_percent:.0f} % kept for the collector")

    check.ok = True
    return check
