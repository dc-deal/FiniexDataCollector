"""
FiniexDataCollector - Packer
Seal the finished files of one collector into a package.

What goes in: every finished `*_ticks.json` that no manifest names yet and whose
last tick was collected before the cutoff. "Collected" is `collected_msc`, UTC for
both collectors. The event time is not: MT5 writes `time_msc` and its file names in
the broker's server time. A file finished but still reaching past the cutoff waits
for the next package; the cutoff bounds a package from above only, so a file that
appears late - recovered from a write-ahead log, or by hand - is taken by the next
run instead of being skipped for good.

Each file is read once: the same bytes are hashed, parsed for the manifest and
written into the zip, so what the manifest describes is exactly what was packed.

What stays out, and is named in the manifest: a file that cannot be read or
parsed, and a file whose `origin.instance_id` is not the folder's mint. Open
`.part` files are listed, never packed.

The package is committed when its manifest exists. Until then it does not exist:
a run that dies leaves a `.part` or a zip without a manifest, both of which the
next run removes.

Location: python/packages/packer.py
"""

import hashlib
import json
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from python.api.log_reader import log_path
from python.api.outage_history import MAX_DAYS, read_outages
from python.packages.ledger import Ledger, packed_fields
from python.packages.lifecycle import stage_package
from python.packages.package_store import (MANIFEST_SUFFIX, PART_SUFFIX,
                                           ZIP_SUFFIX, PackageStore, PackLock,
                                           PackLockHeld, write_json_atomically)
from python.packages.preflight import check_source
# One definition of the quiet minutes for the packer, the schedule and the service:
# two of them disagreed about 00:15 until 2026-11-12, so a hand `pack` could run
# in a minute the schedule kept free.
from python.packages.schedule import in_quiet_minutes
from python.utils.config_loader import PackageSourceConfig
from python.utils.logging_setup import describe_exception

MANIFEST_SCHEMA = 1

# The default level: measured on this archive's JSON (api_app.py), level 9 buys
# little for several times the CPU on a box the collector shares.
COMPRESS_LEVEL = 6

# What a collector writes when its process starts, and what a start is read from.
START_MESSAGE = "FiniexDataCollector Starting"

HASH_BLOCK = 1024 * 1024


def default_cutoff(now: datetime) -> datetime:
    """The UTC day boundary at or before now - a package covers whole days."""
    return now.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def describe_member(name: str, data: bytes) -> Dict[str, Any]:
    """
    The manifest entry of one tick file.

    The tick count is counted, not taken from the summary; the summary's figure
    is reported beside it as `declared_tick_count`. Times are the file's own
    fields under their own names, with no conversion: `time_msc` is the venue's
    clock for Kraken and the broker's server time for MT5, `collected_msc` is UTC
    for both.

    Args:
        name: The file name
        data: Its complete bytes

    Returns:
        The entry

    Raises:
        ValueError: When the bytes are not a tick file
    """
    document = json.loads(data)
    if not isinstance(document, dict):
        raise ValueError("not a JSON object")
    metadata = document.get("metadata") or {}
    origin = metadata.get("origin") or {}
    summary = document.get("summary") or {}
    ticks = document.get("ticks")
    if not isinstance(ticks, list):
        raise ValueError("no tick array")

    first = ticks[0] if ticks else {}
    last = ticks[-1] if ticks else {}
    return {
        "name": name,
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "symbol": metadata.get("symbol"),
        "data_format_version": metadata.get("data_format_version"),
        "instance_id": origin.get("instance_id"),
        "producer": origin.get("producer"),
        "tick_count": len(ticks),
        "declared_tick_count": summary.get("total_ticks"),
        "first_time_msc": first.get("time_msc"),
        "last_time_msc": last.get("time_msc"),
        "first_collected_msc": first.get("collected_msc"),
        "last_collected_msc": last.get("collected_msc"),
    }


def _msc(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def _from_msc(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, timezone.utc)


def _iso(moment: Optional[datetime]) -> Optional[str]:
    return moment.isoformat(timespec="milliseconds") if moment else None


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(HASH_BLOCK), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync(path: Path) -> None:
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def outages_in(log_dir: Path, since: datetime, until: datetime) -> Dict[str, Any]:
    """
    The outage records that began in [since, until), read from the collector's
    `[OUTAGE]` log lines - the same records `/v1/outages` serves.

    Args:
        log_dir: The collector's log folder
        since: Window start, UTC
        until: Window end, UTC, exclusive

    Returns:
        The records, the days without a log file, and the torn lines
    """
    records: List[Dict[str, Any]] = []
    days_missing: List[str] = []
    unreadable = 0
    start = since
    end_inclusive = until - timedelta(milliseconds=1)
    while start <= end_inclusive:
        chunk_end = min(end_inclusive, start +
                        timedelta(days=MAX_DAYS) - timedelta(milliseconds=1))
        window_start = start
        while True:
            answer = read_outages(log_dir, since=window_start, until=chunk_end)
            records.extend(answer["records"])
            unreadable += answer["unreadable_lines"]
            days_missing.extend(
                d for d in answer["days_missing"] if d not in days_missing)
            if not answer["truncated"]:
                break
            window_start = datetime.fromisoformat(answer["next_since"])
        start = chunk_end + timedelta(milliseconds=1)
    # A day past the window is read for late lines; only the window's days count
    # as missing here.
    last_day = (until - timedelta(milliseconds=1)).date().isoformat()
    return {
        "count": len(records),
        "days_missing": [d for d in days_missing if d <= last_day],
        "unreadable_lines": unreadable,
        "records": records,
    }


def collector_starts(log_dir: Path, since: datetime, until: datetime) -> Dict[str, Any]:
    """
    Every start of the collector's process in [since, until), from its log.

    A restart is the one gap no outage record covers: the process that was gone
    could not write one. The start line is what the next process writes first.

    Args:
        log_dir: The collector's log folder
        since: Window start, UTC
        until: Window end, UTC, exclusive

    Returns:
        The start times and the days without a log file
    """
    starts: List[str] = []
    days_missing: List[str] = []
    day = since.date()
    while day <= (until - timedelta(milliseconds=1)).date():
        path = log_path(log_dir, day)
        if not path.exists():
            days_missing.append(day.isoformat())
        else:
            with path.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if START_MESSAGE not in line:
                        continue
                    try:
                        moment = datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").replace(
                            tzinfo=timezone.utc)
                    except ValueError:
                        continue
                    if since <= moment < until:
                        starts.append(moment.isoformat())
        day += timedelta(days=1)
    return {"starts": starts, "days_missing": days_missing}


def pack_source(
    name: str,
    config: PackageSourceConfig,
    store: PackageStore,
    min_free_percent: float,
    cutoff: Optional[datetime] = None,
    now: Optional[datetime] = None,
    packer: Optional[Dict[str, Any]] = None,
    log: Callable[[str], None] = lambda message: None,
    originals_root: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Build the next package of one collector.

    Args:
        name: Source name; the package id's first part
        config: The source's folders
        store: The package store
        min_free_percent: Free disk share that must remain
        cutoff: Upper bound on the last collected tick, UTC. Default: today's
            00:00 UTC
        now: The current time, for tests
        packer: What built the package (version, commit), copied into the manifest
        log: Where progress lines go
        originals_root: Where the packed originals go (stage A). None leaves them
            in the collector's folder

    Returns:
        The run's record: `packed`, `nothing_new`, `refused` or `failed`, with the
        reason. It is also written as the collector's `last_run.json`.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = cutoff or default_cutoff(now)
    started = time.monotonic()
    record: Dict[str, Any] = {
        "source": name,
        "at": now.isoformat(timespec="seconds"),
        "cutoff": cutoff.isoformat(),
    }

    def finish(result: str, **fields: Any) -> Dict[str, Any]:
        # Measured, on the monotonic clock: the open health route shows it, so a
        # consumer can plan its pickup from what a run took, not from a guess.
        record.update(result=result, duration_seconds=round(time.monotonic() - started, 1),
                      **fields)
        try:
            store.write_last_run(name, record)
        except OSError as e:
            log(f"[PACKAGE] {name}: could not record the run: {describe_exception(e)}")
        log(f"[PACKAGE] {name}: {result}" + (f" - {record.get('reason')}" if record.get("reason") else "")
            + (f" - {record.get('package_id')}" if record.get("package_id") else ""))
        return record

    if cutoff.tzinfo is None:
        return finish("refused", reason="the cutoff carries no time zone")
    if cutoff > now:
        return finish("refused", reason=f"the cutoff {cutoff.isoformat()} is in the future")
    if in_quiet_minutes(now):
        return finish("refused", reason="23:55-00:15 UTC stays quiet; run it later")

    try:
        with PackLock(store.folder(name)):
            return _pack_locked(name, config, store, min_free_percent, cutoff, now,
                                packer or {}, record, finish, log, originals_root)
    except PackLockHeld as e:
        return finish("refused", reason=str(e))
    except Exception as e:
        return finish("failed", reason=describe_exception(e))


def _pack_locked(
    name: str,
    config: PackageSourceConfig,
    store: PackageStore,
    min_free_percent: float,
    cutoff: datetime,
    now: datetime,
    packer: Dict[str, Any],
    record: Dict[str, Any],
    finish: Callable[..., Dict[str, Any]],
    log: Callable[[str], None],
    originals_root: Optional[Path]
) -> Dict[str, Any]:
    """The run itself, with the collector's lock held."""
    removed = store.clean_incomplete(name)
    if removed:
        record["removed_incomplete"] = removed

    # Seeded under this collector's lock, so a package of it cannot be committed
    # between the seed's look and its write - which would book it twice.
    ledger = Ledger(store.root)
    try:
        late = ledger.seed(store.manifests(name))
        if late:
            record["entered_late"] = late
    except OSError as e:
        log(f"[LEDGER] {name}: could not seed: {describe_exception(e)}")

    check = check_source(name, config, store, min_free_percent, originals_root)
    record["check"] = check.summary()
    if not check.ok:
        return finish("refused", reason=check.reason)

    package_id, previous_id, sequence = store.next_id(name, cutoff)
    folder = store.ready(name)
    folder.mkdir(parents=True, exist_ok=True)
    zip_part = folder / f"{package_id}{ZIP_SUFFIX}{PART_SUFFIX}"
    zip_path = folder / f"{package_id}{ZIP_SUFFIX}"
    cutoff_msc = _msc(cutoff)

    members: List[Dict[str, Any]] = []
    excluded: List[Dict[str, Any]] = [
        {"name": n, "reason": f"unreadable: {why}"} for n, why in check.unreadable.items()]
    held: List[str] = []
    log(f"[PACKAGE] {name}: {len(check.candidates)} new files, "
        f"{check.candidate_bytes / 1e6:.1f} MB, cutoff {cutoff.isoformat()}")

    try:
        with zipfile.ZipFile(zip_part, "w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=COMPRESS_LEVEL, allowZip64=True) as archive:
            for path in check.candidates:
                try:
                    data = path.read_bytes()
                    modified = datetime.fromtimestamp(
                        path.stat().st_mtime, timezone.utc)
                except OSError as e:
                    excluded.append(
                        {"name": path.name, "reason": f"unreadable: {describe_exception(e)}"})
                    continue
                try:
                    member = describe_member(path.name, data)
                except (ValueError, UnicodeDecodeError) as e:
                    excluded.append(
                        {"name": path.name, "reason": f"not a tick file: {describe_exception(e)}"})
                    continue
                if member["instance_id"] != check.instance_id:
                    excluded.append({
                        "name": path.name,
                        "reason": f"origin.instance_id {member['instance_id']!r} is not the "
                        f"folder's mint {check.instance_id!r}"})
                    continue
                last = member["last_collected_msc"]
                if isinstance(last, (int, float)) and last >= cutoff_msc:
                    held.append(path.name)
                    continue
                info = zipfile.ZipInfo(
                    path.name, date_time=modified.timetuple()[:6])
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, data, compresslevel=COMPRESS_LEVEL)
                del member["instance_id"]
                members.append(member)

        if not members:
            zip_part.unlink()
            return finish("nothing_new", excluded=excluded, held_for_next=len(held))

        # The self-check: every member read back out of the finished zip must be
        # the bytes that were hashed - not only a CRC that matches, the same SHA-256.
        # The originals leave their folder on the strength of this.
        expected = {m["name"]: m["sha256"] for m in members}
        with zipfile.ZipFile(zip_part) as archive:
            if sorted(archive.namelist()) != sorted(expected):
                raise OSError(
                    "the written zip does not hold exactly the packed files")
            for info in archive.infolist():
                if hashlib.sha256(archive.read(info)).hexdigest() != expected[info.filename]:
                    raise OSError(
                        f"the written zip gives back other bytes for {info.filename}")
        _fsync(zip_part)
        zip_sha256 = _sha256_of(zip_part)
        zip_size = zip_part.stat().st_size
        os.replace(zip_part, zip_path)
    except BaseException:
        # Not a package: the next run would remove it anyway, but a half zip left
        # beside the committed ones is a thing somebody might pick up by hand.
        try:
            zip_part.unlink()
        except FileNotFoundError:
            pass
        raise

    previous = store.manifest(previous_id) if previous_id else None
    firsts = [m["first_collected_msc"] for m in members
              if isinstance(m["first_collected_msc"], (int, float))]
    span_from = (datetime.fromisoformat(previous["cutoff"]) if previous
                 else _from_msc(min(firsts)) if firsts else None)

    outages = starts = None
    if config.log_dir and span_from is not None:
        log_dir = Path(config.log_dir)
        outages = outages_in(log_dir, span_from, cutoff)
        starts = collector_starts(log_dir, span_from, cutoff)

    producers = sorted({m["producer"] for m in members if m["producer"]})
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "package_id": package_id,
        "sequence": sequence,
        "collector": name,
        # The same value under the name every Finiex package service uses, so one
        # client reads the collector's and the RAG engine's packages alike
        # (FiniexTestingIDE, 2026-11-12). `collector` stays for the readers it has.
        "source": name,
        "instance_id": check.instance_id,
        "producer": producers[0] if len(producers) == 1 else producers,
        "cutoff": cutoff.isoformat(),
        "previous_package_id": previous_id,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "zip": {"name": zip_path.name, "size_bytes": zip_size, "sha256": zip_sha256,
                "compression": "deflate"},
        "member_count": len(members),
        "tick_count": sum(m["tick_count"] for m in members),
        "span": {"from": _iso(span_from), "until": cutoff.isoformat()},
        "members": members,
        "excluded": excluded,
        "held_for_next": held,
        "open_files": check.open_files,
        "outages": outages,
        "collector_starts": starts,
        "process_starts": starts,
        "packer": packer,
    }
    write_json_atomically(folder / f"{package_id}{MANIFEST_SUFFIX}", manifest)

    # After the commit, never before: a ledger line for a package that does not
    # exist would be worse than a late one. If this write fails, the next seed
    # enters it from the manifest.
    try:
        ledger.append("packed", **packed_fields(manifest))
    except OSError as e:
        log(f"[LEDGER] {name}: could not book {package_id}: {describe_exception(e)}")

    # Stage A: the originals leave the collector's folder for `awaiting/`. Moved,
    # not deleted - until the consumer confirms, they and the zip are two copies.
    staged: Dict[str, Any] = {}
    if originals_root is not None:
        staged = stage_package(name, config, manifest, originals_root, ledger)

    return finish("packed", package_id=package_id, member_count=len(members),
                  zip_size_bytes=zip_size, excluded=len(excluded), held_for_next=len(held),
                  staged=len(staged.get("moved", [])), not_staged=staged.get("skipped", []))
