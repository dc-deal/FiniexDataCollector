"""
FiniexDataCollector - Outage History
Every interruption of the Kraken feed in a time window, read from the log.

`/v1/status` keeps the latest twenty outage records, about two days at the rate
measured in October 2026, and its totals start again with the process. A consumer
comparing a month of our capture against its own feed needs every interruption in
that month, with the trade ids it cost - and the log already holds each one, as an
`[OUTAGE]` line written when the feed is restored and again when the record is
complete. This reads those lines back. Nothing new is recorded for it.

So the history starts with the build that writes the lines, 2026-10-10, and it
reaches as far back as the log files do; nothing deletes them.

A day that has ended does not change, so its records are parsed once and kept
for the life of the process; only a file that grew is read again. Reading a month
on every call would be a month of log on every call.

Location: python/api/outage_history.py
"""

import json
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from python.api.log_reader import LINE, log_path
from python.api.redaction import redact

PREFIX = "[OUTAGE] "

# About ten outages a day were measured in October 2026, so a full month is a few
# hundred. The cap exists for a pathological day, not for an ordinary request.
MAX_RECORDS = 2000
DEFAULT_DAYS = 7
MAX_DAYS = 62

# A resolved line can follow its restored line by up to fifteen minutes, and an
# outage that began before midnight is logged after it. One extra day on the far
# side catches it.
LATE_LINE_DAYS = 1

# Later stages replace earlier ones: a resolved record carries the first trade per
# symbol that the restored one could not yet know.
STAGE_RANK = {"restored": 1, "resolved": 2}

_cache: Dict[Path, Tuple[Tuple[int, int], List[Dict[str, Any]], int]] = {}
_cache_lock = threading.Lock()


def read_outages(
    log_dir: Path,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    limit: int = MAX_RECORDS
) -> Dict[str, Any]:
    """
    Every outage that began in [since, until], oldest first.

    Args:
        log_dir: Directory holding the logs
        since: Earliest outage start; default: seven days before `until`
        until: Latest outage start; default: now
        limit: Most records to return; the rest is named by `next_since`

    Returns:
        The window, what was read, and the records

    Raises:
        ValueError: The window is reversed or longer than MAX_DAYS
    """
    until = _utc(until) or datetime.now(timezone.utc)
    since = _utc(since) or until - timedelta(days=DEFAULT_DAYS)
    if since > until:
        raise ValueError(
            f"since ({since.isoformat()}) is after until ({until.isoformat()})")
    if until - since > timedelta(days=MAX_DAYS):
        raise ValueError(
            f"the window is longer than {MAX_DAYS} days - ask in parts")
    limit = max(1, min(limit, MAX_RECORDS))

    today = datetime.now(timezone.utc).date()
    last_day = min(until.date() + timedelta(days=LATE_LINE_DAYS), today)
    days_read: List[str] = []
    days_missing: List[str] = []
    unreadable = 0
    merged: Dict[str, Dict[str, Any]] = {}

    day = since.date()
    while day <= last_day:
        path = log_path(log_dir, day)
        if not path.exists():
            days_missing.append(day.isoformat())
        else:
            days_read.append(day.isoformat())
            entries, bad = _read_day(path)
            unreadable += bad
            for entry in entries:
                _merge(merged, entry)
        day += timedelta(days=1)

    records = sorted(
        (r for r in merged.values() if since <= _start_of(r) <= until),
        key=_start_of)
    truncated = len(records) > limit
    kept = records[:limit]

    return {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "count": len(kept),
        "truncated": truncated,
        "next_since": records[limit]["timestamp"] if truncated else None,
        "days_read": days_read,
        "days_missing": days_missing,
        "unreadable_lines": unreadable,
        "records": [redact(record) for record in kept],
    }


def _read_day(path: Path) -> Tuple[List[Dict[str, Any]], int]:
    """
    The outage entries of one log file, from the cache while the file is unchanged.

    Args:
        path: One day's log file

    Returns:
        The entries in file order, and how many outage lines could not be parsed
    """
    stat = path.stat()
    key = (stat.st_size, stat.st_mtime_ns)
    with _cache_lock:
        cached = _cache.get(path)
        if cached and cached[0] == key:
            return cached[1], cached[2]

    entries: List[Dict[str, Any]] = []
    bad = 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw in handle:
            if PREFIX not in raw:
                continue
            match = LINE.match(raw.rstrip("\n"))
            if not match or not match.group("message").startswith(PREFIX):
                continue
            try:
                entry = json.loads(match.group("message")[len(PREFIX):])
                _start_of(entry)
            except (ValueError, TypeError, KeyError):
                # A torn last line after a hard stop is the expected case; it is
                # counted rather than guessed at.
                bad += 1
                continue
            entry["logged_at"] = datetime.strptime(
                match.group("ts"), "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc).isoformat()
            entries.append(entry)

    with _cache_lock:
        _cache[path] = (key, entries, bad)
    return entries, bad


def _merge(merged: Dict[str, Dict[str, Any]], entry: Dict[str, Any]) -> None:
    """
    Keep the furthest stage of each outage, keyed by when it began.

    Args:
        merged: Records so far, by start
        entry: One parsed line
    """
    key = entry["timestamp"]
    current = merged.get(key)
    rank = STAGE_RANK.get(entry.get("stage"), 0)
    if current is None or rank >= STAGE_RANK.get(current.get("stage"), 0):
        merged[key] = entry


def _start_of(record: Dict[str, Any]) -> datetime:
    """
    When an outage began.

    Args:
        record: One outage record

    Returns:
        Its start, UTC
    """
    return _utc(datetime.fromisoformat(record["timestamp"]))


def _utc(value: Optional[datetime]) -> Optional[datetime]:
    """
    A datetime as UTC; a naive one is taken to be UTC already, as every time on
    this API is.

    Args:
        value: A datetime or None

    Returns:
        The same moment, timezone-aware
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def clear_cache() -> None:
    """Forget every parsed day; for tests."""
    with _cache_lock:
        _cache.clear()
