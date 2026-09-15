"""
FiniexDataCollector - Log Excerpt
A slice of the collector's log, without a session on the machine.

The log is the only place that records why a reconnect happened, what a recovery
found at startup, or which symbol stopped producing. Reaching it meant an RDP
session, which is why the questions it answers were usually answered by guessing
instead.

One property to state, because the sister project documents the opposite as a
trap: **our timestamps are UTC in both places.** Each line is stamped
`YYYY-MM-DD HH:MM:SS UTC` and each file is named for the UTC date. So a UTC range
in the request means the same thing as a UTC stamp in the file, and no offset has
to be applied anywhere. Nothing here needs to be kept in step with the server's
local clock.

Location: python/api/log_reader.py
"""

import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# `2026-03-29 09:34:35 UTC | INFO     | FiniexDataCollector | message`
LINE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC \| "
    r"(?P<level>\w+)\s*\| (?P<logger>[^|]+)\| (?P<message>.*)$"
)

LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

# A day of DEBUG runs to hundreds of megabytes - the production server produced
# 294 MB across three days. An unbounded answer would be neither transferable nor
# readable, so the cap is part of the contract rather than a safety net.
MAX_LINES = 2000


def log_path(log_dir: Path, day: date) -> Path:
    """
    Path of the log file for one UTC day.

    Args:
        log_dir: Directory holding the logs
        day: The UTC date

    Returns:
        Expected file path, which may not exist
    """
    return log_dir / f"finiexdatacollector_{day.isoformat()}.log"


def available_days(log_dir: Path) -> List[str]:
    """
    UTC dates for which a log file exists.

    Args:
        log_dir: Directory holding the logs

    Returns:
        Sorted ISO dates, newest last
    """
    if not log_dir.exists():
        return []

    days = []
    for path in log_dir.glob("finiexdatacollector_*.log"):
        stem = path.stem.replace("finiexdatacollector_", "")
        try:
            datetime.strptime(stem, "%Y-%m-%d")
            days.append(stem)
        except ValueError:
            continue

    return sorted(days)


def read_log(
    log_dir: Path,
    day: date,
    min_level: str = "INFO",
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    contains: Optional[str] = None,
    limit: int = MAX_LINES
) -> Dict[str, Any]:
    """
    Read one day's log, filtered.

    Lines that do not match the expected shape - a traceback's continuation, for
    instance - are carried through unfiltered when a level filter would otherwise
    drop them silently. A stack trace whose first line was kept and whose body
    vanished is worse than no excerpt.

    Args:
        log_dir: Directory holding the logs
        day: UTC date to read
        min_level: Lowest level to include
        since: Only lines at or after this UTC time
        until: Only lines at or before this UTC time
        contains: Only lines whose message contains this text
        limit: Maximum lines to return, newest kept

    Returns:
        Excerpt document with the lines and what was applied
    """
    path = log_path(log_dir, day)
    limit = max(1, min(limit, MAX_LINES))

    if not path.exists():
        return {"day": day.isoformat(), "exists": False, "lines": [],
                "line_count": 0, "truncated": False,
                "available_days": available_days(log_dir)}

    threshold = LEVELS.index(min_level) if min_level in LEVELS else 0
    kept: List[Dict[str, Any]] = []
    total = 0

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw in handle:
            raw = raw.rstrip("\n")
            if not raw:
                continue

            total += 1
            match = LINE.match(raw)

            if not match:
                # Continuation of a multi-line entry; it belongs to whatever was
                # kept last, so it is kept only if that was.
                if kept:
                    kept.append({"timestamp": None, "level": None,
                                 "logger": None, "message": raw})
                continue

            level = match.group("level")
            if level in LEVELS and LEVELS.index(level) < threshold:
                continue

            stamp = datetime.strptime(
                match.group("ts"), "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc)

            if since and stamp < since:
                continue
            if until and stamp > until:
                continue

            message = match.group("message")
            if contains and contains.lower() not in message.lower():
                continue

            kept.append({
                "timestamp": stamp.isoformat(),
                "level": level,
                "logger": match.group("logger").strip(),
                "message": message
            })

    truncated = len(kept) > limit

    return {
        "day": day.isoformat(),
        "exists": True,
        "min_level": min_level,
        "line_count": len(kept[-limit:]),
        "lines_scanned": total,
        "truncated": truncated,
        "lines": kept[-limit:],
        "available_days": available_days(log_dir)
    }
