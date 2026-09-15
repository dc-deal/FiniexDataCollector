"""
FiniexDataCollector - Archive Inventory
What the collector has written, answered without a shell on the machine.

The question that produced this: on 2026-09-15 the consuming project asked which
tick files span a host migration window, so they could quarantine them. The answer
existed - a file that absorbed a clock correction is one whose header and summary
anchor counters differ - but it could only be computed by someone with access to
the server, and nobody did. A question with a deadline stalled on a shell prompt.

Reading is metadata-only. The tick arrays are the bulk of a file (425 bytes per
tick, up to 50,000 of them) and no inventory question needs them, so they are
never parsed into the answer.

Location: python/api/archive_reader.py
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def _file_entry(path: Path) -> Optional[Dict[str, Any]]:
    """
    Describe one archive file.

    A file that cannot be read is reported as unreadable rather than skipped: a
    missing entry looks like a file that was never written, which is a different
    problem with a different fix.

    Args:
        path: The `*_ticks.json` to describe

    Returns:
        Its inventory entry, or None if it is not a tick file at all
    """
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return {
            "file": path.name,
            "readable": False,
            "error": str(e),
            "size_bytes": path.stat().st_size if path.exists() else 0
        }

    metadata = document.get("metadata") or {}
    summary = document.get("summary") or {}
    ticks = document.get("ticks") or []
    anchor = summary.get("anchor") or {}

    opened_resyncs = metadata.get("anchor_resyncs", 0)
    closed_resyncs = anchor.get("resyncs", 0)

    return {
        "file": path.name,
        "readable": True,
        "symbol": metadata.get("symbol"),
        "data_format_version": metadata.get("data_format_version"),
        "collected_msc_timebase": metadata.get("collected_msc_timebase"),
        "start_time": metadata.get("start_time"),
        "tick_count": len(ticks),
        "declared_tick_count": summary.get("total_ticks"),
        "event_start_msc": ticks[0]["time_msc"] if ticks else None,
        "event_end_msc": ticks[-1]["time_msc"] if ticks else None,
        "collected_start_msc": ticks[0]["collected_msc"] if ticks else None,
        "collected_end_msc": ticks[-1]["collected_msc"] if ticks else None,
        "anchor_resyncs_at_open": opened_resyncs,
        "anchor_resyncs_at_close": closed_resyncs,
        "anchor_max_correction_ms": anchor.get("max_correction_ms", 0),
        # The query the consuming project asked for. Computed here rather than
        # left to the caller: the rule is ours, and a consumer reimplementing it
        # would be reimplementing a detail of our format.
        "absorbed_clock_correction": closed_resyncs > opened_resyncs,
        "size_bytes": path.stat().st_size
    }


def read_archive(
    output_dir: Path,
    data_collector: str,
    symbol: Optional[str] = None,
    only_corrected: bool = False
) -> Dict[str, Any]:
    """
    Inventory the archive files on disk.

    Args:
        output_dir: Base output directory
        data_collector: Collector subdirectory, e.g. "kraken"
        symbol: Restrict to one symbol
        only_corrected: Return only files that absorbed a clock correction

    Returns:
        Inventory document with the files and a short summary
    """
    target = output_dir / data_collector

    if not target.exists():
        return {"collector": data_collector, "files": [], "file_count": 0,
                "open_files": 0, "corrected_files": 0}

    pattern = f"{symbol}_*_ticks.json" if symbol else "*_ticks.json"
    entries: List[Dict[str, Any]] = []

    for path in sorted(target.glob(pattern)):
        entry = _file_entry(path)
        if entry is None:
            continue
        if only_corrected and not entry.get("absorbed_clock_correction"):
            continue
        entries.append(entry)

    # A `.jsonl.part` without its archive file is a run that has not finished -
    # either the current one or a crashed one waiting for recovery.
    open_logs = sorted(p.name for p in target.glob("*_ticks.jsonl.part"))

    return {
        "collector": data_collector,
        "file_count": len(entries),
        "corrected_files": sum(
            1 for e in entries if e.get("absorbed_clock_correction")),
        "unreadable_files": sum(1 for e in entries if not e.get("readable")),
        "open_write_ahead_logs": open_logs,
        "files": entries
    }
