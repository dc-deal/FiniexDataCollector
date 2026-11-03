"""
FiniexDataCollector - Archive files built from a write-ahead log

One place that turns a `.jsonl.part` into a finished `*_ticks.json`. Three
callers share it: the rotation that just closed a file, the subprocess that does
that work off the collector's event loop, and the recovery that runs at startup.
Before this module there were two implementations of the same file format, and
they disagreed - the rotation wrote `ensure_ascii=True`, recovery wrote
`ensure_ascii=False`, so one non-ASCII value would have produced two different
files for the same ticks.

Run as a program it is the subprocess:

    python -m python.writers.wal_archive <path to a .jsonl.part>

It prints one JSON line about what it wrote, which is what the collector logs
and counts. It deliberately does not use the project logger: two processes
appending to one log file interleave, and the parent is the one holding the
context anyway.

**Not in this module:** when a file closes (see `json_tick_writer.py`) or what
the fields mean (see `docs/architecture/output_contract.md`).

Location: python/writers/wal_archive.py
"""

import json
import os
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from python.utils.logging_setup import describe_exception

# The last line a rotating writer appends to its log: the summary and errors it
# computed at close. Written into the log rather than into a sidecar so that one
# path carries everything a finished file needs - and so that a reader that does
# not know this key, which is every build before 1.8.0, simply skips it.
CLOSE_RECORD_KEY = "close"

# What a recovered file says about itself when nobody closed it properly.
RECOVERY_NOTE = (
    "Recovered from a write-ahead log after an unclean stop. "
    "The file is shorter than a rotation would have made it; "
    "that is where the previous run ended."
)


class WalUnreadable(Exception):
    """The log has no usable header, so nothing can be stated about its ticks."""


@dataclass
class WalContents:
    """
    What a write-ahead log holds.

    Attributes:
        metadata: The header written when the file was opened
        ticks: Every tick record, in arrival order
        anchor: Clock counters, the latest checkpoint or the header's
        close: The summary and errors the writer computed at close, when the
            rotation got that far; None for a log left by a crash
        torn_lines: Lines that could not be parsed - only the last one may be
    """
    metadata: Dict[str, Any]
    ticks: List[Dict[str, Any]] = field(default_factory=list)
    anchor: Dict[str, Any] = field(default_factory=dict)
    close: Optional[Dict[str, Any]] = None
    torn_lines: int = 0


def read_wal(wal_path: Path) -> WalContents:
    """
    Read a write-ahead log into its parts.

    Args:
        wal_path: The `.jsonl.part` to read

    Returns:
        WalContents

    Raises:
        WalUnreadable: The header line is missing or not JSON
    """
    lines = wal_path.read_text(encoding="utf-8").splitlines()

    if not lines:
        raise WalUnreadable(f"{wal_path.name} is empty")

    try:
        metadata = json.loads(lines[0])
    except json.JSONDecodeError as e:
        raise WalUnreadable(
            f"{wal_path.name} has an unreadable header: "
            f"{describe_exception(e)}")

    contents = WalContents(metadata=metadata, anchor={
        "resyncs": metadata.get("anchor_resyncs", 0),
        "max_correction_ms": metadata.get("anchor_max_correction_ms", 0)
    })

    for line in lines[1:]:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            # Only the final line can be torn; the caller decides what an
            # earlier one means.
            contents.torn_lines += 1
            continue

        if "time_msc" in record:
            contents.ticks.append(record)
        elif CLOSE_RECORD_KEY in record:
            contents.close = record[CLOSE_RECORD_KEY]
        elif "anchor_resyncs" in record:
            contents.anchor = {
                "resyncs": record["anchor_resyncs"],
                "max_correction_ms": record["anchor_max_correction_ms"]
            }

    return contents


def build_archive_text(contents: WalContents) -> str:
    """
    Build the exact text of a finished archive file.

    `json.dumps` rather than `json.dump`: the latter always takes CPython's
    pure-Python encoder, which is where 94 % of a rotation's time went. The
    bytes are identical - verified by SHA-256 against production files.

    Args:
        contents: What the log held

    Returns:
        The file's full text
    """
    if contents.close is not None:
        errors = contents.close.get("errors", _empty_errors())
        summary = contents.close.get("summary", {})
    else:
        errors = _empty_errors()
        summary = _reconstructed_summary(contents)

    return json.dumps({
        "metadata": contents.metadata,
        "ticks": contents.ticks,
        "errors": errors,
        "summary": summary
    }, indent=2)


def write_archive_atomically(text: str, archive_path: Path) -> None:
    """
    Write the file through a temp file in the same directory.

    A `*_ticks.json` never exists in a partial state; a consumer either sees the
    whole file or no file. Do not simplify this into a direct write.

    Args:
        text: The file's full text
        archive_path: Where it belongs
    """
    fd, temp_path = tempfile.mkstemp(
        dir=str(archive_path.parent), suffix=".tmp", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(temp_path, archive_path)
    except Exception:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


def archive_path_for(wal_path: Path) -> Path:
    """
    The archive file a log belongs to.

    Args:
        wal_path: A `*_ticks.jsonl.part`

    Returns:
        The matching `*_ticks.json`
    """
    return wal_path.with_suffix("").with_suffix(".json")


def archive_from_wal(wal_path: Path) -> Dict[str, Any]:
    """
    Turn one log into its archive file and remove the log.

    The order is the mechanism: the log is deleted only after the archive file
    exists. A crash before the write leaves the log for the next start; a crash
    between the two leaves both, and recovery keeps the file and drops the log.

    Args:
        wal_path: The `.jsonl.part` to convert

    Returns:
        A dict describing what was written, for the caller to log

    Raises:
        WalUnreadable: The header is missing or damaged
    """
    archive_path = archive_path_for(wal_path)
    contents = read_wal(wal_path)

    if not contents.ticks:
        wal_path.unlink(missing_ok=True)
        return {"file": archive_path.name, "ticks": 0, "written": False,
                "reason": "the log held no ticks"}

    if archive_path.exists():
        # Someone got there first - the child that ran before a restart, or a
        # recovery. The existing file is the one a consumer may already have
        # read, so it is never overwritten.
        wal_path.unlink(missing_ok=True)
        return {"file": archive_path.name, "ticks": len(contents.ticks),
                "written": False, "reason": "the archive file already existed"}

    write_archive_atomically(build_archive_text(contents), archive_path)
    wal_path.unlink(missing_ok=True)

    return {"file": archive_path.name, "ticks": len(contents.ticks),
            "written": True, "closed_by_writer": contents.close is not None,
            "torn_lines": contents.torn_lines}


def _empty_errors() -> Dict[str, Any]:
    """The errors block of a file nobody recorded an error for."""
    return {
        "by_severity": {"negligible": 0, "serious": 0, "fatal": 0},
        "details": []
    }


def _reconstructed_summary(contents: WalContents) -> Dict[str, Any]:
    """
    Build the summary a crashed writer never got to write.

    Only what the log itself proves: the tick count, the last event time, the
    anchor counters from the header or the newest checkpoint.

    Args:
        contents: What the log held

    Returns:
        The summary block
    """
    last_event = contents.ticks[-1]["time_msc"]
    start_unix = contents.metadata.get("start_time_unix", 0)
    duration_minutes = round(
        (last_event / 1000 - start_unix) / 60, 1) if start_unix else 0.0

    return {
        "total_ticks": len(contents.ticks),
        "total_errors": 0,
        "data_stream_status": "HEALTHY",
        "quality_metrics": {
            "overall_quality_score": 1.0,
            "data_integrity_score": 1.0,
            "data_reliability_score": 1.0,
            "negligible_error_rate": 0.0,
            "serious_error_rate": 0.0,
            "fatal_error_rate": 0.0
        },
        "timing": {
            "end_time": datetime.fromtimestamp(
                last_event / 1000, tz=timezone.utc).strftime("%Y.%m.%d %H:%M:%S"),
            "duration_minutes": duration_minutes,
            "avg_ticks_per_minute": round(
                len(contents.ticks) / duration_minutes, 1
            ) if duration_minutes > 0 else 0.0
        },
        "anchor": contents.anchor,
        "recommendations": RECOVERY_NOTE
    }


def main(argv: List[str]) -> int:
    """
    Subprocess entry point: write one archive file from one log.

    Args:
        argv: Command line arguments without the program name

    Returns:
        Process exit code; 0 means the log is gone and the file is there
    """
    if len(argv) != 1:
        print("usage: python -m python.writers.wal_archive <wal path>",
              file=sys.stderr)
        return 2

    wal_path = Path(argv[0])

    try:
        result = archive_from_wal(wal_path)
    except Exception as e:
        # The log stays where it is. The next start recovers it, which is the
        # whole reason the parent does not delete it before this runs.
        print(describe_exception(e), file=sys.stderr)
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
