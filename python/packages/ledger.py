"""
FiniexDataCollector - Package Ledger
The book of what was packed, what went out, and what came back confirmed.

The manifests say what each package holds. Nothing said who fetched which package,
when, and whether all of it left - and when two parties disagree about a transfer,
that is the question. The ledger answers it, one JSON line per event in
`<output_dir>/ledger.jsonl`, appended and never rewritten:

  packed            a package was committed: its zip's size and SHA-256, and every
                    member with its name, SHA-256 and tick count - a second record
                    of the contents beside the manifests
  served            a download ended: who, which range, the bytes handed to the
                    connection, and whether that was all of them
  received          the consumer's acknowledgement, with the hash it reported
  receipt_refused   an acknowledgement whose hash was not the package's

**"Served complete" is what this side can know**: every byte was handed to the
connection and the client had not gone. Whether the bytes arrived is what the
receipt says, and only the receipt.

A package committed while the ledger could not be written - or before the ledger
existed - gets its `packed` line from its manifest the next time the ledger is
seeded, marked `entered_late`. The ledger is a record, not a gate: a write that
fails never stops a package or a download.

Several processes write it - the service, and a `pack` run by hand - so every
append holds an operating-system lock on a file beside it.

Location: python/packages/ledger.py
"""

import json
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

LEDGER_NAME = "ledger.jsonl"
LOCK_NAME = ".ledger.lock"
LOCK_WAIT_S = 10.0


@contextmanager
def _exclusive(lock_path: Path) -> Iterator[None]:
    """Hold an operating-system lock on a file for the length of one append."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            deadline = time.monotonic() + LOCK_WAIT_S
            while True:
                try:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def packed_fields(manifest: Dict[str, Any]) -> Dict[str, Any]:
    """The `packed` event of a committed package, from its manifest."""
    return {
        "package_id": manifest["package_id"],
        "collector": manifest["collector"],
        "instance_id": manifest["instance_id"],
        "cutoff": manifest["cutoff"],
        "created_at": manifest["created_at"],
        "zip": {"name": manifest["zip"]["name"], "size_bytes": manifest["zip"]["size_bytes"],
                "sha256": manifest["zip"]["sha256"]},
        "member_count": manifest["member_count"],
        "tick_count": manifest["tick_count"],
        "members": [{"name": m["name"], "sha256": m["sha256"], "tick_count": m["tick_count"]}
                    for m in manifest["members"]],
        "excluded": [e["name"] for e in manifest.get("excluded", [])],
        "held_for_next": list(manifest.get("held_for_next", [])),
    }


class Ledger:
    """Append-only JSON lines, one per event."""

    def __init__(self, root: Path):
        """
        Args:
            root: The packages' root folder; the ledger lives beside the collectors'
                folders
        """
        self._path = Path(root) / LEDGER_NAME
        self._lock_path = Path(root) / LOCK_NAME
        self._thread_lock = threading.Lock()

    @property
    def path(self) -> Path:
        """Where the ledger is."""
        return self._path

    def append(self, event: str, **fields: Any) -> Dict[str, Any]:
        """
        Write one event and make it durable before returning.

        Args:
            event: The event's name
            **fields: Its content

        Returns:
            The record as written
        """
        record = {"event": event,
                  "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                  **fields}
        line = json.dumps(record, separators=(",", ":")) + "\n"
        with self._thread_lock, _exclusive(self._lock_path):
            # A line torn by a crash has no newline; the next event must not be
            # glued onto it, or one torn line becomes two unreadable ones.
            prefix = ""
            if self._path.exists() and self._path.stat().st_size > 0:
                with self._path.open("rb") as handle:
                    handle.seek(-1, os.SEEK_END)
                    if handle.read(1) != b"\n":
                        prefix = "\n"
            with self._path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(prefix + line)
                handle.flush()
                os.fsync(handle.fileno())
        return record

    def read(self, package_id: Optional[str] = None,
             event: Optional[str] = None) -> Dict[str, Any]:
        """
        Every event, oldest first.

        Args:
            package_id: Only this package's events
            event: Only this kind of event

        Returns:
            The events, and how many lines could not be read
        """
        events: List[Dict[str, Any]] = []
        unreadable = 0
        if self._path.exists():
            with self._path.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        unreadable += 1
                        continue
                    if package_id and record.get("package_id") != package_id:
                        continue
                    if event and record.get("event") != event:
                        continue
                    events.append(record)
        return {"count": len(events), "unreadable_lines": unreadable, "events": events}

    def seed(self, manifests: List[Dict[str, Any]]) -> List[str]:
        """
        Give every committed package its `packed` line.

        Args:
            manifests: Every committed package's manifest

        Returns:
            The ids entered now, late
        """
        known = {e.get("package_id")
                 for e in self.read(event="packed")["events"]}
        entered = []
        for manifest in manifests:
            if manifest["package_id"] in known:
                continue
            self.append("packed", entered_late=True, **packed_fields(manifest))
            entered.append(manifest["package_id"])
        return entered
