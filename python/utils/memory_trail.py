"""
FiniexDataCollector - Memory Trail
One log line every ten minutes with what memory, collection and the feed did in it.

`/v1/status` says how much memory the process holds now, and a restart forgets it.
Whether that number settles or creeps over weeks - 140 MB a day after a start in
October 2026, 220 MB eight days later - is a question of slope, and a slope needs
readings that outlive the process. The log keeps them, and `/v1/logs` reads them
back from anywhere: `contains=[MEMORY]`.

Each line covers its own window and carries deltas, never totals since the start,
so two lines either side of a restart do not have to be told apart.

Two memory figures, because on Windows they disagree. `rss_mb` is the working
set, which the system trims under pressure; `private_mb` is what the process has
committed. FiniexRAGEngine read 5.2 GB of the first while its process held 7.2 GB.
Where the platform does not report private memory, it is `null`, not `0`.

Location: python/utils/memory_trail.py
"""

import asyncio
import gc
import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple

import psutil

from python.types.collector_stats import CollectorStats

INTERVAL_SECONDS = 600
MB = 1024 * 1024


def read_memory_mb(process: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Resident and private memory of a process.

    Args:
        process: A psutil.Process

    Returns:
        (rss_mb, private_mb); either is None when the platform will not say
    """
    try:
        info = process.memory_info()
    except Exception:
        return None, None
    private = getattr(info, "private", None)
    return (round(info.rss / MB, 1),
            round(private / MB, 1) if private is not None else None)


def private_mb(process: Any) -> Optional[float]:
    """
    Committed private memory, for /v1/status.

    Args:
        process: A psutil.Process

    Returns:
        Megabytes, or None where the platform does not report it
    """
    return read_memory_mb(process)[1]


class MemoryTrail:
    """
    Writes one `[MEMORY]` line per window.

    Args:
        stats: Live statistics, read and never changed except for the GC window
        ticks_provider: Ticks written per symbol since the start
        log: Where a line goes, at INFO
        interval_seconds: The window
    """

    def __init__(
        self,
        stats: CollectorStats,
        ticks_provider: Callable[[], Dict[str, int]],
        log: Callable[[str], None],
        interval_seconds: float = INTERVAL_SECONDS
    ):
        self._stats = stats
        self._ticks_provider = ticks_provider
        self._log = log
        self._interval = interval_seconds
        self._process = psutil.Process()
        self._previous: Optional[Dict[str, Any]] = None

    async def run(self) -> None:
        """Take the first reading now, then write a line every interval until cancelled."""
        self.line()
        while True:
            await asyncio.sleep(self._interval)
            entry = self.line()
            if entry is not None:
                self._log("[MEMORY] " + json.dumps(entry))

    def line(self) -> Optional[Dict[str, Any]]:
        """
        Read the counters and describe the window since the last reading.

        Returns:
            The line's content, or None for the first reading, which only opens a
            window
        """
        now = self._snapshot()
        previous, self._previous = self._previous, now
        if previous is None:
            return None

        rss, private = read_memory_mb(self._process)
        worst = self._worst_stall_since(previous["wall"])
        outages = None
        if now["outages"] is not None:
            outages = now["outages"] - (previous["outages"] or 0)

        return {
            "schema": 1,
            "window_s": round(now["mono"] - previous["mono"], 1),
            "rss_mb": rss,
            "private_mb": private,
            "threads": self._threads(),
            "gc": {
                "collections": [a - b for a, b in zip(now["gc_counts"], previous["gc_counts"])],
                "ms": round(now["gc_ms"] - previous["gc_ms"], 1),
                "longest_ms": now["gc_longest"],
                "threshold": list(gc.get_threshold()),
            },
            "loop": {
                "over_500ms": now["over_500ms"] - previous["over_500ms"],
                "worst_stall_ms": worst.ms if worst else None,
                "worst_stall_cause": worst.cause if worst else None,
            },
            "outages": outages,
            "ticks": {symbol: count - previous["ticks"].get(symbol, 0)
                      for symbol, count in sorted(now["ticks"].items())},
        }

    def _snapshot(self) -> Dict[str, Any]:
        """
        The counters a window is the difference of.

        Returns:
            One reading
        """
        totals = self._stats.reconnect_totals
        return {
            "mono": time.monotonic(),
            "wall": datetime.now(timezone.utc),
            "gc_counts": list(self._stats.gc.collections),
            "gc_ms": self._stats.gc.total_ms,
            "gc_longest": self._stats.take_gc_window_max(),
            "over_500ms": self._stats.loop_lag.over_500ms,
            "outages": totals.count if totals is not None else None,
            "ticks": dict(self._ticks_provider()),
        }

    def _worst_stall_since(self, start: datetime) -> Any:
        """
        The longest recorded stall inside the window.

        Args:
            start: When the window opened

        Returns:
            A StallEvent, or None when none was recorded
        """
        inside = [s for s in list(self._stats.stalls) if s.at >= start]
        return max(inside, key=lambda s: s.ms) if inside else None

    def _threads(self) -> Optional[int]:
        """
        Thread count, or None when the platform will not say.

        Returns:
            Threads of this process
        """
        try:
            return self._process.num_threads()
        except Exception:
            return None
