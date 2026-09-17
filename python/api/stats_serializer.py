"""
FiniexDataCollector - Stats Serialization
Turns the live CollectorStats object into JSON the API can answer with.

`CollectorStats` is built for the terminal display: it holds datetimes, nested
dataclasses and a few computed properties. None of that survives `json.dumps` on
its own, and a route that reached into it field by field would drift the moment a
field is added - which is how a status endpoint ends up describing last month's
collector.

So the conversion is structural rather than enumerated: dataclasses become dicts,
datetimes become UTC ISO strings, and a new field appears in the payload without
anyone editing this file. The two exclusions are deliberate and named below.

Location: python/api/stats_serializer.py
"""

import psutil

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from python.types.collector_stats import CollectorStats

# Display knobs, not measurements. They describe how much history the terminal
# keeps, which tells a consumer nothing about the collection.
NOT_METRICS = ("max_recent_logs", "max_reconnect_history")


def _plain(value: Any) -> Any:
    """
    Convert one value into something json.dumps accepts.

    Args:
        value: Any member of the stats tree

    Returns:
        The same information as JSON-native types
    """
    if isinstance(value, datetime):
        # Naive values would serialize without an offset and read as local time
        # at the far end - the failure this project already paid for once.
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="seconds")

    if is_dataclass(value) and not isinstance(value, type):
        return {key: _plain(inner) for key, inner in asdict(value).items()}

    if isinstance(value, dict):
        return {str(key): _plain(inner) for key, inner in value.items()}

    if isinstance(value, (list, tuple)):
        return [_plain(inner) for inner in value]

    return value


def serialize_stats(stats: CollectorStats) -> Dict[str, Any]:
    """
    Render the full live metrics.

    Args:
        stats: The collector's live statistics object

    Returns:
        JSON-native representation of every measurement it holds
    """
    payload = {
        name: _plain(value)
        for name, value in vars(stats).items()
        if not name.startswith("_") and name not in NOT_METRICS
    }

    # Computed on DiskSpaceStats and therefore absent from asdict(), while being
    # the part a monitor actually acts on.
    disk = stats.disk_space
    payload["disk_space"].update({
        "free_gb": round(disk.free_gb, 2),
        "total_gb": round(disk.total_gb, 2),
        "percent_free": round(disk.percent_free, 1),
        "status": disk.status
    })

    payload["uptime_seconds"] = uptime_seconds(stats)
    payload["process"] = process_resources()
    return payload


def process_resources() -> Dict[str, Any]:
    """
    What this process is costing the machine, sampled now.

    Collected state lives in `CollectorStats`; this does not, because it is a
    reading rather than a record - the same reason `disk_space` computes its
    derived fields here instead of storing them.

    It exists because the box is shared. Three services run on 8 GB with roughly
    2.4 GB of headroom, and the sister project found its own documented figure
    stale by 380 MB once it measured instead of remembering. A collector that
    runs for weeks is exactly where a slow leak hides, and from a remote session
    this is the only way to see one without a shell on the machine.

    Never raises: a diagnostic that can fail the route carrying it would cost
    more than it reports.

    Returns:
        Resident memory, thread and socket counts and consumed CPU time, or
        `available: false` when psutil cannot say
    """
    try:
        process = psutil.Process()
        with process.oneshot():
            times = process.cpu_times()
            return {
                "available": True,
                "rss_mb": round(process.memory_info().rss / (1024 * 1024), 1),
                "threads": process.num_threads(),
                "open_sockets": _socket_count(process),
                "cpu_seconds": round(times.user + times.system, 1)
            }
    except Exception:
        return {"available": False}


def _socket_count(process: psutil.Process) -> Optional[int]:
    """
    Open network connections, or None when the platform will not say.

    Windows refuses this for a process without the rights to ask, and a refusal
    must not be reported as `0` - zero reads as "none open", which is a
    measurement, while None says nothing was measured.

    Args:
        process: The process to ask

    Returns:
        The count, or None when it could not be determined
    """
    try:
        return len(process.net_connections())
    except Exception:
        return None


def uptime_seconds(stats: CollectorStats) -> int:
    """
    Seconds since collection started.

    Args:
        stats: The collector's live statistics object

    Returns:
        Whole seconds of uptime
    """
    started = stats.start_time
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)

    return int((datetime.now(timezone.utc) - started).total_seconds())


def health_payload(stats: CollectorStats) -> Dict[str, Any]:
    """
    The open route's answer: is it alive, since when, is it connected.

    Deliberately without symbol names, tick rates or file names. Those describe
    what is being traded and how much of it, which an uptime probe does not need
    and an unauthenticated caller should not receive.

    Args:
        stats: The collector's live statistics object

    Returns:
        Liveness payload
    """
    return {
        "status": "ok" if stats.websocket_status == "connected" else "degraded",
        "websocket_status": stats.websocket_status,
        "uptime_seconds": uptime_seconds(stats),
        "started_at": _plain(stats.start_time)
    }
