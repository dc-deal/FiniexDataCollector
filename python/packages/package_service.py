"""
FiniexDataCollector - Package Service
Serve the package API and pack on a weekly schedule, in one process.

The process lowers its own priority at start. It shares a four-core box with the
collector, and packing a week is seconds of Deflate and SHA-256 over hundreds of
megabytes: whatever the collector needs in those seconds, it gets first. Packing
runs in a worker thread, so the API keeps answering while it does.

Location: python/packages/package_service.py
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import psutil
import uvicorn

from python.packages.package_api import create_package_api
from python.packages.package_store import PackageStore
from python.packages.package_tokens import load_package_tokens
from python.packages.packer import pack_source
from python.utils.config_loader import PackagesConfig
from python.utils.logging_setup import describe_exception, get_logger

WEEKDAYS = ("monday", "tuesday", "wednesday",
            "thursday", "friday", "saturday", "sunday")


def next_run(now: datetime, weekday: str, hour: int, minute: int) -> datetime:
    """
    The next weekly run strictly after now, UTC.

    Args:
        now: The current time, UTC
        weekday: Day name
        hour: Hour, UTC
        minute: Minute

    Returns:
        The moment of the next run
    """
    now = now.astimezone(timezone.utc)
    target_day = WEEKDAYS.index(weekday)
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    candidate += timedelta(days=(target_day - now.weekday()) % 7)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate


def lower_priority() -> str:
    """
    Make this process yield to everything at normal priority.

    Returns:
        What was set, for the log
    """
    process = psutil.Process()
    try:
        if hasattr(psutil, "BELOW_NORMAL_PRIORITY_CLASS"):
            process.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
            return "below normal"
        process.nice(10)
        return "nice 10"
    except (psutil.AccessDenied, OSError) as e:
        return f"unchanged ({describe_exception(e)})"


def pack_all(
    config: PackagesConfig,
    store: PackageStore,
    packer: Dict[str, Any],
    log: Callable[[str], None],
    only: Optional[str] = None,
    cutoff: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """
    One run over every configured source, one after the other.

    Args:
        config: The packages block
        store: The package store
        packer: Version and commit, copied into each manifest
        log: Where progress lines go
        only: Restrict to one source
        cutoff: Override the default cutoff (today's 00:00 UTC)

    Returns:
        Each source's run record
    """
    results = []
    for name, source in config.sources.items():
        if only and name != only:
            continue
        results.append(pack_source(
            name, source, store, config.min_free_percent,
            cutoff=cutoff, packer=packer, log=log))
    return results


class PackageService:
    """The API and the weekly schedule."""

    def __init__(self, config: PackagesConfig, packer: Dict[str, Any]):
        """
        Args:
            config: The packages block
            packer: Version and commit of this program
        """
        self._config = config
        self._packer = packer
        self._store = PackageStore(Path(config.output_dir))
        self._logger = get_logger("FiniexPackages")
        self._started_at = datetime.now(timezone.utc)
        self._next_run: Optional[datetime] = None
        self._running = False

    def status(self) -> Dict[str, Any]:
        """The service's own state, for the API."""
        return {
            "started_at": self._started_at.isoformat(timespec="seconds"),
            "next_run": self._next_run.isoformat() if self._next_run else None,
            "packing": self._running,
            "packer": self._packer,
        }

    async def _schedule(self) -> None:
        """Sleep to the next weekly run, pack, repeat."""
        while True:
            self._next_run = next_run(datetime.now(timezone.utc), self._config.weekday,
                                      self._config.hour, self._config.minute)
            self._logger.info(
                f"[PACKAGE] next run {self._next_run.isoformat()}")
            # Sleeping in slices rather than once: a sleep across a clock correction
            # or a suspended VM wakes at the wrong time, and a slice re-reads it.
            while datetime.now(timezone.utc) < self._next_run:
                remaining = (self._next_run -
                             datetime.now(timezone.utc)).total_seconds()
                await asyncio.sleep(max(1.0, min(remaining, 300.0)))
            await self.run_once()

    async def run_once(self) -> List[Dict[str, Any]]:
        """One packing run, off the event loop."""
        self._running = True
        try:
            return await asyncio.to_thread(
                pack_all, self._config, self._store, self._packer, self._logger.info)
        except Exception as e:
            self._logger.error(
                f"[PACKAGE] run failed: {describe_exception(e)}")
            return []
        finally:
            self._running = False

    async def serve(self) -> None:
        """Run the API and the schedule until stopped."""
        self._logger.info(f"[PACKAGE] priority {lower_priority()}")
        registry = load_package_tokens(self._config.tokens)
        if registry.is_empty():
            self._logger.warning(
                "[PACKAGE] no tokens configured - every gated route refuses")
        app = create_package_api(self._store, registry, self.status,
                                 version=self._packer.get("version", ""))
        server = uvicorn.Server(uvicorn.Config(
            app, host=self._config.host, port=self._config.port,
            log_level="warning", access_log=False))
        schedule = asyncio.create_task(self._schedule())
        try:
            await server.serve()
        finally:
            schedule.cancel()
