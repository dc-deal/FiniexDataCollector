"""
FiniexDataCollector - Package Service
Serve the package API, pack on a weekly schedule, and keep the folders in order -
in one process.

The process lowers its own priority at start. It shares a four-core box with the
collector, and packing a week is seconds of Deflate and SHA-256 over hundreds of
megabytes: whatever the collector needs in those seconds, it gets first. Packing
and housekeeping run in worker threads, so the API keeps answering.

Every few minutes the housekeeping settles the folders (lifecycle.py), takes the
verdict, and warns: by Telegram to the operator, at most once a day per reason,
and by withholding the ping to the outside check, which is how FiniexTestingIDE's
basic check learns of it. The service only ever SENDS to Telegram; the collector
polls the same bot for commands, and a second poller would take its updates.

Location: python/packages/package_service.py
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import psutil
import uvicorn

from python.alerts.base import Alert, AlertLevel
from python.alerts.heartbeat import Heartbeat
from python.alerts.telegram_bot import TelegramAlertProvider
from python.packages import lifecycle
from python.packages.ledger import Ledger
from python.packages.package_api import create_package_api
from python.packages.package_store import PackageStore, PackLock, PackLockHeld
from python.packages.package_tokens import load_package_tokens
from python.packages.packer import pack_source
from python.types.collector_stats import CollectorStats
from python.utils.config_loader import PackagesConfig, TelegramConfig
from python.utils.logging_setup import describe_exception, get_logger

WEEKDAYS = ("monday", "tuesday", "wednesday",
            "thursday", "friday", "saturday", "sunday")

HOUSEKEEPING_SECONDS = 600
ALERT_REPEAT = timedelta(days=1)


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
    One run over every configured source, one after the other. Each package's
    originals go to stage A as soon as it is committed.

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
            cutoff=cutoff, packer=packer, log=log,
            originals_root=Path(config.originals_dir)))
    return results


def alert_text(reason: str, verdict: Dict[str, Any], config: PackagesConfig,
               space: Dict[str, Any], runs: Dict[str, Any]) -> str:
    """What the operator reads on the phone for one reason."""
    if reason == "disk_low":
        return (f"The disk has {space['disk']['free_percent']} % free, below "
                f"{config.min_free_percent:.0f} %. Purge confirmed originals: "
                f"POST /v1/packages/purge, or `python -m python.packages purge --apply`.")
    if reason == "receipt_overdue":
        return (f"Not confirmed by the consumer for more than {config.receipt_overdue_days} "
                f"days: {', '.join(verdict.get('overdue', []))}.")
    if reason == "disposable_large":
        return (f"Confirmed originals take {space['disposable']['bytes'] / 1e9:.1f} GB "
                f"(warning at {config.disposable_warn_gb:.0f} GB). Time to purge.")
    wanted = "failed" if reason == "last_run_failed" else "refused"
    which = [f"{name}: {run.get('reason', '')}" for name, run in runs.items()
             if run.get("result") == wanted and name in config.sources]
    return f"The last packing run {wanted}. " + " | ".join(which)


class PackageService:
    """The API, the weekly schedule and the housekeeping."""

    def __init__(self, config: PackagesConfig, packer: Dict[str, Any],
                 telegram: Optional[TelegramConfig] = None):
        """
        Args:
            config: The packages block
            packer: Version and commit of this program
            telegram: The bot to warn through; sending only, never polling
        """
        self._config = config
        self._packer = packer
        self._store = PackageStore(Path(config.output_dir))
        self._ledger = Ledger(self._store.root)
        self._logger = get_logger("FiniexPackages")
        self._started_at = datetime.now(timezone.utc)
        self._next_run: Optional[datetime] = None
        self._running = False
        self._stats = CollectorStats()
        self._warned: Dict[str, datetime] = {}
        self._telegram: Optional[TelegramAlertProvider] = None
        if telegram and telegram.enabled and telegram.bot_token and telegram.chat_id:
            self._telegram = TelegramAlertProvider(
                bot_token=telegram.bot_token, chat_id=telegram.chat_id, enabled=True,
                send_on_error=True, send_on_rotation=False, send_weekly_report=False)

    def status(self) -> Dict[str, Any]:
        """The service's own state, for the API."""
        return {
            "started_at": self._started_at.isoformat(timespec="seconds"),
            "next_run": self._next_run.isoformat() if self._next_run else None,
            "packing": self._running,
            "packer": self._packer,
            "heartbeat": {"enabled": bool(self._config.heartbeat_url),
                          "sent": self._stats.heartbeat.sent,
                          "withheld": self._stats.heartbeat.withheld,
                          "failed": self._stats.heartbeat.failed},
        }

    def health(self) -> Dict[str, Any]:
        """The verdict the heartbeat and the alerts go by."""
        return lifecycle.health(self._config, self._store)

    # ----------------------------------------------------------- schedule

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
            await self.housekeeping()

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

    # ----------------------------------------------------------- housekeeping

    def _seed_ledger(self) -> None:
        """
        Give every package already committed its `packed` line - those built before
        the ledger existed, and any whose line a failed write lost.

        Per collector, under that collector's packing lock: a run committing a
        package while the seed looks would otherwise be booked twice. A collector
        whose lock is held is left to its run, which seeds itself.
        """
        for name in self._config.sources:
            try:
                with PackLock(self._store.folder(name)):
                    late = self._ledger.seed(self._store.manifests(name))
                if late:
                    self._logger.info(
                        f"[LEDGER] entered late: {', '.join(late)}")
            except PackLockHeld:
                continue
            except OSError as e:
                self._logger.error(
                    f"[LEDGER] {name}: could not seed: {describe_exception(e)}")

    def settle_one(self, collector: str) -> None:
        """After a receipt: move that collector's confirmed package on now."""
        source = self._config.sources.get(collector)
        if source is None:
            return
        try:
            lifecycle.settle_source(collector, source, self._store,
                                    Path(self._config.originals_dir), self._ledger,
                                    self._logger.info)
        except Exception as e:
            self._logger.error(
                f"[LIFECYCLE] {collector}: {describe_exception(e)}")

    async def housekeeping(self) -> None:
        """Settle the folders, take the verdict, warn."""
        try:
            await asyncio.to_thread(lifecycle.settle, self._config, self._store,
                                    self._ledger, self._logger.info)
        except Exception as e:
            self._logger.error(
                f"[LIFECYCLE] settle failed: {describe_exception(e)}")
        verdict = self.health()
        if verdict["reasons"]:
            self._logger.warning(
                f"[HEALTH] degraded: {', '.join(verdict['reasons'])}")
        await self._warn(verdict)

    async def _warn(self, verdict: Dict[str, Any]) -> None:
        """Telegram, once a day per reason that holds."""
        if self._telegram is None:
            return
        now = datetime.now(timezone.utc)
        space = lifecycle.storage(self._config)
        runs = self._store.last_runs()
        for reason in verdict["reasons"]:
            last = self._warned.get(reason)
            if last is not None and now - last < ALERT_REPEAT:
                continue
            alert = Alert(level=AlertLevel.WARNING, title=f"Tick packages: {reason}",
                          message=alert_text(
                              reason, verdict, self._config, space, runs),
                          timestamp=now, source="FiniexPackages")
            try:
                if await self._telegram.send_alert(alert):
                    self._warned[reason] = now
            except Exception as e:
                self._logger.warning(
                    f"[HEALTH] could not warn: {describe_exception(e)}")
        for reason in list(self._warned):
            if reason not in verdict["reasons"]:
                del self._warned[reason]

    async def _housekeeping_loop(self) -> None:
        """Housekeeping at start and every few minutes after."""
        while True:
            await self.housekeeping()
            await asyncio.sleep(HOUSEKEEPING_SECONDS)

    # ----------------------------------------------------------- serve

    async def serve(self) -> None:
        """Run the API, the schedule, the housekeeping and the heartbeat until stopped."""
        self._logger.info(f"[PACKAGE] priority {lower_priority()}")
        registry = load_package_tokens(self._config.tokens)
        if registry.is_empty():
            self._logger.warning(
                "[PACKAGE] no tokens configured - every gated route refuses")
        self._seed_ledger()
        app = create_package_api(self._store, registry, self.status, self._ledger,
                                 self._config, on_receipt=self.settle_one,
                                 version=self._packer.get("version", ""))
        server = uvicorn.Server(uvicorn.Config(
            app, host=self._config.host, port=self._config.port,
            log_level="warning", access_log=False))
        tasks = [asyncio.create_task(self._schedule()),
                 asyncio.create_task(self._housekeeping_loop())]
        if self._config.heartbeat_url:
            tasks.append(asyncio.create_task(Heartbeat(
                url=self._config.heartbeat_url,
                interval_seconds=self._config.heartbeat_interval_seconds,
                health_provider=self.health, stats=self._stats).run()))
        try:
            await server.serve()
        finally:
            for task in tasks:
                task.cancel()
