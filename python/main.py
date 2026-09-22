"""
FiniexDataCollector - Main Entry Point
CLI and daemon mode for tick data collection.

Usage:
    python main.py collect              # Start collectors
    python main.py status               # Show collector status

Location: python/main.py
"""

import argparse
import asyncio
import json
import signal
import sys
import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import psutil

from python.api.api_app import create_api
from python.api.build_info import sample_build_info
from python.api.stats_serializer import health_payload, serialize_stats
from python.api.token_loader import load_token_registry
from python.collectors.kraken.quote_cache import QuoteCache
from python.utils.collection_clock import CollectionClock
from python.utils.instance_identity import (
    PRODUCER,
    collected_on,
    mint_or_read
)
from python.utils.instance_lock import InstanceLock
from python.utils.config_loader import ConfigLoader, AppConfig
from python.utils.logging_setup import (add_log_listener, describe_exception,
                                        get_logger, setup_logging)
from python.utils.console_mode import (disable_quick_edit, enable_ansi_colours,
                                       enable_ctrl_c_handling)
from python.viewer.endpoints import EndpointError, default_interval, load_endpoint
from python.viewer.watch import run_viewer
from python.utils.gc_watcher import GcWatcher
from python.utils.live_display import LiveDisplay
from python.types.collector_stats import CollectorStats
from python.types.tick_types import OriginBlock
from python.types.broker_config_types import BrokerConfig, normalize_symbol
from python.exceptions.collector_exceptions import ConfigurationError
from python.collectors.kraken.websocket_client import KrakenWebSocketClient
from python.writers.json_tick_writer import (
    JsonTickWriter,
    recover_orphaned_buffers
)
from python.alerts.telegram_bot import TelegramAlertProvider
from python.scheduler.weekly_jobs import WeeklyJobScheduler


# Statuses a connection can come back FROM. "failed" belongs here: a connect
# attempt that failed and later succeeded is a reconnect, and the production log
# carried eight of those alongside the forced ones.
RECONNECT_FROM = ("disconnected", "reconnecting", "failed")


def is_reconnect(old_status: str, new_status: str) -> bool:
    """
    Decide whether a status change is a completed reconnect.

    Named rather than inlined because it was wrong and silent: the collector
    reported zero reconnects for 173 of them, and an inline condition offers
    nothing to test.

    Args:
        old_status: Status before the change
        new_status: Status after the change

    Returns:
        True when the connection has just come back
    """
    return new_status == "connected" and old_status in RECONNECT_FROM


def reconnect_alert_text(duration_seconds: float, recent_within_hour: int,
                         min_seconds: float, cluster_size: int) -> Optional[str]:
    """
    Decide whether a restored connection is worth a phone alert, and say what.

    Measured over the night of 2026-09-21: seven reconnects, each between 1.3
    and 7.2 s, adding up to 19.9 s of downtime in fifteen hours. That is about
    22 ticks, and none of the seven appeared among the eight longest gaps in the
    file they fell into - those were all quiet market. Six alerts arrived on the
    operator's phone for something that cannot be found in the data afterwards.

    An alert nobody can act on is not free: it trains its reader to swipe, and
    the next one that mattered is swiped with it. So a short, self-healed
    reconnect stays in the log and on `/v1/status`, where it is already
    complete, and two things still reach the phone:

    - **A long outage**, because past the consumer's lag window a whole file is
      refused rather than shortened.
    - **A cluster**, because a host that blips every two hours is weather and one
      that blips four times an hour is degrading - and that difference is the
      only thing a short reconnect can still tell anybody.

    Args:
        duration_seconds: How long the connection was gone
        recent_within_hour: Reconnects in the last hour, this one included
        min_seconds: Outage length that is worth an alert on its own
        cluster_size: Reconnects per hour that are worth one regardless

    Returns:
        The message body, or None when this one belongs in the log alone
    """
    if duration_seconds >= min_seconds:
        return (f"WebSocket reconnected after {duration_seconds:.1f}s down - "
                f"past the {min_seconds:.0f}s mark where a file is at risk")

    if recent_within_hour >= cluster_size:
        return (f"{recent_within_hour} reconnects in the last hour, the latest "
                f"after {duration_seconds:.1f}s. Short ones are normal on this "
                f"host; this many in an hour is not")

    return None


def validate_symbols(symbols: List[str]) -> None:
    """
    Validate symbols list for duplicates.

    Args:
        symbols: List of symbols from config

    Raises:
        ConfigurationError: If duplicates found
    """
    normalized = [normalize_symbol(s) for s in symbols]
    seen = set()
    duplicates = []

    for s in normalized:
        if s in seen:
            duplicates.append(s)
        seen.add(s)

    if duplicates:
        unique_dups = list(set(duplicates))
        raise ConfigurationError(
            f"Duplicate symbols in config: {', '.join(unique_dups)}. "
            f"Remove duplicates from kraken.symbols in app_config.json",
            config_file="app_config.json"
        )


def count_files_in_folder(folder_path: Path, pattern: str) -> int:
    """
    Count matching files in a folder (non-recursive).

    The pattern is required rather than defaulted, and that is the whole point.
    Counting every entry made the archive number include the open write-ahead
    logs, so a fresh start reported one "file" per symbol before a single
    archive file existed. Narrowing it to `*_ticks.json` fixed that and set the
    log count to zero, because this same function counts the log folder too - a
    default that suited three of four callers is what made the second mistake
    invisible. Now every caller says what it is counting.

    Args:
        folder_path: Path to folder
        pattern: Glob the files must match, e.g. `*_ticks.json` or `*.log`

    Returns:
        Number of matching files
    """
    if not folder_path.exists():
        return 0

    try:
        return sum(1 for item in folder_path.glob(pattern) if item.is_file())
    except Exception:
        return 0


def get_folder_size(folder_path: Path) -> int:
    """
    Calculate total size of folder in bytes (recursive).

    Args:
        folder_path: Path to folder

    Returns:
        Total size in bytes
    """
    if not folder_path.exists():
        return 0

    total = 0
    try:
        for entry in os.scandir(folder_path):
            if entry.is_file(follow_symlinks=False):
                total += entry.stat().st_size
            elif entry.is_dir(follow_symlinks=False):
                total += get_folder_size(Path(entry.path))
    except Exception:
        pass

    return total


async def serve_status_api(server: Any, logger: Any) -> None:
    """
    Run the status API, and never let its failure reach the collection.

    uvicorn calls `sys.exit()` when it cannot bind - measured 2026-09-17 as
    `SystemExit(3)` against a port already in use. `SystemExit` derives from
    `BaseException`, so the `except Exception` that used to guard this escaped
    it: the exception left the task, asyncio cancelled everything else, and the
    collector died at startup over its own diagnostic surface. It happened here
    when a development container held port 8110.

    A collector that collects without reporting is worth more than one that
    reports nothing because it is not running.

    Args:
        server: The configured `uvicorn.Server`
        logger: Where the failure is recorded
    """
    try:
        await server.serve()
    except SystemExit as e:
        logger.error(
            f"Status API could not start (exit {e.code}) - the port is most "
            f"likely already in use. Collection continues without it.")
    except Exception as e:
        logger.error(f"Status API stopped: {describe_exception(e)}")


# What CPython's proactor loop - Windows, 3.13 and 3.14 alike - hands the loop's
# exception handler when accept() raises, immediately before it closes the
# listening socket for good (BaseProactorEventLoop._start_serving). uvicorn is
# not told: serve() keeps running, nothing listens, and the edge answers 502.
ACCEPT_FAILED = "Accept failed on a socket"

# The repository root, which is what `python -m python.writers.wal_archive`
# needs as a working directory - derived from this file rather than from where
# the collector happens to be started.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Exit code for a failure a restart cannot fix: a malformed configuration, or an
# output directory another live collector already owns. Everything else exits 1,
# where a service manager's default restart is the right response.
EXIT_CONFIGURATION = 2

# A file of 50,000 ticks took about 4 s to write on the production box. A child
# still running after this has hung; its log stays on disk and the next start
# recovers it, so the ceiling costs nothing but the wait.
EXPORT_TIMEOUT_SECONDS = 300

# How long a graceful stop waits for the archive writers it started. Their logs
# are on disk either way, so this is about leaving a tidy archive, not about
# keeping the data.
EXPORT_SHUTDOWN_WAIT_SECONDS = 60

# Ten samples a second: fine enough to catch a stall, far too little to matter.
LOOP_LAG_INTERVAL_SECONDS = 0.1

# Above this, a stall is written down with what was going on around it. Low
# enough to catch what a consumer would notice, high enough that ordinary
# scheduling jitter does not fill the list.
STALL_ATTRIBUTION_MS = 250


def report_loop_exception(logger: Any) -> Callable[[Any, Dict[str, Any]], None]:
    """
    Build the event loop's exception handler.

    asyncio reports through the standard `logging` module, which this project
    never connects, so without this the one event that silently kills the
    status API went to stderr and never reached the log file. Everything else
    keeps asyncio's default handling.

    Args:
        logger: Where the accept failure is recorded

    Returns:
        A handler for loop.set_exception_handler
    """
    def handler(loop: Any, context: Dict[str, Any]) -> None:
        if context.get("message") == ACCEPT_FAILED:
            error = context.get("exception")
            cause = (describe_exception(error) if error
                     else "no exception given")
            logger.error(
                f"Status API stopped listening: asyncio closed its socket after "
                f"an accept failure ({cause}). It stays unreachable until the "
                f"collector restarts; collection is unaffected.")
            return
        loop.default_exception_handler(context)

    return handler


class FiniexDataCollector:
    """
    Main application class.

    Orchestrates collectors, writers, alerts, scheduler, and monitoring.
    """

    def __init__(self, config: AppConfig, show_display: bool = True):
        """
        Initialize application.

        Args:
            config: Application configuration
        """
        self._config = config
        self._logger = get_logger("FiniexDataCollector")

        # Components
        self._collectors = []
        self._writers = {}
        self._clock: Optional[CollectionClock] = None
        self._telegram: Optional[TelegramAlertProvider] = None
        self._instance_lock: Optional[InstanceLock] = None
        self._last_reconnect_monotonic: Optional[float] = None
        self._origin: Optional[OriginBlock] = None
        self._show_display = show_display
        self._api_server = None
        self._scheduler: Optional[WeeklyJobScheduler] = None

        # Live monitoring. The error and warning counters follow the log: every
        # WARNING and ERROR line counts, whoever wrote it.
        self._stats = CollectorStats()
        add_log_listener(self._stats.record_logged)
        self._live_display: Optional[LiveDisplay] = None

        # Times every garbage collection, so a loop stall can name its cause
        # instead of being reasoned about afterwards.
        self._gc_watcher = GcWatcher(self._stats)

        # State
        self._is_running = False
        self._shutdown_event = asyncio.Event()

        # Track first tick per symbol for logging
        self._first_tick_logged = set()

        # Monitoring tasks
        self._monitoring_tasks = []

        # Archive writers running as subprocesses. Held so a graceful stop can
        # wait for them: their logs are still on disk, so nothing is lost if it
        # cannot, but a file finished after the process it belongs to has gone
        # reads like a file nobody wrote.
        self._export_tasks: set = set()

        # Reconnect tracking
        self._disconnect_time: Optional[datetime] = None
        self._last_reconnect_alert: Optional[datetime] = None

    async def start_collection(self) -> None:
        """Start all configured collectors."""
        self._logger.info("=" * 60)
        self._logger.info("FiniexDataCollector Starting")
        self._logger.info("=" * 60)

        # Setup signal handlers
        self._setup_signal_handlers()
        self._gc_watcher.start()
        asyncio.get_running_loop().set_exception_handler(
            report_loop_exception(self._logger))

        # Claim the output directory FIRST, before anything with a side effect.
        #
        # Two reasons, and the second is what moved this call up here. Recovery
        # cannot distinguish a crashed run's write-ahead log from a running
        # instance's, and on Linux it would delete the live one - that ordering
        # is why the lock exists at all.
        #
        # But it used to be taken after Telegram, the scheduler, the monitoring
        # tasks and the API bind. A second instance therefore announced "Collector
        # Started" to the operator's phone, started a scheduler and reached for
        # port 8110, and only then discovered it was not allowed to run. Under a
        # service manager that restarts on exit, that is one phone alert per
        # restart cycle for as long as the conflict lasts.
        self._instance_lock = InstanceLock(Path(self._config.paths.raw_data_dir))
        self._instance_lock.acquire()

        # Initialize Telegram alerts
        if self._config.telegram.enabled:
            self._telegram = TelegramAlertProvider(
                bot_token=self._config.telegram.bot_token,
                chat_id=self._config.telegram.chat_id,
                enabled=True,
                send_on_error=self._config.telegram.send_on_error,
                send_on_rotation=self._config.telegram.send_on_rotation,
                send_weekly_report=self._config.telegram.send_weekly_report
            )

            if await self._telegram.test_connection():
                # Set report callback for /report command
                self._telegram.set_report_callback(self._send_weekly_report)

                # Start command polling
                self._telegram.start_command_polling()

                await self._telegram.send_info(
                    "Collector Started",
                    f"FiniexDataCollector started with {len(self._config.kraken.symbols)} symbols"
                )

        # Initialize scheduler
        self._scheduler = WeeklyJobScheduler(self._config.scheduler)
        self._scheduler.set_report_callback(self._send_weekly_report)
        self._scheduler.start()

        # Start monitoring tasks
        await self._start_monitoring_tasks()

        # Initialize Kraken collector
        if self._config.kraken.enabled:
            await self._start_kraken_collector()

        self._is_running = True

        # Status API, after the collectors so it answers about a running system
        if self._config.api.enabled:
            await self._start_status_api()

        # A console is a convenience, and a convenience must not be able to stop
        # the measurement: QuickEdit suspends the next write while text is
        # selected, and this display writes from the collector's only event loop.
        quick_edit = disable_quick_edit()
        if quick_edit is False:
            self._logger.warning(
                "Could not turn QuickEdit off on this console. A click in the "
                "window can suspend collection - use --no-display, or uncheck "
                "QuickEdit in the window's properties.")

        # Whether or not a display follows: the log writes colour codes, and a
        # Windows console shows them as text until this is on. With the display
        # it happened by accident, because rich switches it on when it takes the
        # console over; without it the operator read "<-[37mINFO" for an hour.
        if enable_ansi_colours() is False:
            self._logger.warning(
                "This console will not interpret colour codes; log lines here "
                "will carry escape sequences. The log file is unaffected.")

        # Start live display. Off is a supported way to run: the file log is the
        # record, and that is what a service-wrapped instance uses.
        if self._show_display:
            self._live_display = LiveDisplay(self._stats)
            await self._live_display.start()
        else:
            self._logger.info("Live display off - the file log keeps the record")

        # Wait for shutdown
        await self._shutdown_event.wait()

        # Cleanup
        await self._shutdown()

    def _establish_identity(self) -> OriginBlock:
        """
        The identity every file this process writes will carry.

        Minted once at the data root and read on every later start; a process
        that can establish neither refuses to run rather than write files nobody
        can attribute.

        Cached rather than rebuilt, because the writers and the status API state
        the same identity to two different audiences - and two construction
        sites are two things that can come to disagree.

        Returns:
            This process's origin block
        """
        if self._origin is None:
            self._origin = OriginBlock(
                instance_id=mint_or_read(
                    Path(self._config.paths.raw_data_dir)),
                collected_on=collected_on(),
                producer=PRODUCER,
                producer_version=self._config.version
            )
            self._logger.info(
                f"Instance {self._origin.instance_id} "
                f"on {self._origin.collected_on}")

        return self._origin

    async def _start_status_api(self) -> None:
        """
        Serve the status API on the loopback interface.

        Runs as a task on the collector's own event loop. A failure here must
        never reach the collection: a status surface that takes the collector
        down with it is worse than no status surface.
        """
        import uvicorn

        from python.types.tick_types import DATA_FORMAT_VERSION

        app = create_api(
            build=sample_build_info(self._config.version, DATA_FORMAT_VERSION),
            health_provider=lambda: health_payload(self._stats),
            detail_provider=lambda: serialize_stats(self._stats),
            registry=load_token_registry(self._config.api.tokens),
            # Established here as well as in the collector path, because the API
            # can be enabled without the Kraken collector. The call is idempotent
            # and returns what the collector already established.
            origin=self._establish_identity(),
            # The effective configuration, after the user overlay is merged.
            # Secrets are stripped in the route, not here - the redaction rule
            # belongs where it can be tested against a planted credential.
            config_provider=lambda: self._config.model_dump(mode="json"),
            raw_data_dir=Path(self._config.paths.raw_data_dir),
            log_dir=Path(self._config.paths.logs_dir)
        )

        self._api_server = uvicorn.Server(uvicorn.Config(
            app,
            host=self._config.api.host,
            port=self._config.api.port,
            log_level="warning",
            access_log=False,
            # Without a bound, uvicorn waits for in-flight connections forever -
            # and while it waits it still holds the SIGINT handler it installed,
            # so the collector's own shutdown never starts. A held-open download
            # would turn Ctrl+C into nothing visible happening at all.
            timeout_graceful_shutdown=5
        ))

        self._monitoring_tasks.append(asyncio.create_task(
            serve_status_api(self._api_server, self._logger)))
        self._logger.info(
            f"Status API on http://{self._config.api.host}:"
            f"{self._config.api.port}")

    def _export_archive(self, wal_path: Path, archive_path: Path) -> None:
        """
        Hand a closed file to a subprocess that writes it.

        Called by a writer from inside `write_tick`, so it must not block: it
        only starts a task. Writing the file here cost about 1 s plus 66 us per
        tick on the production box, on the collector's only event loop, nine
        times in a row at the UTC day cut - measured 2026-09-20.

        Args:
            wal_path: The write-ahead log holding the ticks and the closing state
            archive_path: The archive file that is owed
        """
        self._stats.record_export_started()
        task = asyncio.create_task(self._run_export(wal_path, archive_path))
        self._export_tasks.add(task)
        task.add_done_callback(self._export_tasks.discard)

    async def _run_export(self, wal_path: Path, archive_path: Path) -> None:
        """
        Run the archive writer for one file and report what it did.

        A failure is left where it is: the log stays on disk, and the next start
        recovers it. That is the whole reason the parent does not delete it.

        Args:
            wal_path: The write-ahead log to convert
            archive_path: The archive file it becomes
        """
        started = time.monotonic()
        name = archive_path.name

        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "python.writers.wal_archive",
                str(wal_path),
                cwd=str(PROJECT_ROOT),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=EXPORT_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            self._stats.record_export_failed(name)
            self._logger.error(
                f"The archive writer for {name} did not finish within "
                f"{EXPORT_TIMEOUT_SECONDS}s - its log stays on disk and the "
                f"next start recovers it")
            return
        except Exception as e:
            self._stats.record_export_failed(name)
            self._logger.error(
                f"Could not run the archive writer for {name}: "
                f"{describe_exception(e)} - its log stays on disk and the next "
                f"start recovers it")
            return

        duration_ms = (time.monotonic() - started) * 1000

        if process.returncode != 0:
            self._stats.record_export_failed(name)
            self._logger.error(
                f"The archive writer refused {name} "
                f"(exit {process.returncode}): "
                f"{stderr.decode('utf-8', 'replace').strip()[:200]} - its log "
                f"stays on disk and the next start recovers it")
            return

        ticks = 0
        try:
            ticks = int(json.loads(stdout.decode("utf-8")).get("ticks", 0))
        except Exception:
            # The file is written; only the count for the report is missing.
            pass

        self._stats.record_export_finished(name, ticks, duration_ms)
        self._logger.info(
            f"Exported {name} ({ticks:,} ticks) in {duration_ms:.0f} ms")

    async def _start_monitoring_tasks(self) -> None:
        """Start background monitoring tasks."""
        # Disk space monitoring
        disk_task = asyncio.create_task(
            self._monitor_disk_space()
        )
        self._monitoring_tasks.append(disk_task)

        # How late this loop runs. It is the instrument for the one defect a
        # tick file cannot show: a blocked loop stamps collected_msc late, and
        # the consuming importer rejects a whole file beyond 30 s of lag.
        lag_task = asyncio.create_task(self._monitor_loop_lag())
        self._monitoring_tasks.append(lag_task)

        # Folder scanning
        folder_task = asyncio.create_task(
            self._monitor_folders()
        )
        self._monitoring_tasks.append(folder_task)

        self._logger.info("Monitoring tasks started")

    async def _await_pending_exports(self) -> None:
        """
        Let the archive writers still running finish before the process ends.

        Nothing is lost if they do not: every one of them has its write-ahead
        log on disk, and the next start recovers it. What the wait buys is a
        stop that leaves the archive complete rather than a directory of logs
        somebody has to trust a later start with.
        """
        if not self._export_tasks:
            return

        pending = list(self._export_tasks)
        self._logger.info(
            f"Waiting for {len(pending)} archive writer(s) to finish")

        _finished, still_running = await asyncio.wait(
            pending, timeout=EXPORT_SHUTDOWN_WAIT_SECONDS)

        if still_running:
            self._logger.warning(
                f"{len(still_running)} archive writer(s) did not finish in "
                f"{EXPORT_SHUTDOWN_WAIT_SECONDS}s - their logs stay on disk and "
                f"the next start recovers them")

    async def _monitor_loop_lag(self) -> None:
        """
        Measure how late the event loop runs its own timers.

        Everything this collector does shares one loop, so a long piece of work
        anywhere delays the stamping of every tick that arrives meanwhile. That
        delay is invisible in a tick file - `collected_msc` simply reads later
        than it should - until it passes the consumer's 30 s window and costs
        the whole file. Ten samples a second cost nothing and make it visible
        on `/v1/status` from off the machine.
        """
        while self._is_running:
            asked_at = time.monotonic()

            # Read before the wait, subtract after: the difference is what the
            # tick handler cost inside THIS window and nothing else. A rate held
            # anywhere else would describe a window somebody else chose.
            work_before = self._stats.tick_work.total_ms
            ticks_before = self._stats.tick_work.ticks

            await asyncio.sleep(LOOP_LAG_INTERVAL_SECONDS)
            lateness = time.monotonic() - asked_at - LOOP_LAG_INTERVAL_SECONDS
            lateness_ms = lateness * 1000
            self._stats.record_loop_lag(lateness_ms)

            # A stall worth explaining is asked what else happened in its own
            # window, rather than attributed afterwards from reasoning. The
            # first such attribution was wrong: the folder scan was blamed and
            # then measured at 15 ms. The second left ten stalls saying
            # `unknown` because nothing was measuring the tick handler.
            if lateness_ms >= STALL_ATTRIBUTION_MS:
                gc_ms, generation = self._gc_watcher.time_spent_since(asked_at)
                self._stats.record_stall(
                    duration_ms=lateness_ms,
                    gc_ms=gc_ms,
                    gc_generation=generation,
                    render_ms=self._stats.scans.render_last_ms,
                    exports_in_flight=self._stats.exports.in_flight,
                    tick_ms=self._stats.tick_work.total_ms - work_before,
                    ticks=self._stats.tick_work.ticks - ticks_before,
                    reconnected=self._reconnected_between(asked_at))

    def _reconnected_between(self, since: float) -> bool:
        """
        Whether the socket came back inside the window that just ended.

        Compared on the MONOTONIC clock, not the wall clock. The reconnect
        events carry wall-clock stamps because a human reads them; correlating
        a stall against those would mean subtracting two readings that a clock
        correction can sit between, which this project treats as a defect rather
        than a style question.

        Args:
            since: The monotonic reading the window started at

        Returns:
            True when a reconnect completed inside it
        """
        last = self._last_reconnect_monotonic
        return last is not None and last >= since

    async def _monitor_disk_space(self) -> None:
        """Monitor disk space usage."""
        interval = self._config.monitoring.disk_space_check_interval_seconds

        self._logger.debug(
            f"[DISK_MONITOR] Started with interval={interval}s"
        )

        while self._is_running:
            try:
                # In a thread for the same reason as the folder scan: on the
                # production box a filesystem call is not free, and anything
                # slow here lands in the next tick's stamp.
                usage, duration_ms = await asyncio.to_thread(
                    self._read_disk_usage)
                self._stats.record_disk_check(duration_ms)

                self._stats.update_disk_space(
                    total=usage.total,
                    used=usage.used,
                    free=usage.free
                )

                percent_free = (usage.free / usage.total *
                                100) if usage.total > 0 else 0

                self._logger.debug(
                    f"[DISK_MONITOR] Status: "
                    f"free={usage.free / (1024**3):.1f}GB ({percent_free:.1f}%), "
                    f"total={usage.total / (1024**3):.1f}GB"
                )

                # Check for critical disk space
                if percent_free < 20 and self._telegram:
                    self._logger.debug(
                        f"[DISK_MONITOR] CRITICAL threshold reached, sending alert"
                    )
                    await self._telegram.send_error(
                        "🚨 Critical Disk Space",
                        f"Only {percent_free:.1f}% free ({usage.free / (1024**3):.1f} GB)\n"
                        f"Total: {usage.total / (1024**3):.1f} GB\n"
                        f"Used: {usage.used / (1024**3):.1f} GB"
                    )

            except Exception as e:
                self._logger.error(
                    f"Disk space check failed: {describe_exception(e)}")
                self._logger.debug(
                    f"[DISK_MONITOR] Exception details:", exc_info=True)

            await asyncio.sleep(interval)

    def _read_disk_usage(self) -> Tuple[Any, float]:
        """
        Read the disk usage of the data directory, in a worker thread.

        Returns:
            The psutil usage record and how long the reading took, in ms
        """
        started = time.monotonic()
        data_path = Path(self._config.paths.raw_data_dir).resolve()

        self._logger.debug(f"[DISK_MONITOR] Checking path: {data_path}")

        usage = psutil.disk_usage(str(data_path))

        return usage, (time.monotonic() - started) * 1000

    def _prepare_symbol_stats(self, symbol: str) -> None:
        """
        Put everything a screen needs about a symbol where a screen can read it.

        The entry exists from here rather than from the first tick, which also
        makes a configured symbol that never trades visible instead of absent.
        The digit count comes from the broker specification: two fixed places
        once rendered ADAUSD's 0.2103 and 0.2104 both as 0.21, a screen
        asserting a spread the book did not have.

        Args:
            symbol: Normalized symbol
        """
        symbol_stats = self._stats.get_symbol_stats(symbol)

        try:
            symbol_stats.digits = BrokerConfig.get_digits(symbol)
        except Exception as e:
            # A missing specification is not worth refusing to collect over; the
            # field stays None, which says "not known" rather than inventing 2.
            self._logger.debug(
                f"No digit count for {symbol}: {describe_exception(e)}")

    def _reconcile_tick_counters(self) -> None:
        """
        Hold the displayed tick count against the one the writer wrote.

        Two counters exist for one number: the writer counts what went into the
        file, and the tick handler keeps a second one for the display, the
        status API and the weekly report. They drifted by exactly one tick at
        every UTC day cut until 2026-09-20 - invisible for weeks, because
        nothing ever compared them.

        The writer wins, because the writer is what the file says. The
        disagreement is counted and logged rather than quietly repaired: a
        resynchronisation nobody hears about would hide the next cause just as
        well as the first one hid this one.
        """
        for symbol, writer in self._writers.items():
            symbol_stats = self._stats.symbols.get(symbol)
            if symbol_stats is None:
                continue

            counted = symbol_stats.current_file_ticks
            written = writer.current_tick_count
            self._stats.record_counter_check(symbol, counted, written)

            if counted != written:
                self._logger.warning(
                    f"Tick counters disagree for {symbol}: the display says "
                    f"{counted:,}, the writer holds {written:,} - taking the "
                    f"writer's count")
                symbol_stats.current_file_ticks = written

    async def _monitor_folders(self) -> None:
        """
        Count the files on disk, off the event loop.

        Counting files and measuring folders is file I/O, and on the production
        box file I/O is expensive: measured 2026-09-21, this scan and the disk
        check stalled the loop by up to 4.3 s about once a minute, which landed
        in every tick's `collected_msc` as arrival lag that was ours rather than
        the venue's. A thread is the right answer here, unlike for the JSON
        encoder that used to write the archive files: these are syscalls, and a
        syscall releases the GIL.
        """
        interval = self._config.monitoring.folder_scan_interval_seconds

        self._logger.debug(
            f"[FOLDER_SCAN] Started with interval={interval}s"
        )

        while self._is_running:
            try:
                # Stays here: it compares two counters this loop owns, and
                # touches no file.
                self._reconcile_tick_counters()

                self._stats.record_folder_scan(
                    await asyncio.to_thread(self._scan_folders))
            except Exception as e:
                self._logger.error(
                    f"Folder scan failed: {describe_exception(e)}")
                self._logger.debug(
                    f"[FOLDER_SCAN] Exception details:", exc_info=True)

            await asyncio.sleep(interval)

    def _scan_folders(self) -> float:
        """
        Walk the archive, the MT5 directory and the logs, and count what is there.

        Runs in a worker thread; it only reads the filesystem and writes counts
        into the stats object.

        Returns:
            How long the walk took, in milliseconds
        """
        scan_start = time.monotonic()

        # Kraken data folder
        kraken_path = Path(self._config.paths.raw_data_dir) / "kraken"

        self._logger.debug(
            f"[FOLDER_SCAN] Scanning Kraken: path={kraken_path}, "
            f"exists={kraken_path.exists()}"
        )

        if kraken_path.exists():
            # Check if files are in sub-folders (per symbol) or directly in kraken folder
            has_subfolders = any(
                item.is_dir()
                for item in kraken_path.iterdir()
            )

            self._logger.debug(
                f"[FOLDER_SCAN] Kraken structure: has_subfolders={has_subfolders}"
            )

            if has_subfolders:
                # Files organized in symbol sub-folders
                kraken_count = sum(
                    count_files_in_folder(symbol_folder, "*_ticks.json")
                    for symbol_folder in kraken_path.iterdir()
                    if symbol_folder.is_dir()
                )

                # Update per-symbol folder counts
                symbol_scans = []
                for symbol_folder in kraken_path.iterdir():
                    if symbol_folder.is_dir():
                        symbol = symbol_folder.name
                        if symbol in self._stats.symbols:
                            count = count_files_in_folder(
                                symbol_folder, "*_ticks.json")
                            self._stats.symbols[symbol].folder_file_count = count
                            symbol_scans.append(f"{symbol}={count}")

                self._logger.debug(
                    f"[FOLDER_SCAN] Per-symbol counts: {', '.join(symbol_scans) if symbol_scans else 'none'}"
                )
            else:
                # Files directly in kraken folder (no sub-folders)
                kraken_count = count_files_in_folder(
                    kraken_path, "*_ticks.json")

                self._logger.debug(
                    f"[FOLDER_SCAN] Flat structure: {kraken_count} files directly in kraken folder"
                )

                # Cannot determine per-symbol counts in flat structure
                # Set folder_file_count to 0 for all symbols
                for symbol in self._stats.symbols:
                    self._stats.symbols[symbol].folder_file_count = 0

            self._logger.debug(
                f"[FOLDER_SCAN] Kraken total: {kraken_count} files"
            )

            self._stats.update_folder_stats(
                "kraken", str(kraken_path), kraken_count)
        else:
            self._logger.debug(
                f"[FOLDER_SCAN] Kraken path does not exist: {kraken_path}"
            )
            self._stats.update_folder_stats(
                "kraken", str(kraken_path), 0)

        # MT5 folder
        if self._config.mt5.enabled and self._config.mt5.raw_data_path:
            mt5_path = Path(self._config.mt5.raw_data_path)

            self._logger.debug(
                f"[FOLDER_SCAN] Scanning MT5: path={mt5_path}, "
                f"exists={mt5_path.exists()}"
            )

            if mt5_path.exists():
                mt5_count = count_files_in_folder(
                    mt5_path, "*_ticks.json")
                self._logger.debug(
                    f"[FOLDER_SCAN] MT5 total: {mt5_count} files"
                )
                self._stats.update_folder_stats(
                    "mt5", str(mt5_path), mt5_count)

        # Logs folder
        logs_path = Path(self._config.paths.logs_dir)

        self._logger.debug(
            f"[FOLDER_SCAN] Scanning Logs: path={logs_path}, "
            f"exists={logs_path.exists()}"
        )

        if logs_path.exists():
            logs_count = count_files_in_folder(logs_path, "*.log")
            self._logger.debug(
                f"[FOLDER_SCAN] Logs total: {logs_count} files"
            )
            self._stats.update_folder_stats(
                "logs", str(logs_path), logs_count)

        duration_ms = (time.monotonic() - scan_start) * 1000
        self._logger.debug(f"[FOLDER_SCAN] Completed in {duration_ms:.0f} ms")

        return duration_ms


    async def _start_kraken_collector(self) -> None:
        """Initialize and start Kraken WebSocket collector."""
        self._logger.info(
            f"Starting Kraken collector for {len(self._config.kraken.symbols)} symbols")

        # One clock for the whole session: a globally non-decreasing series is
        # non-decreasing per symbol too, and a clock correction is counted once
        # rather than once per symbol.
        clock = CollectionClock()
        self._clock = clock
        self._stats.streams = list(self._config.kraken.streams)

        # The file boundary, reported rather than looked up. A screen elsewhere
        # has no way to know it, and the one that used to read it off a local
        # config file read the viewer's machine instead of this one.
        self._stats.max_ticks_per_file = self._config.kraken.max_ticks_per_file

        # One cache for the session, like the clock: the ticker channel fills it
        # and every symbol's trade ticks read their own entry out of it.
        quote_cache = QuoteCache()

        # Create writers for each symbol
        raw_dir = Path(self._config.paths.raw_data_dir)

        # Before recovery, because a recovered file carries the identity too.
        origin = self._establish_identity()

        # Before collecting: turn any write-ahead log left by a crashed run into
        # an archive file. Must happen before the writers open new ones, or the
        # recovery would race the files it is meant to rescue.
        for path in recover_orphaned_buffers(raw_dir, "kraken"):
            self._logger.info(f"Recovered from previous run: {path.name}")

        for symbol in self._config.kraken.symbols:
            normalized = normalize_symbol(symbol)

            writer = JsonTickWriter(
                output_dir=raw_dir,
                symbol=normalized,
                clock=clock,
                broker="Kraken",
                server=self._config.kraken.server_name,
                broker_type=self._config.kraken.broker_type,
                max_ticks_per_file=self._config.kraken.max_ticks_per_file,
                origin=origin,
                data_collector="kraken",
                exporter=self._export_archive
            )

            self._writers[normalized] = writer

            self._prepare_symbol_stats(normalized)

        # Create collector
        collector = KrakenWebSocketClient(
            symbols=self._config.kraken.symbols,
            clock=clock,
            quote_cache=quote_cache,
            streams=self._config.kraken.streams,
            url=self._config.kraken.websocket_url,
            reconnect_initial_delay=self._config.kraken.reconnect_initial_delay_seconds,
            reconnect_max_delay=self._config.kraken.reconnect_max_delay_seconds,
            stale_after=self._config.kraken.stale_after_seconds
        )

        # Set callbacks
        collector.set_tick_callback(self._on_tick_received)
        collector.set_status_callback(self._on_status_changed)

        self._collectors.append(collector)

        # Start collector (non-blocking)
        asyncio.create_task(self._run_collector(collector))

    def _on_status_changed(self, status: str) -> None:
        """
        Handle WebSocket connection status change.

        Args:
            status: New status (connected, disconnected, reconnecting, failed)
        """
        old_status = self._stats.websocket_status
        self._stats.set_websocket_status(status)

        self._logger.debug(
            f"[STATUS] WebSocket status changed: {old_status} → {status}, "
            f"disconnect_time={self._disconnect_time}"
        )

        # Track disconnects (including 'reconnecting' which means connection was lost)
        if status in ["disconnected", "reconnecting"] and old_status == "connected":
            self._disconnect_time = datetime.now(timezone.utc)
            self._logger.debug(
                f"[DISCONNECT] Tracked disconnect at {self._disconnect_time} (status={status})"
            )

        # Track reconnects
        if is_reconnect(old_status, status):
            # Stamped on the monotonic clock, for the stall attribution. The
            # event list below carries the wall-clock time a human reads; a
            # duration must never come from subtracting two of those.
            self._last_reconnect_monotonic = time.monotonic()

            if self._disconnect_time:
                duration = (datetime.now(timezone.utc) -
                            self._disconnect_time).total_seconds()

                self._logger.debug(
                    f"[RECONNECT] Recording reconnect event: "
                    f"duration={duration:.1f}s, "
                    f"disconnect_time={self._disconnect_time}, "
                    f"old_status={old_status}"
                )

                self._stats.record_reconnect("connection_restored", duration)

                # Send alert if cooldown passed
                asyncio.create_task(self._send_reconnect_alert(duration))

                self._disconnect_time = None
            else:
                self._logger.debug(
                    f"[RECONNECT] Connected but no disconnect_time tracked "
                    f"(old_status={old_status}) - possible initial connection"
                )

    def _reconnects_within_the_hour(self, now: datetime) -> int:
        """
        How many reconnects have happened in the last hour, this one included.

        Read off the event list the statistics already keep, so there is no
        second counter to drift from it - the drift between two counters for one
        number is a defect this project has already paid for once.

        Args:
            now: The moment being judged

        Returns:
            Count of reconnect events within the last hour
        """
        cutoff = now - timedelta(hours=1)
        return sum(1 for event in self._stats.reconnect_events
                   if event.timestamp >= cutoff)

    async def _send_reconnect_alert(self, duration_seconds: float) -> None:
        """
        Send reconnect alert if cooldown allows.

        Args:
            duration_seconds: Downtime duration
        """
        if not self._telegram:
            self._logger.debug(
                "[RECONNECT_ALERT] No Telegram configured, skipping")
            return

        # Check cooldown
        cooldown_minutes = self._config.monitoring.reconnect_alert_cooldown_minutes
        now = datetime.now(timezone.utc)

        if self._last_reconnect_alert:
            elapsed = (now - self._last_reconnect_alert).total_seconds() / 60
            self._logger.debug(
                f"[RECONNECT_ALERT] Cooldown check: "
                f"elapsed={elapsed:.1f}min, cooldown={cooldown_minutes}min"
            )
            if elapsed < cooldown_minutes:
                self._logger.debug(
                    f"[RECONNECT_ALERT] Skipping (still in cooldown)"
                )
                return  # Still in cooldown

        # Whole minutes rendered every one of these as "0m downtime", including
        # a 7.2 s outage that was three times the others - a number that told
        # the reader nothing it did not already assume.
        recent = self._reconnects_within_the_hour(now)
        body = reconnect_alert_text(
            duration_seconds,
            recent_within_hour=recent,
            min_seconds=self._config.monitoring.reconnect_alert_min_seconds,
            cluster_size=self._config.monitoring.reconnect_alert_cluster)

        if body is None:
            self._logger.info(
                f"Reconnected after {duration_seconds:.1f}s - below the alert "
                f"threshold, {recent} in the last hour. On /v1/status either way.")
            return

        self._logger.debug(f"[RECONNECT_ALERT] Sending alert: {body}")

        await self._telegram.send_warning("🔌 Connection Restored", body)

        self._last_reconnect_alert = now

        self._logger.debug(
            f"[RECONNECT_ALERT] Alert sent, next allowed at {now + timedelta(minutes=cooldown_minutes)}"
        )

    async def _run_collector(self, collector: KrakenWebSocketClient) -> None:
        """
        Run collector with error handling.

        Args:
            collector: Collector instance
        """
        try:
            await collector.start()
        except Exception as e:
            self._logger.error(f"Collector failed: {describe_exception(e)}")

            if self._telegram:
                await self._telegram.send_error(
                    "Collector Failed",
                    f"Kraken collector stopped: {describe_exception(e)}"
                )

    def _on_tick_received(self, tick) -> None:
        """
        Handle incoming tick, and measure what handling it costs the loop.

        The timing exists because of Befund 28: on 2026-09-21 the loop stalled
        up to 738 ms with the display off, no export running and garbage
        collection accounting for under a tenth of it. Everything the stall
        attribution could name had been ruled out, which left a consumer nobody
        was measuring - and this handler, running nine symbols with a flushed
        write-ahead append per tick, is the largest candidate left.

        Naming it required measuring it. Two `time.monotonic()` reads per tick
        cost a fraction of the 66 microseconds the handler already spends.

        Args:
            tick: TickData instance
        """
        started = time.monotonic()
        try:
            self._handle_tick(tick)
        finally:
            self._stats.record_tick_work((time.monotonic() - started) * 1000)

    def _handle_tick(self, tick) -> None:
        """
        Everything that happens to one arriving tick.

        Args:
            tick: TickData instance
        """
        symbol = self._get_symbol_from_tick(tick)

        # Log first tick for this symbol
        if symbol not in self._first_tick_logged:
            self._first_tick_logged.add(symbol)
            self._logger.info(
                f"First tick: {symbol} | "
                f"bid={tick.bid:.2f} ask={tick.ask:.2f} "
                f"spread={tick.spread_pct:.4f}%"
            )

        # Write tick
        if symbol in self._writers:
            writer = self._writers[symbol]

            # Get current state BEFORE any changes
            old_file = writer.get_current_filepath()
            stats = self._stats.symbols.get(symbol)
            count_before = stats.current_file_ticks if stats else 0

            self._logger.debug(
                f"[TICK] Before processing: symbol={symbol}, "
                f"count_before={count_before}, file={old_file.name if old_file else 'None'}"
            )

            # Update stats FIRST (increments count)
            self._stats.record_tick(
                symbol=symbol,
                bid=tick.bid,
                ask=tick.ask,
                spread_pct=tick.spread_pct,
                real_volume=tick.real_volume,
                quote_age_ms=tick.quote_age_ms
            )

            # The clock only moves when a tick is stamped, so this is the moment
            # its counters can have changed - and where a screen outside this
            # process gets to see them.
            if self._clock is not None:
                self._stats.record_clock(
                    self._clock.resyncs, self._clock.max_correction_ms)

            # Get count AFTER increment - this is the FINAL count for this file
            count_after = self._stats.symbols[symbol].current_file_ticks
            self._logger.debug(
                f"[TICK] After stats update: symbol={symbol}, "
                f"count_after={count_after}, incremented={count_after - count_before}"
            )

            # Write tick (may rotate internally)
            writer.write_tick(tick)

            # Detect rotation (file changed)
            new_file = writer.get_current_filepath()

            self._logger.debug(
                f"[ROTATION_CHECK] symbol={symbol}, "
                f"old_file={old_file.name if old_file else 'None'}, "
                f"new_file={new_file.name if new_file else 'None'}, "
                f"count_after={count_after}, rotation={old_file != new_file if old_file and new_file else False}"
            )

            if old_file and new_file and old_file != new_file:
                # Both numbers come from the writer, because only the writer
                # knows which file this tick landed in. The day cut is checked
                # BEFORE the tick is appended, so the tick that triggered it
                # belongs to the NEW file; the count threshold fires after, so
                # that trigger belongs to the old one. Counting along here got
                # it wrong by exactly one, in either direction, and carried the
                # error into the next file - measured on production: "File
                # rotated: ... (47,369 ticks)" for a file holding 47,368, and
                # "(49,999)" for one holding 50,000.
                self._stats.record_file_created(
                    symbol=symbol,
                    filename=old_file.name,
                    tick_count=writer.last_closed_tick_count
                )

                # Not zero: after a day cut the new file already holds the tick
                # that caused the cut.
                self._stats.symbols[symbol].current_file_ticks = \
                    writer.current_tick_count

                self._logger.debug(
                    f"[ROTATION] Detected: symbol={symbol}, "
                    f"rotated_file={old_file.name}, "
                    f"final_count={writer.last_closed_tick_count}, "
                    f"new_file={new_file.name}, "
                    f"carried_over={writer.current_tick_count}"
                )

                self._logger.info(
                    f"File rotated: {old_file.name} "
                    f"({writer.last_closed_tick_count:,} ticks)")

                # Send rotation notification (if enabled)
                if self._telegram and self._config.telegram.send_on_rotation:
                    self._logger.debug(
                        f"[TELEGRAM] Sending rotation alert: symbol={symbol}, "
                        f"file={old_file.name}, "
                        f"count={writer.last_closed_tick_count}"
                    )
                    asyncio.create_task(
                        self._telegram.send_file_rotation_notice(
                            symbol=symbol,
                            filename=old_file.name,
                            tick_count=writer.last_closed_tick_count
                        )
                    )
        else:
            # No writer, just update stats
            self._stats.record_tick(
                symbol=symbol,
                bid=tick.bid,
                ask=tick.ask,
                spread_pct=tick.spread_pct,
                real_volume=tick.real_volume,
                quote_age_ms=tick.quote_age_ms
            )

    def _get_symbol_from_tick(self, tick) -> str:
        """
        Extract symbol from tick data.

        Args:
            tick: TickData instance

        Returns:
            Normalized symbol string
        """
        return tick.symbol

    async def _send_weekly_report(self) -> bool:
        """
        Send weekly collection report via Telegram.

        Returns:
            True if sent successfully
        """
        if not self._telegram:
            return False

        # Calculate folder sizes (slow, but only once per week)
        self._logger.info("Calculating folder sizes for weekly report...")

        kraken_path = Path(self._config.paths.raw_data_dir) / "kraken"
        kraken_size = get_folder_size(
            kraken_path) if kraken_path.exists() else 0

        mt5_size = 0
        if self._config.mt5.enabled and self._config.mt5.raw_data_path:
            mt5_path = Path(self._config.mt5.raw_data_path)
            mt5_size = get_folder_size(mt5_path) if mt5_path.exists() else 0

        logs_path = Path(self._config.paths.logs_dir)
        logs_size = get_folder_size(logs_path) if logs_path.exists() else 0

        total_size = kraken_size + mt5_size + logs_size

        # Get folder stats
        kraken_stats = self._stats.folders.get("kraken")
        mt5_stats = self._stats.folders.get("mt5")
        logs_stats = self._stats.folders.get("logs")

        # Reconnects this week
        reconnects = self._stats.get_reconnects_this_week()

        # Build report
        uptime_hours = self._stats.get_uptime_hours()
        disk = self._stats.disk_space

        if disk.status == "OK":
            disk_status = "✅"
        elif disk.status == "WARNING":
            disk_status = "⚠️"
        else:
            disk_status = "🚨"

        report_lines = [
            "📊 *Weekly Collection Report*",
            f"{datetime.now(timezone.utc).strftime('%A, %d.%m.%Y %H:%M UTC')}",
            "",
            "⏱️ *Uptime*",
            f"• Runtime: {uptime_hours:.1f} hours",
            f"• Files Created: {self._stats.total_files}",
            f"• Errors: {self._stats.total_errors} | Warnings: {self._stats.total_warnings}",
            "",
            "📁 *Data Storage*",
            f"• Kraken: {kraken_size/(1024**3):.2f} GB ({kraken_stats.file_count if kraken_stats else 0} files)",
            f"• MT5: {mt5_size/(1024**3):.2f} GB ({mt5_stats.file_count if mt5_stats else 0} files)",
            f"• Logs: {logs_size/(1024**3):.2f} GB ({logs_stats.file_count if logs_stats else 0} files)",
            f"• Total Data: {total_size/(1024**3):.2f} GB",
            "",
            "💾 *Disk Space*",
            f"• Total: {disk.total_gb:.1f} GB",
            f"• Used: {disk.used_gb:.1f} GB ({disk.percent_used:.0f}%)",
            f"• Free: {disk.free_gb:.1f} GB ({disk.percent_free:.0f}%) {disk_status}",
            "",
            "🔌 *Connection Health*",
            f"• Reconnects This Week: {len(reconnects)}",
        ]

        # Add reconnect details
        if reconnects:
            for event in reconnects[-3:]:  # Last 3
                time_str = event.timestamp.strftime("%a %d.%m %H:%M")
                duration = int(event.duration_seconds / 60)
                report_lines.append(f"  - {time_str} ({duration}m downtime)")

        report_lines.extend([
            f"• Current Status: {self._stats.websocket_status}",
            "",
            "📈 *Per Symbol*"
        ])

        # Per symbol stats
        for symbol, stats in sorted(self._stats.symbols.items()):
            report_lines.append(
                f"• {symbol}: {stats.file_count} files created")

        report_text = "\n".join(report_lines)

        success = await self._telegram.send_info("Weekly Report", report_text)

        # Reset weekly reconnects after report
        if success:
            self._stats.reset_weekly_reconnects()

        return success

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown handlers (cross-platform)."""
        if sys.platform == "win32":
            # Before registering anything: a parent process can leave Ctrl+C
            # processing switched off, and the setting is inherited. Measured
            # 2026-09-21 - the handler below never ran, the console accepted the
            # event, and the collector kept collecting for a full minute with its
            # archive unwritten. Under NSSM that is the difference between a stop
            # and a kill.
            if enable_ctrl_c_handling() is False:
                self._logger.warning(
                    "Could not re-enable Ctrl+C handling on this console. A stop "
                    "signal may not reach the shutdown handler; the write-ahead "
                    "logs still protect the data, and the next start recovers.")

            # Windows: use signal.signal (SIGTERM not available)
            def win_handler(signum, frame):
                asyncio.create_task(self._signal_handler())

            signal.signal(signal.SIGINT, win_handler)
            # SIGTERM doesn't exist on Windows, skip it
        else:
            # Unix: use asyncio signal handlers (cleaner integration)
            loop = asyncio.get_event_loop()

            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self._signal_handler())
                )

    async def _signal_handler(self) -> None:
        """Handle shutdown signal."""
        self._logger.info("Shutdown signal received")
        self._shutdown_event.set()

    async def _shutdown(self) -> None:
        """Graceful shutdown of all components."""
        self._logger.info("Shutting down...")

        self._gc_watcher.stop()

        # Stop live display first (so we can see logs)
        if self._live_display:
            await self._live_display.stop()

        # Stop monitoring tasks
        for task in self._monitoring_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Stop collectors
        for collector in self._collectors:
            await collector.stop()

        # Finalize writers
        for symbol, writer in self._writers.items():
            # Get final count from stats (source of truth)
            if symbol in self._stats.symbols:
                tick_count = self._stats.symbols[symbol].current_file_ticks
            else:
                tick_count = 0

            filepath = writer.finalize()
            if filepath:
                self._logger.info(f"Finalized: {filepath.name}")
                self._stats.record_file_created(
                    symbol=symbol,
                    filename=filepath.name,
                    tick_count=tick_count
                )

        await self._await_pending_exports()

        # Ask the API to finish before the tasks are cancelled, so an in-flight
        # request is answered rather than dropped.
        if self._api_server:
            self._api_server.should_exit = True

        # Released after the writers, so the directory stays claimed until the
        # last file is on disk.
        if self._instance_lock:
            self._instance_lock.release()

        # Stop scheduler
        if self._scheduler:
            self._scheduler.stop()

        # Stop telegram command polling
        if self._telegram:
            self._telegram.stop_command_polling()

        # Send shutdown notification
        if self._telegram:
            await self._telegram.send_info(
                "Collector Stopped",
                f"FiniexDataCollector stopped. "
                f"Files: {self._stats.total_files}"
            )

        self._is_running = False
        self._logger.info("Shutdown complete")



async def cmd_collect(config: AppConfig, show_display: bool = True) -> None:
    """
    Run collection daemon.

    Args:
        config: The effective configuration
        show_display: Render the live display. Off leaves the file log as the
            only record, which is how a service-wrapped instance runs
    """
    logger = get_logger("FiniexDataCollector")

    # === VALIDATION PHASE ===

    # 1. Validate symbols for duplicates (HARD ERROR)
    logger.info("Validating configuration...")
    validate_symbols(config.kraken.symbols)

    # 2. Load symbol config from Kraken API
    logger.info("Fetching symbol configuration from Kraken API...")
    await BrokerConfig.load_from_api(config.kraken.symbols)
    logger.info(
        f"Loaded {len(BrokerConfig.get_all_symbols())} symbols from Kraken API")

    # 4. Verify all configured symbols are in broker config
    for symbol in config.kraken.symbols:
        normalized = normalize_symbol(symbol)
        if not BrokerConfig.has_symbol(normalized):
            raise ConfigurationError(
                f"Symbol '{symbol}' (normalized: '{normalized}') not found in broker config. "
                f"Available: {', '.join(BrokerConfig.get_all_symbols())}",
                missing_key=normalized
            )

    logger.info("Configuration validated successfully")

    # === START COLLECTION ===
    app = FiniexDataCollector(config, show_display=show_display)
    await app.start_collection()



def cmd_watch(endpoint_name: str, interval: Optional[float]) -> int:
    """
    Draw a running collector, from outside it.

    Deliberately free of the collector's configuration and of its logger: this is
    a second program that reads one HTTP route, and the only thing it shares with
    the collector is the renderer. It prints to stdout directly for that reason -
    a viewer that wrote into the service's log file would corrupt the record it
    exists to display.

    Args:
        endpoint_name: Entry in `user_configs/remote_endpoints.json`
        interval: Seconds between readings, or None to decide from the URL

    Returns:
        Process exit code
    """
    try:
        base_url, token = load_endpoint(endpoint_name)
    except EndpointError as error:
        print(f"Cannot watch '{endpoint_name}': {error}")
        return 1

    seconds = interval if interval else default_interval(base_url)

    # QuickEdit belongs to whichever console carries a live display, and after
    # issue #15 that is this one. In the service there is no console to protect;
    # here a stray click would freeze the screen and, unlike before, nothing else.
    disable_quick_edit()
    enable_ansi_colours()

    # The screen is full of box drawing and emoji, and a console still on a
    # legacy code page cannot encode them. Rich then raises inside the update
    # loop, where the handler swallows it and the screen simply stops moving -
    # a viewer frozen by its own title, which reads exactly like a collector
    # that died. A replacement character is a worse-looking screen and a true
    # one. On a UTF-8 console this changes nothing.
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass

    # The URL, never the token. A command that needs the credential reads it from
    # the overlay, so it never reaches a terminal, a screenshot or a bus message.
    print(f"Watching {base_url} every {seconds:g}s - Ctrl+C to stop")

    try:
        asyncio.run(run_viewer(base_url, token, seconds))
    except KeyboardInterrupt:
        pass

    return 0


def cmd_status(config: AppConfig) -> None:
    """Show current status."""
    logger = get_logger("FiniexDataCollector")

    logger.info("=" * 60)
    logger.info("FiniexDataCollector Status")
    logger.info("=" * 60)
    logger.info(f"Version: {config.version}")
    logger.info("")
    logger.info("Kraken Collector:")
    logger.info(f"  Enabled: {config.kraken.enabled}")
    logger.info(f"  Symbols: {', '.join(config.kraken.symbols)}")
    logger.info(f"  WebSocket: {config.kraken.websocket_url}")
    logger.info("")
    logger.info("Telegram Alerts:")
    logger.info(f"  Enabled: {config.telegram.enabled}")
    logger.info(f"  Configured: {bool(config.telegram.bot_token)}")
    logger.info("")
    logger.info("Scheduler:")
    logger.info(
        f"  Weekly Report: {config.scheduler.report_day} {config.scheduler.report_hour_utc:02d}:{config.scheduler.report_minute_utc:02d} UTC")
    logger.info("")
    logger.info("Monitoring:")
    logger.info(
        f"  Disk Check: every {config.monitoring.disk_space_check_interval_seconds}s")
    logger.info(
        f"  Folder Scan: every {config.monitoring.folder_scan_interval_seconds}s")
    logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="FiniexDataCollector - Tick Data Collection System"
    )

    parser.add_argument(
        "command",
        choices=["collect", "status", "watch"],
        help="Command to execute"
    )

    parser.add_argument(
        "--config",
        type=str,
        default="./configs/app_config.json",
        help="Path to config file"
    )

    parser.add_argument(
        "--no-display",
        action="store_true",
        help="collect without the live display - the file log keeps the record"
    )

    parser.add_argument(
        "--endpoint",
        type=str,
        default="watch",
        help="watch: which entry of user_configs/remote_endpoints.json to read"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="watch: seconds between readings (default: 1 on loopback, 2 remote)"
    )

    args = parser.parse_args()

    # The viewer runs before any of this. It is a different program that happens
    # to share an entry point: it must not load the collector's configuration and
    # above all must not open the collector's log file, because on the production
    # box the service already holds it and two writers on one log is how a record
    # gets shredded by the thing that was watching it.
    if args.command == "watch":
        sys.exit(cmd_watch(args.endpoint, args.interval))

    # Load config
    try:
        config_path = Path(args.config)
        if config_path.exists():
            loader = ConfigLoader(config_path)
            config = loader.load()
        else:
            print(f"Config not found: {config_path}")
            sys.exit(1)
    except Exception as e:
        print(f"Failed to load config: {describe_exception(e)}")
        sys.exit(1)

    # Setup logging (from config - required section)
    setup_logging(
        console_level=config.logging.console_level,
        file_level=config.logging.file_level,
        log_dir=Path(config.paths.logs_dir)
    )

    # Execute command
    try:
        if args.command == "collect":
            asyncio.run(cmd_collect(config, show_display=not args.no_display))
        elif args.command == "status":
            cmd_status(config)
    except ConfigurationError as e:
        logger = get_logger("FiniexDataCollector")
        logger.error(f"Configuration error: {describe_exception(e)}")
        # Deliberately not 1. A service manager restarts on exit by default, and
        # restarting cannot fix a bad configuration or a directory another live
        # instance already owns - it only produces the same failure at whatever
        # interval the manager throttles to. Map this code to "stay stopped" and
        # leave everything else on restart, where a restart is exactly the answer.
        sys.exit(EXIT_CONFIGURATION)
    except Exception as e:
        logger = get_logger("FiniexDataCollector")
        logger.error(f"Fatal error: {describe_exception(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
