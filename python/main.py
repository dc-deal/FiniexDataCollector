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
import signal
import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List

import psutil

from python.utils.config_loader import ConfigLoader, AppConfig
from python.utils.logging_setup import setup_logging, get_logger
from python.utils.live_display import LiveDisplay
from python.types.collector_stats import CollectorStats
from python.types.broker_config_types import BrokerConfig, normalize_symbol
from python.exceptions.collector_exceptions import ConfigurationError
from python.collectors.kraken.websocket_client import KrakenWebSocketClient
from python.writers.json_tick_writer import JsonTickWriter
from python.alerts.telegram_bot import TelegramAlertProvider
from python.scheduler.weekly_jobs import WeeklyJobScheduler


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


def count_files_in_folder(folder_path: Path) -> int:
    """
    Count files in a folder (non-recursive).

    Args:
        folder_path: Path to folder

    Returns:
        Number of files
    """
    if not folder_path.exists():
        return 0

    try:
        return sum(1 for item in folder_path.iterdir() if item.is_file())
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


class FiniexDataCollector:
    """
    Main application class.

    Orchestrates collectors, writers, alerts, scheduler, and monitoring.
    """

    def __init__(self, config: AppConfig):
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
        self._telegram: Optional[TelegramAlertProvider] = None
        self._scheduler: Optional[WeeklyJobScheduler] = None

        # Live monitoring
        self._stats = CollectorStats()
        self._live_display: Optional[LiveDisplay] = None

        # State
        self._is_running = False
        self._shutdown_event = asyncio.Event()

        # Track first tick per symbol for logging
        self._first_tick_logged = set()

        # Monitoring tasks
        self._monitoring_tasks = []

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

        # Start live display
        self._live_display = LiveDisplay(
            self._stats,
            streams=self._config.kraken.streams
        )
        await self._live_display.start()

        # Wait for shutdown
        await self._shutdown_event.wait()

        # Cleanup
        await self._shutdown()

    async def _start_monitoring_tasks(self) -> None:
        """Start background monitoring tasks."""
        # Disk space monitoring
        disk_task = asyncio.create_task(
            self._monitor_disk_space()
        )
        self._monitoring_tasks.append(disk_task)

        # Folder scanning
        folder_task = asyncio.create_task(
            self._monitor_folders()
        )
        self._monitoring_tasks.append(folder_task)

        self._logger.info("Monitoring tasks started")

    async def _monitor_disk_space(self) -> None:
        """Monitor disk space usage."""
        interval = self._config.monitoring.disk_space_check_interval_seconds

        self._logger.debug(
            f"[DISK_MONITOR] Started with interval={interval}s"
        )

        while self._is_running:
            try:
                # Get disk usage for raw data directory
                data_path = Path(self._config.paths.raw_data_dir).resolve()

                self._logger.debug(
                    f"[DISK_MONITOR] Checking path: {data_path}"
                )

                usage = psutil.disk_usage(str(data_path))

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
                self._logger.error(f"Disk space check failed: {e}")
                self._logger.debug(
                    f"[DISK_MONITOR] Exception details:", exc_info=True)

            await asyncio.sleep(interval)

    async def _monitor_folders(self) -> None:
        """Monitor folder file counts."""
        interval = self._config.monitoring.folder_scan_interval_seconds

        self._logger.debug(
            f"[FOLDER_SCAN] Started with interval={interval}s"
        )

        while self._is_running:
            try:
                scan_start = datetime.now(timezone.utc)

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
                            count_files_in_folder(symbol_folder)
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
                                        symbol_folder)
                                    self._stats.symbols[symbol].folder_file_count = count
                                    symbol_scans.append(f"{symbol}={count}")

                        self._logger.debug(
                            f"[FOLDER_SCAN] Per-symbol counts: {', '.join(symbol_scans) if symbol_scans else 'none'}"
                        )
                    else:
                        # Files directly in kraken folder (no sub-folders)
                        kraken_count = count_files_in_folder(kraken_path)

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
                        mt5_count = count_files_in_folder(mt5_path)
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
                    logs_count = count_files_in_folder(logs_path)
                    self._logger.debug(
                        f"[FOLDER_SCAN] Logs total: {logs_count} files"
                    )
                    self._stats.update_folder_stats(
                        "logs", str(logs_path), logs_count)

                scan_duration = (datetime.now(timezone.utc) -
                                 scan_start).total_seconds()
                self._logger.debug(
                    f"[FOLDER_SCAN] Completed in {scan_duration:.2f}s"
                )

            except Exception as e:
                self._logger.error(f"Folder scan failed: {e}")
                self._logger.debug(
                    f"[FOLDER_SCAN] Exception details:", exc_info=True)

            await asyncio.sleep(interval)

    async def _start_kraken_collector(self) -> None:
        """Initialize and start Kraken WebSocket collector."""
        self._logger.info(
            f"Starting Kraken collector for {len(self._config.kraken.symbols)} symbols")

        # Create writers for each symbol
        raw_dir = Path(self._config.paths.raw_data_dir)

        for symbol in self._config.kraken.symbols:
            normalized = normalize_symbol(symbol)

            writer = JsonTickWriter(
                output_dir=raw_dir,
                symbol=normalized,
                broker="Kraken",
                server=self._config.kraken.server_name,
                broker_type=self._config.kraken.broker_type,
                max_ticks_per_file=self._config.kraken.max_ticks_per_file,
                data_collector="kraken"
            )

            self._writers[normalized] = writer

        # Create collector
        collector = KrakenWebSocketClient(
            symbols=self._config.kraken.symbols,
            streams=self._config.kraken.streams,
            url=self._config.kraken.websocket_url,
            reconnect_initial_delay=self._config.kraken.reconnect_initial_delay_seconds,
            reconnect_max_delay=self._config.kraken.reconnect_max_delay_seconds,
            heartbeat_interval=self._config.kraken.heartbeat_interval_seconds
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

        # Track reconnects (any transition back to 'connected' after being disconnected)
        if status == "connected" and old_status in ["disconnected", "reconnecting"]:
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

        # Send alert
        duration_minutes = int(duration_seconds / 60)

        self._logger.debug(
            f"[RECONNECT_ALERT] Sending alert: duration={duration_minutes}m"
        )

        await self._telegram.send_warning(
            "🔌 Connection Restored",
            f"WebSocket reconnected after {duration_minutes}m downtime"
        )

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
            self._logger.error(f"Collector failed: {e}")

            if self._telegram:
                await self._telegram.send_error(
                    "Collector Failed",
                    f"Kraken collector stopped: {e}"
                )

    def _on_tick_received(self, tick) -> None:
        """
        Handle incoming tick from collector.

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
                real_volume=tick.real_volume
            )

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
                # File was rotated!
                # count_after contains the FINAL tick count for the OLD file
                self._stats.record_file_created(
                    symbol=symbol,
                    filename=old_file.name,
                    tick_count=count_after  # Final count of OLD file
                )

                # CRITICAL: Reset tick count for NEW file
                # The new file is empty (writer just started it)
                self._stats.symbols[symbol].current_file_ticks = 0

                self._logger.debug(
                    f"[ROTATION] Detected: symbol={symbol}, "
                    f"rotated_file={old_file.name}, "
                    f"final_count={count_after}, "
                    f"new_file={new_file.name}, "
                    f"stats_reset=0"
                )

                self._logger.info(
                    f"File rotated: {old_file.name} ({count_after:,} ticks)"
                )

                # Send rotation notification (if enabled)
                if self._telegram and self._config.telegram.send_on_rotation:
                    self._logger.debug(
                        f"[TELEGRAM] Sending rotation alert: symbol={symbol}, "
                        f"file={old_file.name}, count={count_after}"
                    )
                    asyncio.create_task(
                        self._telegram.send_file_rotation_notice(
                            symbol=symbol,
                            filename=old_file.name,
                            tick_count=count_after
                        )
                    )
        else:
            # No writer, just update stats
            self._stats.record_tick(
                symbol=symbol,
                bid=tick.bid,
                ask=tick.ask,
                spread_pct=tick.spread_pct,
                real_volume=tick.real_volume
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



async def cmd_collect(config: AppConfig) -> None:
    """Run collection daemon."""
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
    app = FiniexDataCollector(config)
    await app.start_collection()



def cmd_status(config: AppConfig) -> None:
    """Show current status."""
    logger = get_logger("FiniexDataCollector")

    logger.info("=" * 60)
    logger.info("FiniexDataCollector Status")
    logger.info("=" * 60)
    logger.info(f"Version: {config.version}")
    logger.info(f"Environment: {config.environment}")
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
        choices=["collect", "status"],
        help="Command to execute"
    )

    parser.add_argument(
        "--config",
        type=str,
        default="./configs/app_config.json",
        help="Path to config file"
    )

    args = parser.parse_args()

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
        print(f"Failed to load config: {e}")
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
            asyncio.run(cmd_collect(config))
        elif args.command == "status":
            cmd_status(config)
    except ConfigurationError as e:
        logger = get_logger("FiniexDataCollector")
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger = get_logger("FiniexDataCollector")
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
