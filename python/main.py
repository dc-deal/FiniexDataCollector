"""
FiniexDataCollector - Main Entry Point
CLI and daemon mode for tick data collection.

Usage:
    python main.py collect              # Start collectors
    python main.py convert              # Run parquet conversion
    python main.py broker-config        # Fetch broker config
    python main.py status               # Show collector status

Location: python/main.py
"""

import argparse
import asyncio
import signal
import sys
from pathlib import Path
from typing import Optional, List

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
from python.converters.tick_to_parquet import run_weekly_conversion
from python.converters.broker_config_fetcher import fetch_kraken_broker_config


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


class FiniexDataCollector:
    """
    Main application class.

    Orchestrates collectors, writers, alerts, and scheduler.
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
                await self._telegram.send_info(
                    "Collector Started",
                    f"FiniexDataCollector started with {len(self._config.kraken.symbols)} symbols"
                )

        # Initialize scheduler
        self._scheduler = WeeklyJobScheduler(self._config.scheduler)
        self._scheduler.set_conversion_callback(self._run_conversion)
        self._scheduler.set_report_callback(self._send_conversion_report)
        self._scheduler.start()

        # Initialize Kraken collector
        if self._config.kraken.enabled:
            await self._start_kraken_collector()

        self._is_running = True

        # Start live display
        self._live_display = LiveDisplay(self._stats)
        await self._live_display.start()

        # Wait for shutdown
        await self._shutdown_event.wait()

        # Cleanup
        await self._shutdown()

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
                server="kraken_spot",
                max_ticks_per_file=self._config.kraken.max_ticks_per_file,
                data_collector="kraken"
            )

            self._writers[normalized] = writer

        # Create collector
        collector = KrakenWebSocketClient(
            symbols=self._config.kraken.symbols,
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
            status: New status (connected, disconnected, reconnecting)
        """
        self._stats.set_websocket_status(status)

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

        # Update stats
        self._stats.record_tick(
            symbol=symbol,
            bid=tick.bid,
            ask=tick.ask,
            spread_pct=tick.spread_pct
        )

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

            # Check if rotation happened
            old_file = writer.get_current_filepath()
            old_count = writer.current_tick_count

            writer.write_tick(tick)

            # Detect rotation (file changed or count reset)
            new_file = writer.get_current_filepath()
            if old_file and new_file and old_file != new_file:
                # File was rotated
                self._stats.record_file_created(
                    symbol=symbol,
                    filename=old_file.name,
                    tick_count=old_count
                )
                self._logger.info(
                    f"File rotated: {old_file.name} ({old_count:,} ticks)"
                )

            # Check for rotation notification (Telegram)
            if writer.needs_rotation() and self._telegram:
                asyncio.create_task(
                    self._telegram.send_file_rotation_notice(
                        symbol=symbol,
                        filename=writer.get_current_filepath(
                        ).name if writer.get_current_filepath() else "unknown",
                        tick_count=writer.current_tick_count
                    )
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

    async def _run_conversion(self) -> dict:
        """
        Run parquet conversion job.

        Returns:
            Conversion result dict
        """
        return await run_weekly_conversion(
            raw_dir=Path(self._config.paths.raw_data_dir),
            processed_dir=Path(self._config.paths.processed_data_dir),
            data_collector="kraken"
        )

    async def _send_conversion_report(self, result: dict) -> bool:
        """
        Send conversion report via Telegram.

        Args:
            result: Conversion result dict

        Returns:
            True if sent
        """
        if not self._telegram:
            return False

        return await self._telegram.send_weekly_report(
            symbols_processed=result.get("symbols_processed", 0),
            files_converted=result.get("files_converted", 0),
            total_ticks=result.get("total_ticks", 0),
            errors=result.get("errors", 0),
            duration_seconds=result.get("duration_seconds", 0)
        )

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

        # Stop collectors
        for collector in self._collectors:
            await collector.stop()

        # Finalize writers
        for symbol, writer in self._writers.items():
            tick_count = writer.current_tick_count
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

        # Send shutdown notification
        if self._telegram:
            await self._telegram.send_info(
                "Collector Stopped",
                f"FiniexDataCollector stopped. "
                f"Total: {self._stats.total_ticks:,} ticks, {self._stats.total_files} files"
            )

        self._is_running = False
        self._logger.info("Shutdown complete")


async def ensure_broker_config(config: AppConfig, logger) -> Path:
    """
    Ensure broker config exists, fetch from API if not.

    Args:
        config: Application config
        logger: Logger instance

    Returns:
        Path to broker config file

    Raises:
        ConfigurationError: If symbols invalid or API fails
    """
    broker_config_dir = Path(config.paths.broker_configs_dir) / "kraken"
    broker_config_path = broker_config_dir / "kraken_public.json"

    # Always fetch fresh from API at collect start
    logger.info("Fetching broker configuration from Kraken API...")

    try:
        filepath, _ = await fetch_kraken_broker_config(
            output_dir=broker_config_dir,
            symbols=config.kraken.symbols
        )
        logger.info(f"Broker config ready: {filepath}")
        return filepath

    except Exception as e:
        # If fetch fails but we have cached config, use it
        if broker_config_path.exists():
            logger.warning(
                f"API fetch failed ({e}), using cached broker config"
            )
            return broker_config_path

        # No cache, re-raise
        raise


async def cmd_collect(config: AppConfig) -> None:
    """Run collection daemon."""
    logger = get_logger("FiniexDataCollector")

    # === VALIDATION PHASE ===

    # 1. Validate symbols for duplicates (HARD ERROR)
    logger.info("Validating configuration...")
    validate_symbols(config.kraken.symbols)

    # 2. Fetch/ensure broker config (validates symbols against Kraken API)
    broker_config_path = await ensure_broker_config(config, logger)

    # 3. Load BrokerConfig singleton
    BrokerConfig.load_from_file(broker_config_path)
    logger.info(
        f"Loaded {len(BrokerConfig.get_all_symbols())} symbols from broker config")

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


async def cmd_convert(config: AppConfig) -> None:
    """Run parquet conversion manually."""
    logger = get_logger("FiniexDataCollector")
    logger.info("Running manual parquet conversion...")

    result = await run_weekly_conversion(
        raw_dir=Path(config.paths.raw_data_dir),
        processed_dir=Path(config.paths.processed_data_dir),
        data_collector="kraken"
    )

    logger.info(f"Conversion complete: {result}")


async def cmd_broker_config(config: AppConfig) -> None:
    """Fetch and save broker configuration."""
    logger = get_logger("FiniexDataCollector")
    logger.info("Fetching Kraken broker configuration...")

    # Validate symbols first
    validate_symbols(config.kraken.symbols)

    output_dir = Path(config.paths.broker_configs_dir) / "kraken"

    filepath, _ = await fetch_kraken_broker_config(
        output_dir=output_dir,
        symbols=config.kraken.symbols
    )

    logger.info(f"Broker config saved: {filepath}")


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
        f"  Conversion: {config.scheduler.conversion_day} {config.scheduler.conversion_hour_utc:02d}:00 UTC")
    logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="FiniexDataCollector - Tick Data Collection System"
    )

    parser.add_argument(
        "command",
        choices=["collect", "convert", "broker-config", "status"],
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
        elif args.command == "convert":
            asyncio.run(cmd_convert(config))
        elif args.command == "broker-config":
            asyncio.run(cmd_broker_config(config))
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
