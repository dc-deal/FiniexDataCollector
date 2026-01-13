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
from typing import Optional

from python.utils.config_loader import ConfigLoader, AppConfig
from python.utils.logging_setup import setup_logging, get_logger
from python.collectors.kraken.websocket_client import KrakenWebSocketClient
from python.collectors.kraken.symbols import normalize_symbol
from python.writers.json_tick_writer import JsonTickWriter
from python.alerts.telegram_bot import TelegramAlertProvider
from python.scheduler.weekly_jobs import WeeklyJobScheduler
from python.converters.tick_to_parquet import run_weekly_conversion
from python.converters.broker_config_fetcher import fetch_kraken_broker_config


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
        
        # State
        self._is_running = False
        self._shutdown_event = asyncio.Event()
    
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
        
        # Wait for shutdown
        await self._shutdown_event.wait()
        
        # Cleanup
        await self._shutdown()
    
    async def _start_kraken_collector(self) -> None:
        """Initialize and start Kraken WebSocket collector."""
        self._logger.info(f"Starting Kraken collector for {len(self._config.kraken.symbols)} symbols")
        
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
        
        # Set tick callback
        collector.set_tick_callback(self._on_tick_received)
        
        self._collectors.append(collector)
        
        # Start collector (non-blocking)
        asyncio.create_task(self._run_collector(collector))
    
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
        # Find writer for this symbol (extract from tick)
        # Symbol is embedded in tick via the parser
        symbol = self._get_symbol_from_tick(tick)
        
        if symbol in self._writers:
            writer = self._writers[symbol]
            writer.write_tick(tick)
            
            # Check for rotation notification
            if writer.needs_rotation() and self._telegram:
                asyncio.create_task(
                    self._telegram.send_file_rotation_notice(
                        symbol=symbol,
                        filename=writer.get_current_filepath().name if writer.get_current_filepath() else "unknown",
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
        # Symbol is determined by which writer receives the tick
        # For now, use a simple mapping based on configured symbols
        # In production, this would be set by the parser
        for symbol, writer in self._writers.items():
            return symbol  # Return first for now
        return "UNKNOWN"
    
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
        """Setup graceful shutdown handlers."""
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
        
        # Stop collectors
        for collector in self._collectors:
            await collector.stop()
        
        # Finalize writers
        for symbol, writer in self._writers.items():
            filepath = writer.finalize()
            if filepath:
                self._logger.info(f"Finalized: {filepath.name}")
        
        # Stop scheduler
        if self._scheduler:
            self._scheduler.stop()
        
        # Send shutdown notification
        if self._telegram:
            await self._telegram.send_info(
                "Collector Stopped",
                "FiniexDataCollector has been shut down"
            )
        
        self._is_running = False
        self._logger.info("Shutdown complete")


async def cmd_collect(config: AppConfig) -> None:
    """Run collection daemon."""
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
    
    output_dir = Path(config.paths.broker_configs_dir) / "kraken"
    
    filepath = await fetch_kraken_broker_config(
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
    logger.info(f"  Conversion: {config.scheduler.conversion_day} {config.scheduler.conversion_hour_utc:02d}:00 UTC")
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
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Load config
    try:
        config_path = Path(args.config)
        if config_path.exists():
            loader = ConfigLoader(config_path)
            config = loader.load()
        else:
            print(f"Config not found: {config_path}, using defaults")
            from python.utils.config_loader import get_default_config
            config = get_default_config()
    except Exception as e:
        print(f"Failed to load config: {e}")
        sys.exit(1)
    
    # Setup logging
    import logging
    log_level = getattr(logging, args.log_level)
    setup_logging(
        log_dir=Path(config.paths.logs_dir),
        log_level=log_level,
        app_name="FiniexDataCollector"
    )
    
    # Execute command
    if args.command == "collect":
        asyncio.run(cmd_collect(config))
    elif args.command == "convert":
        asyncio.run(cmd_convert(config))
    elif args.command == "broker-config":
        asyncio.run(cmd_broker_config(config))
    elif args.command == "status":
        cmd_status(config)


if __name__ == "__main__":
    main()
