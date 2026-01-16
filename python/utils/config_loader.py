"""
FiniexDataCollector - Configuration Loader
Loads and validates application configuration from JSON files.

Location: python/utils/config_loader.py
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any

from python.exceptions.collector_exceptions import ConfigurationError


@dataclass
class TelegramConfig:
    """Telegram bot configuration."""
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""
    send_on_error: bool = True
    send_on_rotation: bool = False
    send_weekly_report: bool = True


@dataclass
class TransferConfig:
    """Data transfer configuration."""
    provider: str = "local"           # "local", "rsync", "sftp"
    local_output_dir: str = "./output"
    rsync_host: str = ""
    rsync_path: str = ""
    rsync_user: str = ""


@dataclass
class SchedulerConfig:
    """Scheduler configuration for weekly jobs."""
    parquet_conversion_enabled: bool = True
    conversion_day: str = "saturday"  # Day of week
    conversion_hour_utc: int = 6      # Hour in UTC
    report_email_enabled: bool = False
    report_email_address: str = ""


@dataclass
class KrakenCollectorConfig:
    """Kraken WebSocket collector configuration."""
    enabled: bool = True
    websocket_url: str = "wss://ws.kraken.com/v2"
    symbols: List[str] = field(default_factory=lambda: [
        "BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD",
        "MATIC/USD", "AVAX/USD", "LINK/USD", "DOT/USD"
    ])
    reconnect_max_delay_seconds: int = 60
    reconnect_initial_delay_seconds: int = 1
    heartbeat_interval_seconds: int = 30
    max_ticks_per_file: int = 50000


@dataclass
class MT5Config:
    """MT5 data source configuration."""
    enabled: bool = False
    raw_data_path: str = ""           # Path to MT5 JSON output
    watch_interval_seconds: int = 60


@dataclass
class PathsConfig:
    """File path configuration."""
    raw_data_dir: str = "./data/raw"
    processed_data_dir: str = "./data/processed"
    broker_configs_dir: str = "./configs/brokers"
    logs_dir: str = "./logs"


@dataclass
class AppConfig:
    """
    Main application configuration.

    Loaded from configs/app_config.json
    """
    app_name: str = "FiniexDataCollector"
    version: str = "1.0.0"
    environment: str = "production"   # "development", "production"

    paths: PathsConfig = field(default_factory=PathsConfig)
    kraken: KrakenCollectorConfig = field(
        default_factory=KrakenCollectorConfig)
    mt5: MT5Config = field(default_factory=MT5Config)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    transfer: TransferConfig = field(default_factory=TransferConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)


class ConfigLoader:
    """
    Loads and validates application configuration.

    Supports nested dataclass structures from JSON.
    """

    DEFAULT_CONFIG_PATH = Path("./configs/app_config.json")

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize config loader.

        Args:
            config_path: Path to config file (optional, uses default if None)
        """
        self._config_path = config_path or self.DEFAULT_CONFIG_PATH
        self._config: Optional[AppConfig] = None

    def load(self) -> AppConfig:
        """
        Load configuration from JSON file.

        Returns:
            AppConfig instance with loaded settings

        Raises:
            ConfigurationError: If config file missing or invalid
        """
        if not self._config_path.exists():
            raise ConfigurationError(
                "Configuration file not found",
                config_file=str(self._config_path)
            )

        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in config file: {e}",
                config_file=str(self._config_path)
            )

        self._config = self._parse_config(data)
        return self._config

    def _parse_config(self, data: Dict[str, Any]) -> AppConfig:
        """
        Parse JSON data into AppConfig dataclass.

        Args:
            data: Raw JSON data

        Returns:
            Populated AppConfig instance
        """
        return AppConfig(
            app_name=data.get("app_name", "FiniexDataCollector"),
            version=data.get("version", "1.0.0"),
            environment=data.get("environment", "production"),
            paths=self._parse_paths(data.get("paths", {})),
            kraken=self._parse_kraken(data.get("kraken", {})),
            mt5=self._parse_mt5(data.get("mt5", {})),
            telegram=self._parse_telegram(data.get("telegram", {})),
            transfer=self._parse_transfer(data.get("transfer", {})),
            scheduler=self._parse_scheduler(data.get("scheduler", {})),
        )

    def _parse_paths(self, data: Dict[str, Any]) -> PathsConfig:
        """Parse paths configuration section."""
        return PathsConfig(
            raw_data_dir=data.get("raw_data_dir", "./data/raw"),
            processed_data_dir=data.get(
                "processed_data_dir", "./data/processed"),
            broker_configs_dir=data.get(
                "broker_configs_dir", "./configs/brokers"),
            logs_dir=data.get("logs_dir", "./logs"),
        )

    def _parse_kraken(self, data: Dict[str, Any]) -> KrakenCollectorConfig:
        """Parse Kraken collector configuration section."""
        return KrakenCollectorConfig(
            enabled=data.get("enabled", True),
            websocket_url=data.get("websocket_url", "wss://ws.kraken.com/v2"),
            symbols=data.get("symbols", [
                "BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD",
                "MATIC/USD", "AVAX/USD", "LINK/USD", "DOT/USD"
            ]),
            reconnect_max_delay_seconds=data.get(
                "reconnect_max_delay_seconds", 60),
            reconnect_initial_delay_seconds=data.get(
                "reconnect_initial_delay_seconds", 1),
            heartbeat_interval_seconds=data.get(
                "heartbeat_interval_seconds", 30),
            max_ticks_per_file=data.get("max_ticks_per_file", 50000),
        )

    def _parse_mt5(self, data: Dict[str, Any]) -> MT5Config:
        """Parse MT5 configuration section."""
        return MT5Config(
            enabled=data.get("enabled", False),
            raw_data_path=data.get("raw_data_path", ""),
            watch_interval_seconds=data.get("watch_interval_seconds", 60),
        )

    def _parse_telegram(self, data: Dict[str, Any]) -> TelegramConfig:
        """Parse Telegram configuration section."""
        return TelegramConfig(
            enabled=data.get("enabled", False),
            bot_token=data.get("bot_token", ""),
            chat_id=data.get("chat_id", ""),
            send_on_error=data.get("send_on_error", True),
            send_on_rotation=data.get("send_on_rotation", False),
            send_weekly_report=data.get("send_weekly_report", True),
        )

    def _parse_transfer(self, data: Dict[str, Any]) -> TransferConfig:
        """Parse transfer configuration section."""
        return TransferConfig(
            provider=data.get("provider", "local"),
            local_output_dir=data.get("local_output_dir", "./output"),
            rsync_host=data.get("rsync_host", ""),
            rsync_path=data.get("rsync_path", ""),
            rsync_user=data.get("rsync_user", ""),
        )

    def _parse_scheduler(self, data: Dict[str, Any]) -> SchedulerConfig:
        """Parse scheduler configuration section."""
        return SchedulerConfig(
            parquet_conversion_enabled=data.get(
                "parquet_conversion_enabled", True),
            conversion_day=data.get("conversion_day", "saturday"),
            conversion_hour_utc=data.get("conversion_hour_utc", 6),
            report_email_enabled=data.get("report_email_enabled", False),
            report_email_address=data.get("report_email_address", ""),
        )

    def get_config(self) -> AppConfig:
        """
        Get loaded configuration (loads if not yet loaded).

        Returns:
            AppConfig instance
        """
        if self._config is None:
            return self.load()
        return self._config

    def reload(self) -> AppConfig:
        """
        Force reload configuration from file.

        Returns:
            Fresh AppConfig instance
        """
        self._config = None
        return self.load()


def get_default_config() -> AppConfig:
    """
    Get default configuration without file.

    Returns:
        AppConfig with default values
    """
    return AppConfig()


def load_config(config_path: Optional[Path] = None) -> AppConfig:
    """
    Convenience function to load configuration.

    Args:
        config_path: Optional path to config file

    Returns:
        Loaded AppConfig instance
    """
    loader = ConfigLoader(config_path)
    return loader.load()
