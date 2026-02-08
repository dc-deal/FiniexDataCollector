"""
FiniexDataCollector - Configuration Loader
Loads and validates application configuration with Pydantic.

Supports hierarchical config merging:
- configs/app_config.json (defaults, tracked)
- user_configs/app_config.json (overrides, gitignored)

Location: python/utils/config_loader.py
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List

from pydantic import BaseModel, Field, field_validator, ValidationError

from python.exceptions.collector_exceptions import ConfigurationError


class LoggingConfig(BaseModel):
    """
    Logging configuration.

    Args:
        console_level: Console log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        file_level: File log level
    """
    console_level: str = Field(..., min_length=1)
    file_level: str = Field(..., min_length=1)

    @field_validator('console_level', 'file_level')
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(
                f"Invalid log level: '{v}'. Valid: {', '.join(valid_levels)}"
            )
        return v_upper


class TelegramConfig(BaseModel):
    """
    Telegram bot configuration.

    Args:
        enabled: Enable Telegram alerts
        bot_token: Bot API token
        chat_id: Target chat ID
        send_on_error: Send alerts on errors
        send_on_rotation: Send alerts on file rotation
        send_weekly_report: Send weekly summary report
    """
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""
    send_on_error: bool = True
    send_on_rotation: bool = False
    send_weekly_report: bool = True


class SchedulerConfig(BaseModel):
    """
    Scheduler configuration for weekly reports.

    Args:
        report_day: Day of week for report (monday-sunday)
        report_hour_utc: Hour in UTC (0-23)
        report_minute_utc: Minute in UTC (0-59)
    """
    report_day: str = Field(default="saturday")
    report_hour_utc: int = Field(default=6, ge=0, le=23)
    report_minute_utc: int = Field(default=0, ge=0, le=59)

    @field_validator('report_day')
    def validate_day(cls, v: str) -> str:
        """Validate day of week."""
        valid_days = [
            'monday', 'tuesday', 'wednesday', 'thursday',
            'friday', 'saturday', 'sunday'
        ]
        v_lower = v.lower()
        if v_lower not in valid_days:
            raise ValueError(
                f"Invalid day: '{v}'. Valid: {', '.join(valid_days)}"
            )
        return v_lower


class KrakenCollectorConfig(BaseModel):
    """
    Kraken WebSocket collector configuration.

    Args:
        enabled: Enable Kraken collector
        broker_type: Broker type identifier (e.g. 'kraken_spot')
        server_name: Server identifier (defaults to broker_type_server)
        websocket_url: WebSocket URL
        symbols: List of symbols (format: BASE/QUOTE, e.g. BTC/USD)
        streams: List of streams (ticker, trade)
        reconnect_max_delay_seconds: Maximum reconnect delay
        reconnect_initial_delay_seconds: Initial reconnect delay
        heartbeat_interval_seconds: Heartbeat check interval
        max_ticks_per_file: Maximum ticks before file rotation
    """
    enabled: bool = True
    broker_type: str = Field(..., min_length=1)
    server_name: Optional[str] = None
    websocket_url: str = "wss://ws.kraken.com/v2"
    symbols: List[str] = Field(..., min_length=1)
    streams: List[str] = Field(default=["ticker"])
    reconnect_max_delay_seconds: int = Field(default=60, ge=1, le=300)
    reconnect_initial_delay_seconds: int = Field(default=1, ge=1, le=60)
    heartbeat_interval_seconds: int = Field(default=30, ge=10, le=120)
    max_ticks_per_file: int = Field(default=50000, ge=100, le=1000000)

    @field_validator('symbols')
    def validate_symbols(cls, v: List[str]) -> List[str]:
        """Validate symbols format."""
        for symbol in v:
            if '/' not in symbol:
                raise ValueError(
                    f"Symbol must be in BASE/QUOTE format: '{symbol}'"
                )
        return v

    @field_validator('streams')
    def validate_streams(cls, v: List[str]) -> List[str]:
        """Validate streams."""
        valid_streams = {'ticker', 'trade'}
        for stream in v:
            if stream not in valid_streams:
                raise ValueError(
                    f"Invalid stream: '{stream}'. Valid: {', '.join(valid_streams)}"
                )
        return v

    def model_post_init(self, __context: Any) -> None:
        """Set default server_name if not provided."""
        if not self.server_name:
            object.__setattr__(self, 'server_name',
                               f"{self.broker_type}_server")


class MT5Config(BaseModel):
    """
    MT5 data source configuration for monitoring.

    Args:
        enabled: Enable MT5 folder monitoring in reports
        raw_data_path: Path to MT5 JSON output folder
    """
    enabled: bool = False
    raw_data_path: str = ""


class PathsConfig(BaseModel):
    """
    File path configuration.

    Args:
        raw_data_dir: Directory for raw tick data
        broker_configs_dir: Directory for broker configs
        logs_dir: Directory for log files
    """
    raw_data_dir: str = "./data/raw"
    broker_configs_dir: str = "./configs/brokers"
    logs_dir: str = "./logs"


class MonitoringConfig(BaseModel):
    """
    Monitoring configuration for disk space and folder scanning.

    Args:
        disk_space_check_interval_seconds: Disk space check interval
        folder_scan_interval_seconds: Folder file count scan interval
        reconnect_alert_cooldown_minutes: Minutes to wait before next reconnect alert
    """
    disk_space_check_interval_seconds: int = Field(default=60, ge=10, le=600)
    folder_scan_interval_seconds: int = Field(default=60, ge=10, le=600)
    reconnect_alert_cooldown_minutes: int = Field(default=30, ge=1, le=1440)


class AppConfig(BaseModel):
    """
    Main application configuration.

    Args:
        app_name: Application name
        version: Application version
        environment: Environment (development, production)
        logging: Logging configuration
        paths: Path configuration
        kraken: Kraken collector configuration
        mt5: MT5 monitoring configuration
        telegram: Telegram alerts configuration
        scheduler: Scheduler configuration
        monitoring: Monitoring configuration
    """
    app_name: str = "FiniexDataCollector"
    version: str = "1.0.0"
    environment: str = "production"

    logging: LoggingConfig
    paths: PathsConfig = Field(default_factory=PathsConfig)
    kraken: KrakenCollectorConfig
    mt5: MT5Config = Field(default_factory=MT5Config)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)


class ConfigLoader:
    """
    Loads and validates application configuration with Pydantic.

    Supports hierarchical config merging:
    1. configs/app_config.json (defaults, tracked)
    2. user_configs/app_config.json (overrides, gitignored)
    """

    DEFAULT_CONFIG_PATH = Path("./configs/app_config.json")
    USER_CONFIG_PATH = Path("./user_configs/app_config.json")

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize config loader.

        Args:
            config_path: Path to base config file (optional)
        """
        self._config_path = config_path or self.DEFAULT_CONFIG_PATH
        self._config: Optional[AppConfig] = None

    def load(self) -> AppConfig:
        """
        Load and merge configurations.

        Returns:
            AppConfig instance with merged settings

        Raises:
            ConfigurationError: If config invalid or missing
        """
        # Load base config
        if not self._config_path.exists():
            raise ConfigurationError(
                "Configuration file not found",
                config_file=str(self._config_path)
            )

        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                base_data = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in config file: {e}",
                config_file=str(self._config_path)
            )

        # Load user overrides (optional)
        user_data = {}
        if self.USER_CONFIG_PATH.exists():
            try:
                with open(self.USER_CONFIG_PATH, 'r', encoding='utf-8') as f:
                    user_data = json.load(f)
            except json.JSONDecodeError as e:
                raise ConfigurationError(
                    f"Invalid JSON in user config: {e}",
                    config_file=str(self.USER_CONFIG_PATH)
                )

        # Deep merge: base <- user
        merged = self._deep_merge(base_data, user_data)

        # Validate with Pydantic
        try:
            self._config = AppConfig.model_validate(merged)
        except ValidationError as e:
            # Convert Pydantic errors to ConfigurationError
            errors = []
            for error in e.errors():
                loc = '.'.join(str(x) for x in error['loc'])
                errors.append(f"{loc}: {error['msg']}")

            raise ConfigurationError(
                f"Configuration validation failed:\n" + "\n".join(errors),
                config_file=str(self._config_path)
            )

        return self._config

    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.

        Args:
            base: Base dictionary
            override: Override dictionary

        Returns:
            Merged dictionary
        """
        result = base.copy()

        for key, value in override.items():
            if (
                key in result and
                isinstance(result[key], dict) and
                isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def get_config(self) -> AppConfig:
        """
        Get loaded configuration.

        Returns:
            AppConfig instance

        Raises:
            ConfigurationError: If config not loaded
        """
        if self._config is None:
            return self.load()
        return self._config

    def reload(self) -> AppConfig:
        """
        Force reload configuration from files.

        Returns:
            Fresh AppConfig instance
        """
        self._config = None
        return self.load()


def get_default_config() -> AppConfig:
    """
    Get default configuration without file loading.

    Returns:
        AppConfig with default values
    """
    return AppConfig(
        logging=LoggingConfig(console_level="INFO", file_level="INFO"),
        kraken=KrakenCollectorConfig(
            broker_type="kraken_spot",
            symbols=["BTC/USD"]
        )
    )


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
