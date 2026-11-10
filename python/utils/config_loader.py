"""
FiniexDataCollector - Configuration Loader
Loads and validates application configuration with Pydantic.

Supports hierarchical config merging:
- configs/app_config.json (defaults, tracked)
- user_configs/app_config.json (overrides, gitignored)

Location: python/utils/config_loader.py
"""

import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, List

from pydantic import (BaseModel, Field, ValidationError, field_validator,
                      model_validator)

from python.exceptions.collector_exceptions import ConfigurationError
from python.utils.logging_setup import describe_exception


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
        stale_after_seconds: Silence, heartbeats included, after which the
            connection is closed and reopened; Kraken sends one every second
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
    stale_after_seconds: int = Field(default=10, ge=3, le=60)
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


class ApiConfig(BaseModel):
    """
    Status API configuration.

    Args:
        enabled: Whether to serve the API at all
        host: Interface to bind. Loopback by design - a reverse proxy terminates
            TLS and forwards here, and the port never gets a firewall rule
        port: Loopback port
        tokens: Consumer credentials, `name -> {token, grants, active, note}`.
            Belongs in the gitignored user_configs overlay, never here
    """
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=8110, ge=1024, le=65535)
    tokens: Dict[str, Any] = Field(default_factory=dict)


class PackageSourceConfig(BaseModel):
    """
    One collector whose finished files the package service seals.

    Args:
        root: The folder holding the collector's `instance.json`. It is what proves
            the packer reads the right folder: no mint, no package
        subdir: Where the `*_ticks.json` files lie, relative to `root`. Empty when
            they lie in `root` itself, as MT5's do; `kraken` for this collector,
            whose identity sits one level above its files
        log_dir: The collector's own log folder, for the outage records and process
            starts within a package's span. Empty when the producer writes no such
            records, and then the manifest says `null` rather than "none"
    """
    root: str
    subdir: str = ""
    log_dir: str = ""

    @field_validator("subdir")
    @classmethod
    def subdir_stays_inside_root(cls, value: str) -> str:
        """A subdir that climbs out would read files the mint does not vouch for."""
        parts = Path(value).parts
        if Path(value).is_absolute() or ".." in parts:
            raise ValueError(
                f"subdir must be a folder inside root, got {value!r}")
        return value


class PackagesConfig(BaseModel):
    """
    The package service: weekly sealed zips of finished tick files, served to a
    consumer. A program of its own beside the collector - it reads the collector's
    files and never touches the process.

    Args:
        output_dir: Where packages are written: `<collector>/ready/` until the
            consumer confirms, `<collector>/delivered/` after. Relative to the
            service's own checkout, under its gitignored `data/`
        originals_dir: Where the original tick files go once packed:
            `awaiting/` until the consumer confirms, `disposable/` after. Only
            `purge` deletes from it, and only from `disposable/`
        host: Interface to bind. Loopback; the edge forwards `/v1/packages*` here
        port: Loopback port, well clear of the collector's 8110
        weekday: Day of the weekly run, UTC. Sunday: forex is closed until 21:00
            or 22:00 UTC, so the run meets an idle MT5 terminal
        hour: Hour of the weekly run, UTC
        minute: Minute of the weekly run
        min_free_percent: Free disk share that must remain after a package is
            written, and below which the service reports `disk_low`. The
            collector reports `disk_low` at 20 %, so the warning comes first
        receipt_overdue_days: A package waiting longer than this for a verified
            receipt makes the service report `receipt_overdue`
        disposable_warn_gb: Confirmed originals above this size make the service
            report `disposable_large` - a reminder to purge
        purge_target_free_percent: `purge` without `--full` deletes confirmed
            originals, oldest receipt first, until this much of the disk is free,
            and nothing when it already is
        dev_consumers: Token names whose receipts are rehearsals: every check
            runs - complete download, hash - and the ledger books `dev_received`,
            but nothing is marked and nothing moves. Our own pickup client's,
            never a consumer's
        special_hour: Default packing hour of a special run, UTC, on its cutoff day
        special_minute: Its minute
        special_after_seal_minutes: The earliest a special run may pack after its
            cutoff. The collector seals each day at 00:00 UTC and writes the closed
            files within seconds; the margin makes "sealed" a certainty, not a race
        special_max_per_week: Special runs per collector per ISO week - the guard
            against packages piling up
        special_blackout_hours: No special run this close before a weekly run,
            which would otherwise come out almost empty
        heartbeat_url: Ping address of an outside check that alarms when the pings
            stop. Pinged only while the service's health is `ok`. A credential:
            user_configs/ only
        heartbeat_interval_seconds: Seconds between pings
        sources: `name -> PackageSourceConfig`; the name becomes the package id's
            first part, e.g. `kraken`, `mt5`
        tokens: Consumer credentials, `name -> {token, grants, active, note}`, with
            the surfaces `packages` and `receipts`. Belongs in user_configs/, never
            in the tracked file
    """
    output_dir: str = "./data/packages"
    originals_dir: str = "./data/originals"
    host: str = "127.0.0.1"
    port: int = Field(default=8120, ge=1024, le=65535)
    weekday: str = "sunday"
    hour: int = Field(default=20, ge=0, le=23)
    minute: int = Field(default=0, ge=0, le=59)
    min_free_percent: float = Field(default=30.0, ge=0.0, le=95.0)
    receipt_overdue_days: int = Field(default=14, ge=1, le=365)
    disposable_warn_gb: float = Field(default=10.0, ge=0.1)
    purge_target_free_percent: float = Field(default=50.0, ge=1.0, le=95.0)
    dev_consumers: List[str] = Field(default_factory=list)
    special_hour: int = Field(default=1, ge=0, le=23)
    special_minute: int = Field(default=0, ge=0, le=59)
    special_after_seal_minutes: int = Field(default=30, ge=20, le=720)
    special_max_per_week: int = Field(default=2, ge=0, le=7)
    special_blackout_hours: int = Field(default=24, ge=0, le=72)
    heartbeat_url: str = ""
    heartbeat_interval_seconds: int = Field(default=60, ge=30, le=3600)
    sources: Dict[str, PackageSourceConfig] = Field(default_factory=dict)
    tokens: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("weekday")
    @classmethod
    def weekday_is_a_day(cls, value: str) -> str:
        """The schedule's day, by name."""
        days = ("monday", "tuesday", "wednesday", "thursday", "friday",
                "saturday", "sunday")
        if value.lower() not in days:
            raise ValueError(f"weekday must be one of {days}, got {value!r}")
        return value.lower()

    @field_validator("sources")
    @classmethod
    def source_names_are_ids(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        """A source name is part of every package id, so it is kept plain."""
        for name in value:
            if not re.fullmatch(r"[a-z0-9]+", name):
                raise ValueError(
                    f"source name must be lowercase letters and digits, got {name!r}")
        return value

    @model_validator(mode="after")
    def not_in_the_quiet_minutes(self) -> "PackagesConfig":
        """
        23:55 to 00:15 UTC belongs to the day cut and to FiniexTestingIDE's daily
        order; neither the weekly run nor a special run's default time falls into it.
        """
        for what, hour, minute in (("the weekly run", self.hour, self.minute),
                                   ("a special run's default", self.special_hour,
                                    self.special_minute)):
            minute_of_day = hour * 60 + minute
            if minute_of_day >= 23 * 60 + 55 or minute_of_day <= 15:
                raise ValueError(
                    f"{what} at {hour:02d}:{minute:02d} UTC falls "
                    f"into 23:55-00:15 UTC, which stays quiet")
        return self


class MonitoringConfig(BaseModel):
    """
    Monitoring configuration for disk space and folder scanning.

    Args:
        disk_space_check_interval_seconds: Disk space check interval
        folder_scan_interval_seconds: Folder file count scan interval
        reconnect_alert_cooldown_minutes: Minutes to wait before next reconnect alert
        reconnect_alert_min_seconds: Data gap - last message before the drop to
            the feed restored - below which an outage is logged and not
            alerted. Since 2026-10-08 a closed socket is noticed at once and a
            normal drop costs a few seconds - a silent link, which only the 10 s
            watchdog finds, still well under 30 - so the default of 30 s means
            reconnecting itself is failing or the link stayed dead. (Until then the threshold was justified as the lag
            window that gets a whole file refused; that was wrong - an outage
            shortens a file. The figures of 2026-09-21/22 behind it, 1.3 to
            7.2 s per drop, left out the 10-11 s before each was noticed.)
        reconnect_alert_cluster: How many reconnects inside one hour are worth
            an alert whatever their length. A host that blips every two hours is
            weather; one that blips four times an hour is degrading, and that
            distinction is the only thing a short reconnect can still tell you.
        heartbeat_url: Ping address of an outside check, such as healthchecks.io,
            which alarms when the pings stop. Empty is off. It belongs in
            user_configs/, never in the tracked file: whoever holds it can report
            this collector healthy.
        heartbeat_interval_seconds: Seconds between pings. Keep the check's period
            well above it, so that one lost ping is not an alarm.
    """
    disk_space_check_interval_seconds: int = Field(default=60, ge=10, le=600)
    folder_scan_interval_seconds: int = Field(default=60, ge=10, le=600)
    reconnect_alert_cooldown_minutes: int = Field(default=30, ge=1, le=1440)
    reconnect_alert_min_seconds: float = Field(default=30.0, ge=0.0, le=3600.0)
    reconnect_alert_cluster: int = Field(default=4, ge=2, le=100)
    heartbeat_url: str = ""
    heartbeat_interval_seconds: int = Field(default=60, ge=30, le=3600)


class AppConfig(BaseModel):
    """
    Main application configuration.

    Args:
        app_name: Application name
        version: Application version
        logging: Logging configuration
        paths: Path configuration
        kraken: Kraken collector configuration
        mt5: MT5 monitoring configuration
        telegram: Telegram alerts configuration
        scheduler: Scheduler configuration
        monitoring: Monitoring configuration
        api: Status API configuration
        packages: The package service, a separate program; the collector never
            reads this block
    """
    app_name: str = "FiniexDataCollector"
    version: str

    logging: LoggingConfig
    paths: PathsConfig = Field(default_factory=PathsConfig)
    kraken: KrakenCollectorConfig
    mt5: MT5Config = Field(default_factory=MT5Config)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    packages: PackagesConfig = Field(default_factory=PackagesConfig)


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
                f"Invalid JSON in config file: {describe_exception(e)}",
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
                    f"Invalid JSON in user config: {describe_exception(e)}",
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
