"""
FiniexDataCollector - Collector Exceptions
Custom exceptions for tick collection and processing.

Location: python/exceptions/collector_exceptions.py
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any


class CollectorException(Exception):
    """Base exception for all collector errors."""
    pass


class WebSocketConnectionError(CollectorException):
    """Raised when WebSocket connection fails."""

    def __init__(
        self,
        message: str,
        url: str,
        attempt: int = 1,
        max_attempts: int = 10
    ):
        self.url = url
        self.attempt = attempt
        self.max_attempts = max_attempts
        super().__init__(
            f"{message} (URL: {url}, Attempt {attempt}/{max_attempts})"
        )


class WebSocketSubscriptionError(CollectorException):
    """Raised when subscription to channel fails."""

    def __init__(
        self,
        message: str,
        channel: str,
        symbols: List[str]
    ):
        self.channel = channel
        self.symbols = symbols
        super().__init__(
            f"{message} (Channel: {channel}, Symbols: {symbols})"
        )


class MessageParseError(CollectorException):
    """Raised when WebSocket message parsing fails."""

    def __init__(
        self,
        message: str,
        raw_message: str,
        symbol: Optional[str] = None
    ):
        self.raw_message = raw_message
        self.symbol = symbol
        detail = f" for {symbol}" if symbol else ""
        super().__init__(f"{message}{detail}: {raw_message[:200]}")


class TickWriteError(CollectorException):
    """Raised when tick write operation fails."""

    def __init__(
        self,
        message: str,
        filepath: str,
        tick_count: int = 0
    ):
        self.filepath = filepath
        self.tick_count = tick_count
        super().__init__(
            f"{message} (File: {filepath}, Ticks: {tick_count})"
        )


class FileRotationError(CollectorException):
    """Raised when file rotation fails."""

    def __init__(
        self,
        message: str,
        current_file: str,
        new_file: str
    ):
        self.current_file = current_file
        self.new_file = new_file
        super().__init__(
            f"{message} (Current: {current_file} -> New: {new_file})"
        )


class ConfigurationError(CollectorException):
    """Raised when configuration is invalid or missing."""

    def __init__(
        self,
        message: str,
        config_file: Optional[str] = None,
        missing_keys: Optional[List[str]] = None
    ):
        self.config_file = config_file
        self.missing_keys = missing_keys or []

        detail = ""
        if config_file:
            detail += f" (File: {config_file})"
        if missing_keys:
            detail += f" Missing: {missing_keys}"

        super().__init__(f"{message}{detail}")


class BrokerConfigError(CollectorException):
    """Raised when broker config fetch or parse fails."""

    def __init__(
        self,
        message: str,
        broker: str,
        endpoint: Optional[str] = None
    ):
        self.broker = broker
        self.endpoint = endpoint
        detail = f" (Broker: {broker}"
        if endpoint:
            detail += f", Endpoint: {endpoint}"
        detail += ")"
        super().__init__(f"{message}{detail}")


class ParquetConversionError(CollectorException):
    """Raised when JSON to Parquet conversion fails."""

    def __init__(
        self,
        message: str,
        source_file: str,
        target_file: Optional[str] = None
    ):
        self.source_file = source_file
        self.target_file = target_file
        detail = f" (Source: {source_file}"
        if target_file:
            detail += f" -> {target_file}"
        detail += ")"
        super().__init__(f"{message}{detail}")


class AlertDeliveryError(CollectorException):
    """Raised when alert delivery fails."""

    def __init__(
        self,
        message: str,
        provider: str,
        alert_type: str
    ):
        self.provider = provider
        self.alert_type = alert_type
        super().__init__(
            f"{message} (Provider: {provider}, Type: {alert_type})"
        )


@dataclass
class CollectorHealthReport:
    """
    Health report for collector diagnostics.

    Used when collector encounters issues that need reporting.
    """
    collector_name: str
    status: str                          # "healthy", "degraded", "failed"
    uptime_seconds: float
    ticks_collected: int
    errors_count: int
    last_tick_time: Optional[str]
    connection_status: str
    symbols_active: List[str]
    warnings: List[str]
    details: Dict[str, Any]

    def get_report(self) -> str:
        """Generate human-readable health report."""
        lines = [
            "=" * 60,
            f"COLLECTOR HEALTH REPORT: {self.collector_name}",
            "=" * 60,
            f"Status:      {self.status.upper()}",
            f"Uptime:      {self.uptime_seconds:.1f}s",
            f"Ticks:       {self.ticks_collected:,}",
            f"Errors:      {self.errors_count}",
            f"Connection:  {self.connection_status}",
            f"Last Tick:   {self.last_tick_time or 'N/A'}",
            f"Symbols:     {', '.join(self.symbols_active)}",
        ]

        if self.warnings:
            lines.append("\nWarnings:")
            for warning in self.warnings:
                lines.append(f"  ⚠️  {warning}")

        lines.append("=" * 60)
        return "\n".join(lines)
