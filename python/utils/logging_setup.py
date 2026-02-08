"""
FiniexDataCollector - Logging Setup
Custom logger with colored console output and config-driven levels.

Location: python/utils/logging_setup.py
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, TextIO

from python.types.log_level import (
    LogLevel,
    get_log_level,
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL,
    ANSI_RESET,
    ANSI_RED
)


def _print_error(message: str) -> None:
    """
    Print error message in red to stderr.
    Used for logger initialization failures.

    Args:
        message: Error message
    """
    print(f"{ANSI_RED}[LOGGER ERROR] {message}{ANSI_RESET}", file=sys.stderr)


class FiniexLogger:
    """
    Custom logger with colored console output.

    Features:
    - Colored console output (configurable per level)
    - Plain text file output
    - UTC timestamps
    - Compatible interface with standard logging
    """

    def __init__(
        self,
        name: str,
        console_level: LogLevel,
        file_level: Optional[LogLevel] = None,
        log_file: Optional[Path] = None
    ):
        """
        Initialize logger.

        Args:
            name: Logger name
            console_level: Minimum level for console output
            file_level: Minimum level for file output (None = no file logging)
            log_file: Path to log file (required if file_level set)
        """
        self._name = name
        self._console_level = console_level
        self._file_level = file_level
        self._log_file = log_file
        self._file_handle: Optional[TextIO] = None

        # Open file if configured
        if self._file_level and self._log_file:
            try:
                log_path = Path(self._log_file).absolute()
                log_path.parent.mkdir(parents=True, exist_ok=True)

                # Path.open() ist robuster als open()
                self._file_handle = log_path.open("a", encoding="utf-8")
            except Exception as e:
                _print_error(f"Failed to open log file {log_path}: {e}")

    def _log(self, level: LogLevel, message: str) -> None:
        """
        Internal log method.

        Args:
            level: Log level
            message: Log message
        """
        timestamp = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC")

        # Console output (colored)
        if level >= self._console_level:
            colored_line = (
                f"{timestamp} | "
                f"{level.color}{level.name:<8}{ANSI_RESET} | "
                f"{self._name} | {message}"
            )
            print(colored_line)

        # File output (plain text)
        if self._file_handle and self._file_level and level >= self._file_level:
            plain_line = f"{timestamp} | {level.name:<8} | {self._name} | {message}\n"
            try:
                self._file_handle.write(plain_line)
                self._file_handle.flush()
            except Exception as e:
                _print_error(f"Failed to write to log file: {e}")

    def debug(self, message: str) -> None:
        """Log debug message."""
        self._log(DEBUG, message)

    def info(self, message: str) -> None:
        """Log info message."""
        self._log(INFO, message)

    def warning(self, message: str) -> None:
        """Log warning message."""
        self._log(WARNING, message)

    def error(self, message: str) -> None:
        """Log error message."""
        self._log(ERROR, message)

    def critical(self, message: str) -> None:
        """Log critical message."""
        self._log(CRITICAL, message)

    def close(self) -> None:
        """Close file handle."""
        if self._file_handle:
            try:
                self._file_handle.close()
            except Exception:
                pass
            self._file_handle = None


# Global logger registry
_loggers: Dict[str, FiniexLogger] = {}
_global_console_level: Optional[LogLevel] = None
_global_file_level: Optional[LogLevel] = None
_global_log_dir: Optional[Path] = None
_initialized = False


def setup_logging(
    console_level: str,
    file_level: str,
    log_dir: Path
) -> None:
    """
    Initialize global logging configuration.

    MUST be called before any get_logger() calls.

    Args:
        console_level: Console log level name (e.g., "DEBUG", "INFO")
        file_level: File log level name
        log_dir: Directory for log files

    Raises:
        ValueError: If log level is unknown
        RuntimeError: If log_dir cannot be created
    """
    global _global_console_level, _global_file_level, _global_log_dir, _initialized

    try:
        _global_console_level = get_log_level(console_level)
    except ValueError as e:
        _print_error(str(e))
        raise

    try:
        _global_file_level = get_log_level(file_level)
    except ValueError as e:
        _print_error(str(e))
        raise

    _global_log_dir = Path(log_dir)

    try:
        _global_log_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _print_error(f"Failed to create log directory {log_dir}: {e}")
        raise RuntimeError(f"Failed to create log directory: {e}")

    _initialized = True


def _get_log_file() -> Path:
    """Get current log file path (daily rotation)."""
    if not _global_log_dir:
        raise RuntimeError("Logging not initialized")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _global_log_dir / f"finiexdatacollector_{date_str}.log"


def get_logger(name: str) -> FiniexLogger:
    """
    Get or create logger by name.

    Args:
        name: Logger name

    Returns:
        FiniexLogger instance

    Raises:
        RuntimeError: If logging not initialized
    """
    if not _initialized:
        _print_error(
            "Logging not initialized! Call setup_logging() first. "
            "Check 'logging' section in app_config.json"
        )
        raise RuntimeError(
            "Logging not initialized. Ensure 'logging' section exists in app_config.json "
            "with 'console_level' and 'file_level' settings."
        )

    if name not in _loggers:
        _loggers[name] = FiniexLogger(
            name=name,
            console_level=_global_console_level,
            file_level=_global_file_level,
            log_file=_get_log_file()
        )

    return _loggers[name]


def get_collector_logger(collector_name: str) -> FiniexLogger:
    """
    Get logger for a collector.

    Args:
        collector_name: Collector identifier

    Returns:
        FiniexLogger instance
    """
    return get_logger(f"FiniexDataCollector.{collector_name}")


def close_all_loggers() -> None:
    """Close all logger file handles."""
    for logger in _loggers.values():
        logger.close()
    _loggers.clear()
