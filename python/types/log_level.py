"""
FiniexDataCollector - Log Level Types
Log level definitions with priority and color.

Location: python/types/log_level.py
"""

from dataclasses import dataclass
from typing import Dict, Optional


# ANSI Color Codes
ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_RED = "\033[31m"
ANSI_YELLOW = "\033[33m"
ANSI_CYAN = "\033[36m"
ANSI_WHITE = "\033[37m"
ANSI_BRIGHT_RED = "\033[91m"


@dataclass(frozen=True)
class LogLevel:
    """
    Log level definition with priority and color.

    Attributes:
        name: Level name (DEBUG, INFO, etc.)
        priority: Numeric priority (lower = more verbose)
        color: ANSI color code for console output
    """
    name: str
    priority: int
    color: str

    def __str__(self) -> str:
        return self.name

    def __ge__(self, other: "LogLevel") -> bool:
        return self.priority >= other.priority

    def __gt__(self, other: "LogLevel") -> bool:
        return self.priority > other.priority

    def __le__(self, other: "LogLevel") -> bool:
        return self.priority <= other.priority

    def __lt__(self, other: "LogLevel") -> bool:
        return self.priority < other.priority


# Standard log levels
DEBUG = LogLevel(name="DEBUG", priority=10, color=ANSI_CYAN)
INFO = LogLevel(name="INFO", priority=20, color=ANSI_WHITE)
WARNING = LogLevel(name="WARNING", priority=30, color=ANSI_YELLOW)
ERROR = LogLevel(name="ERROR", priority=40, color=ANSI_RED)
CRITICAL = LogLevel(name="CRITICAL", priority=50,
                    color=f"{ANSI_BOLD}{ANSI_BRIGHT_RED}")


# Registry for lookup by name
_LOG_LEVEL_REGISTRY: Dict[str, LogLevel] = {
    "DEBUG": DEBUG,
    "INFO": INFO,
    "WARNING": WARNING,
    "ERROR": ERROR,
    "CRITICAL": CRITICAL,
}


def get_log_level(name: str) -> LogLevel:
    """
    Get LogLevel by name.

    Args:
        name: Level name (case-insensitive)

    Returns:
        LogLevel instance

    Raises:
        ValueError: If level name is unknown
    """
    name_upper = name.upper()

    if name_upper not in _LOG_LEVEL_REGISTRY:
        valid_levels = ", ".join(_LOG_LEVEL_REGISTRY.keys())
        raise ValueError(
            f"Unknown log level: '{name}'. Valid levels: {valid_levels}"
        )

    return _LOG_LEVEL_REGISTRY[name_upper]


def register_log_level(level: LogLevel) -> None:
    """
    Register a custom log level.

    Args:
        level: LogLevel to register
    """
    _LOG_LEVEL_REGISTRY[level.name.upper()] = level


def get_all_log_levels() -> Dict[str, LogLevel]:
    """
    Get all registered log levels.

    Returns:
        Dict of name -> LogLevel
    """
    return _LOG_LEVEL_REGISTRY.copy()
