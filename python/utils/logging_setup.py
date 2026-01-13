"""
FiniexDataCollector - Logging Setup
Configures application logging with console and file handlers.

Location: python/utils/logging_setup.py
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class UTCFormatter(logging.Formatter):
    """Formatter that uses UTC timestamps."""
    
    converter = lambda *args: datetime.now(timezone.utc).timetuple()
    
    def formatTime(self, record, datefmt=None):
        """Format time in UTC."""
        ct = datetime.now(timezone.utc)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime("%Y-%m-%d %H:%M:%S UTC")


def setup_logging(
    log_dir: Optional[Path] = None,
    log_level: int = logging.INFO,
    log_to_file: bool = True,
    log_to_console: bool = True,
    app_name: str = "FiniexDataCollector"
) -> logging.Logger:
    """
    Configure application logging.
    
    Args:
        log_dir: Directory for log files
        log_level: Logging level (default: INFO)
        log_to_file: Enable file logging
        log_to_console: Enable console logging
        app_name: Application name for logger
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Format string
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    formatter = UTCFormatter(log_format)
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_to_file and log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Daily log file
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_file = log_dir / f"{app_name.lower()}_{today}.log"
        
        file_handler = logging.FileHandler(
            log_file,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = "FiniexDataCollector") -> logging.Logger:
    """
    Get logger instance by name.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def get_collector_logger(collector_name: str) -> logging.Logger:
    """
    Get logger for specific collector.
    
    Args:
        collector_name: Name of collector (e.g., "kraken")
        
    Returns:
        Logger instance with collector prefix
    """
    return logging.getLogger(f"FiniexDataCollector.{collector_name}")
