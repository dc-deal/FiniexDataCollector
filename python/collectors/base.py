"""
FiniexDataCollector - Abstract Base Collector
Base class for all tick data collectors.

Location: python/collectors/base.py
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional, Callable

from python.types.tick_types import TickData
from python.exceptions.collector_exceptions import CollectorHealthReport


class AbstractCollector(ABC):
    """
    Abstract base class for tick data collectors.

    Defines interface for MT5, Kraken, and future collectors.
    """

    def __init__(self, name: str, symbols: List[str]):
        """
        Initialize collector.

        Args:
            name: Collector identifier (e.g., "kraken", "mt5")
            symbols: List of symbols to collect
        """
        self._name = name
        self._symbols = symbols
        self._is_running = False
        self._start_time: Optional[datetime] = None
        self._ticks_collected = 0
        self._errors_count = 0
        self._on_tick_callback: Optional[Callable[[TickData], None]] = None

    @property
    def name(self) -> str:
        """Get collector name."""
        return self._name

    @property
    def symbols(self) -> List[str]:
        """Get list of symbols being collected."""
        return self._symbols

    @property
    def is_running(self) -> bool:
        """Check if collector is currently running."""
        return self._is_running

    @property
    def ticks_collected(self) -> int:
        """Get total ticks collected."""
        return self._ticks_collected

    def set_tick_callback(self, callback: Callable[[TickData], None]) -> None:
        """
        Set callback for incoming ticks.

        Args:
            callback: Function to call with each tick
        """
        self._on_tick_callback = callback

    def _emit_tick(self, tick: TickData) -> None:
        """
        Emit tick to registered callback.

        Args:
            tick: Tick data to emit
        """
        self._ticks_collected += 1
        if self._on_tick_callback:
            self._on_tick_callback(tick)

    def _get_uptime_seconds(self) -> float:
        """Get collector uptime in seconds."""
        if not self._start_time:
            return 0.0
        delta = datetime.now(timezone.utc) - self._start_time
        return delta.total_seconds()

    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to data source.

        Returns:
            True if connection successful
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from data source."""
        pass

    @abstractmethod
    async def start(self) -> None:
        """Start tick collection."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop tick collection gracefully."""
        pass

    @abstractmethod
    def get_health_report(self) -> CollectorHealthReport:
        """
        Get current health status.

        Returns:
            CollectorHealthReport with current status
        """
        pass
