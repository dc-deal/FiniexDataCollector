"""
FiniexDataCollector - Abstract Tick Writer
Base class for tick data writers.

Location: python/writers/base.py
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from python.framework.types.tick_types import TickData, TickFileMetadata


class AbstractTickWriter(ABC):
    """
    Abstract base class for tick data writers.
    
    Defines interface for JSON, Parquet, and future writers.
    """
    
    def __init__(
        self,
        output_dir: Path,
        symbol: str,
        max_ticks_per_file: int = 50000
    ):
        """
        Initialize tick writer.
        
        Args:
            output_dir: Base output directory
            symbol: Trading symbol
            max_ticks_per_file: Maximum ticks before file rotation
        """
        self._output_dir = Path(output_dir)
        self._symbol = symbol
        self._max_ticks_per_file = max_ticks_per_file
        self._current_tick_count = 0
        self._total_ticks_written = 0
        self._files_created = 0
    
    @property
    def symbol(self) -> str:
        """Get symbol being written."""
        return self._symbol
    
    @property
    def current_tick_count(self) -> int:
        """Get ticks in current file."""
        return self._current_tick_count
    
    @property
    def total_ticks_written(self) -> int:
        """Get total ticks written across all files."""
        return self._total_ticks_written
    
    @property
    def files_created(self) -> int:
        """Get number of files created."""
        return self._files_created
    
    def needs_rotation(self) -> bool:
        """
        Check if file rotation is needed.
        
        Returns:
            True if current file reached max ticks
        """
        return self._current_tick_count >= self._max_ticks_per_file
    
    @abstractmethod
    def write_tick(self, tick: TickData) -> None:
        """
        Write single tick to current file.
        
        Args:
            tick: Tick data to write
        """
        pass
    
    @abstractmethod
    def rotate_file(self) -> Optional[Path]:
        """
        Close current file and start new one.
        
        Returns:
            Path to closed file, or None if no file was active
        """
        pass
    
    @abstractmethod
    def finalize(self) -> Optional[Path]:
        """
        Finalize and close current file.
        
        Called on shutdown.
        
        Returns:
            Path to finalized file
        """
        pass
    
    @abstractmethod
    def get_current_filepath(self) -> Optional[Path]:
        """
        Get path to current active file.
        
        Returns:
            Path or None if no file active
        """
        pass
    
    @abstractmethod
    def get_lock_filepath(self) -> Optional[Path]:
        """
        Get path to lock file for current active file.
        
        Returns:
            Path to .lock file
        """
        pass
