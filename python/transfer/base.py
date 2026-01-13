"""
FiniexDataCollector - Abstract Transfer Provider
Base class for data transfer providers.

Location: python/transfer/base.py
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class TransferResult:
    """Result of a transfer operation."""
    success: bool
    source_path: Path
    destination_path: str
    bytes_transferred: int = 0
    error_message: Optional[str] = None


class AbstractTransferProvider(ABC):
    """
    Abstract base class for transfer providers.
    
    Defines interface for local, rsync, sftp, and future providers.
    """
    
    def __init__(self, name: str, enabled: bool = True):
        """
        Initialize transfer provider.
        
        Args:
            name: Provider identifier
            enabled: Whether provider is active
        """
        self._name = name
        self._enabled = enabled
        self._transfers_completed = 0
        self._bytes_transferred = 0
        self._errors_count = 0
    
    @property
    def name(self) -> str:
        """Get provider name."""
        return self._name
    
    @property
    def is_enabled(self) -> bool:
        """Check if provider is enabled."""
        return self._enabled
    
    @property
    def transfers_completed(self) -> int:
        """Get total transfers completed."""
        return self._transfers_completed
    
    @property
    def bytes_transferred(self) -> int:
        """Get total bytes transferred."""
        return self._bytes_transferred
    
    def set_enabled(self, enabled: bool) -> None:
        """
        Enable or disable provider.
        
        Args:
            enabled: New enabled state
        """
        self._enabled = enabled
    
    @abstractmethod
    def upload(self, local_path: Path, remote_path: str) -> TransferResult:
        """
        Upload file to destination.
        
        Args:
            local_path: Source file path
            remote_path: Destination path/identifier
            
        Returns:
            TransferResult with status
        """
        pass
    
    @abstractmethod
    def upload_batch(self, files: List[tuple]) -> List[TransferResult]:
        """
        Upload multiple files.
        
        Args:
            files: List of (local_path, remote_path) tuples
            
        Returns:
            List of TransferResult
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test provider connection.
        
        Returns:
            True if connection works
        """
        pass
    
    def get_stats(self) -> dict:
        """
        Get transfer statistics.
        
        Returns:
            Dict with statistics
        """
        return {
            "provider": self._name,
            "enabled": self._enabled,
            "transfers_completed": self._transfers_completed,
            "bytes_transferred": self._bytes_transferred,
            "errors_count": self._errors_count
        }
