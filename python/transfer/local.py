"""
FiniexDataCollector - Local Transfer Provider
Copies files to local output directory.

Used for development and when remote transfer is not configured.

Location: python/transfer/local.py
"""

import shutil
from pathlib import Path
from typing import List, Optional

from python.transfer.base import AbstractTransferProvider, TransferResult
from python.utils.logging_setup import get_logger


class LocalTransferProvider(AbstractTransferProvider):
    """
    Local file copy transfer provider.
    
    Copies files to a local output directory.
    Used as default provider when rsync/sftp not configured.
    """
    
    def __init__(
        self,
        output_dir: Path,
        enabled: bool = True,
        preserve_structure: bool = True
    ):
        """
        Initialize local transfer provider.
        
        Args:
            output_dir: Target output directory
            enabled: Whether provider is active
            preserve_structure: Keep source directory structure
        """
        super().__init__(name="local", enabled=enabled)
        
        self._output_dir = Path(output_dir)
        self._preserve_structure = preserve_structure
        self._logger = get_logger("FiniexDataCollector.transfer.local")
        
        # Ensure output directory exists
        self._output_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def output_dir(self) -> Path:
        """Get output directory."""
        return self._output_dir
    
    def upload(self, local_path: Path, remote_path: str) -> TransferResult:
        """
        Copy file to output directory.
        
        Args:
            local_path: Source file path
            remote_path: Relative destination path
            
        Returns:
            TransferResult with status
        """
        if not self._enabled:
            return TransferResult(
                success=False,
                source_path=local_path,
                destination_path=remote_path,
                error_message="Provider disabled"
            )
        
        if not local_path.exists():
            self._errors_count += 1
            return TransferResult(
                success=False,
                source_path=local_path,
                destination_path=remote_path,
                error_message=f"Source file not found: {local_path}"
            )
        
        try:
            # Build destination path
            if self._preserve_structure:
                dest_path = self._output_dir / remote_path
            else:
                dest_path = self._output_dir / local_path.name
            
            # Ensure destination directory exists
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            shutil.copy2(local_path, dest_path)
            
            # Get file size
            file_size = dest_path.stat().st_size
            
            self._transfers_completed += 1
            self._bytes_transferred += file_size
            
            self._logger.debug(f"Copied: {local_path.name} -> {dest_path}")
            
            return TransferResult(
                success=True,
                source_path=local_path,
                destination_path=str(dest_path),
                bytes_transferred=file_size
            )
            
        except Exception as e:
            self._errors_count += 1
            self._logger.error(f"Copy failed: {local_path} -> {e}")
            
            return TransferResult(
                success=False,
                source_path=local_path,
                destination_path=remote_path,
                error_message=str(e)
            )
    
    def upload_batch(self, files: List[tuple]) -> List[TransferResult]:
        """
        Copy multiple files.
        
        Args:
            files: List of (local_path, remote_path) tuples
            
        Returns:
            List of TransferResult
        """
        results = []
        
        for local_path, remote_path in files:
            result = self.upload(Path(local_path), remote_path)
            results.append(result)
        
        return results
    
    def test_connection(self) -> bool:
        """
        Test if output directory is writable.
        
        Returns:
            True if directory is writable
        """
        try:
            # Test write access
            test_file = self._output_dir / ".write_test"
            test_file.touch()
            test_file.unlink()
            
            self._logger.info(f"Local transfer ready: {self._output_dir}")
            return True
            
        except Exception as e:
            self._logger.error(f"Output directory not writable: {e}")
            return False
    
    def cleanup_old_files(self, max_age_days: int = 30) -> int:
        """
        Remove files older than specified age.
        
        Args:
            max_age_days: Maximum file age in days
            
        Returns:
            Number of files removed
        """
        import time
        
        removed = 0
        cutoff_time = time.time() - (max_age_days * 24 * 60 * 60)
        
        for file_path in self._output_dir.rglob("*"):
            if file_path.is_file():
                if file_path.stat().st_mtime < cutoff_time:
                    try:
                        file_path.unlink()
                        removed += 1
                    except Exception as e:
                        self._logger.warning(f"Failed to remove {file_path}: {e}")
        
        self._logger.info(f"Cleaned up {removed} old files")
        return removed


def create_local_provider_from_config(config) -> LocalTransferProvider:
    """
    Create local transfer provider from app config.
    
    Args:
        config: TransferConfig instance
        
    Returns:
        LocalTransferProvider instance
    """
    return LocalTransferProvider(
        output_dir=Path(config.local_output_dir),
        enabled=(config.provider == "local")
    )
