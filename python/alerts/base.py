"""
FiniexDataCollector - Abstract Alert Provider
Base class for alert/notification providers.

Location: python/alerts/base.py
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """
    Alert message structure.
    
    Used for all notification types.
    """
    level: AlertLevel
    title: str
    message: str
    timestamp: datetime
    source: str = "FiniexDataCollector"
    details: Optional[str] = None
    
    def format_text(self) -> str:
        """
        Format alert as plain text.
        
        Returns:
            Formatted alert string
        """
        level_emoji = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨"
        }
        
        emoji = level_emoji.get(self.level, "📢")
        time_str = self.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        lines = [
            f"{emoji} {self.level.value.upper()}: {self.title}",
            f"Time: {time_str}",
            f"Source: {self.source}",
            "",
            self.message
        ]
        
        if self.details:
            lines.extend(["", "Details:", self.details])
        
        return "\n".join(lines)


class AbstractAlertProvider(ABC):
    """
    Abstract base class for alert providers.
    
    Defines interface for Telegram, Email, and future providers.
    """
    
    def __init__(self, name: str, enabled: bool = True):
        """
        Initialize alert provider.
        
        Args:
            name: Provider identifier
            enabled: Whether provider is active
        """
        self._name = name
        self._enabled = enabled
        self._alerts_sent = 0
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
    def alerts_sent(self) -> int:
        """Get total alerts sent."""
        return self._alerts_sent
    
    def set_enabled(self, enabled: bool) -> None:
        """
        Enable or disable provider.
        
        Args:
            enabled: New enabled state
        """
        self._enabled = enabled
    
    @abstractmethod
    async def send_alert(self, alert: Alert) -> bool:
        """
        Send alert notification.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully
        """
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """
        Test provider connection.
        
        Returns:
            True if connection works
        """
        pass
    
    async def send_info(self, title: str, message: str, details: Optional[str] = None) -> bool:
        """
        Send info-level alert.
        
        Args:
            title: Alert title
            message: Alert message
            details: Optional details
            
        Returns:
            True if sent
        """
        alert = Alert(
            level=AlertLevel.INFO,
            title=title,
            message=message,
            timestamp=datetime.now(timezone.utc),
            details=details
        )
        return await self.send_alert(alert)
    
    async def send_warning(self, title: str, message: str, details: Optional[str] = None) -> bool:
        """
        Send warning-level alert.
        
        Args:
            title: Alert title
            message: Alert message
            details: Optional details
            
        Returns:
            True if sent
        """
        alert = Alert(
            level=AlertLevel.WARNING,
            title=title,
            message=message,
            timestamp=datetime.now(timezone.utc),
            details=details
        )
        return await self.send_alert(alert)
    
    async def send_error(self, title: str, message: str, details: Optional[str] = None) -> bool:
        """
        Send error-level alert.
        
        Args:
            title: Alert title
            message: Alert message
            details: Optional details
            
        Returns:
            True if sent
        """
        alert = Alert(
            level=AlertLevel.ERROR,
            title=title,
            message=message,
            timestamp=datetime.now(timezone.utc),
            details=details
        )
        return await self.send_alert(alert)
    
    async def send_critical(self, title: str, message: str, details: Optional[str] = None) -> bool:
        """
        Send critical-level alert.
        
        Args:
            title: Alert title
            message: Alert message
            details: Optional details
            
        Returns:
            True if sent
        """
        alert = Alert(
            level=AlertLevel.CRITICAL,
            title=title,
            message=message,
            timestamp=datetime.now(timezone.utc),
            details=details
        )
        return await self.send_alert(alert)
