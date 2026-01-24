"""
FiniexDataCollector - Telegram Alert Provider
Sends alerts via Telegram Bot API.

Location: python/alerts/telegram_bot.py
"""

import asyncio
import ssl
from typing import Optional
from urllib.parse import quote

import aiohttp
import certifi

from python.alerts.base import AbstractAlertProvider, Alert, AlertLevel
from python.exceptions.collector_exceptions import AlertDeliveryError
from python.utils.logging_setup import get_logger


class TelegramAlertProvider(AbstractAlertProvider):
    """
    Telegram Bot alert provider.

    Uses Telegram Bot API to send notifications.
    Requires bot token and chat ID from app_config.json.
    """

    API_BASE = "https://api.telegram.org/bot"

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        enabled: bool = True,
        send_on_error: bool = True,
        send_on_rotation: bool = False,
        send_weekly_report: bool = True
    ):
        """
        Initialize Telegram provider.

        Args:
            bot_token: Telegram bot token from BotFather
            chat_id: Target chat/channel ID
            enabled: Whether provider is active
            send_on_error: Send alerts on errors
            send_on_rotation: Send alerts on file rotation
            send_weekly_report: Send weekly conversion report
        """
        super().__init__(name="telegram", enabled=enabled)

        self._bot_token = bot_token
        self._chat_id = chat_id
        self._send_on_error = send_on_error
        self._send_on_rotation = send_on_rotation
        self._send_weekly_report = send_weekly_report
        self._logger = get_logger("FiniexDataCollector.telegram")

        # SSL context with certifi certificates (cross-platform)
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())

    @property
    def is_configured(self) -> bool:
        """Check if bot token and chat ID are configured."""
        return bool(self._bot_token and self._chat_id)

    async def send_alert(self, alert: Alert) -> bool:
        """
        Send alert via Telegram.

        Args:
            alert: Alert to send

        Returns:
            True if sent successfully
        """
        if not self._enabled:
            return False

        if not self.is_configured:
            self._logger.warning(
                "Telegram not configured (missing token or chat_id)")
            return False

        # Check if this alert type should be sent
        if alert.level == AlertLevel.ERROR and not self._send_on_error:
            return False

        # Format message
        message = self._format_telegram_message(alert)

        # Send via API
        try:
            success = await self._send_message(message)
            if success:
                self._alerts_sent += 1
            return success

        except Exception as e:
            self._errors_count += 1
            self._logger.error(f"Failed to send Telegram alert: {e}")
            raise AlertDeliveryError(
                message=str(e),
                provider="telegram",
                alert_type=alert.level.value
            )

    async def test_connection(self) -> bool:
        """
        Test Telegram bot connection.

        Returns:
            True if bot can send messages
        """
        if not self.is_configured:
            return False

        try:
            # Use getMe to verify bot token
            url = f"{self.API_BASE}{self._bot_token}/getMe"

            connector = aiohttp.TCPConnector(ssl=self._ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            self._logger.info(
                                f"Telegram bot connected: @{data['result'].get('username')}"
                            )
                            return True

            return False

        except Exception as e:
            self._logger.error(f"Telegram connection test failed: {e}")
            return False

    async def send_weekly_report(
        self,
        symbols_processed: int,
        files_converted: int,
        total_ticks: int,
        errors: int,
        duration_seconds: float
    ) -> bool:
        """
        Send weekly conversion report.

        Args:
            symbols_processed: Number of symbols processed
            files_converted: Number of files converted
            total_ticks: Total ticks processed
            errors: Number of errors
            duration_seconds: Processing duration

        Returns:
            True if sent
        """
        if not self._send_weekly_report:
            return False

        status = "✅ SUCCESS" if errors == 0 else "⚠️ WITH ERRORS"

        message = (
            f"📊 *Weekly Conversion Report*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Status: {status}\n"
            f"\n"
            f"📈 *Statistics:*\n"
            f"• Symbols: {symbols_processed}\n"
            f"• Files: {files_converted}\n"
            f"• Ticks: {total_ticks:,}\n"
            f"• Errors: {errors}\n"
            f"• Duration: {duration_seconds:.1f}s\n"
            f"\n"
            f"🕐 Completed at: {self._get_utc_time()}"
        )

        return await self._send_message(message, parse_mode="Markdown")

    async def send_file_rotation_notice(
        self,
        symbol: str,
        filename: str,
        tick_count: int
    ) -> bool:
        """
        Send file rotation notification.

        Args:
            symbol: Trading symbol
            filename: Rotated filename
            tick_count: Ticks in file

        Returns:
            True if sent
        """
        if not self._send_on_rotation:
            return False

        message = (
            f"📁 File Rotation: {symbol}\n"
            f"File: {filename}\n"
            f"Ticks: {tick_count:,}"
        )

        return await self._send_message(message)

    async def _send_message(
        self,
        text: str,
        parse_mode: Optional[str] = None
    ) -> bool:
        """
        Send message via Telegram API.

        Args:
            text: Message text
            parse_mode: Optional parse mode (Markdown, HTML)

        Returns:
            True if sent successfully
        """
        url = f"{self.API_BASE}{self._bot_token}/sendMessage"

        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": True
        }

        if parse_mode:
            payload["parse_mode"] = parse_mode

        try:
            connector = aiohttp.TCPConnector(ssl=self._ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, json=payload, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("ok", False)
                    else:
                        error_text = await response.text()
                        self._logger.error(
                            f"Telegram API error {response.status}: {error_text}"
                        )
                        return False

        except asyncio.TimeoutError:
            self._logger.error("Telegram API timeout")
            return False
        except Exception as e:
            self._logger.error(f"Telegram send error: {e}")
            return False

    def _format_telegram_message(self, alert: Alert) -> str:
        """
        Format alert for Telegram.

        Args:
            alert: Alert to format

        Returns:
            Formatted message string
        """
        level_emoji = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨"
        }

        emoji = level_emoji.get(alert.level, "📢")

        lines = [
            f"{emoji} *{alert.title}*",
            f"Level: {alert.level.value.upper()}",
            f"Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
            alert.message
        ]

        if alert.details:
            # Truncate long details for Telegram
            details = alert.details[:500]
            if len(alert.details) > 500:
                details += "..."
            lines.extend(["", f"```\n{details}\n```"])

        return "\n".join(lines)

    def _get_utc_time(self) -> str:
        """Get current UTC time as string."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def create_telegram_provider_from_config(config) -> Optional[TelegramAlertProvider]:
    """
    Create Telegram provider from app config.

    Args:
        config: TelegramConfig instance

    Returns:
        TelegramAlertProvider or None if disabled
    """
    if not config.enabled:
        return None

    return TelegramAlertProvider(
        bot_token=config.bot_token,
        chat_id=config.chat_id,
        enabled=config.enabled,
        send_on_error=config.send_on_error,
        send_on_rotation=config.send_on_rotation,
        send_weekly_report=config.send_weekly_report
    )
