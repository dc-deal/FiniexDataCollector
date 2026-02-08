"""
FiniexDataCollector - Weekly Job Scheduler
Schedules weekly reporting jobs.

Location: python/scheduler/weekly_jobs.py
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional, Callable, Awaitable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from python.utils.logging_setup import get_logger
from python.utils.config_loader import SchedulerConfig


class WeeklyJobScheduler:
    """
    Schedules and manages weekly jobs.

    Primary job: Weekly report every configured day at configured hour (UTC).
    """

    DAY_MAP = {
        "monday": "mon",
        "tuesday": "tue",
        "wednesday": "wed",
        "thursday": "thu",
        "friday": "fri",
        "saturday": "sat",
        "sunday": "sun"
    }

    def __init__(self, config: Optional[SchedulerConfig] = None):
        """
        Initialize scheduler.

        Args:
            config: SchedulerConfig instance (uses defaults if None)
        """
        self._config = config or SchedulerConfig()
        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._logger = get_logger("FiniexDataCollector.scheduler")

        self._report_callback: Optional[Callable[[], Awaitable[bool]]] = None

        self._last_run: Optional[datetime] = None
        self._last_result: Optional[dict] = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._is_running

    @property
    def last_run(self) -> Optional[datetime]:
        """Get last job execution time."""
        return self._last_run

    @property
    def last_result(self) -> Optional[dict]:
        """Get last job result."""
        return self._last_result

    def set_report_callback(
        self,
        callback: Callable[[], Awaitable[bool]]
    ) -> None:
        """
        Set callback for weekly report job.

        Callback should send weekly report and return success status.

        Args:
            callback: Async function to call for report
        """
        self._report_callback = callback

    def start(self) -> None:
        """Start the scheduler."""
        if self._is_running:
            self._logger.warning("Scheduler already running")
            return

        # Schedule weekly report job
        day = self.DAY_MAP.get(
            self._config.report_day.lower(),
            "sat"
        )
        hour = self._config.report_hour_utc
        minute = self._config.report_minute_utc

        trigger = CronTrigger(
            day_of_week=day,
            hour=hour,
            minute=minute,
            timezone="UTC"
        )

        self._scheduler.add_job(
            self._run_weekly_report,
            trigger=trigger,
            id="weekly_report",
            name="Weekly Collection Report",
            replace_existing=True
        )

        self._logger.info(
            f"Scheduled weekly report: {self._config.report_day} "
            f"at {hour:02d}:{minute:02d} UTC"
        )

        self._scheduler.start()
        self._is_running = True
        self._logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop the scheduler."""
        if not self._is_running:
            return

        self._scheduler.shutdown(wait=False)
        self._is_running = False
        self._logger.info("Scheduler stopped")

    async def run_report_now(self) -> dict:
        """
        Run report job immediately (manual trigger).

        Returns:
            Report result dict
        """
        self._logger.info("Manual report triggered")
        return await self._run_weekly_report()

    async def _run_weekly_report(self) -> dict:
        """
        Execute weekly report job.

        Returns:
            Result dict from report callback
        """
        self._last_run = datetime.now(timezone.utc)
        self._logger.info("=" * 60)
        self._logger.info("Starting weekly report")
        self._logger.info("=" * 60)

        result = {
            "success": False,
            "sent": False
        }

        try:
            if self._report_callback:
                sent = await self._report_callback()
                result["success"] = True
                result["sent"] = sent
            else:
                self._logger.warning("No report callback set")

        except Exception as e:
            self._logger.error(f"Report failed: {e}")
            result["success"] = False
            result["error_message"] = str(e)

        self._last_result = result

        self._logger.info(
            f"Report completed: sent={result.get('sent', False)}"
        )

        return result

    def get_next_run_time(self) -> Optional[datetime]:
        """
        Get next scheduled run time.

        Returns:
            Next run datetime or None
        """
        job = self._scheduler.get_job("weekly_report")
        if job:
            return job.next_run_time
        return None

    def get_status(self) -> dict:
        """
        Get scheduler status.

        Returns:
            Status dict
        """
        next_run = self.get_next_run_time()

        return {
            "is_running": self._is_running,
            "schedule": f"{self._config.report_day} {self._config.report_hour_utc:02d}:00 UTC",
            "next_run": next_run.isoformat() if next_run else None,
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "last_result": self._last_result
        }


def create_scheduler_from_config(config: SchedulerConfig) -> WeeklyJobScheduler:
    """
    Create scheduler from config.

    Args:
        config: SchedulerConfig instance

    Returns:
        WeeklyJobScheduler instance
    """
    return WeeklyJobScheduler(config)
