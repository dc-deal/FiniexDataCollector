"""
FiniexDataCollector - Weekly Job Scheduler
Schedules weekly parquet conversion and reporting.

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
    
    Primary job: Parquet conversion every Saturday at 06:00 UTC.
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
        
        self._conversion_callback: Optional[Callable[[], Awaitable[dict]]] = None
        self._report_callback: Optional[Callable[[dict], Awaitable[bool]]] = None
        
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
    
    def set_conversion_callback(
        self,
        callback: Callable[[], Awaitable[dict]]
    ) -> None:
        """
        Set callback for parquet conversion job.
        
        Callback should return dict with:
        - symbols_processed: int
        - files_converted: int
        - total_ticks: int
        - errors: int
        - duration_seconds: float
        
        Args:
            callback: Async function to call for conversion
        """
        self._conversion_callback = callback
    
    def set_report_callback(
        self,
        callback: Callable[[dict], Awaitable[bool]]
    ) -> None:
        """
        Set callback for sending report after conversion.
        
        Args:
            callback: Async function to call with conversion results
        """
        self._report_callback = callback
    
    def start(self) -> None:
        """Start the scheduler."""
        if self._is_running:
            self._logger.warning("Scheduler already running")
            return
        
        # Schedule weekly conversion job
        if self._config.parquet_conversion_enabled:
            day = self.DAY_MAP.get(
                self._config.conversion_day.lower(),
                "sat"
            )
            hour = self._config.conversion_hour_utc
            
            trigger = CronTrigger(
                day_of_week=day,
                hour=hour,
                minute=0,
                timezone="UTC"
            )
            
            self._scheduler.add_job(
                self._run_weekly_conversion,
                trigger=trigger,
                id="weekly_conversion",
                name="Weekly Parquet Conversion",
                replace_existing=True
            )
            
            self._logger.info(
                f"Scheduled weekly conversion: {self._config.conversion_day} "
                f"at {hour:02d}:00 UTC"
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
    
    async def run_conversion_now(self) -> dict:
        """
        Run conversion job immediately (manual trigger).
        
        Returns:
            Conversion result dict
        """
        self._logger.info("Manual conversion triggered")
        return await self._run_weekly_conversion()
    
    async def _run_weekly_conversion(self) -> dict:
        """
        Execute weekly conversion job.
        
        Returns:
            Result dict from conversion callback
        """
        self._last_run = datetime.now(timezone.utc)
        self._logger.info("=" * 60)
        self._logger.info("Starting weekly parquet conversion")
        self._logger.info("=" * 60)
        
        result = {
            "symbols_processed": 0,
            "files_converted": 0,
            "total_ticks": 0,
            "errors": 0,
            "duration_seconds": 0.0,
            "success": False
        }
        
        try:
            if self._conversion_callback:
                result = await self._conversion_callback()
                result["success"] = result.get("errors", 0) == 0
            else:
                self._logger.warning("No conversion callback set")
                result["errors"] = 1
            
        except Exception as e:
            self._logger.error(f"Conversion failed: {e}")
            result["errors"] = 1
            result["error_message"] = str(e)
        
        self._last_result = result
        
        # Send report
        if self._report_callback:
            try:
                await self._report_callback(result)
            except Exception as e:
                self._logger.error(f"Failed to send report: {e}")
        
        self._logger.info(
            f"Conversion completed: {result.get('files_converted', 0)} files, "
            f"{result.get('total_ticks', 0):,} ticks, "
            f"{result.get('errors', 0)} errors"
        )
        
        return result
    
    def get_next_run_time(self) -> Optional[datetime]:
        """
        Get next scheduled run time.
        
        Returns:
            Next run datetime or None
        """
        job = self._scheduler.get_job("weekly_conversion")
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
            "conversion_enabled": self._config.parquet_conversion_enabled,
            "schedule": f"{self._config.conversion_day} {self._config.conversion_hour_utc:02d}:00 UTC",
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
