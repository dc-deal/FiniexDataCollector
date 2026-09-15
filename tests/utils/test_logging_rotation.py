"""
FiniexDataCollector - Log Rotation Tests

The log file is named after the day it covers. A collector runs for weeks, so
the name has to follow the date rather than the process start - which it did
not: the production server produced a single 294 MB file spanning three days
under a name claiming one, because the handle was opened once and never
reconsidered.

Location: tests/utils/test_logging_rotation.py
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from python.utils import logging_setup
from python.types.log_level import ERROR, INFO
from python.utils.logging_setup import FiniexLogger


@pytest.fixture
def log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """
    Point the module's log directory at a temp dir for one test.

    Args:
        tmp_path: pytest temp directory
        monkeypatch: pytest patching helper

    Returns:
        The directory log files will be written to
    """
    target = tmp_path / "logs"
    target.mkdir()
    monkeypatch.setattr(logging_setup, "_global_log_dir", target)
    return target


def set_clock(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    """
    Freeze the module's view of the current time.

    Both the timestamp on each line and the file name derive from it, which is
    the coupling the rollover relies on.

    Args:
        monkeypatch: pytest patching helper
        when: The instant to report
    """
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return when

    monkeypatch.setattr(logging_setup, "datetime", FrozenDatetime)


def build_logger(path: Path) -> FiniexLogger:
    """
    Build a file-logging instance directly, bypassing the module singleton.

    Args:
        path: Initial log file path

    Returns:
        Configured FiniexLogger
    """
    return FiniexLogger(
        name="test",
        console_level=ERROR,
        file_level=INFO,
        log_file=str(path)
    )


def test_a_line_written_the_next_day_lands_in_the_next_file(
    log_dir: Path,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The defect, stated as a test: one process, two days, two files.

    Before the fix both lines went into the file named after day one.
    """
    day_one = datetime(2026, 9, 12, 23, 59, 50, tzinfo=timezone.utc)
    day_two = day_one + timedelta(seconds=20)

    set_clock(monkeypatch, day_one)
    logger = build_logger(log_dir / "finiexdatacollector_2026-09-12.log")
    logger.info("before midnight")

    set_clock(monkeypatch, day_two)
    logger.info("after midnight")

    first = (log_dir / "finiexdatacollector_2026-09-12.log").read_text(encoding="utf-8")
    second = (log_dir / "finiexdatacollector_2026-09-13.log").read_text(encoding="utf-8")

    assert "before midnight" in first
    assert "after midnight" not in first
    assert "after midnight" in second


def test_the_date_in_the_name_matches_the_dates_inside(
    log_dir: Path,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A file named after a day must contain only that day.

    This is the property that actually broke: the server's file was named
    2026-09-12 and carried entries through 2026-09-15, so grepping the logs by
    date was quietly wrong.
    """
    start = datetime(2026, 9, 12, 12, 0, 0, tzinfo=timezone.utc)

    set_clock(monkeypatch, start)
    logger = build_logger(log_dir / "finiexdatacollector_2026-09-12.log")

    for day_offset in range(4):
        set_clock(monkeypatch, start + timedelta(days=day_offset))
        logger.info(f"day {day_offset}")

    for path in log_dir.glob("finiexdatacollector_*.log"):
        date_in_name = path.stem.replace("finiexdatacollector_", "")
        for line in path.read_text(encoding="utf-8").splitlines():
            assert line.startswith(date_in_name), (
                f"{path.name} contains a line dated {line[:10]}")


def test_same_day_lines_keep_one_handle(
    log_dir: Path,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Rollover happens on a date change and not otherwise - reopening the file
    per line would be a different defect with the same symptom fixed.
    """
    when = datetime(2026, 9, 12, 8, 0, 0, tzinfo=timezone.utc)
    set_clock(monkeypatch, when)

    logger = build_logger(log_dir / "finiexdatacollector_2026-09-12.log")
    first_handle = logger._file_handle

    logger.info("one")
    logger.info("two")

    assert logger._file_handle is first_handle
    assert len(list(log_dir.glob("*.log"))) == 1
