"""
FiniexDataCollector - Console Mode and Folder Counting Tests

Two defences against a display telling an operator something untrue, and one of
them can stop the collection outright.

QuickEdit suspends a Windows console's next write while text is selected. The
live display writes from the collector's only event loop, so a stray click stops
the WebSocket reader and the writers with it - and no tick arrives, which is the
one gap the write-ahead log cannot close, because it opens before the safety net.
The sister project measured 13.5 hours of it on the same host.

The folder count is smaller but the same shape: it said "files" and counted the
open write-ahead logs among them.

Location: tests/utils/test_console_and_counting.py
"""

import sys
from pathlib import Path

import pytest

from python.main import count_files_in_folder
from python.utils.console_mode import (
    ENABLE_EXTENDED_FLAGS,
    ENABLE_QUICK_EDIT_MODE,
    disable_quick_edit
)


# =============================================================================
# THE FOLDER COUNT
# =============================================================================

def test_only_finished_archive_files_are_counted(tmp_path: Path) -> None:
    """
    A write-ahead log is not a file the archive has.

    Counting every entry made a fresh start report one "file" per symbol before
    a single archive file existed, and a crashed run left its orphaned logs
    inflating the number afterwards. The display calls it "files", and a reader
    takes that to mean the archive.

    Args:
        tmp_path: pytest temp directory
    """
    (tmp_path / "BTCUSD_20260917_084219_ticks.json").write_text("{}", encoding="utf-8")
    (tmp_path / "ETHUSD_20260917_084216_ticks.json").write_text("{}", encoding="utf-8")
    (tmp_path / "XRPUSD_20260917_085430_ticks.jsonl.part").write_text("{}\n", encoding="utf-8")
    (tmp_path / ".collector.lock").write_text("{}", encoding="utf-8")
    (tmp_path / "instance.json").write_text("{}", encoding="utf-8")

    assert count_files_in_folder(tmp_path, "*_ticks.json") == 2


def test_the_log_folder_is_counted_by_its_own_pattern(tmp_path: Path) -> None:
    """
    The same function counts the log folder, and narrowing it once broke that.

    Fixing the archive count to `*_ticks.json` set the log count to zero on the
    production box the same day - visible as `Logs: 0 files` beside a log file
    that plainly existed. One test covered the case being thought about and none
    covered the other caller, so the pattern is a required argument now and this
    is the test that would have caught it.

    Args:
        tmp_path: pytest temp directory
    """
    (tmp_path / "finiexdatacollector_2026-09-16.log").write_text("x", encoding="utf-8")
    (tmp_path / "finiexdatacollector_2026-09-17.log").write_text("x", encoding="utf-8")
    (tmp_path / "BTCUSD_20260917_084219_ticks.json").write_text("{}", encoding="utf-8")

    assert count_files_in_folder(tmp_path, "*.log") == 2


def test_a_missing_folder_counts_zero(tmp_path: Path) -> None:
    """A folder that does not exist yet is not an error - it is empty."""
    assert count_files_in_folder(tmp_path / "nothing_here", "*_ticks.json") == 0


# =============================================================================
# THE CONSOLE
# =============================================================================

def test_the_flags_are_the_ones_windows_defines() -> None:
    """
    Both constants matter, and the second one is the easy mistake.

    Clearing QuickEdit without setting ENABLE_EXTENDED_FLAGS in the same call
    leaves the console ignoring the change - and reporting success while doing
    so, which is the failure that looks exactly like a fix.
    """
    assert ENABLE_QUICK_EDIT_MODE == 0x0040
    assert ENABLE_EXTENDED_FLAGS == 0x0080


def test_the_call_is_harmless_where_there_is_no_console() -> None:
    """
    It must never raise, anywhere.

    This runs in a container on Linux, in a piped shell, and in a scheduled task
    with no console at all. Refusing to start the collector because a cosmetic
    console flag could not be set would be the larger outage by far - so the
    answer is None, and collection continues.
    """
    result = disable_quick_edit()

    assert result in (True, False, None)
    if sys.platform != "win32":
        assert result is None


def test_a_console_that_refuses_is_not_a_reason_to_stop(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The guard only runs when the console API fails, so the test makes it fail.

    Every other test here leaves the Windows branch untouched - on Linux the
    function returns before it - so the `except` that keeps a console problem
    from reaching the collection was carrying no test at all. Mutating it to
    `raise` left the whole suite green.

    Args:
        monkeypatch: pytest patching helper
    """
    import ctypes

    class HostileConsoleApi:
        """A kernel32 that is simply not there, as in a session with no console."""

        def __getattr__(self, name: str) -> None:
            raise OSError("no console subsystem")

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "windll", HostileConsoleApi(), raising=False)

    assert disable_quick_edit() is False, "a console failure must not escape"
