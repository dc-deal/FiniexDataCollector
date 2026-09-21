"""
FiniexDataCollector - Instance Lock Tests

What the lock prevents is not duplicate work but silent data loss: a second
collector's startup recovery cannot tell a crashed run's write-ahead log from a
running instance's, and on Linux it removes the live one successfully.

Location: tests/utils/test_instance_lock.py
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import psutil
import pytest

from python.exceptions.collector_exceptions import ConfigurationError
from python.utils.instance_lock import LOCK_FILENAME, InstanceLock


def write_lock(directory: Path, pid: int, create_time: float) -> Path:
    """
    Plant a lock file as another process would have left it.

    Args:
        directory: Output directory to claim
        pid: Process id to record
        create_time: Creation time of that process

    Returns:
        Path of the lock file
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / LOCK_FILENAME
    path.write_text(json.dumps({
        "pid": pid,
        "create_time": create_time,
        "output_dir": str(directory)
    }), encoding="utf-8")
    return path


@pytest.fixture
def live_foreign_process():
    """
    A real, live process that is not this one.

    Spawned rather than picked out of the process table: an arbitrary system
    process may report a creation time of 0.0 or refuse to be inspected, and a
    test that depends on which one it happens to find is a test that fails on
    someone else's machine.

    Returns:
        Tuple of (pid, create_time)
    """
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        yield child.pid, psutil.Process(child.pid).create_time()
    finally:
        child.kill()
        child.wait(timeout=5)


def test_an_empty_directory_can_be_claimed(tmp_path: Path) -> None:
    """The ordinary start: nothing owns it, so we do."""
    lock = InstanceLock(tmp_path)
    lock.acquire()

    assert lock.path.exists()
    record = json.loads(lock.path.read_text(encoding="utf-8"))
    assert record["pid"] == os.getpid()


def test_a_live_holder_blocks_the_start(tmp_path: Path, live_foreign_process) -> None:
    """
    The case the lock exists for. The refusal must name the directory, because
    the fix is either to stop the other one or to point this one elsewhere.
    """
    pid, created = live_foreign_process
    write_lock(tmp_path, pid, created)

    with pytest.raises(ConfigurationError) as excinfo:
        InstanceLock(tmp_path).acquire()

    assert str(tmp_path) in str(excinfo.value)
    assert str(pid) in str(excinfo.value)


def test_a_dead_holder_is_taken_over(tmp_path: Path) -> None:
    """
    A crash leaves its lock behind. Refusing to start after a crash would turn a
    protection into an outage, so a stale lock is taken, not obeyed.
    """
    # A pid far above any plausible live process, with a 1970 creation time.
    write_lock(tmp_path, 2_147_483_646, 1.0)

    lock = InstanceLock(tmp_path)
    lock.acquire()

    assert json.loads(lock.path.read_text(encoding="utf-8"))[
        "pid"] == os.getpid()


def test_a_reused_pid_does_not_block_forever(tmp_path: Path, live_foreign_process) -> None:
    """
    Why identity is the pid AND its creation time.

    After a crash the operating system reuses pids. A lock naming a reused pid
    would refuse every future start, so the recorded creation time has to match
    the live process as well.
    """
    pid, _ = live_foreign_process
    write_lock(tmp_path, pid, 1.0)   # same pid, a creation time from 1970

    lock = InstanceLock(tmp_path)
    lock.acquire()

    assert json.loads(lock.path.read_text(encoding="utf-8"))[
        "pid"] == os.getpid()


@pytest.mark.parametrize("content", ["", "not json", '{"pid": 1}'])
def test_a_damaged_lock_is_treated_as_stale(
    tmp_path: Path,
    content: str
) -> None:
    """
    A truncated write during a power cut must not make the collector
    permanently unstartable. Damaged counts as stale.

    Args:
        tmp_path: pytest temp directory
        content: Lock file content that cannot be trusted
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / LOCK_FILENAME).write_text(content, encoding="utf-8")

    InstanceLock(tmp_path).acquire()

    assert json.loads((tmp_path / LOCK_FILENAME).read_text(
        encoding="utf-8"))["pid"] == os.getpid()


def test_release_frees_the_directory(tmp_path: Path) -> None:
    """After a graceful shutdown the next start finds nothing in its way."""
    lock = InstanceLock(tmp_path)
    lock.acquire()
    lock.release()

    assert not lock.path.exists()
    InstanceLock(tmp_path).acquire()


def test_release_without_acquire_is_harmless(tmp_path: Path, live_foreign_process) -> None:
    """
    Shutdown runs after failures too, including one where the lock was never
    taken - it must not remove a lock this process does not own.
    """
    pid, created = live_foreign_process
    write_lock(tmp_path, pid, created)

    InstanceLock(tmp_path).release()

    assert (tmp_path / LOCK_FILENAME).exists(), "released a foreign lock"


# =============================================================================
# WHEN THE REFUSAL HAPPENS, WHICH MATTERS UNDER A SERVICE MANAGER
# =============================================================================

def test_a_refused_start_announces_nothing_and_binds_nothing(
    tmp_path: Path, live_foreign_process, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The lock is taken before anything with a side effect, not after.

    It used to be claimed deep inside the collector setup - after Telegram, the
    scheduler, the monitoring tasks and the API bind. A second instance therefore
    sent "Collector Started" to the operator's phone, started a scheduler and
    reached for the status port, and only then found out it was not allowed to
    run. Under a service manager that restarts on exit that is one phone alert
    per restart cycle, for as long as somebody has a console instance open.

    Args:
        tmp_path: pytest temp directory
        live_foreign_process: A process that really is running
        monkeypatch: pytest patching helper
    """
    import asyncio

    import python.main as main_module
    from python.utils.config_loader import ConfigLoader
    from python.utils.logging_setup import remove_log_listener

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    pid, create_time = live_foreign_process
    write_lock(raw_dir, pid, create_time)

    announced = []

    class LoudTelegram:
        """Anything constructing this has already gone too far."""

        def __init__(self, *args, **kwargs):
            announced.append("telegram")

    class LoudScheduler:
        def __init__(self, *args, **kwargs):
            announced.append("scheduler")

        def set_report_callback(self, callback):
            pass

        def start(self):
            announced.append("scheduler started")

    monkeypatch.setattr(main_module, "TelegramAlertProvider", LoudTelegram)
    monkeypatch.setattr(main_module, "WeeklyJobScheduler", LoudScheduler)

    config = ConfigLoader().load()
    config.paths.raw_data_dir = str(raw_dir)
    config.telegram.enabled = True

    collector = main_module.FiniexDataCollector(config, show_display=False)
    try:
        with pytest.raises(ConfigurationError):
            asyncio.run(collector.start_collection())
    finally:
        remove_log_listener(collector._stats.record_logged)

    assert not announced, (
        f"a start that was refused still did this first: {announced}")


def test_a_failure_a_restart_cannot_fix_gets_its_own_exit_code(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A service manager restarts on exit by default, and that is usually right.

    It is wrong for exactly two failures: a malformed configuration and a
    directory another live collector owns. Neither improves by being retried, so
    both leave through a code the manager can be told to treat as final - while a
    genuine crash keeps exit 1, where an automatic restart is the whole point of
    running under a manager at all.

    Args:
        monkeypatch: pytest patching helper
    """
    import python.main as main_module

    def refuse(*args, **kwargs):
        raise ConfigurationError("another collector owns that directory")

    monkeypatch.setattr(main_module, "cmd_collect", refuse)
    monkeypatch.setattr(sys, "argv", ["main.py", "collect", "--no-display"])

    with pytest.raises(SystemExit) as raised:
        main_module.main()

    assert raised.value.code == main_module.EXIT_CONFIGURATION
    assert main_module.EXIT_CONFIGURATION == 2, (
        "the service definition names this number; changing it silently "
        "reinstates the restart loop it exists to prevent")
