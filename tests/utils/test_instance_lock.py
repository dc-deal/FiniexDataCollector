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
