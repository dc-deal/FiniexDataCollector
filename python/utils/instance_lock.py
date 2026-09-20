"""
FiniexDataCollector - Instance Lock
One collector per output directory.

The shared resource is the output directory, not the code and not the machine. Two
checkouts pointed at the same `data/raw` collide; the same checkout run twice against
different directories does not. So the lock is keyed on where files are written.

What it prevents is not merely duplicate work. `recover_orphaned_buffers()` scans that
directory at startup for write-ahead logs left by a crashed run, and it cannot tell a
crashed run's log from a running instance's. On Windows the attempt to remove a live log
fails and aborts the start; on Linux it **succeeds**, silently taking away the running
instance's only protection for its in-memory buffer. The second case is the dangerous one
and it is the one the development container would hit.

Identity is the process id **and** its creation time. A pid alone is not an identity: after
a crash the operating system reuses it, and a lock naming a reused pid would refuse a start
forever. The pair is unique for as long as the process lives, which is exactly the lifetime
the lock needs to describe.

Location: python/utils/instance_lock.py
"""

import json
import os
from pathlib import Path
from typing import Optional

import psutil

from python.exceptions.collector_exceptions import ConfigurationError
from python.utils.logging_setup import describe_exception, get_collector_logger

LOCK_FILENAME = ".collector.lock"

# psutil reports process creation time as a float that is fixed for the life of a
# process. It crosses a JSON round trip here, so the comparison allows for float
# formatting noise - and no more than that. A loose window is not harmless: some
# system processes report a creation time of 0.0, and a one-second tolerance made
# those match any recorded value below one, which is how this constant was found.
CREATE_TIME_TOLERANCE_S = 0.01


class InstanceLock:
    """
    Marks an output directory as owned by this process for as long as it runs.

    Acquired before recovery and released on graceful shutdown. A lock left behind by a
    crash is detected as stale and taken over - refusing to start after a crash would
    turn a protection into an outage.
    """

    def __init__(self, output_dir: Path):
        """
        Initialize the lock for one output directory.

        Args:
            output_dir: The directory whose files this instance will write
        """
        self._output_dir = Path(output_dir)
        self._path = self._output_dir / LOCK_FILENAME
        self._logger = get_collector_logger("instance_lock")
        self._held = False

    @property
    def path(self) -> Path:
        """Location of the lock file."""
        return self._path

    def acquire(self) -> None:
        """
        Take ownership of the output directory.

        Raises:
            ConfigurationError: If another live collector already owns it
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)

        holder = self._read_holder()

        if holder is not None:
            raise ConfigurationError(
                f"Another collector is already writing to {self._output_dir} "
                f"(pid {holder}). Two instances on one directory would let the "
                f"startup recovery delete the running one's write-ahead logs. "
                f"Stop it first, or point this one at a different raw_data_dir."
            )

        self._write()
        self._held = True

    def release(self) -> None:
        """
        Give up ownership. Safe to call when the lock was never held.
        """
        if not self._held:
            return

        try:
            self._path.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            self._logger.warning(
                f"Could not remove {self._path.name}: {describe_exception(e)}")

        self._held = False

    def _read_holder(self) -> Optional[int]:
        """
        Find out whether a live collector holds this directory.

        A lock file that cannot be read or parsed counts as stale. The alternative -
        refusing to start on a damaged lock - makes a truncated write during a power
        cut permanently fatal, which is worse than the duplicate start it guards.

        Returns:
            Pid of the live holder, or None if the directory is free
        """
        try:
            record = json.loads(self._path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError) as e:
            self._logger.warning(
                f"Unreadable lock file {self._path.name} ({describe_exception(e)}), treating as stale")
            return None

        pid = record.get("pid")
        created = record.get("create_time")

        if not isinstance(pid, int) or not isinstance(created, (int, float)):
            self._logger.warning(
                f"Incomplete lock file {self._path.name}, treating as stale")
            return None

        if pid == os.getpid():
            # Our own lock from an earlier run of this process - re-entering is not
            # a conflict, and this is what a restart inside one process looks like.
            return None

        try:
            process = psutil.Process(pid)
            if abs(process.create_time() - created) <= CREATE_TIME_TOLERANCE_S:
                return pid
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        self._logger.info(
            f"Stale lock from pid {pid} in {self._output_dir}, taking over")
        return None

    def _write(self) -> None:
        """Record this process as the owner."""
        record = {
            "pid": os.getpid(),
            "create_time": psutil.Process().create_time(),
            "output_dir": str(self._output_dir)
        }
        self._path.write_text(json.dumps(record, indent=2), encoding="utf-8")
