"""
FiniexDataCollector - Build Identity
What code this process is running, sampled once at startup.

`version` moves only when a release ships, so between two tags every deploy looks
identical from outside and "is the code I deployed the one that is running?" is
answered by inference. That inference was actually made on 2026-09-15: which of two
releases the live collector ran had to be derived from its uptime and the commit
timestamps, because nothing reported it.

The value is sampled **once, at startup**, and never re-read. A hash read per request
would describe the working tree at that moment, so after a pull without a restart it
would report the new commit while the old code serves - wrong in exactly the one case
the field exists for.

Location: python/api/build_info.py
"""

import platform
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Optional

import websockets

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class BuildInfo:
    """
    Identity of the running process.

    Attributes:
        version: Application version from the configuration
        data_format_version: Schema version of the files this code writes
        commit: Short commit hash, or None outside a git checkout
        dirty: Whether the working tree carried uncommitted changes at startup
        python_version: The interpreter actually running, e.g. `3.12.4`
        started_at: Process start, UTC ISO 8601
        websockets_version: The websockets release this process imported, None
            when the package metadata cannot be read
        websockets_client: The module `websockets.connect` resolves to -
            websockets.asyncio.client or websockets.legacy.client
    """
    version: str
    data_format_version: str
    commit: Optional[str]
    dirty: Optional[bool]
    python_version: str
    started_at: str
    # requirements.txt allows websockets>=12.0, and the two client
    # implementations it can resolve to end a connection differently: measured
    # 2026-10-08, a close frame without a TCP close blocks recv() for 20 s in
    # one and 10 s in the other, and a dead link's close() takes 5 s or 15 s.
    # How the collector classifies a drop depends on which one is running, and
    # until this field nothing said.
    websockets_version: Optional[str] = None
    websockets_client: Optional[str] = None


def _git(*args: str) -> Optional[str]:
    """
    Run a git command in the project root.

    Args:
        *args: Arguments after `git`

    Returns:
        Stripped stdout, or None when git is absent or the call fails
    """
    try:
        result = subprocess.run(
            ["git", *args], cwd=PROJECT_ROOT, capture_output=True,
            text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None

    if result.returncode != 0:
        return None

    return result.stdout.strip()


def sample_build_info(version: str, data_format_version: str) -> BuildInfo:
    """
    Read the build identity. Call once, at startup.

    A deployment from an archive rather than a checkout has no git metadata; that
    is reported as absent rather than guessed, because a fabricated hash is worse
    than a missing one.

    Args:
        version: Application version from the configuration
        data_format_version: Schema version constant

    Returns:
        BuildInfo describing this process
    """
    commit = _git("rev-parse", "--short", "HEAD")
    status = _git("status", "--porcelain")

    return BuildInfo(
        version=version,
        data_format_version=data_format_version,
        commit=commit,
        dirty=None if status is None else bool(status),
        # Which interpreter, not which one anybody assumed. Four different
        # versions were in play on 2026-09-17 - the Dockerfile pinned 3.12, CI
        # ran 3.13, the laptop had 3.13.7, and nobody could say what the server
        # had, because no surface reported it. A suite green on a version
        # production does not run proves less than it looks like.
        python_version=platform.python_version(),
        started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        websockets_version=_installed_version("websockets"),
        websockets_client=getattr(websockets.connect, "__module__", None)
    )


def _installed_version(package: str) -> Optional[str]:
    """
    The version of an installed package, as its metadata states it.

    Args:
        package: Distribution name

    Returns:
        The version string, or None when the metadata is missing - reported as
        absent rather than guessed, like the commit
    """
    try:
        return package_version(package)
    except PackageNotFoundError:
        return None
