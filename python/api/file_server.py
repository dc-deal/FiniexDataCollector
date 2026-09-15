"""
FiniexDataCollector - Archive File Handout
Serving a finished tick file to the project that consumes it.

The alternative this replaces is SFTP, which means handing out shell access to a
machine running three services in order to transfer files from one directory. A
grant on one surface is the narrower thing to give away.

**Only finished files leave, and that is a property of the writer rather than a
check performed here.** A `*_ticks.json` is created by writing a temporary file and
`os.replace()`-ing it into position, so it never exists in a partial state: the
moment the name is there, the content is complete. What is still being collected
lives in memory and in a `.jsonl.part` write-ahead log, which has no `.json`
counterpart yet and is therefore invisible to this module by construction.

The daily close adds a second, coarser boundary on top: once a UTC day ends, that
day's file is final and will never grow.

Location: python/api/file_server.py
"""

import hashlib
import re
from pathlib import Path
from typing import Optional

# `BTCUSD_20260915_100000_ticks.json`. Anchored, so a name is matched whole - the
# pattern is the only thing standing between a request parameter and the file
# system, and a substring match would let `../` through inside a longer string.
ARCHIVE_NAME = re.compile(r"^[A-Z0-9]{1,20}_\d{8}_\d{6}_ticks\.json$")

# Read in blocks rather than whole: a file runs to 20 MB and several consumers may
# ask at once.
HASH_BLOCK = 1024 * 1024


def is_servable_name(name: str) -> bool:
    """
    Decide whether a requested name may be turned into a path at all.

    Rejects anything that is not exactly an archive file name. That covers path
    traversal without reasoning about it: `../../etc/passwd` does not match, and
    neither does a write-ahead log, a lock file or a partially named temp file.

    Args:
        name: The name as it arrived in the request

    Returns:
        True when the name is safe to resolve
    """
    return bool(ARCHIVE_NAME.match(name))


def resolve_archive_file(
    output_dir: Path,
    data_collector: str,
    name: str
) -> Optional[Path]:
    """
    Turn a requested name into a path inside the archive, or refuse.

    The name check above is the guard; this adds a second one that does not rely
    on it. After resolving, the result must still sit inside the collector
    directory - a symlink planted in that directory could otherwise point
    anywhere, and the name pattern cannot see that.

    Args:
        output_dir: Base output directory
        data_collector: Collector subdirectory, e.g. "kraken"
        name: Requested file name

    Returns:
        The path if it exists and is servable, otherwise None
    """
    if not is_servable_name(name):
        return None

    target = output_dir / data_collector
    candidate = (target / name).resolve()

    try:
        candidate.relative_to(target.resolve())
    except ValueError:
        return None

    if not candidate.is_file():
        return None

    return candidate


def checksum(path: Path) -> str:
    """
    SHA-256 of a file, for a consumer to verify a transfer against.

    Args:
        path: File to hash

    Returns:
        Hex digest
    """
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(HASH_BLOCK), b""):
            digest.update(block)

    return digest.hexdigest()
