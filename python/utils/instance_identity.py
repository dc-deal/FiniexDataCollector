"""
FiniexDataCollector - Instance Identity
Which collector wrote this file, stated as something that cannot be mistyped.

A development file reached the consuming project's importer on 2026-09-15 and was
caught by luck: nothing in a tick file said where it came from. The obvious repair
was a configured `environment` field, and it was the wrong one. Our own
configuration defaults to `production` and the development box had never overridden
it, so the field would have stamped every test file as production - and even with
the default flipped, a configuration copied from the server to a laptop still says
whatever the server said. **A declaration travels with the file that carries it;
the truth does not.**

So the producer states an identity and says nothing about what it means. The
consuming project resolves identity to meaning in a registry it owns, and an
identity it has never seen resolves to `unknown`, which its measurement runs refuse.
An unregistered machine is quarantined without anyone having to remember anything.

**Minted at the data root, not derived from the machine.** A container renews its
hostname and `/etc/machine-id` on every rebuild, and an identity that changes on
every rebuild is worse than none - the archive would fill with one-file identities,
which looks like provenance and is not. The data volume survives rebuilds; that is
where identity belongs. It is the sibling of `instance_lock.py` with the opposite
lifetime: the lock dies with the process, the identity outlives it.

`instance.json` belongs in a backup and must never be copied when a data directory
is cloned. Losing it is recoverable - the consuming project holds the id in its
registry and can write it back. Duplicating it is not.

Location: python/utils/instance_identity.py
"""

import json
import re
import secrets
import socket
from datetime import datetime, timezone
from pathlib import Path

from python.exceptions.collector_exceptions import ConfigurationError
from python.utils.logging_setup import describe_exception, get_collector_logger

IDENTITY_FILENAME = "instance.json"

# Twelve lowercase hex characters - the shape `journal_id`, `config_fingerprint`
# and `prompt_hash` already use across the Finiex projects, agreed with
# FiniexTestingIDE so one registry can hold identities from every producer.
INSTANCE_ID = re.compile(r"^[0-9a-f]{12}$")
ID_BYTES = 6

PRODUCER = "finiex-data-collector"


def _read_identity(path: Path) -> str:
    """
    Read an existing identity file.

    A file that exists but cannot be understood is a refusal, never a re-mint.
    Re-minting would hand the archive a fresh identity every time the file was
    damaged, and a stream of one-file identities is worse than no provenance: it
    looks like provenance.

    Args:
        path: The identity file

    Returns:
        The instance id

    Raises:
        ConfigurationError: If the file cannot be read or does not carry a valid id
    """
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise ConfigurationError(
            f"Cannot read the instance identity at {path}: {describe_exception(e)}. "
            f"Refusing to start rather than write files nobody can attribute. "
            f"Restore it from a backup, or from the consumer's registry."
        )

    instance_id = record.get("instance_id")

    if not isinstance(instance_id, str) or not INSTANCE_ID.match(instance_id):
        raise ConfigurationError(
            f"The instance identity at {path} carries no usable instance_id "
            f"({instance_id!r}). Expected twelve lowercase hex characters. "
            f"Refusing to start rather than mint a second identity for a data "
            f"directory that already has one."
        )

    return instance_id


def mint_or_read(data_root: Path) -> str:
    """
    Get this data directory's identity, creating it on first use.

    The create is exclusive (`open(..., "x")`), so two collectors starting in the
    same second cannot both mint - the operating system picks the winner and the
    loser reads what the winner wrote.

    Args:
        data_root: The directory tick files are written under

    Returns:
        The instance id, twelve lowercase hex characters

    Raises:
        ConfigurationError: If no identity can be read or created
    """
    logger = get_collector_logger("identity")
    path = Path(data_root) / IDENTITY_FILENAME

    if path.exists():
        return _read_identity(path)

    Path(data_root).mkdir(parents=True, exist_ok=True)
    instance_id = secrets.token_hex(ID_BYTES)

    record = {
        "instance_id": instance_id,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "created_on": socket.gethostname(),
        "_note": (
            "Identity of this data directory, not of this machine or checkout. "
            "Back it up. Never copy it into another data directory - two "
            "directories sharing an identity cannot be told apart afterwards."
        )
    }

    try:
        with path.open("x", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2)
    except FileExistsError:
        # Another collector minted between the check above and here. Theirs won.
        return _read_identity(path)
    except OSError as e:
        raise ConfigurationError(
            f"Cannot create the instance identity at {path}: {describe_exception(e)}. "
            f"Refusing to start rather than write files nobody can attribute."
        )

    logger.info(f"Minted instance identity {instance_id} for {data_root}")
    return instance_id


def collected_on() -> str:
    """
    Hostname of the collecting machine, for forensics only.

    Agreed with FiniexTestingIDE as a value nothing ever interprets: no consumer
    branches on the string, and it is only ever compared against the last value
    seen for the same identity. That comparison closes the one hole minting leaves
    open - a copied data directory carries its identity with it, and a changed
    hostname under a known identity is the only sign that happened.

    Returns:
        The machine's hostname
    """
    return socket.gethostname()
