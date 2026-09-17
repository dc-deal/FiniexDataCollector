"""
FiniexDataCollector - Instance Identity Tests

A development file reached the consuming project's importer and was caught by
luck. The repair is an identity the producer cannot mistype, minted at the data
root rather than configured - because a configuration copied from the server to a
laptop still says whatever the server said.

The expensive failure is not a missing identity but a CHANGING one: if the mint
did not survive a restart, every start would mint a new id and the archive would
fill with one-file identities. That looks like provenance and is not, which is why
the restart test below is the one FiniexTestingIDE asked for by name.

Location: tests/utils/test_instance_identity.py
"""

import json
from pathlib import Path

import pytest

from python.exceptions.collector_exceptions import ConfigurationError
from python.utils.instance_identity import (
    IDENTITY_FILENAME,
    INSTANCE_ID,
    mint_or_read
)


def test_a_fresh_data_root_is_minted_once(tmp_path: Path) -> None:
    """Twelve lowercase hex, the shape the sister projects already use."""
    instance_id = mint_or_read(tmp_path)

    assert INSTANCE_ID.match(instance_id)
    assert (tmp_path / IDENTITY_FILENAME).exists()


def test_the_identity_survives_a_restart(tmp_path: Path) -> None:
    """
    Start, stop, start - the id must not move.

    The failure this guards is the expensive one: a mint that does not survive
    gives every run its own identity, and an archive of one-file identities is
    worse than no provenance because it looks like provenance.
    """
    first = mint_or_read(tmp_path)
    second = mint_or_read(tmp_path)
    third = mint_or_read(tmp_path)

    assert first == second == third


def test_the_identity_lives_in_the_data_root(tmp_path: Path) -> None:
    """
    Not in the project directory, not derived from the machine.

    A container renews its hostname and machine id on every rebuild; the data
    volume does not. The identity belongs where the files are written, which is
    also what the instance lock is keyed on.
    """
    data_root = tmp_path / "data" / "raw"
    mint_or_read(data_root)

    assert (data_root / IDENTITY_FILENAME).is_file()
    assert not (tmp_path / IDENTITY_FILENAME).exists()


def test_two_data_roots_get_two_identities(tmp_path: Path) -> None:
    """The identity describes a data directory, not a program or a host."""
    assert mint_or_read(tmp_path / "a") != mint_or_read(tmp_path / "b")


def test_the_file_warns_against_copying_itself(tmp_path: Path) -> None:
    """
    Losing it is recoverable - the consumer holds the id in its registry.
    Duplicating it is not: two directories sharing an identity cannot be told
    apart afterwards. Whoever finds this file should learn that from the file.
    """
    mint_or_read(tmp_path)
    record = json.loads(
        (tmp_path / IDENTITY_FILENAME).read_text(encoding="utf-8"))

    assert "copy" in record["_note"].lower()


@pytest.mark.parametrize("content", [
    "",
    "not json",
    '{"instance_id": null}',
    '{"instance_id": "TOO-SHORT"}',
    '{"instance_id": "ABCDEF123456"}',
])
def test_an_unusable_identity_refuses_the_start(
    tmp_path: Path,
    content: str
) -> None:
    """
    A damaged file is a refusal, never a re-mint.

    Re-minting would hand the archive a fresh identity every time the file was
    damaged - the one-file-identity failure again, arrived at from the other
    side. Uppercase hex is refused too: one registry holds identities from three
    producers, and a case-insensitive key is a key that collides.

    Args:
        tmp_path: pytest temp directory
        content: An identity file that cannot be trusted
    """
    (tmp_path / IDENTITY_FILENAME).write_text(content, encoding="utf-8")

    with pytest.raises(ConfigurationError):
        mint_or_read(tmp_path)


def test_the_refusal_says_where_to_look(tmp_path: Path) -> None:
    """
    An operator reading it at three in the morning needs the path and the way
    out, not a stack trace.
    """
    (tmp_path / IDENTITY_FILENAME).write_text("broken", encoding="utf-8")

    with pytest.raises(ConfigurationError) as excinfo:
        mint_or_read(tmp_path)

    message = str(excinfo.value)
    assert IDENTITY_FILENAME in message
    assert "registry" in message.lower() or "backup" in message.lower()


def test_a_simultaneous_mint_loses_to_the_one_that_got_there_first(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Two collectors starting in the same second must not both mint.

    The existence check cannot prevent it - between that check and the write
    lies a window another process fits into. What prevents it is the exclusive
    create, and the only way to test it is to defeat the check that normally
    hides the race: here `exists()` is made to lie, so the file is on disk while
    the code believes it is minting fresh.

    Without `open(..., "x")` the second mint silently overwrites the first, and
    every file written before that moment names an identity nothing carries any
    more.

    Args:
        tmp_path: pytest temp directory
        monkeypatch: pytest patching helper
    """
    first = mint_or_read(tmp_path)

    real_exists = Path.exists

    def blind_to_the_identity(self) -> bool:
        if self.name == IDENTITY_FILENAME:
            return False
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", blind_to_the_identity)

    second = mint_or_read(tmp_path)

    assert second == first, "the later mint overwrote the earlier identity"
    assert json.loads(
        (tmp_path / IDENTITY_FILENAME).read_text(
            encoding="utf-8"))["instance_id"] == first
