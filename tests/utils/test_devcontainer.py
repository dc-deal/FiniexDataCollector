"""
FiniexDataCollector - Development Container Tests

Two lists of editor extensions and a set of mounts are maintained by hand in
three files. Nothing at runtime reads them, so nothing tells anyone when they
drift - which is the shape of every stale thing in this repository.

What the mounts are for is not cosmetic: one of them is the reason a session
transcript survives a rebuild, and its absence once cost a sister project its
entire project history.

Location: tests/utils/test_devcontainer.py
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Set

DEVCONTAINER = Path(".devcontainer/devcontainer.json")
RECOMMENDATIONS = Path(".vscode/extensions.json")
COMPOSE = Path("docker-compose.yml")

# Opens the container and therefore cannot live inside it.
HOST_ONLY = {"ms-vscode-remote.remote-containers"}


def load_jsonc(path: Path) -> Dict[str, Any]:
    """
    Parse a JSON file that carries `//` comments.

    Both files are JSONC - VS Code accepts comments there, and the comments in
    them explain why an entry exists, which is worth more than parser purity.

    Args:
        path: File to read

    Returns:
        Parsed document
    """
    text = re.sub(r"^\s*//.*$", "", path.read_text(encoding="utf-8"), flags=re.M)
    return json.loads(text)


def container_extensions() -> Set[str]:
    """Extensions the dev container installs."""
    return set(load_jsonc(DEVCONTAINER)["customizations"]["vscode"]["extensions"])


def recommended_extensions() -> Set[str]:
    """Extensions a fresh clone is offered."""
    return set(load_jsonc(RECOMMENDATIONS)["recommendations"])


def test_the_two_extension_lists_stay_in_step() -> None:
    """
    A clone is offered what the container installs, plus the host-side entry it
    needs before the container can be opened at all. Any other difference means
    one file was edited and the other forgotten.
    """
    assert recommended_extensions() - container_extensions() == HOST_ONLY
    assert not container_extensions() - recommended_extensions()


def test_the_host_only_extension_is_not_installed_in_the_container() -> None:
    """Dev Containers opens the container; inside it, it has nothing to do."""
    assert not container_extensions() & HOST_ONLY


def test_the_editor_assistant_is_installed_by_the_container() -> None:
    """
    Declared here rather than left to whoever remembers: an extension that has
    to be installed by hand after every rebuild is one that will be missing.
    """
    assert "anthropic.claude-code" in container_extensions()


def test_the_home_directory_survives_a_rebuild() -> None:
    """
    The editor server lives in /root/.vscode-server. Without a persistent home,
    every rebuild re-downloads every extension above - slow, and impossible
    without network.
    """
    assert "devhome:/root" in COMPOSE.read_text(encoding="utf-8")


def test_session_transcripts_are_written_to_the_host() -> None:
    """
    The mount whose absence cost a sister project its history. It sits inside
    the home volume and wins for that path, so transcripts land on the host
    where the backup can reach them rather than in a volume nobody archives.
    """
    assert "~/.claude:/root/.claude" in COMPOSE.read_text(encoding="utf-8")
