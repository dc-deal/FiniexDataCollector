"""
FiniexDataCollector - Tests for choosing which collector to watch

`user_configs/remote_endpoints.json` was a note to whoever was working here. The
viewer is the first program to read it, which turns it into configuration - and
configuration read by a program fails in front of an operator rather than in a
session, so every failure here has to name the file, the entry and the key.

The one thing that must never appear in any of these messages is the token. A
viewer that prints a credential when it cannot parse a file has published it to
the console, the scrollback and any screenshot of either.

Location: tests/viewer/test_endpoints.py
"""

import json
from pathlib import Path

import pytest

from python.viewer.endpoints import (EndpointError, default_interval,
                                     load_endpoint)

SECRET = "a-token-that-must-not-be-printed"


def a_config(tmp_path: Path, endpoints: dict) -> Path:
    """Write an endpoints file and return its path."""
    path = tmp_path / "remote_endpoints.json"
    path.write_text(json.dumps({"endpoints": endpoints}), encoding="utf-8")
    return path


def test_a_named_endpoint_yields_its_url_and_credential(tmp_path: Path) -> None:
    """The ordinary case, which is the whole feature."""
    path = a_config(tmp_path, {"live": {"base_url": "https://box.example",
                                        "token": SECRET}})

    assert load_endpoint("live", path) == ("https://box.example", SECRET)


def test_an_unknown_name_lists_the_ones_that_exist(tmp_path: Path) -> None:
    """
    A typo is the common case, and the answer to it is on hand.

    "no endpoint 'prod'" leaves the operator opening a file; naming what the file
    holds ends the question in the same line.
    """
    path = a_config(tmp_path, {"live": {"base_url": "https://box.example",
                                        "token": SECRET},
                               "local": {"base_url": "http://127.0.0.1:8110",
                                         "token": SECRET}})

    with pytest.raises(EndpointError) as raised:
        load_endpoint("prod", path)

    assert "live" in str(raised.value) and "local" in str(raised.value)


def test_a_missing_file_says_what_to_copy(tmp_path: Path) -> None:
    """A first run on a new machine lands here, and it is not an error state."""
    with pytest.raises(EndpointError) as raised:
        load_endpoint("live", tmp_path / "nothing.json")

    assert "example" in str(raised.value)


def test_a_broken_file_is_reported_without_its_contents(tmp_path: Path) -> None:
    """
    The token sits three lines from whatever the parse error is.

    An error message that echoes the file to help with debugging publishes the
    credential to the console and to every screenshot of it.
    """
    path = tmp_path / "remote_endpoints.json"
    path.write_text('{"endpoints": {"live": {"token": "' + SECRET + '",,}}}',
                    encoding="utf-8")

    with pytest.raises(EndpointError) as raised:
        load_endpoint("live", path)

    assert SECRET not in str(raised.value)


def test_an_entry_without_a_token_names_the_entry(tmp_path: Path) -> None:
    """`/v1/status` is gated, so a viewer without a credential cannot start."""
    path = a_config(tmp_path, {"live": {"base_url": "https://box.example"}})

    with pytest.raises(EndpointError) as raised:
        load_endpoint("live", path)

    assert "live" in str(raised.value) and "token" in str(raised.value)


def test_a_collector_on_this_machine_is_read_more_often() -> None:
    """
    Loopback costs nothing; a request per second through a TLS proxy does not.

    The refresh rate is the one number here somebody would otherwise regret, so
    it follows from where the collector is rather than from a default.
    """
    assert default_interval("http://127.0.0.1:8110") == 1.0
    assert default_interval("http://localhost:8110") == 1.0
    assert default_interval("https://collector.example") == 2.0


def test_the_shell_defaults_to_the_narrow_credential() -> None:
    """
    The example is what a new machine is set up from, so its shape is the rule.

    Two entries in it reach the same collector with very different rights. The
    status shell sits open on a desk all day; pointing its default at the
    operator's credential would leave a token that can also fetch archive files
    and log excerpts lying in a window, for a screen that needs one route.

    The name matters as much as the grant: `watch` is this repository's own
    shell. `viewer` would read as the FiniexViewer project, which is a separate
    peer with its own credentials, and a session cleaning up tokens later would
    have to guess which one it was looking at.
    """
    import json

    document = json.loads(
        Path("user_configs/remote_endpoints.example.json").read_text(
            encoding="utf-8"))
    endpoints = document["endpoints"]

    assert "watch" in endpoints, "the default --endpoint has no example entry"
    assert endpoints["watch"]["grants"] == ["status:detail"], (
        "the status shell's example carries more than the one route it reads")
    assert "viewer" not in endpoints, (
        "'viewer' collides with the FiniexViewer project - use 'watch'")
