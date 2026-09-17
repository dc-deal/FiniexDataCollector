"""
FiniexDataCollector - Application Config Tests

The version lives in configs/app_config.json and is repeated in the README status
line, and nowhere else. It used to have a third home as the AppConfig default, which
is the copy that would have been forgotten; the field is required now, and one test
below keeps that default from coming back.

Location: tests/utils/test_app_config.py
"""

import json
import re
from pathlib import Path

from python.main import is_reconnect
from python.utils.config_loader import AppConfig

CONFIG_PATH = Path("configs/app_config.json")
# The file is tracked as README.md. It was tracked as `readme.md` while the disk
# said `README.md` until 2026-09-17 - git's core.ignorecase on Windows lets the
# index and the working tree disagree silently, and CI, which checks out from the
# index, was the first thing to notice.
README_PATH = Path("README.md")
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def tracked_config() -> dict:
    """
    Read the tracked base config.

    user_configs/ is deliberately not consulted: it is the operator's local overlay
    and must never decide what the shipped defaults are.

    Returns:
        Parsed configs/app_config.json
    """
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def test_the_version_has_no_second_home_in_the_code() -> None:
    """
    One fact, one place.

    `version` carried a Pydantic default that repeated what the tracked config
    already said, and a default exists precisely to be forgotten on the day the
    other copy moves. Required instead - the tracked config always carries it,
    and a config that does not is a defect worth a refusal rather than a silent
    fallback to whatever the last release happened to be.
    """
    assert AppConfig.model_fields["version"].is_required()


def test_version_is_semver() -> None:
    """MAJOR.MINOR.PATCH — a two-part version breaks ordering comparisons."""
    assert SEMVER.match(tracked_config()["version"])


def test_readme_states_the_same_version() -> None:
    """
    The third copy. No mechanism keeps it in step, so the test is the mechanism.

    A README claiming a version the code does not run is what a reader checks
    first and trusts longest.
    """
    match = re.search(r"^> \*\*Version:\*\* (\S+)", README_PATH.read_text(
        encoding="utf-8"), re.MULTILINE)

    assert match, "README status line with '> **Version:** X.Y.Z' not found"
    assert match.group(1) == tracked_config()["version"]


def test_tracked_config_carries_no_live_credentials() -> None:
    """
    The tracked file is the disabled placeholder; the real token lives in the
    gitignored user_configs/ overlay. A populated value here means a secret
    reached the repository.
    """
    telegram = tracked_config().get("telegram", {})

    assert telegram.get("bot_token", "") == ""
    assert telegram.get("chat_id", "") == ""
    assert telegram.get("enabled") is False


def test_a_connection_coming_back_counts_as_a_reconnect() -> None:
    """
    The rule that reported zero reconnects for 173 of them.

    A forced reconnect closes the socket cleanly, so the receive loop ends
    without raising and the status stayed "connected" throughout. "failed" was
    missing from the list as well - a connect attempt that fails and later
    succeeds is a reconnect too, and the production log carried eight.
    """
    assert is_reconnect("reconnecting", "connected")
    assert is_reconnect("disconnected", "connected")
    assert is_reconnect("failed", "connected")


def test_a_connection_that_never_left_is_not_a_reconnect() -> None:
    """Steady state and outbound transitions must not inflate the count."""
    assert not is_reconnect("connected", "connected")
    assert not is_reconnect("connected", "reconnecting")
    assert not is_reconnect("connected", "failed")
    assert not is_reconnect("failed", "failed")


def test_the_detection_window_stays_under_a_minute() -> None:
    """
    Detection costs more than the outage: the staleness threshold is twice the
    heartbeat interval, and a forced reconnect waits three times it. Measured at
    30 s that was 60-90 s of lost market data per event, against 2-3 s to
    reconnect. The ceiling here is what keeps that from drifting back.
    """
    interval = tracked_config()["kraken"]["heartbeat_interval_seconds"]

    assert interval * 3 <= 45, (
        f"a forced reconnect would take up to {interval * 3}s to trigger")


def test_file_logging_is_not_debug_by_default() -> None:
    """
    DEBUG writes roughly a kilobyte per tick. The tracked default is what a
    machine runs for weeks: the production server produced 294 MB across three
    days, of which 99.96 % were DEBUG lines nothing reads afterwards.
    """
    assert tracked_config()["logging"]["file_level"] != "DEBUG"
