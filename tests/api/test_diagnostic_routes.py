"""
FiniexDataCollector - Diagnostic Route Tests

Three routes that answer questions which previously required a session on the
machine: what settings are in force, what has been written, and what the log says.

The archive route has a named origin: on 2026-09-15 the consuming project asked
which tick files spanned a host migration, with a deadline, and the answer could
not be given because nobody with the question had access to the archive.

Location: tests/api/test_diagnostic_routes.py
"""

import json
import warnings
from pathlib import Path
from typing import Any, Dict

import pytest

warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastapi.testclient import TestClient

from python.api.api_app import create_api
from python.api.build_info import BuildInfo
from python.api.redaction import redact_config
from python.api.token_loader import load_token_registry
from python.types.collector_stats import CollectorStats

BUILD = BuildInfo("1.1.0", "1.6.0", "abc1234", False, "2026-09-15T10:00:00+00:00")

BOT_TOKEN = "8256155493:AAplanted-secret-value"
API_TOKEN = "planted-consumer-secret"

FULL = {"token": "tok-full", "grants": [
    "status:detail", "config:effective", "logs:collector", "archive:index"]}
NARROW = {"token": "tok-narrow", "grants": ["status:detail"]}

HEADERS = {"Authorization": f"Bearer {FULL['token']}"}
NARROW_HEADERS = {"Authorization": f"Bearer {NARROW['token']}"}


def config_with_secrets() -> Dict[str, Any]:
    """
    A configuration shaped like the real one, with credentials planted.

    Returns:
        Configuration document
    """
    return {
        "app_name": "FiniexDataCollector",
        "version": "1.1.0",
        "telegram": {
            "enabled": True,
            "bot_token": BOT_TOKEN,
            "chat_id": "837937435"
        },
        "api": {
            "enabled": True,
            "port": 8110,
            "tokens": {"ide": {"token": API_TOKEN, "grants": ["status:detail"]}}
        },
        "kraken": {"symbols": ["BTC/USD"], "streams": ["trade", "ticker"]}
    }


def write_archive_file(
    directory: Path,
    name: str,
    resyncs_open: int = 0,
    resyncs_close: int = 0
) -> None:
    """
    Plant one archive file.

    Args:
        directory: Collector directory
        name: File name
        resyncs_open: Anchor counter in the header
        resyncs_close: Anchor counter in the summary
    """
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(json.dumps({
        "metadata": {
            "symbol": name.split("_")[0],
            "data_format_version": "1.6.0",
            "collected_msc_timebase": "utc",
            "start_time": "2026.09.15 10:00:00",
            "anchor_resyncs": resyncs_open
        },
        "ticks": [
            {"time_msc": 1789000000000, "collected_msc": 1789000000007},
            {"time_msc": 1789000001000, "collected_msc": 1789000001009}
        ],
        "summary": {
            "total_ticks": 2,
            "anchor": {"resyncs": resyncs_close, "max_correction_ms": 1}
        }
    }), encoding="utf-8")


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    """
    The API with all diagnostic routes mounted over a planted archive and log.

    Args:
        tmp_path: pytest temp directory

    Returns:
        TestClient over the application
    """
    raw = tmp_path / "raw"
    kraken = raw / "kraken"
    write_archive_file(kraken, "BTCUSD_20260915_100000_ticks.json", 0, 0)
    write_archive_file(kraken, "ETHUSD_20260915_100000_ticks.json", 0, 2)
    (kraken / "XRPUSD_20260915_110000_ticks.jsonl.part").write_text(
        "", encoding="utf-8")

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "finiexdatacollector_2026-09-15.log").write_text(
        "2026-09-15 08:00:00 UTC | INFO     | FiniexDataCollector | started\n"
        "2026-09-15 09:00:00 UTC | DEBUG    | FiniexDataCollector | a tick\n"
        "2026-09-15 10:00:00 UTC | ERROR    | FiniexDataCollector | boom\n"
        "    the traceback continues here\n",
        encoding="utf-8")

    stats = CollectorStats()
    return TestClient(create_api(
        build=BUILD,
        health_provider=lambda: {"status": "ok"},
        detail_provider=lambda: {"symbols": {}},
        registry=load_token_registry({"full": FULL, "narrow": NARROW}),
        config_provider=config_with_secrets,
        raw_data_dir=raw,
        log_dir=logs
    ))


# =============================================================================
# CONFIGURATION
# =============================================================================

def test_the_effective_configuration_is_served(client: TestClient) -> None:
    """`/v1/build` says which code runs; this says with which settings."""
    payload = client.get("/v1/configs", headers=HEADERS).json()

    assert payload["kraken"]["symbols"] == ["BTC/USD"]
    assert payload["kraken"]["streams"] == ["trade", "ticker"]


def test_no_credential_leaves_through_the_config_route(
    client: TestClient
) -> None:
    """
    The route that leaks is the one nobody reviews again after it works, so the
    check is on the raw body rather than on a field it is expected to be in.
    """
    body = client.get("/v1/configs", headers=HEADERS).text

    assert BOT_TOKEN not in body
    assert API_TOKEN not in body
    assert "837937435" not in body


def test_redaction_covers_a_key_that_did_not_exist_yet() -> None:
    """
    By key name, not by a list of paths. A path list is a promise about today's
    configuration shape, and a section added later would not be in it.
    """
    redacted = redact_config(
        {"future": {"broker_api_secret": "s3cr3t", "symbols": ["BTCUSD"]}})

    assert redacted["future"]["broker_api_secret"] == "<redacted>"
    assert redacted["future"]["symbols"] == ["BTCUSD"]


def test_config_refuses_a_token_without_that_surface(
    client: TestClient
) -> None:
    """Holding `status` is not holding `config`."""
    assert client.get(
        "/v1/configs", headers=NARROW_HEADERS).status_code == 403


# =============================================================================
# ARCHIVE
# =============================================================================

def test_the_archive_is_inventoried(client: TestClient) -> None:
    """Per file: symbol, counts, bounds - and no tick arrays."""
    payload = client.get("/v1/archive", headers=HEADERS).json()

    assert payload["file_count"] == 2
    entry = payload["files"][0]
    assert entry["symbol"] == "BTCUSD"
    assert entry["tick_count"] == 2
    assert entry["declared_tick_count"] == 2
    assert "ticks" not in entry


def test_files_that_absorbed_a_clock_correction_are_findable(
    client: TestClient
) -> None:
    """
    The query the consuming project asked for, with a deadline attached: a file
    whose anchor counter grew while it was open absorbed a correction.
    """
    payload = client.get(
        "/v1/archive?only_corrected=true", headers=HEADERS).json()

    assert payload["file_count"] == 1
    assert payload["files"][0]["symbol"] == "ETHUSD"
    assert payload["files"][0]["absorbed_clock_correction"] is True


def test_an_open_write_ahead_log_is_reported(client: TestClient) -> None:
    """
    A `.jsonl.part` without its archive file is a run that has not finished -
    the current one, or a crashed one waiting for recovery.
    """
    payload = client.get("/v1/archive", headers=HEADERS).json()

    assert payload["open_write_ahead_logs"] == [
        "XRPUSD_20260915_110000_ticks.jsonl.part"]


def test_the_archive_can_be_narrowed_to_one_symbol(client: TestClient) -> None:
    """An eight-symbol archive answers a one-symbol question."""
    payload = client.get("/v1/archive?symbol=ETHUSD", headers=HEADERS).json()

    assert payload["file_count"] == 1
    assert payload["files"][0]["symbol"] == "ETHUSD"


# =============================================================================
# LOG
# =============================================================================

def test_a_day_of_log_is_served(client: TestClient) -> None:
    """The default level hides DEBUG, which is 99.96 % of a production day."""
    payload = client.get(
        "/v1/logs?day=2026-09-15&min_level=INFO", headers=HEADERS).json()

    assert payload["exists"] is True
    levels = [line["level"] for line in payload["lines"] if line["level"]]
    assert "DEBUG" not in levels
    assert "ERROR" in levels


def test_a_multiline_entry_keeps_its_continuation(client: TestClient) -> None:
    """
    A stack trace whose first line was kept and whose body vanished is worse
    than no excerpt at all.
    """
    payload = client.get(
        "/v1/logs?day=2026-09-15&min_level=ERROR", headers=HEADERS).json()

    messages = [line["message"] for line in payload["lines"]]
    assert "boom" in messages
    assert "    the traceback continues here" in messages


def test_a_missing_day_says_which_days_exist(client: TestClient) -> None:
    """A 404 would leave the caller guessing what to ask for instead."""
    payload = client.get("/v1/logs?day=2020-01-01", headers=HEADERS).json()

    assert payload["exists"] is False
    assert "2026-09-15" in payload["available_days"]


def test_a_malformed_day_is_refused(client: TestClient) -> None:
    """400 rather than an empty excerpt that reads like a quiet day."""
    assert client.get(
        "/v1/logs?day=yesterday", headers=HEADERS).status_code == 400


def test_the_log_route_refuses_a_token_without_that_surface(
    client: TestClient
) -> None:
    """The log carries everything the collector says about itself."""
    assert client.get(
        "/v1/logs?day=2026-09-15", headers=NARROW_HEADERS).status_code == 403
