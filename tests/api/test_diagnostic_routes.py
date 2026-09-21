"""
FiniexDataCollector - Diagnostic Route Tests

Three routes that answer questions which previously required a session on the
machine: what settings are in force, what has been written, and what the log says.

The archive route has a named origin: on 2026-09-15 the consuming project asked
which tick files spanned a host migration, with a deadline, and the answer could
not be given because nobody with the question had access to the archive.

Location: tests/api/test_diagnostic_routes.py
"""

import inspect
import json
import re
import warnings
from pathlib import Path
from typing import Any, Dict

import pytest

warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastapi.testclient import TestClient

from python.api.api_app import create_api
from python.api.archive_reader import read_archive
from python.api.build_info import BuildInfo
from python.api.redaction import redact_config
from python.api.token_loader import load_token_registry
from python.types.collector_stats import CollectorStats
from python.types.tick_types import OriginBlock

ORIGIN = OriginBlock(
    instance_id="a3f8c21d9b04",
    collected_on="collector-prod",
    producer="finiex-data-collector",
    producer_version="1.2.0"
)

BUILD = BuildInfo("1.1.0", "1.6.0", "abc1234", False, "3.13.7", "2026-09-15T10:00:00+00:00")

BOT_TOKEN = "8256155493:AAplanted-secret-value"
API_TOKEN = "planted-consumer-secret"

FULL = {"token": "tok-full", "grants": [
    "status:detail", "config:effective", "logs:collector", "archive:index"]}
NARROW = {"token": "tok-narrow", "grants": ["status:detail"]}
# Authenticated, entitled to nothing - the floor every gated route must refuse.
NOBODY = {"token": "tok-nobody", "grants": []}

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
    resyncs_close: int = 0,
    tick_count: int = 2,
    instance_id: str = None
) -> None:
    """
    Plant one archive file.

    Args:
        directory: Collector directory
        name: File name
        resyncs_open: Anchor counter in the header
        resyncs_close: Anchor counter in the summary
        tick_count: How many ticks to write, for tests that need a realistic size
        instance_id: The producing identity. None plants a file from before 1.7.0,
            which carried no `origin` block at all
    """
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(json.dumps({
        "metadata": {
            "symbol": name.split("_")[0],
            "data_format_version": "1.6.0",
            "collected_msc_timebase": "utc",
            "start_time": "2026.09.15 10:00:00",
            "anchor_resyncs": resyncs_open,
            **({"origin": {"instance_id": instance_id,
                           "collected_on": "a-host",
                           "producer": "finiex-data-collector",
                           "producer_version": "1.2.1"}}
               if instance_id else {})
        },
        "ticks": [
            {"time_msc": 1789000000000 + i * 1000,
             "collected_msc": 1789000000007 + i * 1000,
             "bid": 45000.0 + i, "ask": 45010.0 + i, "last": 45005.0 + i,
             "spread_points": 100, "spread_pct": 0.022,
             "quote_age_ms": 249, "tick_flags": "BUY", "session": "24h"}
            for i in range(tick_count)
        ],
        "summary": {
            "total_ticks": tick_count,
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
        origin=ORIGIN,
        registry=load_token_registry(
            {"full": FULL, "narrow": NARROW, "nobody": NOBODY}),
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


# =============================================================================
# THE SURFACE AS A WHOLE
# =============================================================================

# Every route that answers without a credential, named on purpose. The third
# one is FastAPI's own schema: it exists by default rather than by decision, and
# a surface that nobody wrote down is one nobody reviews.
OPEN_ROUTES = {"/v1/health", "/v1/build", "/openapi.json"}


def test_no_route_is_authenticated_but_ungated(client: TestClient) -> None:
    """
    The one weakness of this auth model, walked rather than read.

    Authentication is inherited from the shared bearer dependency, so a route
    added later cannot forget it. The surface half is declared per route, and a
    route mounted without it is authenticated but **ungated** - reachable by any
    valid token, and indistinguishable from a gated one by reading the code.

    The package ships `assert_no_identity_route_is_ungated` for this, but it
    only covers routes carrying a path identity and refuses to run without one.
    Every route here is a collection route by design - a date or a symbol is not
    something a grant is written against - so the equivalent is walked here.
    """
    nobody = {"Authorization": f"Bearer {NOBODY['token']}"}
    walked = []

    for route in client.app.routes:
        path = getattr(route, "path", "")
        if not path or path in OPEN_ROUTES:
            continue

        walked.append(path)

        # A path parameter needs a value before the route resolves at all. Any
        # value does: the grant is refused before the name is looked up.
        callable_path = re.sub(r"\{[^}]+\}", "x", path)
        response = client.get(
            callable_path, headers=nobody, params={"day": "2026-09-15"})

        assert response.status_code == 403, (
            f"{path} answered {response.status_code} to a token holding "
            f"nothing - it is authenticated but ungated")

    assert sorted(walked) == ["/v1/archive", "/v1/configs", "/v1/files/{name}",
                              "/v1/logs", "/v1/status"], (
        "a gated route appeared or disappeared; listing them here is what keeps "
        "a router that drops out of the app from leaving the walk green while "
        "the surface it protected goes unreachable")


def test_no_route_does_its_work_on_the_event_loop(client: TestClient) -> None:
    """
    A route defined with `def` runs in a threadpool; `async def` runs on the loop.

    That distinction is the whole reason the file transfer does not cost the
    collection: handing out a 22 MB archive file, walking the archive to build
    the register, or reading a day of log are all blocking work, and the loop
    they would block is the one that stamps every tick's arrival time. The
    consuming project fetches files in a loop, so this is not a rare event.

    Nothing in FastAPI warns about the change. One keyword moves the work onto
    the loop, and it would show as arrival lag in the data rather than as an
    error anywhere.
    """
    on_the_loop = [
        route.path for route in client.app.routes
        if getattr(route, "path", "").startswith("/v1")
        and inspect.iscoroutinefunction(getattr(route, "endpoint", None))]

    assert not on_the_loop, (
        f"these routes would block the collector's event loop: {on_the_loop}")


def test_the_open_routes_stay_open(client: TestClient) -> None:
    """
    The other direction. An uptime probe that suddenly needs a credential is an
    outage nobody sees until the probe has been red for a day.
    """
    for path in sorted(OPEN_ROUTES):
        assert client.get(path).status_code == 200, path


def test_the_interactive_docs_are_not_served(client: TestClient) -> None:
    """
    The schema is enough for a consumer to integrate against. A rendered,
    try-it-out console on a diagnostic surface is a different thing, and it was
    never decided on.
    """
    assert client.get("/docs").status_code == 404
    assert client.get("/redoc").status_code == 404


def test_credentials_in_a_log_line_are_masked(tmp_path: Path) -> None:
    """
    A log is free text, so the key-name rule has nothing to work with. What
    reaches it is a bot token inside a URL or a bearer header in a traceback,
    and a diagnostic route that serves them turns a convenience into a leak.
    """
    from datetime import date

    from python.api.log_reader import read_log

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "finiexdatacollector_2026-09-15.log").write_text(
        "2026-09-15 08:00:00 UTC | INFO     | X | GET https://api.telegram.org"
        "/bot8256155493:AAHHwyeyjy8s0DEOo14VOSQ1JKD4pXwPS7Q/sendMessage\n"
        "2026-09-15 08:01:00 UTC | INFO     | X | nothing secret here\n",
        encoding="utf-8")

    excerpt = read_log(logs, date(2026, 9, 15))

    assert excerpt["redacted_lines"] == 1, "the count has to be surfaced"
    assert "AAHHwyeyjy8s0DEOo14VOSQ1JKD4pXwPS7Q" not in json.dumps(excerpt)
    assert "nothing secret here" in json.dumps(excerpt)


# =============================================================================
# HANDING OUT A FILE
# =============================================================================

DOWNLOADER = {"token": "tok-dl", "grants": ["archive:index", "files:*"]}


@pytest.fixture
def download_client(tmp_path: Path) -> TestClient:
    """
    A client holding `files:*`, over an archive with one finished file and one
    write-ahead log that has not become a file yet.

    Args:
        tmp_path: pytest temp directory

    Returns:
        TestClient over the application
    """
    raw = tmp_path / "raw"
    kraken = raw / "kraken"
    write_archive_file(kraken, "BTCUSD_20260915_100000_ticks.json")
    # Above the compression threshold. The two-tick file above is 371 bytes and
    # is left uncompressed, so a compression test using it would pass for the
    # wrong reason.
    write_archive_file(
        kraken, "SOLUSD_20260915_100000_ticks.json", tick_count=400)
    (kraken / "ETHUSD_20260915_110000_ticks.jsonl.part").write_text(
        '{"symbol": "ETHUSD"}\n', encoding="utf-8")
    (kraken / ".collector.lock").write_text("{}", encoding="utf-8")

    return TestClient(create_api(
        build=BUILD,
        health_provider=lambda: {"status": "ok"},
        detail_provider=lambda: {"symbols": {}},
        origin=ORIGIN,
        registry=load_token_registry(
            {"dl": DOWNLOADER, "narrow": NARROW, "nobody": NOBODY}),
        raw_data_dir=raw
    ))


def dl_headers() -> Dict[str, str]:
    """Authorization header for the token holding `files:*`."""
    return {"Authorization": f"Bearer {DOWNLOADER['token']}"}


def test_a_finished_file_is_handed_out(download_client: TestClient) -> None:
    """The transfer this replaces SFTP for."""
    response = download_client.get(
        "/v1/files/BTCUSD_20260915_100000_ticks.json", headers=dl_headers())

    assert response.status_code == 200
    assert response.json()["metadata"]["symbol"] == "BTCUSD"


def test_the_register_can_carry_a_checksum(download_client: TestClient) -> None:
    """
    What makes a transfer verifiable. Off by default, because hashing reads the
    whole archive and a register is asked for far more often than a transfer is
    checked.
    """
    plain = download_client.get("/v1/archive", headers=dl_headers()).json()
    hashed = download_client.get(
        "/v1/archive?with_checksum=true", headers=dl_headers()).json()

    assert plain["files"][0]["sha256"] is None
    assert len(hashed["files"][0]["sha256"]) == 64


def test_the_checksum_matches_what_is_served(
    download_client: TestClient
) -> None:
    """A digest a consumer cannot reproduce from the bytes is worse than none."""
    import hashlib

    entry = download_client.get(
        "/v1/archive?with_checksum=true", headers=dl_headers()).json()["files"][0]
    body = download_client.get(
        f"/v1/files/{entry['file']}", headers=dl_headers()).content

    assert hashlib.sha256(body).hexdigest() == entry["sha256"]


def test_an_unfinished_file_cannot_be_reached(
    download_client: TestClient
) -> None:
    """
    The property the operator asked for, and it holds by construction rather
    than by a check: what is still being collected has no `.json` name yet.
    """
    response = download_client.get(
        "/v1/files/ETHUSD_20260915_110000_ticks.jsonl.part",
        headers=dl_headers())

    assert response.status_code == 404


@pytest.mark.parametrize("name", [
    "../../../etc/passwd",
    r"..\windows\win.ini",
    ".collector.lock",
    "BTCUSD_20260915_100000_ticks.json.bak",
    "%2e%2e%2fsecret",
])
def test_nothing_outside_the_archive_can_be_requested(
    download_client: TestClient,
    name: str
) -> None:
    """
    The name is the only thing standing between a request and the file system.

    All of these answer 404 rather than distinguishing "malformed" from "not
    there" - the difference would turn the route into a probe for what exists.

    Args:
        download_client: Client holding `files:*`
        name: A name that must not resolve
    """
    response = download_client.get(f"/v1/files/{name}", headers=dl_headers())

    assert response.status_code in (404, 400), name
    assert b"root:" not in response.content


def test_a_token_without_the_files_surface_is_refused(
    download_client: TestClient
) -> None:
    """Holding the register is not holding the archive."""
    response = download_client.get(
        "/v1/files/BTCUSD_20260915_100000_ticks.json",
        headers={"Authorization": f"Bearer {NOBODY['token']}"})

    assert response.status_code == 403


def test_a_path_escaping_the_archive_is_refused_even_if_the_name_passes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The second net, tested by breaking the first one.

    After the name check, `target / name` cannot escape by path arithmetic - the
    pattern permits no separators. What it cannot see is a symlink or a junction
    planted in the archive directory under a valid archive name, which resolves
    somewhere else entirely.

    Rather than plant one - creating a symlink needs privileges on Windows, and a
    guard that is only tested where it does not run is not tested - the first
    check is disabled and the second is asked to hold alone. That is exactly the
    situation it exists for: a name pattern loosened by a later change.

    Args:
        tmp_path: pytest temp directory
        monkeypatch: pytest patching helper
    """
    from python.api import file_server

    secret = tmp_path / "secret.json"
    secret.write_text('{"not": "yours"}', encoding="utf-8")
    (tmp_path / "raw" / "kraken").mkdir(parents=True)

    monkeypatch.setattr(file_server, "is_servable_name", lambda name: True)

    escaped = file_server.resolve_archive_file(
        tmp_path / "raw", "kraken", "../../secret.json")

    assert escaped is None, "a resolved path outside the archive was served"


# =============================================================================
# TRANSFER
# =============================================================================

def test_a_large_response_is_compressed_when_asked(
    download_client: TestClient
) -> None:
    """
    Tick JSON is the same keys on every line and compresses about twentyfold.
    Over a link this is the difference between a gigabyte a week and forty
    megabytes.
    """
    response = download_client.get(
        "/v1/files/SOLUSD_20260915_100000_ticks.json",
        headers={**dl_headers(), "Accept-Encoding": "gzip"})

    assert response.status_code == 200
    assert response.headers.get("content-encoding") == "gzip"


def test_a_client_that_does_not_ask_gets_plain_json(
    download_client: TestClient
) -> None:
    """
    Negotiated, not imposed. A consumer written before this existed keeps
    working unchanged, which is what makes it safe to add without a version.
    """
    response = download_client.get(
        "/v1/files/SOLUSD_20260915_100000_ticks.json",
        headers={**dl_headers(), "Accept-Encoding": "identity"})

    assert response.status_code == 200
    assert "content-encoding" not in response.headers


def test_compression_does_not_change_what_arrives(
    download_client: TestClient
) -> None:
    """
    The register's SHA-256 is over the uncompressed file. A client decodes
    before it hashes, so the digest has to survive the round trip - otherwise
    every verified transfer would fail exactly when compression is on.
    """
    import hashlib

    entry = download_client.get(
        "/v1/archive?with_checksum=true", headers=dl_headers()).json()["files"][0]

    body = download_client.get(
        f"/v1/files/{entry['file']}",
        headers={**dl_headers(), "Accept-Encoding": "gzip"}).content

    assert hashlib.sha256(body).hexdigest() == entry["sha256"]


def test_a_tiny_response_is_left_alone(download_client: TestClient) -> None:
    """
    Below the threshold the gzip header costs more than it saves. /v1/health is
    about a hundred bytes and is polled on an interval.
    """
    response = download_client.get(
        "/v1/health", headers={"Accept-Encoding": "gzip"})

    assert response.status_code == 200
    assert "content-encoding" not in response.headers


def test_the_log_route_defaults_to_the_newest_day(client: TestClient) -> None:
    """
    Omitting the day means the newest file present, deliberately not "today".

    From a remote session the box's own date boundary is unknown. A few minutes
    after midnight UTC, "today" is an almost empty file and yesterday is the
    finished one - and in both cases what somebody means by "the log" is the
    newest one there is.

    Args:
        client: TestClient over the diagnostic routes
    """
    with_day = client.get("/v1/logs?day=2026-09-15", headers=HEADERS).json()
    without = client.get("/v1/logs", headers=HEADERS).json()

    assert without["day"] == with_day["day"], "did not pick the only day present"
    assert without["line_count"] == with_day["line_count"]


def test_an_empty_log_directory_answers_rather_than_guesses(
    tmp_path: Path
) -> None:
    """
    No log files at all is a state, not an error.

    Defaulting to today's date there would report a missing file for a day that
    was never going to exist, which reads like a fault. Saying so plainly, with
    an empty list of days, is the honest answer.

    Args:
        tmp_path: pytest temp directory
    """
    empty = tmp_path / "logs"
    empty.mkdir()

    probe = TestClient(create_api(
        build=BUILD,
        health_provider=lambda: {"status": "ok"},
        detail_provider=lambda: {"symbols": {}},
        origin=ORIGIN,
        registry=load_token_registry({"full": FULL}),
        log_dir=empty
    ))

    payload = probe.get("/v1/logs", headers=HEADERS).json()

    assert payload["exists"] is False
    assert payload["day"] is None
    assert payload["available_days"] == []


def test_the_register_names_the_identity_that_wrote_each_file(
    tmp_path: Path
) -> None:
    """
    A consumer has to be able to decide before transferring, not after.

    It costs nothing while one instance writes into a directory. From the moment
    two have - which is exactly what pointing a new deployment at an existing
    archive root does - "which files here did an identity I do not know write"
    would otherwise mean downloading the archive to read twelve characters out of
    each file. At 50,000 ticks that is about 22 MB per answer.

    The argument is the one that already put `data_format_version` in the
    register; FiniexTestingIDE made it back to us on 2026-09-17 and it is theirs
    as much as ours.

    Args:
        tmp_path: pytest temp directory
    """
    kraken = tmp_path / "kraken"
    write_archive_file(kraken, "BTCUSD_20260917_100000_ticks.json",
                       instance_id="cac17e8c4d70")
    write_archive_file(kraken, "ETHUSD_20260917_100000_ticks.json",
                       instance_id="db9a1776313e")

    entries = {e["file"]: e for e in read_archive(tmp_path, "kraken")["files"]}

    assert entries["BTCUSD_20260917_100000_ticks.json"]["instance_id"] == "cac17e8c4d70"
    assert entries["ETHUSD_20260917_100000_ticks.json"]["instance_id"] == "db9a1776313e"


def test_a_file_from_before_provenance_says_so_with_null(tmp_path: Path) -> None:
    """
    `null`, and never a guess.

    Files below 1.7.0 carry no `origin` block, and nothing can infer afterwards
    which instance wrote them - that is the whole reason the consuming project
    labels them from a dated attestation instead. Reporting anything but `null`
    here would be this project's oldest mistake in a new place.

    Args:
        tmp_path: pytest temp directory
    """
    kraken = tmp_path / "kraken"
    write_archive_file(kraken, "BTCUSD_20260915_100000_ticks.json")

    entry = read_archive(tmp_path, "kraken")["files"][0]

    assert "instance_id" in entry, "the key must be present, so its absence is not the answer"
    assert entry["instance_id"] is None
