"""
FiniexDataCollector - Status API Tests

The surface exists so that "is it running, and which version" stops being an
inference. On 2026-09-15 that inference was actually made - from process uptime
against commit timestamps - and it was right by luck rather than by evidence.

Two properties carry most of the weight here: the open routes must stay free of
anything an uptime probe has no business receiving, and the gated route must
refuse a token that was never granted the surface.

Location: tests/api/test_status_api.py
"""

import warnings
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

warnings.filterwarnings("ignore", category=DeprecationWarning)

import asyncio
import sys
from dataclasses import asdict
from typing import List

from fastapi.testclient import TestClient

from python.api.api_app import create_api
from python.api.build_info import BuildInfo
from python.api.stats_serializer import (
    _socket_count,
    health_payload,
    serialize_stats
)
from python.main import serve_status_api
from python.api.token_loader import ConsumerToken, load_token_registry
from python.types.collector_stats import CollectorStats
from python.types.tick_types import OriginBlock

ORIGIN = OriginBlock(
    instance_id="a3f8c21d9b04",
    collected_on="collector-prod",
    producer="finiex-data-collector",
    producer_version="1.2.0"
)

BUILD = BuildInfo(
    version="1.1.0",
    data_format_version="1.6.0",
    commit="abc1234",
    dirty=False,
    python_version="3.13.7",
    started_at="2026-09-15T10:00:00+00:00"
)

READER = {"token": "tok-reader", "grants": ["status:detail"], "note": "probe"}
STRANGER = {"token": "tok-stranger", "grants": [], "note": "no surface"}


@pytest.fixture
def stats() -> CollectorStats:
    """
    A collector with one symbol and a disk reading.

    Returns:
        Populated CollectorStats
    """
    stats = CollectorStats()
    stats.websocket_status = "connected"
    stats.total_files = 3

    symbol = stats.get_symbol_stats("BTCUSD")
    symbol.current_file_ticks = 412
    symbol.last_bid = 79383.7
    symbol.last_ask = 79383.8
    symbol.last_quote_age_ms = 249
    symbol.last_tick_time = datetime.now(timezone.utc)

    stats.update_disk_space(
        total=500 * 1024 ** 3, used=300 * 1024 ** 3, free=200 * 1024 ** 3)
    return stats


@pytest.fixture
def client(stats: CollectorStats) -> TestClient:
    """
    The API as main.py builds it.

    Args:
        stats: Live statistics the routes read through

    Returns:
        TestClient over the application
    """
    return TestClient(create_api(
        build=BUILD,
        health_provider=lambda: health_payload(stats),
        detail_provider=lambda: serialize_stats(stats),
        origin=ORIGIN,
        registry=load_token_registry(
            {"reader": READER, "stranger": STRANGER})
    ))


def as_reader() -> Dict[str, str]:
    """Authorization header for the token granting `status:detail`."""
    return {"Authorization": f"Bearer {READER['token']}"}


# =============================================================================
# THE OPEN ROUTES
# =============================================================================

def test_health_answers_without_a_credential(client: TestClient) -> None:
    """An uptime probe carries no token; that exemption is the point."""
    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["websocket_status"] == "connected"


def test_health_publishes_nothing_about_what_is_collected(
    client: TestClient
) -> None:
    """
    Symbol names and tick counts describe what is being traded and how much.
    An unauthenticated caller receives liveness, not the trading universe.
    """
    body = client.get("/v1/health").text

    assert "BTCUSD" not in body
    assert "current_file_ticks" not in body


def test_build_reports_the_running_code(client: TestClient) -> None:
    """
    Open because the repository is public - a commit hash discloses nothing that
    is not already on GitHub.
    """
    payload = client.get("/v1/build").json()

    assert payload["version"] == "1.1.0"
    assert payload["data_format_version"] == "1.6.0"
    assert payload["commit"] == "abc1234"
    assert payload["dirty"] is False


def test_build_does_not_change_between_requests(client: TestClient) -> None:
    """
    Sampled once at startup. Read per request, it would report a new commit
    after a pull while the old code still serves - wrong in exactly the one
    case the route exists for.
    """
    assert client.get("/v1/build").json() == client.get("/v1/build").json()


# =============================================================================
# THE GATED ROUTE
# =============================================================================

def test_detail_refuses_an_anonymous_caller(client: TestClient) -> None:
    """No credential is a 401, before any question of permission."""
    assert client.get("/v1/status").status_code == 401


def test_detail_refuses_an_unknown_token(client: TestClient) -> None:
    """A token the registry does not carry holds nothing."""
    response = client.get(
        "/v1/status", headers={"Authorization": "Bearer nonsense"})

    assert response.status_code == 401


def test_detail_refuses_a_token_without_the_surface(client: TestClient) -> None:
    """
    Authenticated but not permitted is a 403, not a 401 - a denial the caller
    can act on rather than one that sends them hunting for a bad credential.
    """
    response = client.get(
        "/v1/status", headers={"Authorization": f"Bearer {STRANGER['token']}"})

    assert response.status_code == 403


def test_detail_serves_the_live_metrics(client: TestClient) -> None:
    """The whole CollectorStats object, not a hand-picked subset."""
    payload = client.get("/v1/status", headers=as_reader()).json()

    assert payload["symbols"]["BTCUSD"]["current_file_ticks"] == 412
    assert payload["symbols"]["BTCUSD"]["last_quote_age_ms"] == 249
    assert payload["total_files"] == 3
    assert payload["uptime_seconds"] >= 0


def test_detail_carries_the_computed_disk_status(client: TestClient) -> None:
    """
    `status` and the gigabyte figures are properties, so they are absent from a
    plain dataclass conversion - and they are the part a monitor acts on.
    """
    disk = client.get("/v1/status", headers=as_reader()).json()["disk_space"]

    assert disk["status"] == "WARNING"
    assert disk["free_gb"] == 200.0


def test_detail_timestamps_carry_their_offset(client: TestClient) -> None:
    """
    A naive timestamp reads as local time at the far end. This project has paid
    for that once already.
    """
    payload = client.get("/v1/status", headers=as_reader()).json()

    assert payload["start_time"].endswith("+00:00")
    assert payload["symbols"]["BTCUSD"]["last_tick_time"].endswith("+00:00")


def test_display_knobs_are_not_served_as_metrics(client: TestClient) -> None:
    """
    How much history the terminal keeps is not a measurement, and a consumer
    reading it would be reading the display's configuration.
    """
    payload = client.get("/v1/status", headers=as_reader()).json()

    assert "max_recent_logs" not in payload
    assert "max_reconnect_history" not in payload


def test_a_grant_naming_an_unknown_surface_fails_at_parse_time() -> None:
    """
    The vocabulary is closed. A surface typo is refused at boot rather than
    becoming a denial at request time that nobody can explain.
    """
    with pytest.raises(Exception):
        ConsumerToken(token="x", grants=["statsu:detail"])


def test_the_status_carries_the_producing_identity(client: TestClient) -> None:
    """
    The consumer must be able to learn an identity BEFORE the first file.

    An identity it has never seen resolves to `unknown`, and `unknown` refuses a
    measurement run at admission - so a freshly deployed collector would deliver
    files nothing may be measured against until someone read its identity off the
    machine's disk over a shell. The route is what makes that a request instead.
    """
    payload = client.get("/v1/status", headers=as_reader()).json()

    assert payload["origin"] == {
        "instance_id": "a3f8c21d9b04",
        "collected_on": "collector-prod",
        "producer": "finiex-data-collector",
        "producer_version": "1.2.0"
    }


def test_the_identity_has_the_same_shape_a_file_carries(
    client: TestClient
) -> None:
    """
    One structure, not two.

    A consumer that parses the block out of a tick file must be able to parse the
    one from this route with the same code; a route-only spelling would be a
    second contract to keep in step with the first.
    """
    served = client.get("/v1/status", headers=as_reader()).json()["origin"]

    assert set(served) == set(asdict(ORIGIN))


def test_the_identity_stays_off_the_open_routes(client: TestClient) -> None:
    """
    `/v1/build` is open because a commit hash discloses nothing the public
    repository does not. An identity is not that: it names one machine's data
    directory, and it is the key a consumer's trust registry is built on. It
    belongs behind the same grant as the symbol names.
    """
    for route in ("/v1/health", "/v1/build"):
        body = client.get(route).text
        assert "a3f8c21d9b04" not in body, f"{route} discloses the identity"
        assert "origin" not in client.get(route).json()


# =============================================================================
# THE SURFACE MUST NOT COST THE COLLECTION
# =============================================================================

def test_a_status_api_that_cannot_bind_does_not_kill_the_collector() -> None:
    """
    The diagnostic surface is worth less than the data it describes.

    uvicorn calls `sys.exit()` when the port is taken, and `SystemExit` is a
    `BaseException` - so the `except Exception` that used to guard this never
    saw it. The exception left the task, asyncio cancelled the rest, and the
    collector died at startup. Measured on 2026-09-17: a development container
    held port 8110 and a live collector refused to run because of it.

    The failure only appears when something else fails, so the test makes that
    something else fail.
    """
    class RefusingServer:
        """A uvicorn.Server that cannot bind, behaving exactly as uvicorn does."""

        async def serve(self) -> None:
            sys.exit(3)

    recorded: List[str] = []

    class RecordingLogger:
        def error(self, message: str) -> None:
            recorded.append(message)

    asyncio.run(serve_status_api(RefusingServer(), RecordingLogger()))

    assert recorded, "the failure was swallowed without a trace"
    assert "port" in recorded[0].lower()


def test_a_status_api_that_stops_later_does_not_kill_the_collector() -> None:
    """The ordinary exception path still holds; the new clause did not replace it."""
    class FailingServer:
        async def serve(self) -> None:
            raise RuntimeError("socket went away")

    recorded: List[str] = []

    class RecordingLogger:
        def error(self, message: str) -> None:
            recorded.append(message)

    asyncio.run(serve_status_api(FailingServer(), RecordingLogger()))

    assert "socket went away" in recorded[0]


# =============================================================================
# DIAGNOSIS FROM A DISTANCE
# =============================================================================

def test_the_status_reports_what_the_process_costs(client: TestClient) -> None:
    """
    Three services share 8 GB on that box, with roughly 2.4 GB of headroom.

    A collector runs for weeks, which is exactly where a slow leak hides, and
    from a remote session there is no shell to ask. The sister project found its
    own documented memory figure stale by 380 MB the day it measured instead of
    remembering.
    """
    payload = client.get("/v1/status", headers=as_reader()).json()

    process = payload["process"]
    assert process["available"] is True
    assert process["rss_mb"] > 0
    assert process["threads"] >= 1
    assert process["cpu_seconds"] >= 0


def test_a_refused_socket_count_is_unknown_and_not_zero(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Windows refuses the connection list to a process without the rights to ask.

    Reported as `0` that reads as "none open", which is a measurement nobody
    made. `None` says the opposite, and the difference is the whole reason this
    project exists in the shape it does.

    Args:
        monkeypatch: pytest patching helper
    """
    class RefusingProcess:
        def net_connections(self) -> None:
            raise PermissionError("access denied")

    assert _socket_count(RefusingProcess()) is None


def test_the_process_block_never_fails_the_route(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A diagnostic that can take down the route carrying it costs more than it
    reports. So psutil failing outright is an answer, not an exception.

    Args:
        monkeypatch: pytest patching helper
    """
    import python.api.stats_serializer as serializer

    def unavailable() -> None:
        raise RuntimeError("no such process")

    monkeypatch.setattr(serializer.psutil, "Process", unavailable)

    assert serializer.process_resources() == {"available": False}


def test_build_states_which_interpreter_is_running(client: TestClient) -> None:
    """
    Four Python versions were in play on 2026-09-17 and nobody could say which
    one production had.

    The Dockerfile pinned 3.12, CI ran 3.13, the development laptop had 3.13.7,
    and the server was unknowable from anywhere - no surface reported it. A suite
    green on a version production does not run proves less than it looks like,
    and this is the field that turns that from an assumption into a question with
    an answer.
    """
    payload = client.get("/v1/build").json()

    assert payload["python_version"] == BUILD.python_version
    assert payload["python_version"].count(".") == 2, "want major.minor.patch"


def test_the_interpreter_version_is_sampled_not_invented() -> None:
    """
    It describes the process that is answering, so it comes from the process.

    Read per request it would still be right - the interpreter cannot change
    under a running process - but it is sampled with the rest of the build
    identity for one reason: every field on this route describes the same
    instant, and a mixture is harder to reason about than a snapshot.
    """
    import platform

    from python.api.build_info import sample_build_info

    sampled = sample_build_info("9.9.9", "9.9.9")

    assert sampled.python_version == platform.python_version()
