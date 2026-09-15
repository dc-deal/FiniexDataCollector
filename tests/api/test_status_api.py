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

from fastapi.testclient import TestClient

from python.api.api_app import create_api
from python.api.build_info import BuildInfo
from python.api.stats_serializer import health_payload, serialize_stats
from python.api.token_loader import ConsumerToken, load_token_registry
from python.types.collector_stats import CollectorStats

BUILD = BuildInfo(
    version="1.1.0",
    data_format_version="1.6.0",
    commit="abc1234",
    dirty=False,
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
