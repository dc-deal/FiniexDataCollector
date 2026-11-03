"""
FiniexDataCollector - The heartbeat to the outside check

An outside check alarms when pings stop, so the only thing that can go wrong in
the dangerous direction is a ping that should not have gone out: it tells the
operator's phone that a sick collector is fine. Hence most of this is about
withholding, and about the address staying out of every text.

Location: tests/alerts/test_heartbeat.py
"""

import asyncio
import types
from typing import Any, Dict, List, Optional

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

import python.alerts.heartbeat as heartbeat_module
from python.alerts.heartbeat import Heartbeat
from python.api.stats_serializer import health_payload, serialize_stats
from python.types.collector_stats import CollectorStats
from python.utils.config_loader import MonitoringConfig

SECRET = "0a1b2c3d-0000-4000-8000-000000000000"
URL = f"https://hc-ping.example/{SECRET}"
HEALTHY = {"status": "ok", "reasons": []}


class Recorder:
    """A logger that keeps what it is given."""

    def __init__(self) -> None:
        self.lines: List[str] = []

    def info(self, message: str) -> None:
        self.lines.append(message)

    warning = info


class FakeResponse:
    """What `session.get()` hands back."""

    def __init__(self, status: int) -> None:
        self.status = status

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


class FakeSession:
    """Counts the requests and answers each with a status, or raises."""

    def __init__(self, status: int = 200,
                 error: Optional[Exception] = None) -> None:
        self.status = status
        self.error = error
        self.requested: List[str] = []

    def get(self, url: str) -> FakeResponse:
        self.requested.append(url)
        if self.error:
            raise self.error
        return FakeResponse(self.status)


def build(health: Any = None, url: str = URL,
          interval: float = 60) -> tuple:
    """
    A heartbeat with a recording logger.

    Args:
        health: What the health provider returns, or a callable to use as it
        url: The ping address
        interval: Seconds between beats

    Returns:
        (heartbeat, stats, logger)
    """
    stats = CollectorStats()
    provider = health if callable(health) else (lambda: health or HEALTHY)
    heartbeat = Heartbeat(url, interval, provider, stats)
    logger = Recorder()
    heartbeat._logger = logger
    return heartbeat, stats, logger


def beat(heartbeat: Heartbeat, session: FakeSession) -> None:
    """Run one beat."""
    asyncio.run(heartbeat.beat(session))


def test_a_healthy_collector_pings_the_address() -> None:
    heartbeat, stats, _ = build()
    session = FakeSession()

    beat(heartbeat, session)

    assert session.requested == [URL]
    assert stats.heartbeat.sent == 1
    assert stats.heartbeat.last_sent_at is not None
    assert stats.heartbeat.withheld == 0
    assert stats.heartbeat.failed == 0


def test_a_degraded_collector_sends_nothing() -> None:
    """A ping here would tell the phone that a disconnected collector is fine."""
    heartbeat, stats, _ = build(
        {"status": "degraded", "reasons": ["not_connected"]})
    session = FakeSession()

    beat(heartbeat, session)

    assert session.requested == []
    assert stats.heartbeat.sent == 0
    assert stats.heartbeat.withheld == 1
    assert stats.heartbeat.last_withheld_reasons == "not_connected"
    assert stats.heartbeat.last_withheld_at is not None


def test_a_failing_collector_sends_nothing() -> None:
    heartbeat, stats, _ = build(
        {"status": "failing", "reasons": ["write_ahead_log_lost", "disk_low"]})
    session = FakeSession()

    beat(heartbeat, session)

    assert session.requested == []
    assert stats.heartbeat.last_withheld_reasons == (
        "write_ahead_log_lost, disk_low")


def test_it_goes_by_the_answer_the_route_gives() -> None:
    """Judged a second time here, the two could disagree; it reads the provider."""
    stats = CollectorStats()
    heartbeat = Heartbeat(URL, 60, lambda: health_payload(stats), stats)
    heartbeat._logger = Recorder()
    session = FakeSession()

    stats.set_websocket_status("connected")
    beat(heartbeat, session)
    assert session.requested == [URL]

    stats.record_tick_failed("BTCUSD")
    beat(heartbeat, session)
    assert session.requested == [URL], "pinged over a failing tick handler"

    stats.record_tick_handled("BTCUSD")
    beat(heartbeat, session)
    assert session.requested == [URL, URL], "did not resume after recovery"


def test_a_health_provider_that_raises_sends_nothing_and_does_not_escape() -> None:
    """Not knowing is not healthy, and the failure stays in this module."""
    def broken() -> Dict[str, Any]:
        raise RuntimeError("no idea")

    heartbeat, stats, _ = build(broken)
    session = FakeSession()

    beat(heartbeat, session)

    assert session.requested == []
    assert stats.heartbeat.sent == 0
    assert stats.heartbeat.failed == 1


def test_an_answer_of_the_check_that_is_not_success_is_not_a_beat() -> None:
    """A deleted check answers 404: nothing is watching, and that must show."""
    heartbeat, stats, logger = build()

    beat(heartbeat, FakeSession(status=404))

    assert stats.heartbeat.sent == 0
    assert stats.heartbeat.failed == 1
    assert "404" in stats.heartbeat.last_failure
    assert any("404" in line for line in logger.lines)


def test_an_unreachable_check_is_counted_and_does_not_escape() -> None:
    heartbeat, stats, _ = build()

    beat(heartbeat, FakeSession(error=OSError("no route to host")))

    assert stats.heartbeat.failed == 1
    assert stats.heartbeat.last_failed_at is not None
    assert "no route to host" in stats.heartbeat.last_failure


def test_the_address_stays_out_of_the_log_and_out_of_the_status() -> None:
    """aiohttp puts the URL into some of its errors, and the URL is the credential."""
    for error in (ValueError(f"Cannot connect to {URL}"),
                  ValueError(f"bad path /{SECRET}")):
        heartbeat, stats, logger = build()

        beat(heartbeat, FakeSession(error=error))

        text = " ".join(logger.lines) + stats.heartbeat.last_failure
        assert stats.heartbeat.failed == 1
        assert SECRET not in text
        assert SECRET not in str(serialize_stats(stats))


def test_only_a_change_of_state_is_logged() -> None:
    """A beat every minute must not become a line every minute."""
    states = [
        {"status": "degraded", "reasons": ["not_connected"]},
        {"status": "degraded", "reasons": ["not_connected"]},
        {"status": "degraded", "reasons": ["export_failed"]},
        HEALTHY,
        HEALTHY,
        {"status": "failing", "reasons": ["tick_handler_failing"]},
    ]
    feed = iter(states)
    heartbeat, _, logger = build(lambda: next(feed))
    session = FakeSession()

    for _ in states:
        beat(heartbeat, session)

    assert len(logger.lines) == 4, logger.lines
    assert "not_connected" in logger.lines[0]
    assert "export_failed" in logger.lines[1]
    assert "reaching" in logger.lines[2]
    assert "tick_handler_failing" in logger.lines[3]


def test_a_beat_really_reaches_a_socket_and_a_404_really_counts() -> None:
    """The fake above assumes how aiohttp is called; this one does not."""
    async def scenario() -> tuple:
        pings: List[str] = []

        async def ping(request: web.Request) -> web.Response:
            pings.append(request.path)
            return web.Response(text="OK")

        app = web.Application()
        app.router.add_get(f"/{SECRET}", ping)
        server = TestServer(app)
        await server.start_server()

        stats = CollectorStats()
        good = Heartbeat(str(server.make_url(f"/{SECRET}")), 60,
                         lambda: HEALTHY, stats)
        wrong = Heartbeat(str(server.make_url("/deleted-check")), 60,
                          lambda: HEALTHY, stats)
        good._logger = wrong._logger = Recorder()

        async with ClientSession() as session:
            await good.beat(session)
            sent_by_good = stats.heartbeat.sent
            await wrong.beat(session)

        await server.close()
        return pings, sent_by_good, stats

    pings, sent_by_good, stats = asyncio.run(
        asyncio.wait_for(scenario(), timeout=30))

    assert pings == [f"/{SECRET}"]
    assert sent_by_good == 1
    assert stats.heartbeat.sent == 1
    assert stats.heartbeat.failed == 1
    assert "404" in stats.heartbeat.last_failure


def test_it_waits_one_interval_before_the_first_beat_and_stops_on_cancel(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    At startup the feed is not connected yet, and a heartbeat withheld for that
    would be a warning on every start. Cancelling is how shutdown ends it.
    """
    events: List[Any] = []

    async def driven_sleep(seconds: float) -> None:
        events.append(seconds)
        if events.count(seconds) == 3:
            raise asyncio.CancelledError()

    async def counted_beat(session: Any) -> None:
        events.append("beat")

    monkeypatch.setattr(heartbeat_module, "asyncio",
                        types.SimpleNamespace(sleep=driven_sleep))
    heartbeat, _, _ = build(interval=45)
    monkeypatch.setattr(heartbeat, "beat", counted_beat)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(heartbeat.run())

    assert events == [45, "beat", 45, "beat", 45]


def test_the_default_configuration_has_no_address() -> None:
    assert MonitoringConfig().heartbeat_url == ""


def test_stats_say_it_is_on_and_at_what_interval() -> None:
    _, stats, _ = build(interval=120)

    assert stats.heartbeat.enabled is True
    assert stats.heartbeat.interval_seconds == 120
    assert CollectorStats().heartbeat.enabled is False
