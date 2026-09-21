"""
FiniexDataCollector - Tests for rebuilding statistics out of a status payload

The viewer draws a machine it cannot see. Everything on its screen came through
`/v1/status` and back out of `stats_from_payload`, so a defect here is invisible
in the only way that matters: the numbers still render, and they are wrong.

Three failures are guarded, and all three have a cost attached rather than a
principle. A field added to the statistics and not carried across is a screen
that keeps drawing while quietly describing an older collector. A payload from a
DIFFERENT build - newer or older than the viewer - must degrade one specific way
each: a key this build does not know is dropped, a field it needs and does not
get is named out loud. And a timestamp that arrives without an offset must be
read as UTC, because the alternative is a screen reporting the collector's
uptime in the viewer's timezone.

Location: tests/viewer/test_stats_from_payload.py
"""

import json
from datetime import datetime, timezone

import pytest

from python.api.stats_serializer import serialize_stats
from python.types.collector_stats import CollectorStats, ReconnectEvent
from python.types.log_level import WARNING
from python.viewer.stats_from_payload import (CONTENT_TYPES, PayloadMismatch,
                                              stats_from_payload,
                                              untyped_fields)

# Values that are not measurements and therefore never leave the collector.
NOT_SENT = ("max_recent_logs", "max_reconnect_history")


def populated_stats() -> CollectorStats:
    """A statistics object with something in every container."""
    stats = CollectorStats()
    stats.record_tick("ADAUSD", 0.2103, 0.2104, 0.0475, 12.5, quote_age_ms=7)
    stats.symbols["ADAUSD"].digits = 4
    stats.streams = ["trade", "ticker"]
    stats.max_ticks_per_file = 50000
    stats.record_file_created("ADAUSD", "ada_ticks.json", 4711)
    stats.record_logged(WARNING, "collector", "a warning")
    stats.record_loop_lag(12.5)
    stats.record_export_started()
    stats.record_export_finished("ada_ticks.json", 4711, 900.0)
    stats.record_counter_check("ADAUSD", 99, 98)
    stats.record_gc_pause(2, 1.5)
    stats.record_render(7.0)
    stats.record_stall(300.0, 1.0, 0, 250.0, 0)
    stats.record_clock(3, 17)
    stats.record_folder_scan(5.0)
    stats.record_disk_check(2.0)

    event = ReconnectEvent(timestamp=datetime.now(timezone.utc),
                           reconnected_at=None, duration_seconds=4.0,
                           reason="socket closed")
    stats.reconnect_events.append(event)
    stats.last_reconnect = event
    return stats


def over_the_wire(stats: CollectorStats) -> dict:
    """The payload as a viewer receives it, JSON round trip included."""
    return json.loads(json.dumps(serialize_stats(stats)))


def test_everything_the_collector_sends_survives_the_journey() -> None:
    """
    The whole object, not a sample of it.

    Serializing the rebuilt statistics and comparing against the payload is what
    makes this test outlive the fields it was written for: a member added to the
    statistics is covered the day it is added, without anyone remembering to
    extend an assertion list here.
    """
    payload = over_the_wire(populated_stats())

    again = serialize_stats(stats_from_payload(payload))

    # Two readings taken in this process rather than carried in the payload.
    drifted = {name: (payload[name], again.get(name))
               for name in payload
               if name not in ("process", "uptime_seconds")
               and payload[name] != again.get(name)}

    assert not drifted, f"these did not survive the round trip: {drifted}"


def test_every_field_that_cannot_state_its_own_type_is_registered() -> None:
    """
    The one thing a new field can break here, caught before a screen does.

    A fresh CollectorStats carries `[]`, `{}` or `None` for these, so the object
    itself cannot say what belongs inside. Everything else is inferred. Adding a
    field of that shape without an entry in CONTENT_TYPES fails this test - which
    is cheaper than a viewer that renders it as raw dictionaries.
    """
    missing = [name for name in untyped_fields() if name not in CONTENT_TYPES]

    assert not missing, (
        f"add these to CONTENT_TYPES in stats_from_payload: {missing}")


def test_a_registered_type_that_nothing_needs_is_gone() -> None:
    """A stale entry is dead configuration, and dead configuration goes."""
    stale = [name for name in CONTENT_TYPES if name not in untyped_fields()]

    assert not stale, f"CONTENT_TYPES still names fields nobody has: {stale}"


def test_a_newer_collector_does_not_break_an_older_viewer() -> None:
    """
    An unknown key is dropped rather than raised on.

    The collector is deployed to the box and the viewer sits on a laptop, so they
    are routinely different builds. In that direction there is nothing to do: a
    measurement this viewer cannot draw is a measurement it does not draw.
    """
    payload = over_the_wire(populated_stats())
    payload["ticks_per_furlong"] = 42

    stats = stats_from_payload(payload)

    assert stats.total_files == 1
    assert not hasattr(stats, "ticks_per_furlong")


def test_an_older_collector_is_named_rather_than_guessed_at() -> None:
    """
    The other direction, which cannot be absorbed.

    A required field the payload does not carry means that instance predates this
    viewer. Inventing a value would put a number on screen nobody measured, so it
    raises - and the message names the class, because "the viewer is broken" and
    "that collector is older than this viewer" send the operator to different
    machines.
    """
    payload = over_the_wire(populated_stats())
    del payload["last_file"]["tick_count"]

    with pytest.raises(PayloadMismatch) as raised:
        stats_from_payload(payload)

    assert "FileInfo" in str(raised.value)


def test_an_older_collector_that_omits_a_whole_member_still_draws() -> None:
    """
    A member the payload does not carry at all keeps its default.

    This is the case production is in right now: `digits`, `streams` and
    `max_ticks_per_file` exist in this build and not in the one on the box. The
    screen has to come up anyway, showing what that collector does report.
    """
    payload = over_the_wire(populated_stats())
    del payload["streams"]
    del payload["max_ticks_per_file"]

    stats = stats_from_payload(payload)

    assert stats.streams == []
    assert stats.max_ticks_per_file == 0


def test_a_timestamp_without_an_offset_is_utc_and_not_local_time() -> None:
    """
    The failure this project has already paid for once, on a screen.

    A naive string read as local time would move every duration on the viewer by
    the operator's UTC offset - two hours in summer here, and silently.
    """
    payload = over_the_wire(populated_stats())
    payload["start_time"] = "2026-09-21T10:03:26"

    stats = stats_from_payload(payload)

    assert stats.start_time == datetime(2026, 9, 21, 10, 3, 26,
                                        tzinfo=timezone.utc)


def test_something_that_is_not_a_status_payload_says_so() -> None:
    """A proxy answering HTML must not turn into a half-drawn screen."""
    with pytest.raises(PayloadMismatch):
        stats_from_payload(["not", "a", "collector"])
