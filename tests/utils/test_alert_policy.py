"""
FiniexDataCollector - Tests for when a restored connection is worth a phone alert

Measured over the night of 2026-09-21: seven reconnects, each between 1.3 and
7.2 s, adding up to 19.9 s of downtime in fifteen hours. That is roughly 22
ticks, and not one of the seven appeared among the eight longest gaps in the
file it fell into - those were all quiet market. Six alerts reached the
operator's phone for something that cannot be found in the data afterwards.

An alert nobody can act on is not free. It trains its reader to swipe, and the
next one that mattered is swiped with it. So the question this file defends is
not "did something happen" but "would the reader do anything differently" - the
same test the output contract applies to a field.

Two things still reach the phone: an outage long enough to put a file at risk,
and a cluster, because a host that blips every two hours is weather and one that
blips four times an hour is degrading.

Location: tests/utils/test_alert_policy.py
"""

from python.main import reconnect_alert_text

MIN_SECONDS = 30.0
CLUSTER = 4


def test_a_short_self_healed_reconnect_stays_off_the_phone() -> None:
    """
    The exact case that produced six alerts in one night.

    It is still logged and still on `/v1/status`, where it is complete. What it
    stops being is an interruption.
    """
    assert reconnect_alert_text(2.19, 1, MIN_SECONDS, CLUSTER) is None
    assert reconnect_alert_text(7.22, 2, MIN_SECONDS, CLUSTER) is None


def test_an_outage_that_puts_a_file_at_risk_does_reach_it() -> None:
    """
    Thirty seconds is not a round number - it is the consumer's lag window.

    Past it the importer refuses the whole file rather than shortening it, so
    that is the point where an outage stops being cosmetic and starts costing
    an archive.
    """
    message = reconnect_alert_text(45.0, 1, MIN_SECONDS, CLUSTER)

    assert message is not None
    assert "45.0s" in message, "the reader needs the length, not a category"


def test_the_threshold_itself_counts_as_over_it() -> None:
    """A boundary that excludes its own value makes the config lie by one."""
    assert reconnect_alert_text(30.0, 1, MIN_SECONDS, CLUSTER) is not None


def test_a_cluster_of_short_ones_is_worth_saying() -> None:
    """
    The one thing a short reconnect can still tell somebody.

    Individually they are weather on this host - FiniexRAGEngine measured it
    losing DNS and outbound TCP several times a day, from five sides. Four in an
    hour is a different statement about the same machine.
    """
    message = reconnect_alert_text(2.19, 4, MIN_SECONDS, CLUSTER)

    assert message is not None
    assert "4 reconnects" in message
    assert "2.2s" in message, "the cluster message still carries the last length"


def test_one_short_of_a_cluster_stays_quiet() -> None:
    """Otherwise the threshold is decoration."""
    assert reconnect_alert_text(2.19, 3, MIN_SECONDS, CLUSTER) is None


def test_the_duration_is_never_rendered_as_whole_minutes() -> None:
    """
    Every alert that night read "after 0m downtime", including a 7.22 s outage
    that was three times the others.

    Whole minutes turned every real measurement into the same zero - a field
    that asserts less than the code knows, which is the defect this project
    exists to avoid, printed on a phone.
    """
    message = reconnect_alert_text(75.4, 1, MIN_SECONDS, CLUSTER)

    assert "75.4s" in message
    assert "0m" not in message and "1m" not in message
