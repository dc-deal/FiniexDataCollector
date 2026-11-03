"""
FiniexDataCollector - Tests for the stack sampler that watches the event loop

Every attribution arm before this one answers for one candidate: garbage
collection, the display, the tick handler, a named thread. Twenty stalls across
two production nights reported `unknown` with all of them reading zero, because
whatever was blocking the loop was not on anybody's list. This sampler has no
list - it reads the loop's own stack while the loop is stuck.

Which makes it the one instrument here that can lie in a way nobody would
notice: a sampler that never captures returns an empty string, and an empty
string reads exactly like "the loop was fine". So these tests drive a real
blocked thread rather than calling the formatter directly, and one of them
exists only to prove the sampler stays quiet when nothing is wrong.

Location: tests/utils/test_stall_sampler.py
"""

import threading
import time

from python.utils.stall_sampler import StallSampler


def a_sampler() -> StallSampler:
    """A sampler that reacts fast enough for a test to wait on it."""
    return StallSampler(capture_after_seconds=0.05,
                        sample_interval_seconds=0.01)


def block_for(seconds: float) -> None:
    """Hold the thread without yielding, under a findable name."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pass


def test_a_loop_that_stops_checking_in_gets_its_stack_read() -> None:
    """
    The whole point: a stall that nothing measured still says where it was.

    The sampler is started from the thread it is meant to watch, that thread
    then blocks without calling `note_alive`, and the capture has to name the
    function that was blocking.
    """
    sampler = a_sampler()
    captured = {}

    def pretends_to_be_the_loop():
        sampler.start()
        sampler.note_alive()
        started = time.monotonic()
        block_for(0.3)
        captured["window"] = started

    thread = threading.Thread(target=pretends_to_be_the_loop)
    thread.start()
    thread.join()
    sampler.stop()

    assert sampler.captures >= 1
    assert "block_for" in sampler.blocked_in(captured["window"])


def test_a_loop_that_keeps_checking_in_is_never_sampled() -> None:
    """
    Silence is the trigger, not elapsed time.

    Without this, a sampler that fired on a timer would attach a stack to every
    stall and to every quiet second alike - and a cause that is always present
    is worth nothing, while looking like the answer.
    """
    sampler = a_sampler()

    def pretends_to_be_the_loop():
        sampler.start()
        for _ in range(30):
            sampler.note_alive()
            time.sleep(0.01)

    thread = threading.Thread(target=pretends_to_be_the_loop)
    thread.start()
    thread.join()
    sampler.stop()

    assert sampler.captures == 0
    assert sampler.blocked_in(0.0) == ""


def test_a_stack_from_an_earlier_stall_is_not_offered_to_a_later_window() -> None:
    """
    A capture belongs to the window it was taken in, and to no other.

    The sampler keeps the last stack it read, so without the timestamp check a
    stall at 07:00 would be handed the stack of a stall at 03:45 and would name
    it as its cause with no sign that it is stale.
    """
    sampler = a_sampler()

    def pretends_to_be_the_loop():
        sampler.start()
        sampler.note_alive()
        block_for(0.2)

    thread = threading.Thread(target=pretends_to_be_the_loop)
    thread.start()
    thread.join()
    sampler.stop()

    assert sampler.blocked_in(0.0) != ""
    a_window_that_opened_later = time.monotonic() + 1.0
    assert sampler.blocked_in(a_window_that_opened_later) == ""


def test_one_stall_is_sampled_once_however_long_it_lasts() -> None:
    """
    Re-reading the same stack every tick would cost more than the stall does.

    A one-second block at a 10 ms sample interval is a hundred opportunities to
    format a stack inside a window that is already too slow.
    """
    sampler = a_sampler()

    def pretends_to_be_the_loop():
        sampler.start()
        sampler.note_alive()
        block_for(0.4)

    thread = threading.Thread(target=pretends_to_be_the_loop)
    thread.start()
    thread.join()
    sampler.stop()

    assert sampler.captures == 1


def test_a_sampler_that_was_never_started_answers_nothing() -> None:
    """
    No thread id means no stack, and it must not mean an exception.

    A diagnostic that raises takes the collector down with it, which is a worse
    outcome than the stall it was built to explain.
    """
    sampler = StallSampler()

    assert sampler.blocked_in(0.0) == ""
    assert sampler._read_loop_stack() == ""
