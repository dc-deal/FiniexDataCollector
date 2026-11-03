"""
FiniexDataCollector - Tests for describe_exception

Several exceptions this collector meets turn into an empty string. Written into
a log line directly they leave "Command polling error: " and nothing after the
colon - measured on production 2026-09-19, during the midnight close. These
tests pin the helper, and keep every place that writes a caught exception into
text on it, because one site left on the old form is exactly where the next
empty line comes from.

Location: tests/utils/test_describe_exception.py
"""

import asyncio
import re
from pathlib import Path

from python.utils.logging_setup import describe_exception

# A caught exception interpolated bare into an f-string: `{e}`.
BARE_EXCEPTION = re.compile(r"\{e\}")


def test_an_exception_without_text_is_still_named() -> None:
    """The case that produced the empty production line."""
    assert describe_exception(asyncio.TimeoutError()) == "TimeoutError"
    assert describe_exception(ConnectionResetError()) == "ConnectionResetError"


def test_an_exception_with_text_keeps_its_text_behind_its_type() -> None:
    """The type is added, the message is not lost."""
    error = OSError(64, "The specified network name is no longer available")

    assert describe_exception(error) == (
        "OSError: [Errno 64] The specified network name is no longer available")


def test_no_caught_exception_is_written_into_text_bare() -> None:
    """
    Every `{e}` in the source goes through describe_exception.

    A convention held by one helper only holds while nobody adds a site that
    skips it, and the site that skips it is invisible until the exception it
    catches happens to have no text.
    """
    offenders = []

    for source in sorted(Path("python").rglob("*.py")):
        for number, line in enumerate(
                source.read_text(encoding="utf-8").splitlines(), start=1):
            if BARE_EXCEPTION.search(line):
                offenders.append(f"{source}:{number}: {line.strip()}")

    assert not offenders, (
        "caught exceptions written into text without describe_exception:\n"
        + "\n".join(offenders))
