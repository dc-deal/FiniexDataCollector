"""
FiniexDataCollector - Tests for handing a finished file to an archive writer

Writing the archive file used to happen on the collector's only event loop.
Measured on production 2026-09-20: about 1 s per file plus 66 us per tick, nine
files in a row at the UTC day cut, 21 s in which nothing else ran - no socket
read, no other symbol written, no API answered. Ticks that arrived meanwhile
were stamped that late, and the consuming importer rejects a whole file beyond
30 s of lag.

So a closed file is handed over instead: the ticks are already on disk in the
write-ahead log, the closing state goes in as its last line, and a subprocess
turns the log into the archive file and removes it afterwards. The ordering the
whole mechanism rests on is unchanged - the log outlives the window in which the
archive does not exist yet - it simply happens in another process.

What these tests defend: the handed-over file is byte for byte the file the
inline path would have written, the log is never gone early, and an export that
never happens costs nothing because recovery finishes the job.

Location: tests/writers/test_archive_export.py
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import List, Tuple

import pytest

from python.types.tick_types import TickData
from python.utils.collection_clock import CollectionClock
from python.writers.json_tick_writer import (
    JsonTickWriter,
    recover_orphaned_buffers
)
from python.writers.wal_archive import (
    CLOSE_RECORD_KEY,
    RECOVERY_NOTE,
    archive_from_wal,
    read_wal
)

SYMBOL = "BTCUSD"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FrozenDatetime(datetime):
    """
    A clock that does not move between two writers.

    The metadata carries the moment a file was opened and the summary the moment
    it was closed, so two writers started a millisecond apart produce different
    bytes for the same ticks - and the comparison that matters here is byte for
    byte.
    """

    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 9, 20, 12, 0, 0, tzinfo=tz or timezone.utc)


def build_writer(output_dir: Path, exporter=None) -> JsonTickWriter:
    """
    Build a writer the way main.py does.

    Args:
        output_dir: Base output directory
        exporter: Handed the closed log and the archive file it owes

    Returns:
        Configured JsonTickWriter
    """
    return JsonTickWriter(
        output_dir=output_dir,
        symbol=SYMBOL,
        clock=CollectionClock(),
        broker="Kraken",
        server="kraken_websocket",
        broker_type="kraken_spot",
        max_ticks_per_file=50000,
        data_collector="kraken",
        exporter=exporter
    )


def collect(writer: JsonTickWriter, ticks: List[TickData]) -> None:
    """Feed a series through the writer."""
    for tick in ticks:
        writer.write_tick(tick)


def archives(output_dir: Path) -> List[Path]:
    """Finished archive files in an output directory."""
    return sorted((output_dir / "kraken").glob("*_ticks.json"))


def test_the_handed_over_file_is_the_file_the_writer_would_have_written(
    tmp_path: Path,
    tick_series: List[TickData],
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Byte for byte, or the change is a format change nobody announced.

    The consuming importer reads these files; an export that reordered a key or
    shifted an indent would be a new format wearing the old version number.
    """
    monkeypatch.setattr(
        "python.writers.json_tick_writer.datetime", FrozenDatetime)

    inline_dir = tmp_path / "inline"
    handed_dir = tmp_path / "handed"

    inline = build_writer(inline_dir)
    collect(inline, tick_series)
    inline.finalize()

    handed_over: List[Tuple[Path, Path]] = []
    handed = build_writer(handed_dir, exporter=lambda wal, archive:
                          handed_over.append((wal, archive)))
    collect(handed, tick_series)
    handed.rotate_file()

    # What the subprocess does, in this process so the test stays deterministic.
    archive_from_wal(handed_over[0][0])

    assert len(archives(inline_dir)) == 1
    assert len(archives(handed_dir)) == 1
    assert archives(handed_dir)[0].read_bytes() == \
        archives(inline_dir)[0].read_bytes()


def test_the_file_is_encoded_the_way_every_existing_file_was(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    `json.dumps` has to write exactly what `json.dump` wrote.

    Every file this project has ever produced went through `json.dump`, which
    escapes non-ASCII and keeps insertion order at two-space indent. The switch
    to `json.dumps` was made for speed - it is 4.7x faster because `json.dump`
    always takes CPython's pure-Python encoder - and the comparison between the
    inline and the handed-over path cannot see a change here, because both now
    share one builder. This is the test that can: it holds the output against
    the encoder the importer has been reading all along, with a non-ASCII value
    in it on purpose.
    """
    writer = JsonTickWriter(
        output_dir=tmp_path,
        symbol=SYMBOL,
        clock=CollectionClock(),
        broker="Kraken Börse",
        server="kraken_websocket",
        broker_type="kraken_spot",
        data_collector="kraken")
    collect(writer, tick_series)
    writer.finalize()

    written = archives(tmp_path)[0].read_text(encoding="utf-8")
    expected = StringIO()
    json.dump(json.loads(written), expected, indent=2)

    assert written == expected.getvalue()
    assert "B\\u00f6rse" in written, "non-ASCII must stay escaped, as before"
    # Named rather than derived: re-encoding a parsed document repeats whatever
    # order the file had, so a builder that sorted its keys would agree with
    # itself. The importer reads by name, but a consumer diffing two files or a
    # human reading one does not.
    assert list(json.loads(written).keys()) == [
        "metadata", "ticks", "errors", "summary"]


def test_the_log_outlives_the_window_in_which_the_archive_is_missing(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    The handover must not shorten the window the write-ahead log exists for.

    Between the rotation and the exporter's work there is a moment with no
    archive file. If the log were dropped at rotation, that moment would hold
    the ticks nowhere.
    """
    handed_over: List[Tuple[Path, Path]] = []
    writer = build_writer(tmp_path, exporter=lambda wal, archive:
                          handed_over.append((wal, archive)))
    collect(writer, tick_series)
    writer.rotate_file()

    wal_path, archive_path = handed_over[0]

    assert wal_path.exists(), "the ticks would be nowhere"
    assert not archive_path.exists(), "the exporter has not run yet"
    assert wal_path.with_suffix("").with_suffix(".json") == archive_path

    last_line = json.loads(wal_path.read_text(
        encoding="utf-8").splitlines()[-1])

    assert CLOSE_RECORD_KEY in last_line, "the closing state was not handed on"
    assert last_line[CLOSE_RECORD_KEY]["summary"]["total_ticks"] == \
        len(tick_series)


def test_the_exporter_is_given_paths_that_work_from_anywhere(
    tmp_path: Path,
    tick_series: List[TickData],
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The exporter does not share this process's working directory.

    The subprocess runs from the repository root so it can import the package,
    while `raw_data_dir` is relative in the shipped configuration. On the first
    live run every export failed with FileNotFoundError on a path that was
    perfectly correct where it was built.
    """
    monkeypatch.chdir(tmp_path)
    handed_over: List[Tuple[Path, Path]] = []
    writer = build_writer(Path("data") / "raw", exporter=lambda wal, archive:
                          handed_over.append((wal, archive)))
    collect(writer, tick_series)
    writer.rotate_file()

    wal_path, archive_path = handed_over[0]

    assert wal_path.is_absolute() and archive_path.is_absolute()
    assert wal_path.exists(), "the path must resolve to the log that exists"


def test_a_handover_that_throws_does_not_reach_the_tick_that_triggered_it(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    The handover happens inside `write_tick`, on the tick path.

    Whatever goes wrong starting a subprocess - a missing interpreter, a
    refused handle - must cost this file's export, which the next start repairs
    from the log, and not the collection.
    """
    def refuse(wal: Path, archive: Path) -> None:
        raise OSError("no process for you")

    writer = build_writer(tmp_path, exporter=refuse)
    collect(writer, tick_series)

    writer.rotate_file()          # must not raise
    writer.write_tick(tick_series[0])

    logs = sorted((tmp_path / "kraken").glob("*_ticks.jsonl.part"))

    assert len(logs) == 2, "the closed log is still there, and a new one is open"


def test_an_export_that_never_happens_is_recovered_as_a_complete_file(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    The failure mode of the whole design: the child dies, or never starts.

    Nothing is lost, and the recovered file is not a shortened one - it carries
    the summary the writer computed at close, because that went into the log.
    """
    writer = build_writer(tmp_path, exporter=lambda wal, archive: None)
    collect(writer, tick_series)
    writer.rotate_file()
    writer._close_wal(delete=False)

    recovered = recover_orphaned_buffers(tmp_path, "kraken")

    assert len(recovered) == 1
    document = json.loads(recovered[0].read_text(encoding="utf-8"))

    assert len(document["ticks"]) == len(tick_series)
    assert document["summary"]["recommendations"] != RECOVERY_NOTE, (
        "a file closed by its writer must not describe itself as recovered")


def test_a_log_from_a_crash_still_reads_as_recovered(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    The other half: without a closing record the file says what it is.

    A crash between two ticks leaves a log nobody closed. That file is shorter
    than a rotation would have made it, and it says so.
    """
    writer = build_writer(tmp_path)
    collect(writer, tick_series)
    writer._close_wal(delete=False)
    writer._current_file = None

    recovered = recover_orphaned_buffers(tmp_path, "kraken")

    assert len(recovered) == 1
    document = json.loads(recovered[0].read_text(encoding="utf-8"))

    assert document["summary"]["recommendations"] == RECOVERY_NOTE


def test_the_archive_writer_runs_as_a_program_and_removes_the_log(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    The subprocess itself, started the way main.py starts it.

    Everything else here calls the function. This is the only test that proves
    the module can be run at all: the module path, the working directory and the
    exit code are what production depends on, and none of them fail in-process.
    """
    handed_over: List[Tuple[Path, Path]] = []
    writer = build_writer(tmp_path, exporter=lambda wal, archive:
                          handed_over.append((wal, archive)))
    collect(writer, tick_series)
    writer.rotate_file()
    wal_path, archive_path = handed_over[0]

    finished = subprocess.run(
        [sys.executable, "-m", "python.writers.wal_archive", str(wal_path)],
        cwd=str(PROJECT_ROOT), capture_output=True, text=True)

    assert finished.returncode == 0, finished.stderr
    assert archive_path.exists()
    assert not wal_path.exists(), "the log is dropped only after the file"

    reported = json.loads(finished.stdout)

    assert reported["ticks"] == len(tick_series)
    assert reported["written"] is True
    assert reported["closed_by_writer"] is True


def test_an_existing_archive_file_is_never_overwritten(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    A restart can leave a child and a recovery both holding the same log.

    Whoever arrives second finds the file already there. It is the one a
    consumer may already have read, so it stays untouched and only the log goes.
    """
    handed_over: List[Tuple[Path, Path]] = []
    writer = build_writer(tmp_path, exporter=lambda wal, archive:
                          handed_over.append((wal, archive)))
    collect(writer, tick_series)
    writer.rotate_file()
    wal_path, archive_path = handed_over[0]

    archive_path.write_text('{"written":"by somebody else"}', encoding="utf-8")
    result = archive_from_wal(wal_path)

    assert result["written"] is False
    assert not wal_path.exists()
    assert json.loads(archive_path.read_text(encoding="utf-8")) == \
        {"written": "by somebody else"}


def test_the_closing_record_does_not_disturb_a_reader_of_the_log(
    tmp_path: Path,
    tick_series: List[TickData]
) -> None:
    """
    The closing record shares the log with the ticks, so it must not be one.

    `read_wal` sorts the lines by what they hold; a build that does not know the
    key skips it, which is what makes the log readable by an older collector.
    """
    handed_over: List[Tuple[Path, Path]] = []
    writer = build_writer(tmp_path, exporter=lambda wal, archive:
                          handed_over.append((wal, archive)))
    collect(writer, tick_series)
    writer.rotate_file()

    contents = read_wal(handed_over[0][0])

    assert len(contents.ticks) == len(tick_series)
    assert contents.close is not None
    assert contents.torn_lines == 0
