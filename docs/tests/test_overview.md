# Test overview

What each suite defends and why it exists. Every entry here was written after something went
wrong, which is the most useful thing a test map can record.

**Not in this document:** how to run them — `python -m pytest` from the project root, and
`pytest.ini` puts the root on `sys.path` so the invocation does not matter.

## Principles

**Fixtures are synthetic.** No test depends on a live WebSocket or on collected data.
`tests/conftest.py` builds tick series and loads a broker config through the real loading path.

**Time is driven, not waited for.** The `steerable_clock` fixture patches the clock's time
source, so a backwards NTP step is provoked deterministically. Waiting for a real one is not a
test strategy.

**Mutation-check every guard.** After writing a test for a defence, break the defence and
confirm the test fails. This has caught four tests that were green against a broken
implementation, including two written the same hour.

**Test the contract, not the implementation.** The invariant tests assert what the consuming
importer enforces, so they keep meaning after a refactor.

## The suites

### `tests/collectors/test_message_parser.py`

Every local timestamp enters the data here, and a trade meets the quote it executed against
here. Real WebSocket payloads with the clock driven by hand.

Defends: ticker updates produce no ticks of their own (interleaving the two time bases steps
`time_msc` backwards on nearly every channel change); a trade carries the quote it executed
against; `quote_age_ms` is `null` and never `0` when nothing was observed; a crossed quote
never reaches a tick; the taker side survives into the archive; a clock correction never
reverses arrival times; a one-tick spread is never reported as zero.

That last one came from live data, not review: `int(spread_raw / tick_size)` truncated
`0.00999999999999801` to 0, and 59 % of LTCUSD ticks claimed no spread.

### `tests/collectors/test_stale_detection.py`

A dropped connection costs the trades made while the dead socket is still believed alive, and on
2026-09-18 that was 537 trades in four drops, 30–40 s of each spent noticing. Silence is now
judged every second, because Kraken sends a heartbeat every second.

Defends: a silent feed is reopened on the first check past the threshold; a feed that sends its
heartbeat is left alone; and — the half that matters more — a blocked event loop is not mistaken
for a dead feed. The midnight cut blocks the loop for 17.85 s while the socket holds unread
messages, so a short threshold without that exemption would force a reconnect at every day cut.
A feed that really died during a block is still reopened on the next check. The time source and
the sleep are driven by hand; waiting for a real outage is not a test strategy.

### `tests/writers/test_json_tick_writer.py`

Two groups: what the file declares about itself, and what it must satisfy to be imported at
all. The second mirrors the six invariants of FiniexTestingIDE's `TickImportValidator` — a
guard, not the authority.

Also: the anchor counters differ between header and summary exactly when a file absorbed a
clock correction, and every symbol of a session declares the same cumulative count.

### `tests/writers/test_write_ahead_log.py`

The invariant: at every instant the ticks are in the archive file, in the write-ahead log, or
in both — never in neither.

Covers each flow by name: normal rotation, regular shutdown, crash, the window between writing
the archive file and deleting the log, a torn final line, a log with no ticks, an unreadable
header. And one test that makes `os.replace` throw, because on the happy path both orderings
of write-then-delete end identically.

Also: a shutdown that arrives just after a rotation names no file. `finalize()` used to hand back
the path of the file it had only opened, and the shutdown log then reported a file nobody wrote —
measured on the production box, thirteen names against twelve files. That log is where someone
looks to decide whether a stop was clean.

### `tests/writers/test_daily_close.py`

A file bounded only by tick count spans however many days it needs — DASHUSD needs 24 to reach
50,000 at its production rate. "Delete files older than N days" then has no well-defined
subject, and nobody can say whether more ticks will still arrive for a date already read.

Defends: a file covers exactly one UTC day; the first tick of a new day starts the new file
(the cut is checked *before* the append, unlike the count); the count threshold still applies
inside a day; a day with no ticks produces no file rather than an empty one; no write-ahead log
survives a day roll.

### `tests/utils/test_collection_clock.py`

The clock exists for one rare event: the OS stepping backwards. Every case drives the time
source by hand.

Defends: a well-behaved clock passes through untouched; a backwards step is clamped to the
last value; the series stays non-decreasing across the whole correction window; every clamped
stamp is counted; the largest correction survives a later smaller one; the values are epoch
milliseconds, which is what rules out `time.monotonic()` as a substitute.

### `tests/utils/test_instance_lock.py`

What the lock prevents is not duplicate work but silent data loss: a second collector's startup
recovery cannot tell a crashed run's write-ahead log from a running instance's, and on Linux it
removes the live one successfully.

Defends: a live holder blocks the start; a dead one is taken over; a reused PID does not block
forever (the lock stores the holder's process start time, and the tolerance is 10 ms — at 1 s,
Windows system processes reporting a start time of zero matched anything); a damaged lock is
treated as stale; release is idempotent.

### `tests/utils/test_instance_identity.py`

A development file reached the consuming project's importer and was caught by luck. The repair
is an identity minted at the data root, not configured — a configuration copied from the server
to a laptop still says whatever the server said.

Defends: the identity survives a restart (the expensive failure is not a missing identity but a
changing one — an archive of one-file identities looks like provenance and is not); it lives in
the data root and not in the checkout; two data roots get two identities; a damaged file is a
refusal and never a re-mint; the refusal names the path and the way out; and a mint racing
another mint loses to it.

That last test exists because the mutation check found the gap: with `open(..., "w")` in place
of `"x"` every other test still passed. It defeats the existence check on purpose, since that
check is what normally hides the race.

### `tests/utils/test_logging_rotation.py`

The log file is named after the day it covers, and a collector runs for weeks. It did not
rotate: the handle was opened once and never reconsidered, and the production server produced
294 MB across three days under a name claiming one.

One test guards the opposite error — reopening per line would cure the symptom and introduce a
different defect.

### `tests/utils/test_app_config.py`

The version exists three times: in `configs/app_config.json`, as the `AppConfig` default, and
in the README status line. Two are machine-readable and guarded here; the README is checked by
a regex because nothing else keeps it in step.

Also: the tracked config carries no live credentials; the reconnect rule recognises a
connection coming back (it reported zero for 173 of them); the dead-connection threshold stays
short; file logging is not DEBUG by default.

### `tests/utils/test_console_and_counting.py`

Two defences against a display saying something untrue, and one of them can stop the collection.

A Windows console in QuickEdit mode suspends the next write while text is selected, and the live
display writes from the collector's only event loop — so one stray click stops the WebSocket
reader and the writers with it. No tick arrives, which is the one gap the write-ahead log cannot
close: it opens before the safety net, and a gap in a tick series is the same bytes as a quiet
market. FiniexRAGEngine measured 13.5 hours of it on the same host.

Defends: the console call never raises, anywhere, and returns `None` where there is no console;
`ENABLE_EXTENDED_FLAGS` is set alongside the cleared QuickEdit bit, without which the console
ignores the change *and reports success*; and the folder count includes only finished archive
files, not the open write-ahead logs it used to count as "files".

The failing-console test exists because the mutation check found the hole: every other test
returns before the Windows branch, so the `except` that keeps a console problem away from the
collection was carrying no test at all.

### `tests/writers/test_archive_export.py`

Writing an archive file ran on the collector's only event loop: about 1 s per file plus 66 µs per
tick, measured on production, and nine files at the UTC day cut meant 21 s in which nothing else
ran. A closed file is handed to a subprocess instead — the ticks are already on disk in the
write-ahead log, and the closing state goes in as its last line.

Defends: the handed-over file is **byte for byte** the file the inline path would have written
(with the clock frozen, because the metadata carries the moment a file was opened); the log
outlives the window in which the archive is missing, so the ticks are never nowhere; an export
that never happens is recovered as a *complete* file rather than a shortened one, because the
closing record travelled with it; a log from a real crash still says it was recovered; an existing
archive file is never overwritten; and the module actually runs as a program, started the way
main.py starts it — the module path, the working directory and the exit code are what production
depends on and none of them can fail in-process.

One test exists because a mutation slipped through: the inline and the handed-over path share one
builder now, so comparing them cannot see a change in the file format at all. That one holds the
output against `json.dump`, the encoder every existing file was written with, with a non-ASCII
value in it and the top-level key order named rather than derived.

### `tests/utils/test_tick_counters.py`

The tick handler in `main.py` had no test at all, which is why a counting error ran for weeks:
every UTC day cut reported the closed file one tick too large and started the next one one tick
too small, and the error was carried until the next cut cancelled it. Measured on production —
"File rotated: … (47,369 ticks)" for a file holding 47,368, and "(49,999)" for one holding 50,000.

Defends: the day cut reports what the file holds and carries the triggering tick into the new
file, because the cut is checked *before* the tick is appended; a file that fills up reports the
tick that filled it, because that threshold fires *after*; the two counters are compared while
the collector runs, with the writer winning; and — the part a direct call would not have caught —
the comparison is actually reached from the folder monitor, since a check nobody calls observes
nothing.

### `tests/utils/test_diagnostics.py`

The two numbers a remote session reads when it cannot open a shell: how late the event loop has
been running, and whether the files that were handed to an archive writer arrived.

Defends: the worst stall is kept with the moment it happened; a wake-up that came early counts as
no lag rather than as negative; an export in flight is what was handed over and not yet reported;
a failure names the file that is still owed; and the gauge cannot go below zero.

Also, and this is the half that needed a thread to test: the folder scan and the disk reading run
**off the event loop**. Both tests check which thread the work actually ran in, because the defect
they guard against is invisible in behaviour — the numbers come out the same either way, and the
only trace is arrival lag in the tick data.

### `tests/utils/test_error_counters.py`

The error and warning counters were initialised and never raised — nothing called
`record_error` or `record_warning`. The display said "No errors or warnings", `/v1/status` and the
weekly report said 0 and 0, while the production log carried forced reconnects. They now follow
the log through a listener.

Defends: WARNING counts as a warning and ERROR/CRITICAL as errors while INFO counts as nothing; a
failing listener never costs the log line itself; the collector actually wires its stats to the
log — the shape the original defect had was a working counter nothing was connected to; and the
display renders a logged message as text, because that code had never run with an entry in it and
Rich raises on a message holding a closing-tag shape like `[/red]`.

### `tests/utils/test_describe_exception.py`

`TimeoutError`, `ConnectionResetError` and aiohttp's `ClientPayloadError` turn into an empty
string, so a caught exception interpolated bare leaves a log line naming no cause. Production
logged "Command polling error: " and nothing more during the 2026-09-19 midnight close.

Defends: the helper names an exception without text and keeps the text of one that has it; and no
source file writes a caught exception into text without it — a convention held by one helper only
holds while nobody adds a site that skips it.

### `tests/utils/test_doc_links.py`

A link is a claim that a file is there under that name, and on Windows and macOS the filesystem
answers to any spelling. So a wrong one survives every local check and breaks in the two places
that are case-sensitive and that nobody watches: GitHub's link resolution, and CI on a real Linux
filesystem.

It compares against the directory listing rather than calling `Path.exists()` — and deliberately
avoids `Path.resolve()`, which on Windows silently returns the real on-disk spelling and thereby
repairs the exact mistake the test exists to find. It did that on the first run, which is how the
limitation was discovered.

The defect behind it was not a spelling at all: git tracked `readme.md` while the disk said
`README.md`, because `core.ignorecase` lets the index and the working tree disagree without ever
saying so. CI checks out from the index, so CI was the first thing to notice.

### `tests/utils/test_devcontainer.py`

Two lists of editor extensions and a set of mounts maintained by hand across three files, with
nothing at runtime that reads them — so nothing reports the drift. One mount is why a session
transcript survives a rebuild; its absence once cost a sister project its entire history.

### `tests/api/test_status_api.py`

The surface exists so "is it running, and which version" stops being an inference. On
2026-09-15 that inference was actually made — from process uptime against commit timestamps —
and it was right by luck rather than by evidence.

Defends: the open routes publish nothing an uptime probe has no business receiving; the gated
route refuses an anonymous caller, an unknown token, and a valid token without that surface; a
grant naming a surface that does not exist fails at parse time rather than silently granting
nothing; and the producing identity is served on the gated route, in the shape a tick file
carries it, while staying off both open routes.

Also: an accept failure — after which CPython's proactor loop closes the listening socket on
Windows — reaches the log file instead of stderr, through the handler a real loop calls, while
every other loop event keeps asyncio's default handling. And the route carries `loop_lag` and
`exports`, the two figures that make a blocked loop visible from off the machine.

### `tests/api/test_diagnostic_routes.py`

Config, archive inventory, log excerpt, and the file handover. The archive route has a named
origin: the consuming project asked which tick files spanned a host migration, with a deadline,
and the answer could not be given because nobody holding the question had access to the
archive.

Defends: **no route runs on the event loop** — a route defined with `def` is served from a
threadpool, `async def` is not, and one keyword would put a 22 MB file transfer, an archive walk
or a day of log onto the loop that stamps every tick; no credential leaves through the config
route, including a key that did not exist when the redaction was written; only finished files can
be fetched, never an open write-ahead log;
a path escaping the archive is refused even when its name passes the pattern; the served bytes
match the register's checksum; no route is authenticated but ungated; and compression changes
the size without changing what arrives.

## What is not covered

The **live display** has no tests beyond a render pass with populated values. Its formatters
(`_format_quote_age`, `_format_last_tick`, `_digits_for`) are pure functions and would be cheap
to pin.

The **Windows paths** cannot be exercised here: the dev container is Linux, the production
machine is Windows Server. The signal handling, the console encoding and the start script are
verified only on the machine that runs them — the same gap the RAG engine found two real
defects in on its first production run.
