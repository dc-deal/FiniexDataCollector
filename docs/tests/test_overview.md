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

### `tests/utils/test_collection_clock.py`

The clock exists for one rare event: the OS stepping backwards. Every case drives the time
source by hand.

Defends: a well-behaved clock passes through untouched; a backwards step is clamped to the
last value; the series stays non-decreasing across the whole correction window; every clamped
stamp is counted; the largest correction survives a later smaller one; the values are epoch
milliseconds, which is what rules out `time.monotonic()` as a substitute.

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
connection coming back (it reported zero for 173 of them); the staleness detection window
stays under a minute; file logging is not DEBUG by default.

## What is not covered

The **live display** has no tests beyond a render pass with populated values. Its formatters
(`_format_quote_age`, `_format_last_tick`, `_digits_for`) are pure functions and would be cheap
to pin.

The **Windows paths** cannot be exercised here: the dev container is Linux, the production
machine is Windows Server. The signal handling, the console encoding and the start script are
verified only on the machine that runs them — the same gap the RAG engine found two real
defects in on its first production run.
