# Durability

What happens to collected ticks when the process does not end politely. Written after a
machine shutdown cost 188,253 ticks across eight symbols.

**Not in this document:** what the resulting file contains (see
[output contract](output_contract.md)).

## The problem it solves

Ticks buffer in memory until a file rotates. Rotation was driven by tick count alone, which
made the exposure unbounded in **time** rather than volume:

| Symbol | ticks/h (production) | time to reach 50,000 |
|---|---|---|
| XRPUSD | 627 | 12 h |
| BTCUSD | 305 | 4 days |
| DASHUSD | 77 | **24 days** |

A thin symbol holds its buffer for most of a month. After 67 hours of uptime the production
collector had 188,253 ticks in RAM and none of them anywhere else.

A file now also closes at the UTC day boundary (see
[output contract](output_contract.md#file-boundaries)), which bounds that to a day — but the
write-ahead log below is what makes the bound irrelevant, and the daily close exists for a
different reason: finality, not durability.

## Two mechanisms, different jobs

**Atomic write** protects the *reader*. The archive file is written to a temp file in the
target directory and `os.replace()`d into place, so a `*_ticks.json` never exists in a partial
state. There is no window in which a consumer can read half a file. Do not "simplify" this
into a direct write to the final path.

**Write-ahead log** protects the *data*. Every tick is appended to
`{SYMBOL}_{TIMESTAMP}_ticks.jsonl.part` and flushed before it counts as collected. Measured
cost: **2.5 µs per tick**, against a production rate of 0.8 ticks per second.

Line 1 of the log is the metadata header. `start_time`, the device clock and the anchor
counters exist only in the writer's memory and cannot be reconstructed afterwards, so they are
written at open rather than at finalize.

## The ordering is the whole mechanism

The log is deleted **after** the archive file is written, never before.

    1. append tick to .jsonl.part          data in the log
    2. ... rotation threshold reached
    3. write .json atomically              data in BOTH places
    4. delete .jsonl.part                  data in the archive file

A window where the data sits in two places is recoverable. A window where it sits in neither
is not. `tests/writers/test_write_ahead_log.py` contains a test that makes `os.replace` throw,
because on the happy path both orderings end identically — which is what would make the wrong
one survive a review.

## Recovery

`recover_orphaned_buffers()` runs once at startup, before any writer opens a file, and turns
leftover logs into archive files through the same atomic path.

| Situation | What recovery does |
|---|---|
| Log present, no archive file | Rebuild the file, delete the log |
| Archive file already present | Keep the file, drop the log — **never overwrite**, a consumer may already have read it |
| Final line torn by a crash mid-write | Skip that line, keep the rest |
| Header unreadable | Rename to `.corrupt` and stop. Inventing metadata would produce a file stating things nobody measured |
| Header but no ticks | Remove the log, write nothing. A zero-tick file is noise, not evidence |
| Recovery itself crashes | Before the write: retried next start. After it: the case above. A partial `.json` cannot exist |

A recovered file is **shorter** than a rotation would have made it. That is deliberate: it is
where the previous run ended, and a short file is the honest artifact.

## The anchor checkpoint

`summary.anchor` describes the clock at file *close*, which a crashed process cannot report.
The log therefore writes a checkpoint line whenever the counters change — rare enough to cost
nothing, 14 times in a measured night. Without it a recovered file would repeat its opening
counters and thereby claim that no correction happened inside it.

## What this does not cover

`flush()` puts the line into the operating system; it does not force it to the platter.
A process crash loses nothing, and an orderly shutdown loses nothing, but a power cut can lose
what the OS had not yet written. `os.fsync()` would close that gap at 895 µs per tick instead
of 2.5 µs — still only 0.07 % duty cycle at production rates. It is not enabled; the failure
actually observed was an orderly shutdown.

## What was removed

The `.lock` sidecar. It was created and deleted alongside each file and **nothing ever read
it**, in this project or the consuming one, while the README claimed it prevented processing
of active files. The write-ahead log now marks an open file and carries its contents, so
keeping the lock would mean two markers for one state.
