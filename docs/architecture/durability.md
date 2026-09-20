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
    3. append the closing record           the log now holds a complete file
    4. write .json atomically              data in BOTH places
    5. delete .jsonl.part                  data in the archive file

A window where the data sits in two places is recoverable. A window where it sits in neither
is not. `tests/writers/test_write_ahead_log.py` contains a test that makes `os.replace` throw,
because on the happy path both orderings end identically — which is what would make the wrong
one survive a review.

Steps 4 and 5 happen in **another process** (below). The ordering is unchanged by that: the log
outlives the window in which the archive does not exist yet, whoever closes it.

## Who writes the file, and why not this process

Writing the archive used to happen inline, on the collector's only event loop. Measured on
production 2026-09-20: about **1 s per file plus 66 µs per tick**, and at the UTC day cut nine
files close within seconds of each other — 21 s in which nothing else ran. No socket was read, no
other symbol was written, the status API did not answer. Every tick that arrived meanwhile got its
`collected_msc` stamped that late, up to 17.85 s measured the night before, against an importer
that refuses a whole file beyond 30 s of lag.

So a closed file is **handed over**: the ticks are already on disk, line by line, so nothing is
serialized here.

    rotation:   append the closing record, close the log (keep it), open the next file
    subprocess: python -m python.writers.wal_archive <log>
                -> build the file, write it atomically, delete the log

Measured on a development laptop, 50,000 ticks: the loop stalls **6-14 ms** with the handover,
against **774 ms** inline. A worker thread is not the answer — the fast C encoder holds the GIL
for its whole run, so the loop still stalled 114 ms.

**A failure costs nothing but time.** If the child dies, hangs past its timeout, or never starts,
the log stays exactly where it is and the next start recovers it. That is what the parent gives up
by not deleting it, and the reason it must not.

**The graceful stop writes inline instead.** There is no loop left to protect, and a child started
at that moment would outlive the process meant to wait for it.

`/v1/status` carries `exports` (handed over, finished, failed, in flight, and the file a failure
left owed) and `loop_lag`, which measures the stall directly rather than leaving it to be inferred
from tick timestamps.

## The closing record

The last line a rotation appends to its log is the summary and errors block it computed at close —
the end time, the anchor counters, the tick count. Without it a file built from the log could only
state what a reader can infer, and would describe itself as recovered.

It shares the log with the ticks, as its own kind of line, so a reader that does not know the key
skips it: a log written by this build is still readable by the one before it.

## Recovery

`recover_orphaned_buffers()` runs once at startup, before any writer opens a file, and turns
leftover logs into archive files through the same atomic path.

| Situation | What recovery does |
|---|---|
| Log present, no archive file | Rebuild the file, delete the log |
| Log carries a closing record | Rebuild it **as the rotation would have**, not as a recovered file — the writer's own summary is in there |
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
