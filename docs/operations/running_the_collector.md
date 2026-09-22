# Running the collector

How to start it, how to stop it, and what the display is telling you. Applies to any machine —
the server-specific paths live in the operator's private notes, not here.

**Not in this document:** what the produced files contain (see
[output contract](../architecture/output_contract.md)).

## Starting

From the project root, as a module:

    python -m python.main collect      # collect until stopped
    python -m python.main status       # show state without collecting
    python -m python.main watch        # draw a collector running elsewhere

The file-path form (`python python/main.py`) fails with `No module named 'python'` — the
package root is not on `sys.path` that way.

**Set `PYTHONUTF8=1` on Windows.** The live display and the log lines carry box drawing and
emoji; a cp1252 console truncates the display and kills a piped run outright.

`watch` is a different program that happens to share this entry point: it reads one HTTP
route and draws the screen, loads none of this configuration, and writes to no log file.
See [watching a collector](watching_a_collector.md).

**On the production box it runs as a service**, with no console at all — see
[running as a service](running_as_a_service.md). The commands above are how it is run by hand,
which is development and the occasional deliberate check.

**Use a virtualenv**, matching the sister projects:

    python -m venv .venv
    .venv\Scripts\Activate.ps1          # PowerShell
    pip install -r requirements.txt

Docker in this repository is the **development** environment: the compose service runs
`tail -f /dev/null` and never starts the collector. Production runs from a virtualenv.

## One collector per output directory

Starting a second one against the same `raw_data_dir` is **refused**, and the refusal is the
first thing that happens — before Telegram, before the scheduler, before the status port. The
message names the pid that holds it:

    Another collector is already writing to data
aw (pid 4892).

This is not about duplicate work. Startup recovery cannot tell a crashed run's write-ahead log
from a running instance's: on Windows removing a live log fails and aborts the start, and on
Linux it **succeeds**, taking away the running instance's only protection.

A lock left by a crash is **taken over**, not obeyed — the holder is identified by pid *and*
process creation time, so a reused pid cannot lock the directory forever, and a truncated lock
file counts as stale. Verified 2026-09-21: second start refused, first process killed outright,
third start took over and recovered the log the kill left behind.

**Exit codes**, which matter once a service manager is in front of it:

| Code | Meaning | What a manager should do |
|---|---|---|
| 0 | stopped cleanly | nothing |
| 1 | crashed | restart |
| 2 | bad configuration, or the directory is taken | **stay stopped** — a retry produces the same failure |

## What reaches the phone, and what does not

Telegram carries the things somebody would act on, and deliberately not the rest.

**A reconnect is normally silent.** Measured over the night of 2026-09-21: seven of them, each
1.3 to 7.2 s, 19.9 s in total — about 22 ticks, and not one appeared among the eight longest gaps
in the file it fell into, which were all quiet market. This host loses outbound connectivity
several times a day; FiniexRAGEngine measured that from five sides, and nothing on our side
prevents it. Six alerts for an event invisible in the data trains the reader to swipe, and the
next one that mattered is swiped with it.

Two still arrive:

| | Why |
|---|---|
| downtime past `reconnect_alert_min_seconds` (30 s) | past the consumer's lag window a whole file is refused, not shortened |
| `reconnect_alert_cluster` (4) inside one hour | blips every two hours are weather; four in an hour is a machine degrading |

Both carry the real length in seconds. Whole minutes rendered every reconnect that night as
"0m downtime", including the 7.22 s one that was three times the others.

Everything else stays in the log and on `/v1/status`, where `reconnect_events` is complete.

## Stopping

**Ctrl+C, once.** Not the window close button, not `Stop-Process`, not `kill -9`.

A graceful stop finalizes every open file, so the run ends with complete archive files and no
leftovers. Since the write-ahead log exists this is a courtesy rather than a requirement — a
hard stop now costs the last tick instead of every open buffer — but a clean stop still leaves
a tidier archive, and the buffers can be large: a production collector had 188,253 ticks
across eight symbols after 67 hours.

Confirm that the shutdown actually ran:

    grep "Shutting down" logs/finiexdatacollector_<date>.log
    ls data/raw/kraken/*.jsonl.part

A remaining `.jsonl.part` means that symbol's file was never finalized. Nothing is lost — the
next start rebuilds it — but it tells you the stop was not clean.

On Linux both SIGINT and SIGTERM are handled through the event loop. On Windows the handler is
installed with `signal.signal`, which only runs while the main thread executes bytecode.

**A stop from a service manager was verified on 2026-09-21**, and it found something first.
NSSM's `AppStopMethodConsole` detaches from its own console, attaches to the service's, and
raises a console Ctrl+C there. Reproducing exactly that sequence: the collector **ignored it for
a full minute** and its archive was never written. The cause was not the handler — it was that
**Ctrl+C processing can be switched off in a process and is INHERITED by everything it
launches**, and the shell running the test had it off. The event is accepted by the console, the
handler never runs, and nothing is logged. The collector now calls
`SetConsoleCtrlHandler(NULL, FALSE)` for itself before registering the handler, so it no longer
depends on what launched it. With that one call, the same test stopped it in **0.15 s**, wrote
the archive, removed the write-ahead log, and the file passed every import invariant.

**A kill is still not a loss.** Had the stop failed, the write-ahead log would have held every
tick and the next start would have rebuilt the file — which is what the 2026-09-20 host reset
demonstrated on the real archive. The difference a graceful stop makes is a finished archive
file instead of a recovery.

## Reading the live display

    💾 Disk: 250.0 GB free (50%) │ 🕐 Clock: steady │ Last Check: …

    Symbol    Bid        Ask        Spread %   Quote age   Status
    BTCUSD    79,383.7   79,383.8   0.0001     42 ms       ✅ 2s
    ADAUSD    0.210300   0.210400   0.0453     7.6 s       ⸻ 13m
    DASHUSD   54.38      54.42      0.0736     no quote    ⚠ 4.5h

**Quote age** is the age of the quote the spread came from. Green under 250 ms, yellow to one
second, red above. `no quote` means none had been observed — distinct from a fresh one, which
is why the underlying field is nullable. A spread without its age cannot be judged.

**Clock** reads `steady` while the OS clock behaves, and reports the correction count and
largest step once it does not. The clamp is invisible in the data by design, so if this line
is not watched nobody learns the machine's clock is stepping.

**Status** is the time since that symbol's last tick, not a boolean. Red above an hour. A thin
symbol looking quiet and a dead feed are both "not recent"; only the elapsed time tells them
apart.

## When a connection drops

The collector closes and reopens a connection that has been silent for `stale_after_seconds`
and resubscribes both channels. Silence counts every message, heartbeats included, and Kraken
sends one every second on a live connection — measured at a largest gap of 1.03 s on a quiet
pair — so the check runs every second. Kraken sends a ticker snapshot on subscribe, so the
quote cache is refreshed before the first trade arrives — measured at 235 ms after a reconnect
rather than the length of the outage.

Each drop costs the market data of its detection window plus about 6 s to reconnect and
resubscribe. At the shipped 10 s that is roughly 11 s of noticing. Before 2026-09-19 the check
ran every 10 s and reconnected at three times that: 41–51 s of silence per drop on the liquid
pairs, measured by `trade_id` gaps on production — 537 trades in four drops in one evening.

## Reading a rotation in the log

Two lines describe every closed file, and they come from different places:

    Closed: BTCUSD_..._ticks.json (47368 ticks) - handed to the archive writer   <- the writer
    File rotated: BTCUSD_..._ticks.json (47,368 ticks)                            <- the handler
    Exported BTCUSD_..._ticks.json (47,368 ticks) in 840 ms                       <- the subprocess

**They are a cross-check, not a repetition.** The first counts the buffer that became the file,
the second the counter the display and `/v1/status` show, the third what the subprocess actually
wrote. Until 2026-09-20 the middle one disagreed with the other two by one tick at every day cut.
`counter_check` on `/v1/status` now compares the first two once a minute; `mismatches` above zero
is the signal.

**A blocked event loop is not a dead feed.** While a file closes nothing reads the socket — the
midnight cut blocked the loop for 17.85 s — and messages wait there unread. A check that wakes
late therefore skips its judgement for that round; the next one sees what the receive loop read
in the meantime. Without that exemption a 10 s threshold would force a reconnect at every day
cut.

## Where things are

    data/raw/<collector>/     archive files and their write-ahead logs
    logs/                     one file per UTC day
    configs/app_config.json   tracked defaults, credentials blank and disabled
    user_configs/             gitignored overlay, deep-merged over the defaults
