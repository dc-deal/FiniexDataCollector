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

**A reconnect is normally silent.** Measured over the night of 2026-09-21: seven of them,
recorded at 1.3 to 7.2 s each. Those figures ran from the moment a drop was noticed to the socket
handshake and so left out the 10-11 s of silence before it was noticed - the record was corrected
on 2026-10-08 - but even counted in full the drops were not among the longest gaps of the files
they fell into. This host loses outbound connectivity several times a day; FiniexRAGEngine
measured that from five sides, and nothing on our side prevents it. Six alerts for an event
invisible in the data trains the reader to swipe, and the next one that mattered is swiped with
it.

Two still arrive:

| | Why |
|---|---|
| a data gap past `reconnect_alert_min_seconds` (30 s) | a normal drop costs a few seconds and a silent link about fifteen, so thirty means reconnecting itself is failing or the link stayed dead |
| `reconnect_alert_cluster` (4) inside one hour | blips every two hours are weather; four in an hour is a machine degrading |

Both carry the length in seconds, how the drop was detected and how many attempts it took.
Whole minutes rendered every reconnect that night as "0m downtime", including the 7.22 s one
that was three times the others. The length is the data gap - last message before the drop to
the feed restored - not the window from noticing the drop to the handshake, which is what the
alert judged until 2026-10-08. (The threshold's old justification, that past the consumer's lag
window a whole file is refused, was wrong: an outage shortens a file. Kraken replays nothing after
a reconnect, so the ticks that follow are fresh.)

Everything else stays on `/v1/status` - the latest twenty outage records, and totals that count
every one - and in the log, as one `[OUTAGE]` line per outage when the feed is restored and
another when the record is complete.

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

A drop costs the trades Kraken makes while nobody is listening, so the first thing that matters
is noticing it. Three things end a connection, whichever comes first:

- **The receive loop ends** - Kraken closed the socket, or the connection was reset or lost.
  Noticed at once. Until 2026-10-08 this end was handed back by `asyncio.gather(...,
  return_exceptions=True)` and ignored, and in 136 of 137 drops between 2026-09-20 and 2026-10-08
  the socket was already closing when the silence watchdog found it, 10-11 s later.
- **A close frame on a connection left open.** Kraken can send its close frame and keep the TCP
  connection open; `recv()` then waits for the next keepalive ping, 20 s later. The watchdog reads
  the connection's state every second and takes the frame as the end.
- **Silence** for `stale_after_seconds` - a link that stopped answering without ending. Silence
  counts every message, heartbeats included, and Kraken sends one every second on a live
  connection, measured at a largest gap of 1.03 s on a quiet pair.

A connection given up on is dropped at once rather than closed politely, which on a dead link used
to wait out the 5 s close timeout. Then both channels are subscribed again. Their answers are read
by the receive loop and matched to their stream by `req_id`: until 2026-10-08 `subscribe()` read
one message per stream and kept it if it looked like an answer, so Kraken's status message - its
first on every connection - was discarded unseen and the first trade answer was logged as the
ticker's, in 169 of 169 subscription cycles on production. A pair that neither answers nor
delivers data within 10 s is listed in the record and the feed carries on; only a stream that
produced nothing at all is reconnected, and a refusal that arrives after those 10 s still counts
as a refusal. A single message that cannot be handled - a format Kraken changed - costs that
message and a WARNING, never the connection. Kraken sends a ticker snapshot on subscribe, so the quote
cache is refreshed before the first trade arrives - measured at 235 ms after a reconnect rather
than the length of the outage.

**What a drop costs now.** Measured 2026-10-08 against a local far side, both websockets
implementations: a closed or reset connection is noticed after about 0.1 s and the feed is back
after about 1.1 s; a close frame on an open connection after about 1.1 and 2.1 s; a silent link
after 10.2 and 11.2 s. Against the real Kraken from the development laptop, a connection cut
locally was noticed after 15 ms and restored after 3.4 s - 1 s of backoff and 2.3 s of handshake
and subscription. Before 2026-09-19 the check ran every 10 s and reconnected at three times that:
41-51 s of silence per drop on the liquid pairs, measured by `trade_id` gaps on production - 537
trades in four drops in one evening.

**The backoff starts over only after a restored connection has stayed up for 10 s.** It used to
start over at every handshake, which with drops noticed at once would dial a far side that
accepts and closes again every few seconds - about 150 attempts in ten minutes in a simulation
with the 2.3 s handshake measured against Kraken, the limit Kraken documents per IP, with a
ten-minute ban beyond it. The 10 s watchdog used to hide this by accident. Ten seconds keeps even a
far side that closes just past the window at 45 attempts. A longer window costs data instead: when
a degraded Kraken drops connections every 20-50 s, a 60 s window held the delay at its 60 s
maximum - 62 % offline in the same simulation (2026-10-09), against 9 % with ten seconds.

Every drop is one record on `/v1/status` and an `[OUTAGE]` line in the log when the feed is
restored and again when the record is complete, with what it cost per symbol in trade ids - see
[the status API](../architecture/status_api.md).

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
