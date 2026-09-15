# Running the collector

How to start it, how to stop it, and what the display is telling you. Applies to any machine —
the server-specific paths live in the operator's private notes, not here.

**Not in this document:** what the produced files contain (see
[output contract](../architecture/output_contract.md)).

## Starting

From the project root, as a module:

    python -m python.main collect      # collect until stopped
    python -m python.main status       # show state without collecting

The file-path form (`python python/main.py`) fails with `No module named 'python'` — the
package root is not on `sys.path` that way.

**Set `PYTHONUTF8=1` on Windows.** The live display and the log lines carry box drawing and
emoji; a cp1252 console truncates the display and kills a piped run outright.

**Use a virtualenv**, matching the sister projects:

    python -m venv .venv
    .venv\Scripts\Activate.ps1          # PowerShell
    pip install -r requirements.txt

Docker in this repository is the **development** environment: the compose service runs
`tail -f /dev/null` and never starts the collector. Production runs from a virtualenv.

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
installed with `signal.signal`, which only runs while the main thread executes bytecode; an
interactive Ctrl+C is the reliable path there, and a signal delivered to a service is not
something this project has verified.

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

The collector detects a stale connection after roughly three times
`heartbeat_interval_seconds` and reconnects on its own, resubscribing both channels. Kraken
sends a ticker snapshot on subscribe, so the quote cache is refreshed before the first trade
arrives — measured at 235 ms after a reconnect rather than the length of the outage.

Each drop costs the market data of its detection window. At the shipped interval of 10 s that
is roughly 30–40 s; it was 105 s at the previous 30 s setting.

## Where things are

    data/raw/<collector>/     archive files and their write-ahead logs
    logs/                     one file per UTC day
    configs/app_config.json   tracked defaults, credentials blank and disabled
    user_configs/             gitignored overlay, deep-merged over the defaults
