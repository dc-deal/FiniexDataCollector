# Running the collector as a service

A console is the one thing that can suspend this collector, and a console nobody restarts is the
one thing that can lose a night. This document is how it runs with neither — as a Windows service
under NSSM, or as a systemd unit on Linux.

**Not in this document:** starting and stopping it by hand (see
[running the collector](running_the_collector.md)), or watching one that is already running (see
[watching a collector](watching_a_collector.md)).

**Status: installed on the production box 2026-09-23, reboot not yet done.** Everything below
except the reboot has been measured, and the numbers are named where they matter. Steps 1 to 3 of
the acceptance test passed on installation day: the service starts, `/v1/build` answers with a
real commit rather than `null`, and the narrow token reaches `/v1/status` and is refused **403**
on `/v1/archive`, `/v1/logs` and `/v1/configs`.

**The autostart is already armed**, which is worth separating from the test: a host reset before
the scheduled reboot brings the collector back by itself. Step 4 verifies that rather than
enabling it.

## Why, in two outages

- **2026-09-20, 19:48 UTC.** The host reset the machine. Caddy came back, because it is a
  service. The collector did not, because it hung on a console somebody had started by hand. The
  reset cost 15 minutes; the missing restart cost **12 h 35 min** — 98 % of the outage.
- **QuickEdit, repeatedly.** A console in QuickEdit mode suspends the next write while text is
  selected, and the display writes from the collector's only event loop. One stray click stops the
  WebSocket reader. A sister project's connectivity watcher froze for 13 h 33 min on
  the same box — and their data series was untouched, because the frozen process was their
  INSTRUMENT. Here the instrument and the producer are the same process, so the same freeze
  takes the ticks with it. That difference is the whole argument for the split.

A service has no interactive console, so neither failure has anywhere to happen.

## Windows, under NSSM

**There is a working precedent on the same machine**: Caddy runs from `C:\nssm\nssm.exe` with
startup type *Automatic (Delayed Start)*. Mirror it rather than invent a second pattern.

From an elevated PowerShell:

```powershell
$nssm    = 'C:\nssm\nssm.exe'
$root    = 'C:\Users\Administrator\Documents\code\FiniexDataColl_v12'
$python  = "$root\.venv\Scripts\python.exe"

& $nssm install FiniexDataCollector $python '-m' 'python.main' 'collect' '--no-display'
& $nssm set FiniexDataCollector AppDirectory  $root
& $nssm set FiniexDataCollector DisplayName   'FiniexDataCollector'
& $nssm set FiniexDataCollector Description   'Kraken tick collection'
& $nssm set FiniexDataCollector Start         SERVICE_DELAYED_AUTO_START
# NOT in this block, and not from the command line at all - see below.
# & $nssm set FiniexDataCollector ObjectName '.\Administrator' '<password>'
& $nssm set FiniexDataCollector AppEnvironmentExtra 'PYTHONUTF8=1'
```

### Two things to check before installing, not after

Both come from FiniexRAGEngine's install on this same machine, and both are invisible until a
service is the thing running.

**A service has no shell, so measure where the variables actually live.** Theirs found
`DATABASE_URL` persisted *nowhere* - it had been typed into whichever shell started the engine,
which is why the console worked for months and a service could not have.

```powershell
'PYTHONUTF8','FINIEX_COLLECTOR_TOKENS' | ForEach-Object {
  "{0}  Machine={1}  User={2}" -f $_,
    [bool][Environment]::GetEnvironmentVariable($_,'Machine'),
    [bool][Environment]::GetEnvironmentVariable($_,'User')
}
```

This collector reads exactly one environment variable, `FINIEX_COLLECTOR_TOKENS`, and falls back
to `api.tokens` in the overlay when it is absent - so as long as the tokens live in
`user_configs/app_config.json`, nothing is lost. `PYTHONUTF8` is the one that would be: it is
typed into the shell for a hand start, and under the service it has to come from
`AppEnvironmentExtra` above.

**`git` refuses a repository owned by somebody else, and `/v1/build` goes quiet about it.** The
commit is read with `git rev-parse` at startup; the helper returns `None` on any failure, so the
route answers `"commit": null` rather than erroring - silent, and it removes the one field that
says which code is running. FiniexRAGEngine saw exactly that under `LocalSystem`. Running as
`.\Administrator` should match the checkout's owner, but the guard is one line, system-wide, and
free:

```powershell
git config --system --add safe.directory "C:/Users/Administrator/Documents/code/FiniexDataColl_v12"
```

It is a **per-path** refusal, so it returns the moment the checkout moves or the service account
changes. Check `/v1/build` under the service, not by hand - a hand check runs as you.

### The settings that are not defaults

The settings that are not defaults, each for a measured reason:

```powershell
# A graceful stop took 0.15 s in one run and 5.1 s in another, depending on what the
# writers and the socket had in flight. NSSM's default console-stop timeout is 1500 ms,
# which would have escalated to TerminateProcess in the second case.
& $nssm set FiniexDataCollector AppStopMethodConsole 20000

# Exit 2 means a malformed configuration, or an output directory another live collector
# owns. Neither improves by being retried - a restart only produces the same refusal at
# whatever interval the throttle allows. Everything else keeps the default restart,
# which is the entire point of running under a manager.
& $nssm set FiniexDataCollector AppExit Default Restart
& $nssm set FiniexDataCollector AppExit 2 Exit

# Whatever escapes before logging is initialised has nowhere else to go - and the
# message explaining an exit 2 is exactly that: it goes to stderr before the log file
# exists, so without this the operator gets an event-log line saying the process ended
# and nothing about why. FiniexRAGEngine hit that on their first install. The file names
# match theirs on purpose, so "where is the output" has one answer on this box.
& $nssm set FiniexDataCollector AppStdout "$root\logs\service.out.log"
& $nssm set FiniexDataCollector AppStderr "$root\logs\service.err.log"
& $nssm set FiniexDataCollector AppRotateFiles 1

Start-Service FiniexDataCollector
```

**`--no-display` is not optional here**, and not only because a service has no console. Measured
on this box on 2026-09-21: one display frame cost up to 3.9 s on the collector's only event loop,
and 21 stalls over 500 ms in eight minutes became zero with it off. Watch it with
`python -m python.main watch` instead, from anywhere.

## The account, and why it is not in the block above

**`ObjectName` must be `.\Administrator`**: the virtualenv and the MT5 export directory live in
that profile, and `LocalSystem` cannot see either.

**Set it through NSSM's own dialog, not on the command line:**

```powershell
C:
ssm
ssm.exe edit FiniexDataCollector
```

Tab **Log on** → `.\Administrator` and the real password in the masked field → **Edit service**.

Two reasons, and the first one cost an outage on 2026-09-23. **NSSM does not validate the
password** - it stores whatever it is given and reports `Set parameter "ObjectName"`, so a
placeholder pasted out of a document is accepted silently. Windows validates it at start, and
answers `Cannot start service ... on computer '.'` with nothing else. The service looks broken
and the credential is the only thing wrong. Whatever else happens, **never paste a line
containing a placeholder password** - a value NSSM accepts and Windows rejects is the worst of
both.

The second reason is that PowerShell writes every command line to
`$env:APPDATA\Microsoft\Windows\PowerShell\PSReadLine\ConsoleHost_history.txt`, in plain text,
and that file outlives the session.

When a service will not start, the reason is in the event log rather than in the console:

```powershell
Get-EventLog -LogName System -Newest 20 |
  Where-Object { $_.Message -like "*FiniexDataCollector*" } |
  Format-List TimeGenerated, EntryType, EventID, Message
```

Event **7000** with "due to a logon failure", or **1069**, is the credential. A path or an access
denial is something else, and the distinction is one query rather than a guess.

## Linux, under systemd

Nothing in this repository is Windows-specific — no `.ps1`, no `.bat`, no `.sh`, and the three
`sys.platform` checks it contains are all console handling that returns `None` elsewhere. The
suite runs on `ubuntu-latest` and `windows-latest` on every push, which is how that is known
rather than assumed.

`/etc/systemd/system/finiex-collector.service`:

```ini
[Unit]
Description=FiniexDataCollector - Kraken tick collection
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=finiex
WorkingDirectory=/opt/finiex/FiniexDataCollector
Environment=PYTHONUTF8=1
ExecStart=/opt/finiex/FiniexDataCollector/.venv/bin/python -m python.main collect --no-display

# SIGINT rather than SIGTERM: on Linux both are handled through the event loop, and
# SIGINT is the path the interactive stop uses, so the service exercises the same code.
KillSignal=SIGINT
TimeoutStopSec=20

# The same split as NSSM's AppExit. Exit 2 is a configuration refusal or a directory
# another live collector owns; retrying it produces the same refusal.
Restart=on-failure
RestartPreventExitStatus=2
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now finiex-collector
journalctl -u finiex-collector -f
```

## After a reboot, on a box with three services

Three Finiex services share this machine on 4 vCPU and 8 GB, all on delayed auto start. Two
things are worth knowing before somebody debugs a non-defect:

**FiniexRAGEngine's first start after a reboot may fail on purpose.** It needs PostgreSQL; if the
database is not up yet its schema guard raises, the process exits 1 and NSSM restarts it until it
succeeds. That is correct by design, and the failed start in the event log is expected rather
than a fault.

**This collector has no such dependency and does not use that pattern.** A failed first
connection is caught inside the process and retried with backoff - it waits for the network
rather than exiting, so a reboot produces no restart cycle here. If you ever see this service
restart at boot, that is a real failure and not the normal shape.

**Exit 2 means the same thing in all three projects**: do not retry. A stale configuration, a
missing identity, an output directory another live instance owns. Agreed with FiniexRAGEngine on
2026-09-22 so that a service definition reads the same way without reading anyone's source.

## Stopping, and what a stop is worth

A stop sends a console Ctrl+C on Windows and SIGINT on Linux; both reach the same handler, which
finalises every open file. **Verified on 2026-09-21** by reproducing NSSM's exact sequence — the
collector exited in 0.15 s, wrote its archive, removed the write-ahead log, and the file passed
every import invariant.

That verification found something first, and it is the reason the collector now calls
`SetConsoleCtrlHandler(NULL, FALSE)` for itself at startup: **Ctrl+C processing can be switched
off in a process and is inherited by everything it launches.** Launched from a shell that had it
off, the collector ignored a console Ctrl+C for a full minute and never wrote its archive — with
the event accepted, the handler never running, and nothing logged anywhere.

**A failed stop is not a loss.** Every tick is in the write-ahead log before it counts as
collected, and the next start rebuilds the file — which is what the 2026-09-20 host reset
demonstrated on the real archive: nine logs recovered, 115,919 ticks, no torn line. What a
graceful stop buys is a finished archive file instead of a recovery.

## Two instances on one directory

Starting a console instance while the service runs is **refused**, before anything with a side
effect happens — no Telegram announcement, no scheduler, no port bind. The message names the pid
that holds the directory, and the process exits `2`, which both service definitions above are told
to treat as final.

The reverse case is handled too: a lock left behind by a crash is detected as stale and taken
over, so a service can always start after an unclean stop.

## The acceptance test

A service definition is proven by a reboot and by nothing else. In order:

1. **[passed 2026-09-23]** `Start-Service` / `systemctl start`, then `/v1/build` answers with
   the expected commit.
   **`"commit": null` is the failure to look for**, not an error page: it means git refused the
   repository to the service account, and the route said nothing about it.
2. Stop it. The log ends with `Shutdown complete`, and `data/raw/kraken/*.jsonl.part` is empty.
3. Start it again, and start a console instance too. The console one must refuse with exit 2 and
   the service must be undisturbed.

   Also, and this is the one that was nearly missed: **call every gated route with the narrow
   token.** `/v1/status` must answer 200 and `/v1/archive`, `/v1/logs` and `/v1/configs` must
   answer **403**. A grant is only narrow if something refuses; a token that is merely *named*
   narrow reads identically until the day it does not. Passed 2026-09-23.
4. **Reboot the machine.** The collector must be collecting before anyone logs in — check
   `/v1/build` for a `started_at` within a minute or two of boot, without opening a session.
5. Leave it through one UTC day cut. Nine files close at once there, and the archive index should
   show no open write-ahead log older than the cut.

Step 4 is the one the whole exercise exists for, and the only one that cannot be faked.
