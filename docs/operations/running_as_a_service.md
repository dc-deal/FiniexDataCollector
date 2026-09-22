# Running the collector as a service

A console is the one thing that can suspend this collector, and a console nobody restarts is the
one thing that can lose a night. This document is how it runs with neither — as a Windows service
under NSSM, or as a systemd unit on Linux.

**Not in this document:** starting and stopping it by hand (see
[running the collector](running_the_collector.md)), or watching one that is already running (see
[watching a collector](watching_a_collector.md)).

**Status: written, not yet installed.** Everything below except the reboot itself has been
measured; the numbers are named where they matter. A service definition is only proven by an
actual reboot, and that has not happened. Treat the last section as the acceptance test rather
than a formality.

## Why, in two outages

- **2026-09-20, 19:48 UTC.** The host reset the machine. Caddy came back, because it is a
  service. The collector did not, because it hung on a console somebody had started by hand. The
  reset cost 15 minutes; the missing restart cost **12 h 35 min** — 98 % of the outage.
- **QuickEdit, repeatedly.** A console in QuickEdit mode suspends the next write while text is
  selected, and the display writes from the collector's only event loop. One stray click stops the
  WebSocket reader. A sister project lost 13.5 hours to this on the same box.

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
& $nssm set FiniexDataCollector ObjectName    '.\Administrator' '<password>'
& $nssm set FiniexDataCollector AppEnvironmentExtra 'PYTHONUTF8=1'
```

Then the four settings that are not defaults, each for a measured reason:

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

# Whatever escapes before logging is initialised has nowhere else to go.
& $nssm set FiniexDataCollector AppStdout "$root\logs\service_stdout.log"
& $nssm set FiniexDataCollector AppStderr "$root\logs\service_stderr.log"
& $nssm set FiniexDataCollector AppRotateFiles 1

Start-Service FiniexDataCollector
```

**`--no-display` is not optional here**, and not only because a service has no console. Measured
on this box on 2026-09-21: one display frame cost up to 3.9 s on the collector's only event loop,
and 21 stalls over 500 ms in eight minutes became zero with it off. Watch it with
`python -m python.main watch` instead, from anywhere.

**`ObjectName` must be `.\Administrator`**: the virtualenv and the MT5 export directory live in
that profile, and `LocalSystem` cannot see either.

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

1. `Start-Service` / `systemctl start`, then `/v1/build` answers with the expected commit.
2. Stop it. The log ends with `Shutdown complete`, and `data/raw/kraken/*.jsonl.part` is empty.
3. Start it again, and start a console instance too. The console one must refuse with exit 2 and
   the service must be undisturbed.
4. **Reboot the machine.** The collector must be collecting before anyone logs in — check
   `/v1/build` for a `started_at` within a minute or two of boot, without opening a session.
5. Leave it through one UTC day cut. Nine files close at once there, and the archive index should
   show no open write-ahead log older than the cut.

Step 4 is the one the whole exercise exists for, and the only one that cannot be faked.
