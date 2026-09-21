# Watching a collector from somewhere else

The collector draws its own screen today, and that screen has twice been the thing that stopped
the collection. This document is about the replacement: a second program that shows the same
screen over HTTP, from any machine, and cannot affect the collector at all.

**Not in this document:** what the collector writes (see
[output contract](../architecture/output_contract.md)), how to start and stop the collector
itself (see [running the collector](running_the_collector.md)), or what the status route serves
(see [status API](../architecture/status_api.md)).

## Why it exists

Two failures, both properties of one process doing both jobs:

- **A console in QuickEdit mode suspends the next write while text is selected.** The live
  display writes from the collector's only event loop, so one stray click stops the WebSocket
  reader. A sister project lost 13.5 hours to this on the same box; here it showed up on
  2026-09-18 as a 24-second stall, six seconds short of the lag window that costs a whole file.
- **Drawing is expensive on that console.** Measured on the production box on 2026-09-21: one
  frame cost up to 3.9 s, and the loop stalled that long about once a minute. With the display
  off, 21 stalls over 500 ms in eight minutes became zero.

A viewer in its own process fixes both by construction. What freezes is no longer what collects.

## Starting it

    python -m python.main watch                          # the entry named "watch"
    python -m python.main watch --endpoint watch_local   # on the box itself
    python -m python.main watch --interval 5             # override the refresh rate

It reads `user_configs/remote_endpoints.json` for the base URL and the credential. Nothing else
is needed — no config file, no log directory, no write access.

## The credential it needs

`/v1/status` is gated on `status:detail`, so there is no way in without a token. It gets **its
own**, carrying that grant and nothing else: this shell sits open on a desk all day, and a token
that can also fetch archive files and log excerpts is a credential left lying in a window.

**Name it `collector_watch`.** Not `viewer` — that reads as the FiniexViewer project, which is a
separate peer with its own credentials, and a session cleaning up tokens later would have to
guess which one it was looking at.

Generate it on the box, in PowerShell as Administrator (verified on Windows PowerShell 5.1):

```powershell
$bytes = New-Object byte[] 20
[System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
$token = ($bytes | ForEach-Object { $_.ToString('x2') }) -join ''
$token
```

Forty hex characters from the cryptographic RNG — `Get-Random` is not one, and this is a
credential.

Then two places, neither of them tracked by git:

**On the box**, in `user_configs/app_config.json`, so the collector accepts it:

```json
"api": {
  "tokens": {
    "collector_watch": {
      "token": "<the forty characters>",
      "grants": ["status:detail"],
      "note": "python -m python.main watch - the status shell, read-only"
    }
  }
}
```

That section is a deep-merge overlay: adding this key leaves the other consumers alone. The
collector reads tokens at startup, so it needs a restart to see a new one.

**On whichever machine runs the shell**, in `user_configs/remote_endpoints.json`, under the entry
`--endpoint` names. `remote_endpoints.example.json` carries the shape.

**Print the URL, never the token.** A command that needs it reads it from the file.

**Refresh rate** defaults to once a second on loopback and once every two seconds anywhere else.
Every redraw is an HTTP request, which is free on loopback and not free through a TLS proxy.
`--interval` overrides it.

**Set `PYTHONUTF8=1` on Windows.** The screen is box drawing and emoji; a console on a legacy
code page cannot encode them. The viewer degrades to replacement characters rather than freezing,
but a UTF-8 console is what it is meant to look like.

Ctrl+C stops it. Killing it at any moment costs nothing — it holds no state.

## What it shows about itself

Everything on the screen was measured on another machine, so the viewer states its own condition
as well as the collector's:

- **The frame turns red** the moment a reading fails, with the clock time of the last successful
  one and how long ago that was — `⛔ no answer since 11:56:12 UTC (2m 17s ago)`. Before any
  reading has arrived it says `never answered` rather than inventing a time.
- **The reason is a sentence, not a code**, because the common failures send you to different
  places:

  | On screen | What to do |
  |---|---|
  | `unreachable: … connection refused` | the collector is not running |
  | `no answer within the timeout` | it is running and not answering — check the box |
  | `401 - the token was not accepted` | wrong credential, or the wrong instance |
  | `403 - … lacks the status:detail grant` | the token is fine; add the grant on the box |
  | `payload this viewer cannot read: …` | the two builds differ; the message names the field |

- **A clock disagreement is shown.** Uptime is computed from the collector's start time against
  the viewer's clock, so a machine running minutes off would print an uptime that never happened.
  The collector's own uptime figure is compared against the local one and the difference appears
  when it exceeds five seconds.

**The old numbers stay on screen during an outage, marked as old.** Blanking them would throw
away what the collector last said, which is usually the interesting part.

## What it deliberately cannot do

**Read-only, and not by discipline.** The viewer issues `GET /v1/status` and nothing else. There
is no write path to add a button to, so a viewer that could stop the collection would have to be
built on purpose. Control stays where it is: the Telegram commands, and the operator on the box.

**Nothing in the collector depends on the viewer.** It can be absent, killed, or running twice at
once, and the collector neither knows nor cares.

## When the two builds differ

The collector is deployed to the box and the viewer usually runs from a checkout on a laptop, so
they are routinely different versions. The two directions are not symmetric, on purpose:

- **A newer collector** sending a measurement this viewer does not know: the field is dropped and
  the screen draws. Nothing to do.
- **An older collector** missing a field the viewer expects: a whole member falls back to its
  default and draws (a limit that is not reported shows the tick count with no denominator, not a
  denominator from somewhere else), while a missing *required* value inside one is refused by
  name, on screen, rather than guessed at.
