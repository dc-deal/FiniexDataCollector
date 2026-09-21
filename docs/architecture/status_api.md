# The status API — what the collector will tell you about itself

The collector produces files somebody else consumes, and for a long time the only way to
learn whether it was running, which version, or what it had written was to open a session
on the machine. On 2026-09-15 that cost something concrete twice: which of two releases
was live had to be derived from process uptime against commit timestamps, and a question
from the consuming project — *which tick files span the host migration window* — went
unanswered past its deadline because the person with the question had no access to the
archive.

This document covers the routes and what each answers. **How to reach it and how tokens
work is [connect_contract.md](connect_contract.md)**; this file assumes you are already
authenticated.

Not in here: bar rendering or parquet conversion. Nothing on this surface changes
anything — see *Boundaries* at the end.

---

## The routes

```
GET /v1/health         open              is it alive, since when, is it connected
GET /v1/build          open              which code is running
GET /openapi.json      open              the schema: every route and parameter
GET /v1/status         status:detail     the complete live metrics
GET /v1/configs        config:effective  the settings actually in force
GET /v1/archive        archive:index     what has been written, per file
GET /v1/files/{name}   files:*           one finished archive file
GET /v1/logs           logs:collector    one UTC day of the log, filtered
```

Three are open. That exemption is written down rather than implied, and the reasoning is
per route rather than a general policy — see below.

---

## `GET /v1/health` — open

Liveness, uptime and connection state:

```json
{
  "status": "ok",
  "websocket_status": "connected",
  "uptime_seconds": 241200,
  "started_at": "2026-09-12T05:00:00+00:00"
}
```

`status` is `ok` while the WebSocket is connected and `degraded` otherwise.

**Open because an uptime probe carries no credential**, and a probe that needs one is a
probe that stops working the day a token rotates.

**It publishes no symbol names, tick counts or file names.** Those describe what is being
traded and how much of it, which an uptime probe does not need. That is the whole reason
`/v1/status` exists as a separate, gated route rather than as more fields here. A test
asserts the absence rather than trusting the intent.

## `GET /v1/build` — open

```json
{
  "version": "<app version, from configs/app_config.json>",
  "data_format_version": "<format version, from python/types/tick_types.py>",
  "commit": "0c2f88e",
  "python_version": "3.12.4",
  "dirty": false,
  "started_at": "2026-09-15T10:00:00+00:00"
}
```

Two properties are deliberate.

**Sampled once, at startup, and never re-read.** A hash read per request would describe
the working tree at that moment — so after a `git pull` without a restart it would report
the new commit while the old code serves. That is wrong in exactly the one case the route
exists for. A running process does not acquire code, and this field says so.

**Its own route rather than a field on `/v1/health`.** Health is state and gets polled on
an interval; build identity is constant for the process's lifetime. Keeping them apart
leaves the health payload, which a consumer reads on a schedule, unchanged.

**Open because this repository is public** — a commit hash discloses nothing that is not
already readable on GitHub. Behind a private repository the same field would fingerprint
the exact version and therefore its known defects. If this repository ever goes private,
this route is the first thing to gate.

`python_version` says which interpreter is answering. Four were in play on 2026-09-17 — the
Dockerfile pinned 3.12, CI ran 3.13, the development laptop had 3.13.7, and the server's was
unknowable from anywhere, because no surface reported it. A suite green on a version
production does not run proves less than it looks like.

Note `version` and `data_format_version` are different numbers and move for different
reasons: the first is what this program is, the second is what its output files promise.

## `GET /openapi.json` — open

The schema: every route, every parameter, every response shape. FastAPI generates it, so
it exists whether or not anyone decided on it — which is the reason it is named here.

**It is not reachable from outside, and this paragraph used to imply it was.** FastAPI mounts
the schema at the application root, and the TLS edge forwards `/v1/*` only — so a consumer
sees a 404 and cannot tell that apart from the schema being switched off. FiniexTestingIDE
hit exactly that on 2026-09-17 and had recorded it as a gap on our side.

The practical consequence: **the register and the architecture documents are the interface
description** for anyone off the machine. Whether the schema should move under `/v1/` to
become discoverable is an open decision, not an oversight — it lists route names, which tells
a reader that an archive and a log exist.

`/docs` and `/redoc`, the rendered consoles, are **off**. The schema is what a consumer
needs; a try-it-out console on a diagnostic surface is a different thing and was never
decided on. A test asserts both stay 404.

Open because it describes the shape of the surface and none of its contents. Note it does
list the route names, so it tells a reader that an archive and a log exist — behind a
private repository that would be worth weighing, the way `/v1/build` is.

## `GET /v1/status` — `status:detail`

The live `CollectorStats` object, serialized whole: per-symbol tick counts, last quote and
its age, file counts, reconnect history, recent log entries, disk space and uptime.

The conversion is **structural rather than enumerated** — dataclasses become objects,
datetimes become UTC ISO strings, and a field added to `CollectorStats` appears here
without anyone editing the serializer. A route that picked fields by hand would drift the
moment the collector grew one, which is how a status endpoint ends up describing last
month's system.

Two deliberate exceptions:

- `max_recent_logs` and `max_reconnect_history` are **omitted**. They describe how much
  history the terminal display keeps, which is configuration, not measurement.
- `disk_space` is **extended** with `free_gb`, `total_gb`, `percent_free` and `status`.
  Those are computed properties, so a plain dataclass conversion drops them — and they are
  the part a monitor acts on.

**`loop_lag` is the instrument for the one defect a tick file cannot show.** Everything the
collector does shares one event loop, so a long piece of work anywhere delays the stamping of
every tick that arrives meanwhile. In the file that reads as a `collected_msc` a few seconds
later than it should — nothing says why — until the lag passes the importer's 30 s window and
the whole file is refused. Ten samples a second measure how late the loop runs its own timers:
`max_ms` with the moment it happened, `over_500ms`, the last reading and the sample count. On
production 2026-09-20 the UTC day cut blocked the loop for 21 s; the archive writers now run as
subprocesses because of it.

**`exports` counts those subprocesses**: handed over, finished, failed, in flight, the last file
with its tick count and duration, and the file a failure left owed. A failed export is not lost
data — its write-ahead log stays on disk and the next start recovers it — but it is a file the
archive does not have yet, and `last_failed_file` is what names it.

**`scans` says what the background work costs**, now that it no longer shows up as stamping
lag: the folder walk and the disk reading run in a worker thread, and these are their last and
worst durations. They were on the event loop until 2026-09-21, where they stalled it by up to
4.3 s about once a minute — measured in the tick data itself, as arrival lag that was ours
rather than the venue's. If `loop_lag` ever rises together with these, they are back on the
loop.

**`counter_check` is the guard on the tick counts this route serves.** The writer counts what
goes into the file; the tick handler keeps a second count for the display, for `symbols[…]
.current_file_ticks` here, and for the weekly report. Two counters for one number drift: these
two disagreed by exactly one tick at every UTC day cut until 2026-09-20, and nothing compared
them. Now they are compared once a minute — `mismatches` staying at zero is the evidence that
the counts are sound, and anything else names the symbol and both numbers. The writer wins,
because the writer is what the file will say.

**`total_errors`, `total_warnings` and `recent_logs` count the log.** Every line written at
ERROR or CRITICAL counts as an error, every WARNING as a warning, since the process started;
`recent_logs` holds the most recent of them, as many as the display keeps. They are derived through a listener on the logger
rather than raised by each error path, because the error paths never raised them: until
2026-09-19 all three were constants — 0, 0 and empty — in every payload ever served, while the
production log carried forced reconnects. A forced reconnect is an ERROR line and counts; the
reconnects themselves are in `reconnect_events`.

Alongside the stats, the payload carries **`origin`** — the same block a tick file
carries, field for field, so a consumer parses one structure and not two:

```json
"origin": {
  "instance_id": "a3f8c21d9b04",
  "collected_on": "collector-prod",
  "producer": "finiex-data-collector",
  "producer_version": "<app version>"
}
```

**This route exists so an identity can be learned before the first file.** FiniexTestingIDE
resolves an identity it has never seen to `unknown`, and `unknown` refuses a measurement run
at admission — so a freshly deployed collector delivers files nothing may be measured
against until somebody registers it. Without this field the only ways to learn the new id
are a shell on the machine or the first file that arrives, which is the wrong order.

**Here and not on `/v1/build`, which is open.** The build route discloses a commit hash,
which the public repository already shows. An identity is the key a consumer's trust
registry is keyed on and names one machine's data directory; it belongs behind the same
grant as the symbol names.

The payload also carries **`process`** — resident memory, thread and socket counts and
consumed CPU time, sampled per request:

```json
"process": {"available": true, "rss_mb": 118.4, "threads": 12, "open_sockets": 9, "cpu_seconds": 431.2}
```

It is a reading rather than a record, which is why it is computed here and not stored in
`CollectorStats`. It exists because the box is shared: three services on 8 GB with roughly 2.4 GB
of headroom, and a collector that runs for weeks is where a slow leak hides. The sister project
found its own documented memory figure stale by 380 MB the day it measured instead of remembering.

`open_sockets` is **`null`, never `0`**, where the platform refuses the question — Windows does
for a process without the rights to ask. Zero would read as "none open", which is a measurement
nobody made. And the whole block answers `available: false` rather than raising: a diagnostic that
can fail the route carrying it costs more than it reports.

## `GET /v1/configs` — `config:effective`

The configuration actually in force, after `user_configs/app_config.json` has been merged
over the tracked defaults. `/v1/build` says which code runs; this says with which settings.

**Every credential is removed before it leaves**, by two independent rules:

- **By key name**, recursively — anything containing `token`, `secret`, `password`,
  `credential` or `apikey`, plus `chat_id`. Not by a list of paths: a path list is a
  promise about today's configuration shape, and a section added later would silently not
  be in it.
- **By shape**, using the credential vocabulary shared with the sister projects, which
  recognises a bearer token, a DSN password or a bot token wherever it sits inside a
  string value.

The key rule cannot see a secret that reached a value by accident; the shape rule cannot
know that `chat_id` is private. Hence both. The cost is the opposite error — a harmless
key named `market_key` is redacted for nothing. An over-redacted diagnostic is an
annoyance; an under-redacted one is an incident.

A test plants a real-shaped bot token and a consumer token in the configuration and
asserts neither appears anywhere in the response body — not in the field they are expected
in, in the body.

## `GET /v1/archive` — `archive:index`

What the collector has written. Per file: symbol, tick count, the declared count from the
summary, event and arrival bounds, format version, the anchor counters — and **`instance_id`**,
the identity that wrote it.

The identity is in the register for the same reason `data_format_version` is: **a consumer has
to be able to decide before transferring.** It costs nothing while one instance writes into a
directory, and from the moment two have — which is what pointing a new deployment at an
existing archive root does — "which files here did an identity I do not know write" would
otherwise mean downloading the archive to read twelve characters out of each file. At 50,000
ticks that is roughly 22 MB per answer. Requested by FiniexTestingIDE on 2026-09-17, with our
own argument.

It is **`null`** for anything below 1.7.0, and that is the honest answer rather than a gap:
those files were written before provenance existed, and nothing can infer afterwards which
instance wrote them.

```
?symbol=BTCUSD          one symbol
?only_corrected=true    only files that absorbed a clock correction
```

**`absorbed_clock_correction`** is the field this route was built for. A file whose
`anchor_resyncs` grew between its header (written at open) and its summary (written at
close) contains a tick whose timestamp was held back by the monotonicity clamp. That is
the only durable record of a clock correction — the clamp works precisely by making the
event invisible everywhere else, which is what makes it safe for the import and invisible
for an investigation.

The rule is computed here rather than left to the caller: it is a detail of our file
format, and a consumer reimplementing it would be reimplementing ours.

**Metadata only.** The tick arrays are the bulk of a file — 425 bytes per tick, up to
50,000 of them — and no inventory question needs them. `open_write_ahead_logs` lists any
`.jsonl.part` without its archive file: the run that is collecting now, or a crashed one
waiting for recovery.

## `GET /v1/files/{name}` — `files:*`

Hands out one finished archive file. This is the transfer that replaces SFTP, which would
have meant shell access to a machine running three services in order to move files out of
one directory.

**Only finished files can be reached, and that is a property of the writer rather than a
check performed here.** A `*_ticks.json` is written to a temporary file and `os.replace()`-d
into position, so the name never exists before the content is complete. What is still being
collected lives in memory and in a `.jsonl.part`, which has no `.json` counterpart and does
not match this route's name pattern. The daily close adds a coarser boundary on top: once a
UTC day ends, that day's file is final.

**The name is the only thing between a request and the file system.** It is matched whole
against the archive pattern — anchored, so nothing longer containing a valid name passes —
and the resolved path is then required to sit inside the collector directory. The second
check exists for what the first cannot see: a symlink planted in that directory under a
valid name. Everything that fails either check is a `404`, including a malformed name, so
the route cannot be used to probe what exists on the disk.

Pair it with `/v1/archive?with_checksum=true`, which adds a SHA-256 per file. Hashing is
off by default because it reads the whole archive, and a register is asked for far more
often than a transfer is verified.

The name is a path parameter, so the grant is `files:<name>` and a consumer entitled to the
archive holds `files:*`. A per-file grant is possible; it is what the model allows, not
what it expects.

## `GET /v1/logs` — `logs:collector`

**`day` is optional, and omitting it means the newest day present — deliberately not today.**
From a remote session the box's own date boundary is unknown: a few minutes after midnight UTC
"today" is an almost empty file while yesterday is the finished one, and in both cases what
somebody means by "the log" is the newest one there is. An empty log directory answers with
`day: null` and an empty `available_days` rather than reporting a missing file for a date that was
never going to exist.


One UTC day of the log.

```
?day=2026-09-15         required, YYYY-MM-DD
?min_level=WARNING      default INFO
?since= / ?until=       ISO timestamps
?contains=reconnect     substring of the message
?limit=500              capped at 2000
```

**Every timestamp is UTC** — in the request, in the file name, and in each line, which is
stamped `YYYY-MM-DD HH:MM:SS UTC`. Nothing is converted at either end. This is worth
stating because the sister project documents the opposite as a trap: their query is UTC
while their file carries the server's local clock, and the offset has to be applied by
hand. Do not add a conversion here; it would be wrong.

**Every line passes through the credential vocabulary before it leaves**, and the number
of lines that were altered is reported as `redacted_lines`. A log is free text, so the key
rule has nothing to work with — what reaches it is a bot token inside a URL or a bearer
header in a traceback. The count is surfaced rather than swallowed: a reader trusts what a
diagnostic hands them, so a line altered without saying so is worse than one withheld.

**The day is a query parameter, not a path segment.** A path segment becomes the grant
name — `logs:2026-09-15` — which would demand a grant per calendar day. Without one, the
surface itself is the permission. The same reasoning applies to `symbol` on `/v1/archive`.

A line that does not match the expected shape — a traceback's continuation — is carried
through with whatever was kept before it. A stack trace whose first line survived the
level filter and whose body vanished is worse than no excerpt.

An unknown day answers `exists: false` with `available_days`, rather than a 404 that
leaves the caller guessing. A malformed day is a 400.

---

## Transfer

Responses above 1 KB are gzipped when the caller asks for it. Measured on a real archive
file: **430,516 bytes plain, 21,470 compressed — twentyfold**, for about three milliseconds
of CPU.

That ratio is not luck. A tick file is the same set of keys repeated on every line, which
is close to the best case for a dictionary coder. At the collection rate this is the
difference between roughly a gigabyte a week and forty-five megabytes.

**Level 6, not the library default of 9.** Measured on the same file: level 9 reaches 24.8x
for 8 ms and lzma 30.7x for 41 ms. Neither is worth the CPU on a four-core machine shared
with two other services — thirteen percent fewer bytes for fourteen times the work is the
wrong trade when the link is not the constraint.

**Nothing below 1 KB is touched.** `/v1/health` is about a hundred bytes and is polled on an
interval; the gzip header would cost more than it saves.

Two properties worth stating because a consumer depends on them:

- **It is negotiated, never imposed.** A client that sends no `Accept-Encoding` receives
  plain JSON. That is what made this safe to add without moving `data_format_version`: a
  consumer written before it exists keeps working unchanged.
- **The register's `sha256` is over the uncompressed file.** An HTTP client decodes before
  it hashes, so a verified transfer produces the same digest either way. Confirmed against
  a live collector rather than assumed — otherwise every checksum check would have failed
  exactly when compression was on.

## Boundaries

**Read-only, and it cannot change anything.** Every route reads through a provider
callable rather than holding a reference to the collector, so the surface is read-only by
construction rather than by discipline.

**It cannot take the collector down.** The server runs as a task on the collector's own
event loop with its own exception guard. A status surface that stops the collection is
worse than no status surface.

**It can stop listening without stopping, on Windows.** When `accept()` raises, CPython's
proactor loop (3.13 and 3.14 alike) closes the listening socket for good and tells only the
loop's exception handler; uvicorn keeps running on a socket that no longer exists, and the edge
answers 502. The collector installs a handler that writes this into the log file as an ERROR —
asyncio's default would have printed it to stderr, outside the log — so the cause is on record.
It does not bring the API back: that takes a collector restart. Not observed in production so
far.

**It is off unless `api.enabled` is set**, and it binds loopback. The port never gets a
firewall rule; reaching it from elsewhere is the reverse proxy's job.

**No route from the auth package.** `finiex_auth` ships dependencies, the token registry,
the credential vocabulary and a route walk — not routes. Every route above is this
project's own.
