# Tick packages

A consumer that imports our ticks wants the transfer to be one step: fetch a package, check
it, import it, file the zip unchanged as its raw source of truth, acknowledge it. Packing files
by hand is where the errors came from. The package service does the packing, once a week, for
every collector on the machine, the Kraken collector and the MT5 terminal's collector alike.

**Not in here:** what happens to a package and its original files after it is built - stage A,
the receipt, stage B, `purge` - which is [package_lifecycle.md](package_lifecycle.md), with a
walk through the folders; the collector's own routes
([status_api.md](../architecture/status_api.md)); what a tick file states about itself
([output_contract.md](../architecture/output_contract.md)); and how the collector runs as a
service ([running_as_a_service.md](running_as_a_service.md)).

---

## What a package is

- **One zip per collector per run**, standard Deflate (never Deflate64), ZIP64 where needed.
  Its members are the finished `*_ticks.json` files, byte for byte, flat.
- **A manifest beside it** describing every member. The package exists from the moment the
  manifest does: both are written under a temporary name and renamed into place, the manifest
  last. A zip without a manifest is a run that died, and the next run removes it.
- **An id `<collector>-<sequence>-<cutoff date>`**, e.g. `kraken-0003-20261123`. Each manifest
  names its predecessor in `previous_package_id`, so a missing package shows as a broken chain.
- **Nothing listed ever changes.** Not the zip, not the manifest. The consumer's
  acknowledgement is a file of its own. A confirmed package moves from `ready/` to
  `delivered/`, its bytes unchanged, and stays listed.

## What goes in

**Every finished file that no manifest names yet, whose last tick was collected before the
cutoff.** "Collected" is the last tick's `collected_msc`, UTC for both collectors. The event
time is not used for this: MT5 writes `time_msc`, and its file names, in the broker's server
time, two or three hours ahead of UTC.

- **The cutoff** is a UTC day boundary: by default the one at or before the run, so a package
  covers whole days. The weekly run is Sunday 20:00 UTC, which makes the cutoff Sunday 00:00.
  Sunday evening because forex is closed until 21:00 or 22:00 UTC: the run meets an idle MT5
  terminal. An MT5 file still open on Friday stays open over the weekend - no ticks arrive to
  fill it - and goes into the package of the week it closes in.
- **The cutoff bounds a package from above only.** A finished file that still reaches past it
  is held for the next package, and listed in `held_for_next`. A file that appears late (one
  recovered from a write-ahead log, or by hand) is taken by the next run, never skipped. So every
  finished file goes into exactly one package.
- **Nothing is packed between 23:55 and 00:15 UTC.** The day cut writes files then, and
  FiniexTestingIDE's bot places its daily order then. A run started in those minutes refuses,
  and the configuration refuses a schedule inside them.

**What stays out, and is named:**

| | where it is named | why |
|---|---|---|
| a file that cannot be read, or is not a tick file | `excluded`, with the reason | packing it would hand on a defect as data |
| a file whose `origin.instance_id` is not the folder's mint | `excluded`, with both ids | a copied file, or another collector's |
| an open `*.part` | `open_files`, with size and time | still being written, or an orphan to recover by hand |

The orphan case is real for MT5: it writes a file's footer and then renames it, so a failed
rename leaves a complete, valid file under the `.part` name. Whether that file is done is a
judgement for a person, not for the packer.

## The mint

A collector folder must hold its `instance.json`. **No mint, no package.** It is the proof that
the packer reads the folder it means to. Then every member's `origin.instance_id` must equal the
mint's, or the member stays out.

`root` in the configuration is the folder that holds the mint. `subdir` is where the tick
files lie, relative to it: `kraken` for this collector, whose identity sits in the data root
one level above its files, and empty for MT5, whose files lie beside the mint. A `subdir` that
climbs out of `root` is refused at load.

**What this cannot catch:** a stale folder, or one copied with its genuine mint. Both pass
every check here. The consumer's registry of identities and its own coverage checks are what
see those.

## Before a run: the access check

The folders sit wherever their programs put them, MT5's inside a terminal's AppData, and the
package service may run as another account. So a run first checks, and refuses with the
reason when anything fails:

1. the source folder exists and its mint is readable, with a usable id;
2. the files can be listed, and every new file can be opened for reading;
3. the package folder takes a probe file: written, flushed, renamed, read back, deleted. So do
   the originals folder and the source folder, because stage A moves the packed originals out
   of the one into the other, and a folder that refuses that has to be found before the zip is
   written, not after;
4. writing the new files' full size would still leave `min_free_percent` of the disk free
   (default 30). The collector shares the disk and reports `disk_low` at 20 % free, and a
   package must never be what gets it there.

`python -m python.packages check` runs exactly this and packs nothing. Run it after every
change to the configuration or the machine.

## The manifest

| field | |
|---|---|
| `package_id`, `sequence`, `collector` | |
| `instance_id` | the folder's mint, which every member matched |
| `producer` | from the members' `origin` blocks |
| `cutoff`, `previous_package_id`, `created_at` | |
| `zip` | `name`, `size_bytes`, `sha256` of the zip itself, `compression` |
| `member_count`, `tick_count` | |
| `span` | `from` (the previous package's cutoff, or the first member's first tick) to `until` (the cutoff) |
| `members[]` | per file: `name`, `size_bytes`, `sha256`, `symbol`, `data_format_version`, `producer`, `tick_count` (counted), `declared_tick_count` (the summary's), `first_time_msc`/`last_time_msc` (event time, as the file writes it), `first_collected_msc`/`last_collected_msc` (UTC) |
| `excluded[]`, `held_for_next[]`, `open_files[]` | see above |
| `outages` | the outage records that began within the span, read from the collector's log, the same records [`/v1/outages`](../architecture/status_api.md) serves; `days_missing` names log days that do not exist. **`null` for a collector that writes no such records**, never an empty list |
| `collector_starts` | every start of the collector's process within the span. A restart is the one gap no outage record covers, because the process that was gone could not write one. `null` like `outages` |
| `packer` | program, version and commit that built it |

Outage records exist from 2026-10-10, the build that began writing them. A span before that
has none, and that means "not recorded", not "no outages".

## The routes

On the collector's public name, behind the edge, under `/v1/packages`. The service binds
loopback.

| route | grant | |
|---|---|---|
| `GET /v1/packages` | `packages` | every package, oldest first, with `state` (`ready`, `delivered`) and `received_at`; `?collector=`, `?unacknowledged=true`; the `health` verdict, `storage` (originals per stage, disk) and each collector's last run |
| `GET /v1/packages/ledger` | `packages` | the ledger, below; `?package_id=`, `?event=` |
| `GET /v1/packages/{package_id}` | `packages:<id>` | the manifest, and the receipt |
| `GET /v1/packages/{package_id}/zip` | `packages:<id>` | the zip; repeatable, and resumable with `Range` |
| `POST /v1/packages/{package_id}/receipt` | `receipts:<id>` | "received and imported"; body `{"note": "...", "zip_sha256": "..."}`, both optional |
| `GET /v1/packages/purge` | `purge` | what a purge would delete now; `?target_free_percent=` |
| `POST /v1/packages/purge` | `purge` | delete confirmed originals until the target; body `{"target_free_percent": 50}` optional |
| `GET /v1/packages/health` | open | `status` `ok` or `degraded`, `reasons` as codes, the next run, each collector's last result |

- **No route packs.** A package is built by the weekly schedule or by `pack` on the machine,
  never because a request asked; `tests/packages/test_packages.py` holds the full list of
  routes so that one added later is looked at.
- **`purge` is the operator's, and a surface of its own.** A consumer that can confirm a package
  must not also be able to make the evidence go. The remote purge always stops at the target;
  a full purge is a command on the machine. [package_lifecycle.md](package_lifecycle.md) says
  what a purge proves before it deletes.
- **Health names reasons, not detail.** `disk_low`, `receipt_overdue`, `disposable_large`,
  `last_run_failed`, `last_run_refused`; a refusal's text names paths on the machine, and that
  is behind the token, in the list.

- **The first acknowledgement stands.** It answers `201`; a repeated one answers `200` with the
  original date, so a consumer retrying after a lost answer cannot move it. An acknowledged
  package stays listed, and the acknowledgement deletes nothing.
- **`zip_sha256` makes a receipt name the bytes.** It is the hash the consumer computed over
  what it holds. Given, it must be the package's, or the acknowledgement is refused with `409`
  and nothing is marked received: the consumer holds something other than this package. The
  receipt keeps it, so "received" then means "these exact bytes", not only "a package of this
  name".
- **Fetching is not acknowledging.** `receipts` is a surface of its own, so a token can be
  given the downloads without the right to mark anything received.
- **No route parses a tick file.** Each reads a manifest, a receipt or a zip that already
  exists. The parsing happens once per file, when its package is built.
- **The grant vocabulary is the service's own**, `packages` and `receipts`, and its tokens
  live in its own configuration. The collector validates every grant against its own
  surfaces at boot, so a `packages:*` in the collector's configuration would make a collector
  that restarts refuse to start.

## The ledger

The manifests say what each package holds. Nothing said who fetched which package, when, and
whether all of it left, and when two parties disagree about a transfer, that is the question.
`<output_dir>/ledger.jsonl` answers it: one JSON line per event, appended and never rewritten, no
database.

| event | written when | carries |
|---|---|---|
| `packed` | a package is committed, after its manifest | the zip's size and SHA-256, and every member's name, SHA-256 and tick count: a second record of the contents beside the manifests |
| `served` | a download ends, finished or not | consumer, `range`, `status`, `content_length`, `bytes_sent`, `complete`, `disconnected` |
| `received` | the first acknowledgement | consumer, note, `zip_sha256`, `verified` |
| `receipt_refused` | an acknowledgement with another hash | consumer, the reported and the expected hash |

- **`complete` means every byte was handed to the connection while the client was still there.**
  That is all this side can know. The server buffers, so `bytes_sent` runs ahead of what the client
  read; measured on 2026-11-06 with a client that read 64 KB and walked away, the line said 512 KB
  of 8.9 MB, `complete: false`, `disconnected: true`. Whether the bytes arrived is what the receipt
  says, and only the receipt.
- **A package committed before the ledger existed**, or whose line a failed write lost, gets its
  `packed` line from its manifest the next time the ledger is seeded: at every service start and
  every run, marked `entered_late`.
- **A record, not a gate.** A ledger that cannot be written stops no package and no download; the
  error goes to the service's log.
- **Several processes write it**, the service and a `pack` run by hand, so each append holds an
  operating-system lock on `.ledger.lock` beside it. A line torn by a crash is counted as
  `unreadable_lines` when the ledger is read, and the next event starts a line of its own.

## Why a program of its own

It was built during a restart freeze of the collector, and it had to go live without touching
it. That fixed the shape, and the shape is right beyond the freeze:

- **Parsing a week of files does not belong in the process that stamps the ticks.** On
  2026-11-05 one unfiltered `/v1/archive` call parsed every archive file inside the collector,
  and its event loop lagged over 500 ms 35 times in ten minutes. The packer parses in its own
  process, at below-normal priority, so the collector gets the CPU first.
- **It runs from a checkout of its own, never from the running collector's directory.** `git`
  in a running collector's directory is ruled out anyway. Beyond that, the collector writes
  its archive files from subprocesses that import the code from disk, so new code in its
  directory is a new version of the collector, freeze or not.

Measured on a laptop on 2026-11-05, with files shaped like production's at 50,000 ticks each:
267 MB of tick files packed in 5.9 s, the process peaking at 140 MB. The Kraken zip was a 28th
of its members' size.

## Running it

The `packages` block in `user_configs/app_config.json` of the service's own checkout. Source
paths are absolute, because the service runs from a different directory than the collector;
its own folders default to `./data/packages` and `./data/originals` under its checkout, where
`data/` is gitignored:

```json
{
  "telegram": { "enabled": true, "bot_token": "<the collector's>", "chat_id": "<the collector's>" },
  "packages": {
    "port": 8120,
    "weekday": "sunday", "hour": 20, "minute": 0,
    "min_free_percent": 30,
    "receipt_overdue_days": 14,
    "disposable_warn_gb": 10,
    "purge_target_free_percent": 50,
    "heartbeat_url": "<the outside check's ping address>",
    "sources": {
      "kraken": { "root": "<collector checkout>\\data\\raw", "subdir": "kraken",
                  "log_dir": "<collector checkout>\\logs" },
      "mt5":    { "root": "<terminal data folder>\\MQL5\\Files" }
    },
    "tokens": {
      "<consumer>": { "token": "<40 hex characters>", "grants": ["packages:*", "receipts:*"] },
      "<operator>": { "token": "<40 hex characters>", "grants": ["packages:*", "purge:originals"] }
    }
  }
}
```

- **Telegram is sending only.** The service warns through the collector's bot but never polls
  it: the collector reads the bot's commands, and a second reader would take its updates. At
  most one message per reason per day; a reason that clears and comes back is news again.
- **The heartbeat** pings the outside check only while health is `ok`, the collector's pattern,
  and is a credential like the bot token: `user_configs/` only.
- `paths.logs_dir` stays at its default: the service's log is its own, under its own checkout,
  and never the collector's.

```powershell
python -m python.packages check                          # prove the access, pack nothing
python -m python.packages pack                           # one run now, cutoff today 00:00 UTC
python -m python.packages pack --source mt5 --cutoff 2026-11-15
python -m python.packages settle                         # bring the folders in line, now
python -m python.packages purge                          # what a purge to the target would delete
python -m python.packages purge --apply [--full] [--target 50]
python -m python.packages serve                          # API, schedule, housekeeping, heartbeat
```

`pack` takes the same lock as the service's scheduled run, so a run by hand while the
service is packing is refused, not doubled. A lock left by a process that died is taken over.

As a service, the same NSSM pattern as the collector's
([running_as_a_service.md](running_as_a_service.md)), with `serve` as the command and
`AppExit 2 Exit`. Exit 2 is a configuration refusal, which a restart cannot fix.

At the edge, one block in the collector's site, beside the one for `/v1/*`. Caddy takes the
more specific path first:

```caddyfile
handle /v1/packages* {
	reverse_proxy 127.0.0.1:8120
}
```
