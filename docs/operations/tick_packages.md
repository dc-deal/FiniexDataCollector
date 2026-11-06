# Tick packages

A consumer that imports our ticks wants the transfer to be one step: fetch a package, check
it, import it, file the zip unchanged as its raw source of truth, acknowledge it. Packing files
by hand is where the errors came from. The package service does the packing, once a week, for
every collector on the machine, the Kraken collector and the MT5 terminal's collector alike.

**Not in here:** the collector's own routes ([status_api.md](../architecture/status_api.md)),
what a tick file states about itself ([output_contract.md](../architecture/output_contract.md)),
and how the collector runs as a service ([running_as_a_service.md](running_as_a_service.md)).

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
  acknowledgement is a file of its own, and it deletes nothing.

## What goes in

**Every finished file that no manifest names yet, whose last tick was collected before the
cutoff.** "Collected" is the last tick's `collected_msc`, UTC for both collectors. The event
time is not used for this: MT5 writes `time_msc`, and its file names, in the broker's server
time, two or three hours ahead of UTC.

- **The cutoff** is a UTC day boundary: by default the one at or before the run, so a package
  covers whole days. The weekly run is Monday 01:00 UTC, which makes the cutoff Monday 00:00.
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
2. the files can be listed, and every new file can be opened for reading. The packer only
   ever reads a source folder, so nothing is written there, not even a probe;
3. the package folder takes a probe file: written, flushed, renamed, read back, deleted;
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
| `GET /v1/packages` | `packages` | every package, oldest first, with `received_at`; `?collector=`, `?unacknowledged=true`; and each collector's last run |
| `GET /v1/packages/{package_id}` | `packages:<id>` | the manifest, and the receipt |
| `GET /v1/packages/{package_id}/zip` | `packages:<id>` | the zip; repeatable, and resumable with `Range` |
| `POST /v1/packages/{package_id}/receipt` | `receipts:<id>` | "received and imported"; body `{"note": "..."}` optional |
| `GET /v1/packages/health` | open | alive, the next run, and what each collector's last run did |

- **The first acknowledgement stands.** It answers `201`; a repeated one answers `200` with the
  original date, so a consumer retrying after a lost answer cannot move it. An acknowledged
  package stays listed, and the acknowledgement deletes nothing.
- **Fetching is not acknowledging.** `receipts` is a surface of its own, so a token can be
  given the downloads without the right to mark anything received.
- **No route parses a tick file.** Each reads a manifest, a receipt or a zip that already
  exists. The parsing happens once per file, when its package is built.
- **The grant vocabulary is the service's own**, `packages` and `receipts`, and its tokens
  live in its own configuration. The collector validates every grant against its own
  surfaces at boot, so a `packages:*` in the collector's configuration would make a collector
  that restarts refuse to start.

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

The `packages` block in `user_configs/app_config.json` of the service's own checkout. Paths
are absolute, because the service runs from a different directory than the collector:

```json
{
  "telegram": { "enabled": false },
  "packages": {
    "output_dir": "C:\\FiniexPackages",
    "port": 8120,
    "weekday": "monday", "hour": 1, "minute": 0,
    "min_free_percent": 30,
    "sources": {
      "kraken": { "root": "<collector checkout>\\data\\raw", "subdir": "kraken",
                  "log_dir": "<collector checkout>\\logs" },
      "mt5":    { "root": "<terminal data folder>\\MQL5\\Files" }
    },
    "tokens": {
      "<consumer>": { "token": "<40 hex characters>", "grants": ["packages:*", "receipts:*"] }
    }
  }
}
```

`paths.logs_dir` stays at its default: the service's log is its own, under its own checkout,
and never the collector's.

```powershell
python -m python.packages check                          # prove the access, pack nothing
python -m python.packages pack                           # one run now, cutoff today 00:00 UTC
python -m python.packages pack --source mt5 --cutoff 2026-11-09
python -m python.packages serve                          # the API and the weekly schedule
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
