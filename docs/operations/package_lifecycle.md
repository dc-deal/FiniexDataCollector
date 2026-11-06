# Package lifecycle

A package is built once a week, but the original tick files it was built from have to go
somewhere, and the disk they fill is the collector's. Deleting them after packing would leave
one copy, on the same disk, until the consumer has fetched it. Keeping them forever fills the
disk. This document is the path in between: where a package and its originals are at each
step, what moves them on, and what has to be proven before anything is deleted.

**Not in here:** how a package is built and what goes into it, the manifest, the routes and the
ledger - [tick_packages.md](tick_packages.md).

---

## The folders

Under the package service's own checkout, in its gitignored `data/`:

```
data\packages\<collector>\ready\              zip + manifest, waiting for the consumer
data\packages\<collector>\delivered\          zip + manifest + receipt, confirmed
data\packages\ledger.jsonl                    the book of every step
data\originals\awaiting\<collector>\<id>\     stage A: packed and checked by us
data\originals\disposable\<collector>\<id>\   stage B: confirmed by the consumer; may be purged
```

The collectors' own folders - `data\raw\kraken` in the collector's checkout, `MQL5\Files` in
the MT5 terminal's data folder - keep only what is not packed yet: open files, and finished
ones waiting for the next cutoff. Everything stays on one drive, so every move is a rename:
instant, and never a copy.

## The steps

1. **Pack.** The weekly run builds the zip, reads every member back out of it and compares its
   SHA-256 with the original's, and writes the manifest. The package is in `ready/`. A zip that
   gives back other bytes is no package, and nothing moves.
2. **Stage A.** Right after, the originals move from the collector's folder to
   `originals\awaiting\<collector>\<id>\` - each one only if it is still the bytes the manifest
   describes. Moved, not deleted: the zip and the originals are two copies on our side.
3. **Fetch.** On the consumer's schedule, not ours. Every download is booked in the ledger.
4. **Receipt.** The consumer acknowledges the package with the SHA-256 it computed over the zip.
   A matching hash makes it **confirmed**; another hash is refused with `409` and changes
   nothing; a receipt without a hash is kept but proves nothing, and nothing moves on.
5. **Stage B.** A confirmed package moves to `delivered/`, and its originals from `awaiting/` to
   `disposable/`. The bytes now exist on two machines.
6. **Purge.** The only step that deletes, only from `disposable/`, and only what it proves again
   on the spot (below). The zip in `delivered/` stays.

**`settle` keeps the folders in line with the manifests and receipts.** It runs after every
packing run, right after a receipt, every ten minutes in the service, and at its start. A move a
crash cut short - zip already in `delivered/`, manifest not yet - is found from either side and
finished. A file another process holds open (Windows refuses to move it) stays where it is and
is tried again next time.

## Purge

Before a package's originals go, all of this must hold, or the package stays and the reasons
are listed:

- the receipt names the zip's hash;
- our zip in `delivered/` still has the manifest's hash, recomputed now - otherwise the
  originals are our last copy;
- the folder holds exactly the manifest's files: none missing, none extra;
- every file has the manifest's name, size and SHA-256.

The hash alone proves the bytes. Name, size and count are checked anyway because they cost
nothing and make the failure readable. Every file deleted is booked as `purged`, with its name,
size and hash.

| | deletes | where |
|---|---|---|
| `purge` | nothing - lists what would go | on the machine |
| `purge --apply` | confirmed originals, oldest receipt first, until `purge_target_free_percent` (default 50) of the disk is free; nothing when it already is | on the machine |
| `purge --apply --full` | every confirmed package | on the machine only |
| `GET /v1/packages/purge` | nothing - lists what would go | remote, grant `purge` |
| `POST /v1/packages/purge` | as `purge --apply`, target optional in the body | remote, grant `purge` |

A full purge is not offered remotely. The default is a safety valve, not housekeeping: with
the disk above the target it does nothing at all.

## Warnings

The service takes its verdict every ten minutes. `degraded`, with a reason:

| reason | when |
|---|---|
| `disk_low` | less than `min_free_percent` (30) of the disk is free; the collector itself warns at 20 |
| `receipt_overdue` | a package has waited more than `receipt_overdue_days` (14) for a confirming receipt |
| `disposable_large` | confirmed originals take more than `disposable_warn_gb` (10): time to purge |
| `last_run_failed`, `last_run_refused` | a collector's latest packing run failed, or refused to start |

The operator hears of it by Telegram, at most once a day per reason. The consumer hears of it
through the outside check: the service pings it only while healthy, so a `degraded` service
goes quiet there, and an outside check that alarms is what the consumer's basic check reads.
`/v1/packages/health` shows the status and the reason codes to anyone.

## A walk through, with two MT5 files

The names are made up; the folders are real. `Files\` is the MT5 terminal's `MQL5\Files\`,
`data\` the package service's.

**Sunday 15 Nov, before the run.**

```
Files\
    instance.json                              mint 4abe814c11fa
    EURUSD_20261110_093000_ticks.json          finished; last tick Wed 11 Nov 14:02 UTC
    GBPUSD_20261112_120000_ticks.json          finished; last tick Fri 13 Nov 21:58 UTC
    USDJPY_20261114_230000_ticks.json.part     still open
data\packages\mt5\ready\                       empty
data\originals\awaiting\mt5\                   empty
data\originals\disposable\mt5\                 empty
```

**Sunday 15 Nov, 20:00 UTC - the weekly run.** The access check passes. Both finished files
are in no package and their last tick is before the cutoff (Sunday 00:00 UTC), so both go in;
USDJPY is open and only named. The zip is written as `.zip.part`, read back member by member,
renamed. The manifest is written: the package exists (`packed`). The originals move out
(`staged`).

```
Files\
    instance.json
    USDJPY_20261114_230000_ticks.json.part         untouched
data\packages\mt5\ready\
    mt5-0002-20261115.zip
    mt5-0002-20261115.manifest.json
data\originals\awaiting\mt5\mt5-0002-20261115\
    EURUSD_20261110_093000_ticks.json
    GBPUSD_20261112_120000_ticks.json
```

**Tuesday 17 Nov - the consumer fetches.** It lists, sees `mt5-0002-20261115` in `ready`,
downloads the zip (`served`, complete), checks its hash, imports, files the zip, and
acknowledges with `zip_sha256`: `201`, `received` with `verified: true`.

**Right after - stage B.** The package moves to `delivered/` with its receipt (`delivered`),
the originals to `disposable/` (`disposable`).

```
Files\
    instance.json
    USDJPY_20261114_230000_ticks.json.part
data\packages\mt5\delivered\
    mt5-0002-20261115.zip
    mt5-0002-20261115.manifest.json
    mt5-0002-20261115.receipt.json
data\originals\disposable\mt5\mt5-0002-20261115\
    EURUSD_20261110_093000_ticks.json
    GBPUSD_20261112_120000_ticks.json
```

**Whenever the disk asks for it - purge.** `purge` lists the package as deletable: receipt
verified, our zip recomputed and matching, 2 of 2 files with the manifest's names, sizes and
hashes. `purge --apply` - if the disk is below the target - deletes the two files and books
`purged`. `disposable\mt5\` is empty; the zip stays in `delivered\`.

**When something does not fit.**

- The consumer reports another hash: `409`, nothing moves; the package stays in `ready\`, the
  originals in `awaiting\`, and the ledger has `receipt_refused`.
- The consumer acknowledges without a hash: kept, proves nothing; nothing moves, and after
  14 days the package is `receipt_overdue`.
- No receipt for 14 days: a Telegram message, health `degraded`, the outside check alarms.
- A file in `disposable\` was changed, removed or joined by another: `purge` keeps that package
  and names the file.
- USDJPY closes on Wednesday: it lies finished in `Files\` and goes into the next Sunday's
  package.
