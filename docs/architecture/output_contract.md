# Output contract

A tick file is read by a different project, written by a different team, at a different time.
Everything here exists so that reader never has to infer anything — and because every rule
below was written after a field told someone something it did not know.

**Not in this document:** how the timestamps are produced (see [time model](time_model.md)),
and how a file survives a crash (see [durability](durability.md)).

## The rule underneath everything

**A field must never assert something the code does not know.**

The MT5 collector wrote `broker_utc_offset_hours: 0` into 4410 files while the truth was `+3`.
Nothing was broken; a number was simply stated with more confidence than it was held. Three
consequences follow, and they explain decisions that otherwise look arbitrary:

- `broker_utc_offset_hours` was **removed** from this collector's output rather than
  corrected. The offset belongs to the importer's registry, which knows it, not to the writer,
  which does not.
- `quote_age_ms` is `null` and never `0` when no quote has been observed. Zero would claim a
  quote seen in that same millisecond — and 13.7 % of ticks in a measured night genuinely had
  an age of zero, so the two would have been indistinguishable.
- An estimated value never enters the archive looking like a measured one.

## What a file states about itself

| Field | Meaning |
|---|---|
| `data_format_version` | Schema version of the output. A constant of the code, not a configurable input. Scoped per collector — it does not move in lockstep with the MT5 collector. |
| `collected_msc_timebase` | Time base of `collected_msc`. `"utc"` from 1.5.0. Its **absence** means device-local, and the importer refuses to guess. |
| `data_collector` / `broker_type` | Who wrote it, and which broker profile applies on import. |
| `origin` | Which instance produced the file — an identity, not a declaration. From 1.7.0; see [below](#where-a-file-comes-from). |
| `anchor_resyncs` / `anchor_max_correction_ms` | Clock corrections absorbed, cumulative over the session. In the header at file open, repeated in `summary.anchor` at close — a file whose two states differ absorbed one inside itself. |
| `quote_age_ms` | Age of the quote `bid`/`ask` were taken from. `null` when no quote was known. |
| `start_time` | Mandatory. An import without it fails. |

`DATA_FORMAT_VERSION` and `COLLECTED_MSC_TIMEBASE` live together as module constants in
`python/types/tick_types.py`. They are referenced, never retyped: the version was once a
literal in two places, which is the shape a drift bug takes.

## Where a file comes from

A development file reached FiniexTestingIDE's importer on 2026-09-15 and was caught by luck.
Nothing in a tick file said which machine had written it, and a file from a test run is
indistinguishable from a real one by inspection — that is what makes the test file dangerous
rather than merely useless.

The obvious repair was an `environment: "production" | "development"` field, and it was the
wrong one. This collector's configuration defaults to `production` and the development box had
never overridden it, so the field would have stamped the very file that caused the incident as
production. Even with the default flipped, a configuration copied from the server to a laptop
still says whatever the server said. **A declaration travels with the file that carries it; the
truth does not.**

So from 1.7.0 the producer states an identity and says nothing about what it means:

```json
"origin": {
  "instance_id": "a3f8c21d9b04",
  "collected_on": "kraken-prod-01",
  "producer": "finiex-data-collector",
  "producer_version": "1.2.0"
}
```

- **`instance_id`** — twelve lowercase hex, minted once into `instance.json` at the data root
  and read on every later start. Deliberately not derived from the machine: a container renews
  its hostname and `/etc/machine-id` on every rebuild, and an identity that changes per rebuild
  is worse than none — the archive fills with one-file identities, which looks like provenance
  and is not. The data volume survives rebuilds, so that is where identity belongs.
- **`collected_on`** — the hostname, forensic only. Nothing branches on it. It is compared
  against the last value seen for the same identity, and that comparison closes the one hole
  minting leaves open: a cloned data directory carries its identity with it, and a changed
  hostname under a known identity is the only trace of that.
- **`producer` / `producer_version`** — which program at which app version. Not the format
  version; the two move independently by design.

**The meaning lives in the consumer.** FiniexTestingIDE maps identity to trust in a registry it
owns, and an identity it has never seen resolves to `unknown`, which its measurement runs
refuse. An unregistered machine is quarantined without anyone having to remember to configure
anything — which is exactly the property the `environment` field would not have had.

A collector that can neither read nor mint an identity **refuses to start**, and a damaged
`instance.json` is a refusal too, never a re-mint: re-minting would hand the archive a fresh
identity every time that file was damaged, reaching the one-file-identity failure from the
other side. `instance.json` belongs in a backup and must never be copied when a data directory
is cloned. Losing it is recoverable — the consumer's registry holds the id and can write it
back. Duplicating it is not: two directories sharing an identity cannot be told apart
afterwards.

## What a tick carries

`last` is the execution price — what was traded. `bid`, `ask`, `spread_points` and
`spread_pct` describe the **quote the trade executed against**, taken from the ticker channel
and stated with its age.

Kraken's trade channel reports executions, and an execution happens at one price. Without the
ticker subscription every tick carries `bid == ask` and a spread of zero: correct, and
expensive, because a backtest paying no spread produces a curve that is too favourable and a
parameter sweep then optimises against a cost that does not exist.

`tick_flags` carries the taker side, `BUY` or `SELL`. A buy lifted the ask, a sell hit the
bid. That is what makes a later spread reconstruction of pre-1.6.0 files tractable — only the
width has to be modelled, not the direction.

`trade_id` is Kraken's own identifier for the execution, carried through from 1.7.0 — it was
discarded until then. It makes a tick addressable in the exchange's terms, so a duplicate
delivered across a reconnect is recognisable as the same execution instead of being inferred
from matching prices and timestamps.

**Prefer `spread_pct` over `spread_points`.** The first divides by the price; the second
quantises to the tick grid and therefore depends on `tick_size` being right. It was wrong for
three of nine symbols in the README table until 2026-09-15.

## The invariants an import enforces

A file is rejected **whole** — up to `max_ticks_per_file`, irreversibly, because the importer
never repairs — if any of these fail:

1. Row count matches `summary.total_ticks`.
2. `time_msc` never steps backwards. **Non-decreasing, not strictly increasing**: two ticks
   legitimately share a millisecond when a market order sweeps the book, and on BTCUSD 72 % of
   ticks do.
3. `collected_msc` never steps backwards, same rule.
4. `collected_msc` sits within ±30 s of `time_msc`.
5. `timestamp` agrees with `time_msc` within one second.
6. Prices are positive and not crossed.

Across files of one symbol, arrival and event ranges must not overlap or reorder. Equality is
allowed, and it is common: a rotation cuts at an exact tick count without regard for
millisecond groups, so over half of all rotation boundaries carry the same `time_msc` on both
sides. The consuming validator compares strictly (`<`), which is what makes this work.

`tests/writers/test_json_tick_writer.py` mirrors these locally. That mirror is a guard, not
the authority: before calling a format change done, run the real
`TickImportValidator.validate_file()` from FiniexTestingIDE against files a live collector
actually wrote.

## File boundaries

A file closes when either is reached: `max_ticks_per_file`, or the UTC day boundary. The day
cut is checked **before** a tick is appended, so the first tick of a new day starts the new
file rather than landing in the old one — the tick-count check runs after, and applying the
same order to the day would have defeated the purpose.

**A file therefore covers exactly one UTC day**, which is what makes a past day final: nothing
more will be added to it, it can be handed over once, and an age-based retention rule has a
well-defined subject. A day with no ticks produces no file; the gap is visible from the
missing file rather than from an empty one.

The filename stamp is advanced by a second when a name is already taken. It is an identifier
rather than data — the true open time is `start_time`, and the consuming importer orders files
by their tick bounds, explicitly not by their names. Without this, two rotations of one symbol
inside the same second would overwrite each other silently, which the daily close makes
reachable.

## Version history of the contract

- **1.5.0** — `collected_msc_timebase` declared, anchor counters added,
  `broker_utc_offset_hours` removed, `data_collector` actually written.
- **1.6.0** — `bid`/`ask` on a trade tick are the quote it executed against, with
  `quote_age_ms`. Below 1.6.0 a Kraken trade tick has `bid == ask` and a zero spread.

- **1.7.0** — every file names the instance that produced it (`origin`), and a trade tick
  carries Kraken's `trade_id`. Below 1.7.0, provenance can only be inferred from the directory
  a file was found in.

A format change is announced to FiniexTestingIDE **before** it goes live, on the bus topic
`collector-output-contract`, naming the version and a commit that exists on the remote.
