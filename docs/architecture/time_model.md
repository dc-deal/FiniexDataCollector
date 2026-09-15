# Time model

Three clocks meet in every tick, and confusing them is the most expensive mistake this
project has made. This document names which is which and why the combination is not
interchangeable.

**Not in this document:** what the fields mean to a consumer (see
[output contract](output_contract.md)).

## The three clocks

| | Source | Appears as |
|---|---|---|
| **Exchange event time** | Kraken's own timestamp on a trade | `time_msc` |
| **Our arrival time** | The OS clock, via `CollectionClock` | `collected_msc` |
| **Quote observation time** | The same clock, when the ticker update arrived | implied by `quote_age_ms` |

Measured on the archive, arrival trails the event by a median of 8–11 ms and up to 113 ms.
That distance is real network and exchange latency, not error.

## One clock per collection session

`CollectionClock` is created once in `main.py` and handed to the parser that stamps and to
every writer that reports. Not one per symbol, and not one per component.

A globally non-decreasing series is non-decreasing in every subsequence, so one clock keeps
every symbol's file monotonic. And a clock correction is counted **once** rather than once per
symbol, which is what makes `anchor_resyncs` comparable to the MT5 collector's.

## Why the clock clamps

`time.time()` is explicitly not monotonic. An NTP correction can step it backwards — after
standby, in a migrated VM, whenever the system time source finds a large offset. A backwards
step puts a smaller `collected_msc` behind a larger one, and the importer rejects the file for
it.

This is not theoretical. In one 4h25m run the clamp fired **14 times across 11 of 46 files**,
each a 1 ms step. The importer rejects on direction, not magnitude, so without the clamp a
quarter of that run would have been refused.

**`time.monotonic()` is not a substitute.** It has no epoch, so its values cannot be compared
against `time_msc`. `collected_msc` has to be both epoch-based *and* non-decreasing, and that
combination is exactly what the clamp produces.

Clamping alone would hide the broken clock it works around — it smooths the very evidence — so
every correction is counted and logged, and the counters appear in the file header and again
in `summary.anchor`.

## Two rules that follow

**Never derive a duration by subtracting two wall-clock readings.** A correction between them
produces a negative duration.

**One reading per moment.** The `timestamp` string is derived from `time_msc`, not read from
the clock a second time. A second reading can land on the other side of a correction and then
disagree with the value it is supposed to describe — which, with the clamp in place, would
lose the file to the timestamp check rather than the monotonicity one.

## UTC everywhere, with two deliberate exceptions

All timestamps are timezone-aware UTC. The exceptions are `local_device_time` in the metadata,
whose purpose is to be the machine's wall clock, and the terminal live display.

## The streams are not synchronised

The ticker channel overwrites a per-symbol slot in `QuoteCache`; the trade channel reads
whatever stands in it. That is a latch, not an alignment.

Pairing a trade with the quote current at its *exchange* event time would mean buffering
trades and waiting for updates that may arrive later — inventing a correspondence the data
does not carry, since the two timestamps come from different clocks with an unknown offset.

Instead the desynchronisation is measured and written down as `quote_age_ms`. What the two
streams *do* share is the clock, which is why that number is always non-negative and means
anything at all.
