# CLAUDE.md — FiniexDataCollector Project Rules

Rules and conventions for working in this repository, for assistants and humans alike.
Sister documents exist in FiniexTestingIDE and FiniexRAGEngine; where a rule is shared
across the Finiex projects it is worded the same way on purpose.

This collector has one job: produce tick files that another project can import without
guessing. Almost every rule below exists because a field once said something it did not
know.

---

## AI-assisted development

This project is built with AI assistance (Claude Code). That is stated openly rather than
hidden: the conventions in this file exist so a session produces the same kind of work the
last one did, instead of re-deriving the project's habits from the README and a long
`main.py`.

Every line is reviewed by the operator before it is committed. The assistant never commits.

---

## Working style

- **State confidence, ask when low.** Communicate implementation confidence as a
  percentage. Below ~95 %, or when a change is public-facing or hard to reverse, ask
  focused, numbered questions before executing instead of guessing.
- **Addressing.** The human is "the operator"; German (informal *du*) is fine in chat.
  **All artifacts stay English** — code, comments, docs, issues, commit messages, handover
  documents. The language of the conversation never sets the language of a file. A German
  chat about a runbook still produces an English runbook.
  **Gitignored is not an exemption.** `ISSUE_*.md`, `INTERNAL_*.md` and `SESSION_*.md` are
  artifacts too — private, not exempt.
- **Verify before reporting.** A claim about this repository is checked against this
  repository. A claim about the consuming project is checked against its validator, not
  against a copy of its rules kept here — a copy drifts, the original is the gate.
- **Measure rather than assume.** Several decisions in this project were made from numbers
  taken off the real archive (spread distribution, arrival lag, quote staleness). When a
  number is cheap to obtain, obtain it.

## Architecture planning

Plan first, build second. Non-trivial changes get a plan the operator sees before
implementation starts. Every plan ends with an architecture confidence in percent; below
~95 % it asks numbered questions instead of guessing.

## Commit policy

- **Never create git commits.** The operator commits manually after reviewing each change.
- **Commit messages describe the change, not the tooling** — concise and imperative, no
  automated trailers.
- A large mechanical change (a formatting convention, a rename sweep) is its own commit
  with nothing else in it, or it is not done at all. Half-converted is worse than either.

## Versioning & releases

Two version numbers exist here and they are **not** the same thing. Confusing them is the
easiest way to tell a consumer something untrue.

- **App version** — `MAJOR.MINOR.PATCH`, what this program is. It lives in
  `configs/app_config.json`, is mirrored by the `AppConfig` Pydantic default, and appears a
  third time in the README status line. `tests/utils/test_app_config.py` guards all three;
  bump them in one change.
- **`data_format_version`** — what the *output files* promise, a constant in
  `python/types/tick_types.py`. Scoped per collector: it does not move in lockstep with the
  app version, nor with the MT5 collector's. It moves when a field is added, removed, or
  changes meaning — and FiniexTestingIDE is told in the same breath, because a consumer
  reading an unchanged version number will not look for a changed field.
- **The operator tags releases.** The assistant never runs `gh release` and never tags,
  the same way it never commits and never closes issues. Bump the version string in the
  change that finishes the batch; the tag follows when it merges.
- **Release notes are the tag's description on GitHub.** `export_github_issues.sh` pulls
  them into `github_issues/release_notes/<tag>.md`, which is where a session looks to see
  what a past version actually delivered.

## Session start

Read first, in order:

- **GitHub issue #8** — *FiniexDataCollector Vision & Roadmap*.
- **GitHub issue #11** — the tick data contract: what the output states about itself and
  why. Anything touching the file format starts here.
- The latest `SESSION_*.md` in the project root, if one exists — build state and next
  steps from the previous session.
- `github_issues/` — a local snapshot of the tracker, refreshed with
  `export_github_issues.sh` (gitignored, run from the host). The dev container has no
  GitHub API access of its own, so the snapshot is how a session gets issue context.

---

## The output contract

This is the part that must never drift silently. FiniexTestingIDE's import pipeline reads
these files; a field that changes meaning without changing its version costs an archive,
not a bug report.

### What a file states about itself

| Field | Meaning |
|---|---|
| `data_format_version` | Schema version of the collector output. A **constant of the code**, not a configurable input — it identifies the code that wrote the file. Scoped per collector; it does not move in lockstep with the MT5 collector. |
| `collected_msc_timebase` | Time base of `collected_msc`. `"utc"` from 1.5.0. Its **absence** means device-local, and the importer refuses to guess. |
| `data_collector` / `broker_type` | Who wrote it and which broker profile applies. `broker_type` selects the import offset registry entry. |
| `anchor_resyncs` / `anchor_max_correction_ms` | Clock corrections absorbed, cumulative over the session. In the header at file open, repeated in `summary.anchor` at close. |
| `quote_age_ms` | Age of the quote `bid`/`ask` were taken from. `null` — never `0` — when no quote was known. |
| `start_time` | Mandatory. An import without it fails. |

`DATA_FORMAT_VERSION` and `COLLECTED_MSC_TIMEBASE` live together as module constants in
`python/types/tick_types.py`. They are referenced, never retyped: the version was once a
literal in two places, which is exactly the shape a drift bug takes.

### The rule underneath all of it

**A field must never assert something the code does not know.**

The MT5 collector wrote `broker_utc_offset_hours: 0` into 4410 files while the truth was
`+3`. Nothing was broken — a number was simply stated with more confidence than it was
held. That is why:

- `broker_utc_offset_hours` was removed from this collector's output rather than corrected.
  The offset belongs to the importer's registry, which knows it, not to the writer, which
  does not.
- `quote_age_ms` is `null` and not `0` when no quote has been observed. Zero would claim a
  quote seen in that same millisecond.
- An estimated value never enters the archive looking like a measured one. Reconstruction
  of historical spreads belongs in the consumer's cost model, where it is a parameter that
  can be varied — not baked into data where it is permanent and indistinguishable from a
  measurement.

### The invariants an import enforces

A file is rejected whole — up to 50,000 ticks, irreversibly, because the importer never
repairs — if any of these fail:

1. Row count matches `summary.total_ticks`.
2. `time_msc` never steps backwards. **Non-decreasing, not strictly increasing** — two
   ticks legitimately share a millisecond when a market order sweeps the book.
3. `collected_msc` never steps backwards, same rule.
4. `collected_msc` sits within the plausibility window of `time_msc` (±30 s).
5. `timestamp` agrees with `time_msc` within one second (the string is truncated to the
   second).
6. Prices are positive and not crossed (`ask >= bid`).

`tests/writers/test_json_tick_writer.py` mirrors these locally. That mirror is a guard, not
the authority: before calling a format change done, run the real
`TickImportValidator.validate_file()` from FiniexTestingIDE against files a live collector
actually wrote.

### Atomic writes are load-bearing; `.lock` files are not

`_atomic_write()` creates the file with `tempfile.mkstemp(suffix=".tmp")` in the target
directory and `os.replace()`s it into place at finalize. A `*_ticks.json` therefore **never
exists in a partial state** — there is no window in which a consumer can read half a file.

The `.lock` sidecar is decoration. No consumer reads it. Do not "simplify" the atomic write
into a direct `json.dump` to the final path: the lock file would not catch what that breaks.

---

## The time model

- **One clock per collection session.** `CollectionClock` in
  `python/utils/collection_clock.py` is created once and handed to the parser that stamps
  and to every writer that reports. A globally non-decreasing series is non-decreasing in
  every subsequence, so one clock keeps every symbol's file monotonic — and a clock step is
  counted once, not once per symbol.
- **All timestamps UTC, timezone-aware.** The two deliberate exceptions are
  `local_device_time` in the metadata (its purpose is to be the machine's wall clock) and
  the terminal live display.
- **`time.time()` is not monotonic**, and a backwards NTP step would cost the file. The
  clock clamps a backwards reading to the previous value and counts it.
- **`time.monotonic()` is not a substitute.** It has no epoch, so its values cannot be
  compared against the exchange event time in `time_msc`. `collected_msc` has to be both
  epoch-based *and* non-decreasing; that combination is what the clamp produces.
- **Never derive a duration by subtracting two wall-clock readings.** A clock correction
  between them produces a negative duration.
- **One reading per moment.** The `timestamp` string is derived from `time_msc`, not read
  from the clock a second time — a second reading can land on the other side of a
  correction and disagree with the value it is supposed to describe.

---

## Quotes and spreads

Kraken's `trade` channel reports executions, and an execution happens at one price. Without
the `ticker` channel every tick carries `bid == ask` and a spread of zero — correct, and
expensive: a backtest paying no spread is too favourable, and a sweep then optimises
against a cost that does not exist.

- **Ticker updates feed `QuoteCache` and produce no ticks of their own.** This is not a
  style choice. A ticker tick's `time_msc` is our local receive time; a trade tick's is the
  exchange event time, trailing it by a measured median of 8–11 ms. Interleaved in one
  file, the two step `time_msc` backwards on nearly every channel alternation and the file
  is rejected.
- **`event_trigger: "bbo"`, not the API default `"trades"`.** The default fires at the
  moment just after an execution, when the trade has consumed the top of book and it stands
  momentarily wider — a systematically unrepresentative sample, then carried forward until
  the next trade. Measured on ETHUSD it inflated the median spread by a factor of 21.
- **A crossed quote is dropped, not stored.** The previous quote stands and ages visibly
  through `quote_age_ms` rather than failing invisibly.
- **The spread belongs in the data; fees do not.** The spread is a property of the public
  order book — identical for every participant, observable once, valid for all. Fees are
  account-specific (Kraken has volume tiers) and belong in the consumer's cost
  configuration, never in a tick file.

---

## Code conventions

- **Double quotes**, `autopep8` + `isort`. The sister projects are on `ruff` with single
  quotes; do not mix the two conventions here. Converting is a decision of its own, taken
  as a single mechanical commit or not at all.
- **Module docstring with a `Location:` line** at the top of every file, matching the
  existing files.
- **Google-style docstrings** with `Args:` and `Returns:` on public functions.
- **Comments explain why, not what.** A comment that restates the line above it is noise; a
  comment naming the failure a line prevents is the reason the line survives a refactor.
- **Dead code is removed on sight, not noted for later.** The same applies to dead
  configuration: a key with no reader goes.
- **No maintained counts in prose** — not in a heading, not in a sentence. What counts at
  runtime may count; what a human has to keep in step will go stale. The README said "8
  crypto pairs" directly above the list of eight.

---

## Testing

- `pytest` from the project root; `pytest.ini` sets `pythonpath = .` so the suite runs
  regardless of how it is invoked.
- **Fixtures are synthetic.** No test depends on a live WebSocket or on collected data.
  `tests/conftest.py` builds tick series and a broker config through the real loading path.
- **Time is driven, not waited for.** The `steerable_clock` fixture patches the clock's time
  source so a backwards NTP step is provoked deterministically. Waiting for a real one is
  not a test strategy.
- **Mutation-check new tests.** After writing a test for a guard, break the guard and
  confirm the test fails. A suite that stays green when the clamp is disabled is decoration.
  This caught a genuinely weak test: it collected its samples only *after* the clock step,
  so the series looked monotonic either way.
- **Test the contract, not the implementation.** The invariant tests assert what the
  importer enforces, so they keep meaning after a refactor.

---

## The container is disposable — its home is not

`docker-compose.yml` mounts `~/.claude` into the container. Without it, a rebuild deletes
every session transcript — silently, with no prompt. A sister project lost an entire
project history that way.

Docker here is the **development** environment: the compose service runs
`tail -f /dev/null` and never starts the collector. Production runs from a virtualenv on
the server, the same pattern the sister projects use — the box is Windows, the collector
has to read a local broker terminal's output directory, and a container buys isolation
nobody needs at the cost of a Linux VM.

**A hard kill costs the in-memory buffer.** Ticks buffer until rotation or `finalize()`, so
a service stop path that does not deliver a graceful signal loses up to a full file per
symbol. Any service wrapper must be configured to send an interrupt, not to terminate.

---

## The cross-project bus is operator-initiated

The shared folder at `/bus` carries messages between the Finiex projects. Four rules come
with it, because the bus has no locking and no read receipts:

- **Never read or write it unprompted** — not at session start, not "while I am here", not
  because a message might have arrived. Writing lands in another project's inbox; reading
  pulls another project's material into this conversation. There is deliberately no
  mechanical gate: two were built on the existing peers, both worked, and both were removed,
  because a prompt standing behind the rule invites the discipline to be delegated to a
  dialog box.
- **An empty inbox is not evidence that nothing arrived.** A `note` enters no inbox and an
  `answer` closes the item it answers, so both are invisible there. Pair `bus_inbox` with
  `bus_threads` and report "nothing" only when both are empty.
- **Re-read the topic immediately before sending**, not once before composing. The window
  between reading a thread and writing into it is where crossed messages are born.
- **Name your session in the first line of every message**: `[chat: <first 8 of the session
  id> · <what this chat is working on>]`, then a blank line, then the message.

This peer is `datacollector`. Setup lives in `.mcp.json` (gitignored; `.mcp.json.example` is
the tracked counterpart) and the `/bus` mount in `docker-compose.yml`, indirected through
`FINIEX_BUS_PATH` so no private path reaches a public file.

---

## Issues

- `ISSUE_*.md` in the project root are drafts for transfer to the tracker (gitignored).
- **Draft → operator review → upload on OK.** Never push an issue to the tracker
  unprompted. On the operator's OK: create or patch on GitHub, **verify by re-reading what
  the tracker now holds** — a success exit code is not evidence the body arrived intact —
  then delete the root draft and refresh the `github_issues/` snapshot.
- **Comments vs body:** additions to a not-yet-begun issue go into the **body**; the body
  stays the spec. Once implementation has started, progress and decisions land as dated
  implementation-notes comments.
- **List issues as a checklist, never a table:** `- [ ]` / `- [x]` + `#N` + a short
  description — not the title, which GitHub renders from the `#N` reference.
- **Never close issues.** The operator closes them at merge. Ticking a roadmap checkbox to
  show progress is fine; `gh issue close` is not.
- Code references use the form `path/to/file.py#L12-L20`.

---

## Project layout

```
python/
  collectors/kraken/    websocket_client, message_parser, quote_cache
  writers/              base, json_tick_writer
  types/                tick_types (incl. the format constants), broker_config_types
  utils/                collection_clock, config_loader, logging_setup, live_display
  alerts/               telegram_bot
  scheduler/            weekly_jobs
  main.py               CLI entry point: collect | status
tests/
  collectors/ writers/ utils/   mirroring the source tree
configs/                tracked defaults; user_configs/ overlays them and is gitignored
```

**Configuration overlay:** `configs/app_config.json` is the tracked baseline with every
credential blank and disabled; `user_configs/app_config.json` overrides it by deep merge and
is gitignored. A live credential must never appear in the tracked file.

---

## After each feature (five-point review)

"Code done" is not "done". When a feature or fix is finished, walk these five and state what
each needs — the operator decides and applies:

1. **Tests** — new behaviour gets tests; changed behaviour updates them; mutation-check them.
2. **Docs** — new structures get documented, touched flows get their doc updated.
3. **README** — check whether the change touches it (status, quickstart, output format).
4. **Issues** — fold implementation decisions and deviations back into the issue the work
   came from.
5. **The consuming project** — if the output contract moved, the version moves with it and
   FiniexTestingIDE is told. A format change nobody was told about is the failure this
   project is built to avoid.
