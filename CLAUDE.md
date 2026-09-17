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
  **Gitignored is not an exemption.** `ISSUE_*.md`, `INTERNAL_*.md` and `HANDOFF_*.md` are
  artifacts too — private, not exempt.
- **Verify before reporting — against the repository, never against memory of it.** A claim
  about this repository is checked by reading this repository. A claim about the consuming
  project is checked against its validator, not against a copy of its rules kept here.
  Measured 2026-09-15: three corrections had to be sent to another project in two days, and
  all three were statements made from an intention rather than a check — a commit hash that
  was never pushed, three files that had been deleted, and a README line reported as fixed
  that had never been touched. Each cost one grep to prevent.
- **A closing summary states what IS, not what was meant.** "Corrected:" and "Fixed:" are
  claims about the working tree; confirm them there before writing them down. The operator
  reviews the diff against the report, so an item listed as done and absent from the diff
  costs more attention than it saved.
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
- The latest `HANDOFF_*.md` in the project root, if one exists — build state and next
  steps from the previous session. **A handoff is a snapshot, not a document.** It opens
  with the UTC timestamp it was written at and the commit it describes, so staleness is
  visible in one line — check that stamp against `git log -1`, or against `/v1/build` where
  a collector is running, before trusting a number in it. **The operator deletes it once the
  new chat has taken it in; the assistant never does**, the same way it never commits and
  never closes issues. The deletion is the point: a handoff left lying around is read weeks
  later as if it were current. Anything worth keeping for the record moves to
  `github_issues/root_internal_archive/` — the project root holds only what is currently in
  play.
- `github_issues/root_internal_archive/production_server.md` — what runs on the
  production box, which ports are taken, where TLS terminates, and what must not be
  restarted. Read it before touching the server or choosing a port: three Finiex
  services share that machine, and the two things a session has got wrong there so far
  were both assumptions this file now answers. It is a reference rather than a
  snapshot, but it carries the date it was read off the machine — check a version
  number against `git log -1` or `/v1/build` on the box before trusting it.
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

### Durability: two mechanisms, different jobs

**Atomic write protects the reader.** `_atomic_write()` creates the file with
`tempfile.mkstemp(suffix=".tmp")` in the target directory and `os.replace()`s it into place, so
a `*_ticks.json` **never exists in a partial state**. Do not "simplify" it into a direct
`json.dump` to the final path.

**The write-ahead log protects the data.** Every tick is appended to a `.jsonl.part` sidecar
and flushed before it counts as collected, and that log is deleted **after** the archive file
is written, never before. A window with the data in two places is recoverable; a window with
it in neither is not.

Both are explained in `docs/architecture/durability.md`, including every recovery case. Read
it before touching `_finalize_current_file()` or `recover_orphaned_buffers()` — the ordering
looks arbitrary and is not, and on the happy path both orderings end identically, which is
what makes the wrong one survive a review.

The `.lock` sidecar was **removed** in 1.6.0. Nothing ever read it — not here, not in the
consuming project — while the README claimed it prevented processing of active files. The
write-ahead log now marks an open file and carries its contents.

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

## Documentation

Docs live in `docs/`, and `docs/documentation_index.md` is the navigation point — links only,
order within a section is reading priority. **Keeping them current is not optional**, the same
way tests are not.

- `docs/architecture/` — the output contract, the time model, durability
- `docs/operations/` — running the collector
- `docs/tests/` — what each suite defends and why it exists

Three rules carried over from the sister projects, because they are what keeps a doc worth
reading:

- **Open with the problem, not a label**, and say what is NOT in the document. A reader who
  landed in the wrong file should find that out in the first three lines.
- **A document that carries a convention is named HERE in the same change.** A guide nobody is
  pointed at is a guide nobody follows. This applies to corrections too: if a fix changes what
  a future session should believe, it is not finished until the place that session reads has
  been updated.
- **No maintained counts in prose.** Not in a heading, not in a sentence. What counts at
  runtime may count; what a human has to keep in step will go stale.

New structures and features get documented; a touched flow gets its doc updated. A new test
suite gets an entry in `docs/tests/test_overview.md` in the same change.

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
- **Mutation-check new tests, every time.** After writing a test for a guard, break the guard
  and confirm the test fails. This has caught four tests that were green against a broken
  implementation, two of them written the same hour: one collected its samples only *after*
  the clock step so the series looked monotonic either way; one exercised an overwrite guard
  with a log that had no ticks, so a different branch spared the file; and the write-ahead
  ordering could not fail on the happy path at all — it needed a write that throws.
  **A guard whose failure mode only appears when something else fails needs a test that makes
  that something else fail.**
- **Before a route counts as done, start the collector and call it.** Not a suite
  requirement — a hand check, once, after startup is through. `TestClient` is an in-process
  ASGI transport: it never binds a port, never starts the server, never exercises the bind
  address. Twenty-eight API tests passed while nothing had ever listened. The bind address
  is what this catches — `0.0.0.0` is required inside a container, where containment comes
  from the compose publish, and wrong on the server, where the process runs in a virtualenv
  with no publish rule in front of it.
- **Test the contract, not the implementation.** The invariant tests assert what the
  importer enforces, so they keep meaning after a refactor.

---

## The container is disposable — its home is not

`docker-compose.yml` mounts `~/.claude` into the container. Without it, a rebuild deletes
every session transcript — silently, with no prompt. A sister project lost an entire
project history that way.

**The mount protects against a rebuild, not against switching environments.** A session's
transcript folder is derived from the working directory, which is `/app` in the container
and the Windows path on the host — two folders for one session id. A session continued in
the other environment finds only the half written there; measured 2026-09-15 on one session
split 3313 / 1748 entries, with 1565 reachable from one side only. **Pick one environment
per project and stay in it.** The container is the one to pick: the bus tool server resolves
`/bus`, which exists nowhere else. All containers share `/app`, so several projects' sessions
land in one folder — an inconvenience, not a loss.

Transcripts are archived daily by a scheduled task into `~/.claude/conversation_backups/`,
verified by checksum. That archive is what survives a mistake in any of the above; the
folder's README carries the restore command.

Docker here is the **development** environment: the compose service runs
`tail -f /dev/null` and never starts the collector. Production runs from a virtualenv on
the server, the same pattern the sister projects use — the box is Windows, the collector
has to read a local broker terminal's output directory, and a container buys isolation
nobody needs at the cost of a Linux VM.

**A hard kill costs one tick, not a buffer — since the write-ahead log.** Every tick is
appended to the `.jsonl.part` sidecar and **flushed** before it counts as collected
(`_append_to_wal`), so a process killed outright loses at most the tick in flight and the
next start rebuilds the file from the log. This paragraph said the opposite until
2026-09-17, and the difference matters: the old wording argued against any service wrapper
that cannot guarantee a graceful signal, and that argument is now obsolete.

**It still holds for a build older than the write-ahead log**, which is what production ran
until the 1.7.0 rollout: there the buffer lives in RAM and nowhere else, up to
`max_ticks_per_file` per symbol. Check which build a process is before deciding how
carefully it has to be stopped.

A graceful stop is still preferable — it closes files instead of leaving logs to recover
from — but it is no longer the difference between keeping and losing the data.

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

**Never name a commit hash before it is on the remote.** A local hash is invisible to the
recipient and can be rewritten under them by an amend — which is exactly what happened to the
1.6.0 announcement. Announce the version, and send the hash when it is pushed.

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

**Archive boundaries:** a file closes at `max_ticks_per_file` or at the UTC day boundary,
whichever comes first, and therefore covers exactly one day. `docs/architecture/output_contract.md`
has the reasoning; the day cut is checked *before* a tick is appended, unlike the count.

**Configuration overlay:** `configs/app_config.json` is the tracked baseline with every
credential blank and disabled; `user_configs/app_config.json` overrides it by deep merge and
is gitignored. A live credential must never appear in the tracked file.

---

## The closing report

**It is the only thing guaranteed to be read.** A long turn scrolls, a session is resumed, a
compaction folds the middle away — so a finding, a measurement or a question posted along the
way cannot be assumed to have arrived. The report is not a summary of what the operator
already knows; it is the first and possibly only delivery. Repeat rather than reference: "as
mentioned above" points at something that may not be visible.

**Fixed structure, in this order.** The further up, the more decision it carries. A section
with nothing in it is omitted, never printed empty.

1. **Which issue** — the number, which PART of it, and explicitly what is NOT in it. Ends
   with a **proposed commit message**, ready to paste into `git commit -m`: ONE short line,
   roughly 50-70 characters, naming what changed rather than repeating the issue's title.
   Where the work splits into commits a reviewer would want apart — a behaviour change sitting
   beside a rename, say — propose one line per commit and say which goes first and why. Never
   an attribution trailer of any kind; the assistant proposes the text and never commits.
2. **Suite** — the pass count and how the delta accounts for itself.
3. **What was built** — with the measurements.
4. **What to watch in the review** — where the work is least certain, where it deviated from
   the plan, and above all **what could not be verified here**. This project cannot exercise
   the production platform: the dev container is Linux, the server is Windows. Signal
   handling, console encoding and anything touching `.ps1` is unverified by construction, and
   that line is what decides where the operator spends their attention.
5. **Open findings** — full format.
6. **Open minors** — one numbered line each.
7. **Fixed directly** — one line each, with its finding number.
8. **Open questions** — re-asked until answered, in their own section so they cannot sink
   into prose.

**Findings are numbered continuously across the session, and a number is permanent.** It
identifies that one finding, is never reused, and never shifts when another closes — which is
what makes "Befund 7" mean the same thing in an hour. Only OPEN findings are presented, so a
list has gaps, and a gap is information: that number is closed, not missing.

**Every presented finding carries four things:**

1. a link **with a line anchor** — `path/to/file.py#L78-L88`, never a bare path. A config file
   is no exception. A finding without a location moves the search to the operator.
2. **(a) urgency** — `EMPFOHLEN` / `KANN WARTEN` / `NUR WENN X`, with the reason in the line.
3. **(b) effort** — `KLEIN` / `MITTEL` / `GROSS`.
4. **(c) confidence in %**.

**Confidence in the FINDING and confidence in the FIX are two numbers.** An item can be a
provable defect at 100 % whose repair is a design decision at 60 % — and then it is reported,
not built. A fix is taken directly only at effort KLEIN **and** ≥ 97 % on a low-risk item, and
**every direct fix is named in the report with its number**: a change nobody announced is
indistinguishable from one nobody asked for.

## After each feature (five-point review)

"Code done" is not "done". When a feature or fix is finished, walk these five and state what
each needs — the operator decides and applies:

1. **Tests** — new behaviour gets tests; changed behaviour updates them; mutation-check them.
2. **Docs** — new structures get documented, touched flows get their doc updated, a new
   test suite gets its entry in `docs/tests/test_overview.md`. See the Documentation section.
3. **README** — check whether the change touches it (status, quickstart, output format).
4. **Issues** — fold implementation decisions and deviations back into the issue the work
   came from.
5. **The consuming project** — if the output contract moved, the version moves with it and
   FiniexTestingIDE is told. A format change nobody was told about is the failure this
   project is built to avoid.
