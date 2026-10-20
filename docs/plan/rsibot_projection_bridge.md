# Direct Quants MFOP1 reader

cryptofeed-rs owns acquisition, native semantics, durable publication and replay.
The `marketfeed-observations` binary is a read-only structural MFOP1 reader for
the opt-in `core.market_observations.cryptofeed_projection` Quants client.
hlscreen and Quants consume independently; neither is a second collector.

This slice does not wire live transport, calculate strategy features, persist
feature state, authenticate native financial values or admit research datasets.
The actual native-source comparison remains a separate producer operation.
Historical usable time remains unknown, even when original exchange, receipt
and owner-time approximation fields are present.

## Protocol and application boundary

The binary accepts one UTF-8 JSONL request at a time, with no command-line
arguments. Every request/response has `schema_version: 1` and exactly one command
or result object. Requests are `open`, `next` and `ack`:

```json
{"schema_version":1,"open":{"directory":"/absolute/feed","resume":null}}
{"schema_version":1,"next":{}}
```

`opened` returns the acknowledged checkpoint. An `offer` carries a process-local
ID, the exact `before`/`after` cursor pairs, the full typed projection group and
its seal state. `ack` must name that exact ID and `after` checkpoint; the matching
`acknowledged` response is required before the client publishes progress. A
cursor pair contains `target` and optional `source` cursors, each with `feed_id`,
`sequence` and `offset`. IDs alone are not restart checkpoints.

The reader stages only one pending group. Another `next`, mismatched ACK,
duplicate open, malformed/oversize request or storage/validation failure stops
the process. EOF with an unacknowledged offer fails. `caught_up` preserves the
last ACK pair and seal state. Empty, bootstrap, control and seal groups follow
the same application/ACK protocol as market groups; there is no metadata prescan
or market-only filtering.

All four result kinds explicitly report `validation: "mfop1_structure"`,
`native_source_verified: false` and `availability_time_ns: null`. Successful
structural checks cannot establish original native-value authenticity, continuity,
historical feature availability or economic suitability.

The synchronous Python callback must finish the whole application and return
exactly `None`. Exceptions, async/lazy returns and all other return values poison
the client before ACK. An ACK error/timeout after callback success also poisons
it while preserving the old published checkpoint. Discard provisional state and
rebuild; no exactly-once external side-effect or durable feature claim follows.
Native coroutine/generator objects are closed on rejection; custom awaitables
and async generators are never automatically driven.

## Bounds and restart

Complete request lines, including LF, are capped at 16 KiB. Complete responses
are capped at 16 MiB and fully serialized into bounded staging before emission.
The projection limits are 8 MiB/group, 8,192 items, 10 owners, 16 MiB aggregate
bootstrap payloads, 10,000 catalog entries and 4,096 pending inputs. These are
logical/serialization limits: the underlying feed reader separately permits a
32 MiB outer record, and no RSS or uninterruptible-filesystem bound is claimed.

The Python client verifies an explicit executable SHA-256, uses bounded POSIX
pipe I/O, bounds stderr and response waiting, and kills/reaps its owned process
group on failure. Callback execution, process creation and filesystem hashing
remain synchronous OS/application work. Per-message fingerprinting has a measured
cost; this slice grants no live-throughput guarantee. macOS/Linux are supported;
the Python pipe implementation explicitly refuses unsupported operating systems.

`resume` accepts one exact paired checkpoint. Rust reconstructs its validator and
metadata in physical order up to that pair; this is bounded-memory prefix replay,
not a bounded-time seek or restoration of Python features. The caller must restore
and bind its own state, source/configuration/version and cursor together before
continuing feature work. The client persists no feature checkpoint.

## Build, diagnostics and verification

```sh
cargo build --locked -p marketfeed-daemon --bin marketfeed-observations
```

Use the exact binary hash with the Quants client. The optional Quants
`scripts.data.observe_cryptofeed_projection` CLI records bounded diagnostic
counters and the last acknowledged pair into a new report path. Counters are
staged per offer and published only after ACK; failures retain the last confirmed
state. Its report is diagnostic progress, not a strategy checkpoint. A sealed
feed exits immediately after its final group; replaying it repeatedly is not a
live data soak.

Acceptance includes actual public-factory spot/perps captures through native
reexecution, committed MFOP groups, the real bridge subprocess and the real Quants
client; every paired resume boundary; growing committed prefixes; exact native
values; malformed/oversize I/O; pending EOF; wrong ACK; callback/lazy failure and
uncertain ACK cleanup; and preservation of legacy CLI and manifest behavior.
Keep failed preflights and source-specific receipts. Source/artifact pins, local
fixtures and hosted checks qualify only their recorded scope.

The [Razer verification contract](razer_shared_feed_verification.md) retains the
live causal transport, durable outbound intent/result, persistent feature-state,
strict availability/dataset and extended operational gates. No model/economic
run, orders, activation, deployment, merge or release is included here.

## Source-bound local result

The final 550-input source `0a25563a511e018cb22b19520b66465ed5066d195130797aebce86e8ee5a1ce8`
passes all eight local gates: 1,580 workspace tests, 57 bootstrap and 67 public
owner/native tests on Rust 1.85, formatting, diff, Clippy/docs with denied warnings,
and the documented default daemon command. The Rust bridge tests exercise actual
spot and perps native captures. The exported perps fixture passes the real Python
client at all 78 groups and all 79 paired resume boundaries, including callback
exception and async-return rejection before ACK. The actual diagnostic CLI passes
sealed 78-group/15-item and five-group stops with exact cursor pairs and unchanged
fixture files. Quants passes 37 combined tests; these are local infrastructure
checks, with no Linux consumer or live/feature qualification implied.

Independent review reproduced and closed the null-present command-sibling gap,
and the Quants async/lazy false-ACK bug. Existing daemon default execution is
preserved with `default-run = "marketfeed"`; the actual legacy CLI failure and
repair are retained. Exact sources, artifact hashes, raw-log hashes, independent
receipt identities and preserved failures are in the
[local qualification](../evidence/rsibot-projection-bridge-20261020/validation.json).

Resume-check scope: Each of all 79 paired cursor boundaries reopens and checks exactly one next group (or final sealed idle); this is not a full remaining-suffix replay.

Publication constraint: GitHub rejected the initial push because the credential
lacks workflow-edit scope. The proposed extra hosted default-CLI step remains
in the preserved local patch; publication retains the parent workflow. All 550
compiled inputs and eight local check results are unchanged, including actual
default-CLI exit zero. The qualification and publication workflow hashes are
recorded separately; no extra hosted CLI regression step is claimed.
