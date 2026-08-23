# EventPulse E2 Snapshot V2 Implementation Report

## Outcome

Implemented an additive `SnapshotProcessorV2` in `marketfeed-event-pulse`. It consumes strict `MechanicsInputV2`, retains MARKET semantics in V2, replays bounded decision prefixes, and authors canonical E1 mechanics snapshots with the exact per-family cursor projection frozen by the published root Snapshot V2 contract.

This result is repo-ready library evidence only. E2 remains `blocked:fixture-provenance`; fixture authoring, source qualification, capture, runtime, deployment, execution, and trading authority remain unverified and false.

## Contract binding

- Root merge: `4d3e0f0398d3e113a79df7ac901f38912eaa8edd`
- Root tree: `273163e3d06578065f7327a90a1b9fbfcded3a6d`
- Snapshot V2 contract SHA-256: `b9062e8e8bdc08e61f92b7890fe4d1dcebbb2eb975cc145c34ddf19f94be28af`
- Embedded contract and amendment paths are repository-bound `text eol=lf`.

## RED evidence

The first focused test compile failed before production implementation with:

```text
error[E0432]: unresolved import `marketfeed_event_pulse::SnapshotProcessorV2`
```

The regression remained and turned GREEN only after the additive public API and implementation were present.

## Implemented behavior

- Constructor binds a checked `ProspectiveCaptureAdmissionV2`, the matching non-forgeable truthful-empty System policy, and immutable Snapshot authoring.
- SYSTEM input is rejected before candidate source, feature, record, cache, seal, revision, or predecessor mutation.
- MARKET input stays `MechanicsInputV2`; family state stores and compares `MarketCursorV2` directly and feature eligibility is family-keyed end to end.
- Nonmarket CLOCK and COVERAGE records retain exact V1 wire semantics while projecting their own source identity, connection epoch, native cursor, availability, and payload hash.
- Strict replay order uses the shared V2 authoritative raw-coordinate comparator. Equal-time inputs are grouped before phase observation.
- Prefix storage is bounded at 65,536 and accepts repeated family records; projection authors the latest six MARKET, three CLOCK, and six COVERAGE cursors.
- Derived display packing is checked as `frame * 2^32 + action * 2^16 + item`; frame values above `2^31 - 1` return typed no-authorship without sealing or consuming a revision.
- Timestamp conversion uses Euclidean nanosecond-to-microsecond flooring, including negative sub-microsecond instants.
- Successful seal, revision, predecessor, and cache commit together only after canonical E1 validation; failed incomplete or unrepresentable snapshots leave all four unchanged.
- Direct strict inputs and canonical V2 JSONL replay produce byte-identical snapshots and content hashes.

## GREEN evidence

- `cargo test -p marketfeed-event-pulse --test snapshot_v2`: 7 passed.
- `cargo test -p marketfeed-event-pulse`: full EventPulse suite passed, including 54 V1 snapshot tests and all V1/V2 wire, cursor, replay, prospective, and preflight regressions.
- `cargo test --workspace`: passed, including doc tests.
- `cargo +1.85.0 test --workspace`: passed, including doc tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo +1.85.0 clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: `advisories ok, bans ok, licenses ok, sources ok`.
- `git check-attr text eol -- crates/event-pulse/contracts/snapshot-v2/*`: both embedded assets report `text: set`, `eol: lf`.
- `git diff --check`: passed.

## Residuals

- No fixture or evidence package was authored.
- No MFR transformer, adapter, capture, filesystem, network, runtime, source-qualification, paper, canary, live, execution, or trading surface changed.
- This slice does not promote EventPulse beyond its existing evidence/risk-only authority ceiling.

## Reviewer repair successor

### RED

The focused successor suite compiled 12 tests and failed five direct counterexamples before the repair:

- rejected native gap snapshot lacked `SEQUENCE_GAP`;
- mutated duplicate snapshot lacked `SEQUENCE_GAP`;
- Binance snapshot-overlap Book delta failed with `book delta sequence is not contiguous`;
- a cached decision `T` was re-authored with a new revision after ingest at `T+1`;
- the exact-family feature-capacity failure did not author `QUEUE_DROP`.

An additional optional-family regression then exposed that a family cause could author a flag without participating in feature owner eligibility; the OI row incorrectly selected `INSUFFICIENT_COVERAGE` instead of exact `SOURCE_INVALIDATED`.

### GREEN

- Rejected state-invalidating V2 MARKET inputs now commit only the invalidating candidate family state plus a preallocated, generation-bounded family fault event. They never become feature, clock, coverage, causal, or availability evidence.
- Family causes are replayed in authoritative input order, clear only on a valid greater generation for the same family, and participate in feature eligibility as well as flags.
- Exact-family feature capacity latches `QUEUE_DROP`, clears that family's feature/book/causal contribution, blocks same-generation evidence, and admits greater-generation recovery. An optional OI mutation invalidates only `open_interest_change`; the Trade feature does not inherit `SOURCE_INVALIDATED`.
- V2 Book feature projection distinguishes snapshot from delta: the first delta accepts `U <= lastUpdateId <= u`; later deltas use the cursor-validated exact `pu` chain. V1 Book behavior remains unchanged.
- A successful cached decision remains byte- and revision-identical after admissible future input; only a successful later decision replaces the cache and advances predecessor/revision.

### Successor gate evidence

- `cargo test -p marketfeed-event-pulse --test snapshot_v2`: 13 passed.
- `cargo test -p marketfeed-event-pulse --all-targets --all-features`: passed, including 54 V1 snapshot-mechanics tests.
- `cargo +1.85 test -p marketfeed-event-pulse --all-targets --all-features`: passed.
- `cargo test --workspace --all-targets --all-features`: passed.
- `cargo +1.85 test --workspace --all-targets --all-features`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo +1.85.0 clippy --workspace --all-targets --all-features -- -D warnings`: passed. (`+1.85` lacks the installed Clippy component; `+1.85.0` is the pinned installed Clippy toolchain.)
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: `advisories ok, bans ok, licenses ok, sources ok`.
- `git diff --check`: passed.

The successor remains repo-ready library evidence only. Fixture provenance, source qualification, capture, runtime, deployment, execution, and trading authority remain outside scope and unverified.

## Reviewer repair successor 2

### RED

The first counted-log regression failed exactly at `left: 15, right: 16`: the accepted log count omitted a replayable rejected MARKET fault. A second focused regression then failed at `left: 15, right: 16` for a mutated Clock record, proving sidecar invalidity was neither keyed nor retained in the V2 fault replay state. The Book recovery regression failed with `FeatureQueueDrop`, proving that a same-epoch accepted resnapshot could repair cursor state while leaving its V2 feature window invalid.

### GREEN

- The immutable topology now preallocates one bounded fault key for each of six MARKET families, three Clock sources, and six Coverage sources. Rejected state-invalidating inputs retain only the precise keyed candidate state and typed `Sequence` cause; no rejected payload becomes feature, clock, coverage, or causal evidence.
- Queue drops invalidate only their exact MARKET family, Clock, or Coverage slot. Their checked Euclidean-floored availability participates in the replayed causal maximum.
- `buffered_record_count` is the literal accepted-record plus fault-event total. The 65,536 global ceiling reserves 21 accepted recovery records (two per MARKET family, one per Clock/Coverage key) and 15 fault slots, leaving an ordinary capacity of 65,500. A public boundary regression fills that capacity, records a MARKET drop, admits both Warming and Live greater-generation records, records a Clock drop, admits its greater-generation recovery, and remains below the literal ceiling.
- Cursor gaps and mutations map uniformly to `Cause::Sequence`. A same-epoch accepted Book snapshot can recover only a Sequence resync and its exact V2 Book feature window. QueueDrop remains generation-latched and only greater generation receives its reserved recovery path.
- Clock/Coverage causes flow through the existing typed V1 consequence map, while MARKET causes remain family-keyed. Optional OI/LIQ invalidity therefore remains degraded and cannot invalidate unrelated Trade eligibility.

### Successor 2 gate evidence

- `cargo test -p marketfeed-event-pulse --test snapshot_v2`: 18 passed, including the literal 65,536-capacity path.
- `cargo test -p marketfeed-event-pulse --all-features`: passed, including 54 V1 snapshot-mechanics tests and every V1/V2 wire, cursor, replay, prospective, and preflight regression.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 test -p marketfeed-event-pulse --all-features`: passed.
- `cargo test --workspace --all-features`: passed, including doc tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 clippy -p marketfeed-event-pulse --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: `advisories ok, bans ok, licenses ok, sources ok`.
- `git diff --check`: passed.

This repair does not alter the authority ceiling: Snapshot V2 is repo-ready pure library evidence only, while E2 remains blocked on fixture provenance and all runtime, capture, evidence, execution, and trading authority remains false.

## Reviewer repair successor 3

### RED

The reserve audit found that each new fault replaced the key's recovery allowance, so repeated ordinary cycles could reset accounting before a boundary drop. It also found that a recovery candidate could commit invalidating source state or a capacity fault before downstream feature validation completed. New regressions cover twelve ordinary fault/recovery cycles before the literal ordinary boundary, a same-generation Book Sequence resnapshot at that boundary, an epoch-reuse failure, a malformed Book feature failure, and a valid retry compared with an independently constructed fresh processor.

### GREEN

- Every configured MARKET-family, Clock, and Coverage key now owns constructor-preallocated immutable fault-use and recovery-use counters. Ordinary history and ignored recovery duplicates do not spend those boundary reserves, and a used exact-key fault reserve fails closed with `SNAPSHOT_V2_FAULT_RESERVE_EXHAUSTED`; another configured key remains independently recoverable.
- Recovery sessions are bounded to the topology width. A same-generation Book snapshot can enter the reserved path only while that exact Book key has an active Sequence cause. QueueDrop still requires a strictly greater generation.
- Source state, feature runtime, active cause, accepted/fault log, last order, and reserve allowance are evaluated as candidate state. Epoch reuse or feature validation failure commits none of them; the original fault remains retryable.
- A valid retry after both failed recovery classes authors canonical bytes and a content hash identical to a fresh processor consuming the same committed prefix.

### Successor 3 gate evidence

- `cargo test -p marketfeed-event-pulse --test snapshot_v2 --no-default-features -- --test-threads=1`: 21 passed, including the literal-capacity tests.
- `cargo test -p marketfeed-event-pulse --all-targets --all-features`: passed, including 21 Snapshot V2 and 54 unchanged V1 snapshot-mechanics tests.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: `advisories ok, bans ok, licenses ok, sources ok`.
- `git diff --check`: passed.

This successor is still repo-ready library evidence only. No fixture, capture, source qualification, runtime, deployment, execution, or trading authority is claimed.

## Reviewer repair successor 4

### RED

The first boundary regression failed when Book generation 1 was accepted but the same PUBLIC connection's Quote generation 1 returned an error, proving recovery quota was allocated only to the triggering family. A second MARKET regression reached snapshot authorship only after explicitly refreshing its Trade/OI/Liquidation connection sidecars, confirming that lifecycle recovery must include subject Clock/Coverage state rather than leaving stale aggregate evidence readable.

### GREEN

- Every MARKET fault resolves its immutable configured connection and atomically allocates a bounded recovery plan for every family on that connection plus the subjects' Clock/Coverage keys. PUBLIC therefore owns Quote/Book together; MARKET owns Trade/OpenInterest/Liquidation together.
- The triggering valid greater generation activates one shared connection generation. Sibling MARKET families must use that exact generation, while sidecars must bind the recovered subject generation and preserve their own source-generation continuity.
- Connection advance clears subject Coverage feature eligibility transactionally; `SourceStateMachineV2` remains the lifecycle authority and fans the generation advance across sibling family and sidecar state. No MARKET V2 cursor is lowered into V1.
- Fault installation clones and validates the entire recovery topology before committing source, feature, cause, log, order, or reserve state. If any required key has exhausted its immutable quota, the new fault returns `SNAPSHOT_V2_RECOVERY_RESERVE_EXHAUSTED` mutation-free.
- The literal-boundary PUBLIC regression consumes both Quote/Book recovery records and its sidecar refreshes, rejects a second Book fault without changing the accepted/fault count, and produces canonical bytes/hash identical to an independently reconstructed processor. The Clock boundary regression independently proves exact-key exhaustion cannot steal another Clock key's reserve.

### Successor 4 gate evidence

- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p marketfeed-event-pulse --test snapshot_v2 --no-default-features`: 23 passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p marketfeed-event-pulse --all-targets --all-features`: passed, including 54 unchanged V1 snapshot-mechanics tests and all V1/V2 contract, cursor, replay, prospective, and preflight regressions.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: `advisories ok, bans ok, licenses ok, sources ok`.
- `git diff --check`: passed.

The authority ceiling is unchanged: this is repo-ready pure Snapshot V2 library evidence. Fixture provenance, source qualification, capture, runtime, deployment, execution, and trading authority remain outside scope and unverified.

## Reviewer repair successor 5

### RED

An independent below-capacity counterexample activated MARKET recovery with Trade generation 1, then supplied OI generation 2. The processor accepted it as ordinary input because recovery matching was exact-key-only; this advanced the shared connection and poisoned the valid OI generation 1 retry. The same false green existed for PUBLIC Book generation 1 followed by Quote generation 2.

### GREEN

- Before capacity handling or any candidate mutation, MARKET ingest now resolves any remaining recovery session for the input's immutable configured connection. Once the plan has an activated generation, every MARKET family on that connection must use that exact generation.
- The connection lookup remains effective after the triggering family's own two-record recovery allowance is consumed and its exact session is removed, as long as any sibling recovery remains active.
- Rejected drift does not change source state, feature runtime, accepted/fault count, last order, active causes, recovery quota, cache, seal, revision, or predecessor. An exact-generation retry at the same authoritative coordinate succeeds.
- MARKET Trade/OI/Liquidation and PUBLIC Quote/Book regressions compare the repaired processor against an independently reconstructed processor that never saw the rejected drift; canonical JSON and content hashes are identical.

### Successor 5 gate evidence

- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p marketfeed-event-pulse --test snapshot_v2 --no-default-features`: 24 passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: passed all advisory, ban, license, and source checks.
- `git diff --check`: passed.

The successor remains repo-ready pure library evidence only. Fixture provenance, capture, source qualification, runtime, deployment, execution, and trading authority remain unverified and outside scope.

## Reviewer repair successor 6

### RED

The unactivated-plan counterexample faulted a connection through its configured trigger, then sent a greater-generation sibling before the trigger's recovery input. Both MARKET OpenInterest and PUBLIC Quote could activate the plan because the generation guard only constrained an already activated generation. This bypassed the immutable trigger identity and could consume shared lifecycle state before the intended recovery record.

### GREEN

- MARKET ingest now resolves any recovery plan on the input's immutable connection before capacity handling. When the plan has no activated generation, only the exact `connection_trigger` key may proceed, and only when the existing typed recovery predicate accepts its generation and cause.
- A sibling cannot activate the plan even with a greater generation. The permitted same-generation exception remains limited to an exact Book trigger repairing a Sequence cause; QueueDrop still requires greater generation.
- Trigger identity and generation are checked before source, feature runtime, accepted/fault count, order, recovery allowance, cache, seal, revision, or predecessor mutation.
- Below-capacity MARKET and PUBLIC regressions, plus the literal-boundary PUBLIC path, reject sibling drift mutation-free, activate through the exact trigger, accept exact-generation siblings, and match independently reconstructed fresh processors byte-for-byte and hash-for-hash.

### Successor 6 gate evidence

- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p marketfeed-event-pulse --test snapshot_v2 --no-default-features`: 24 passed.
- `cargo test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 test --workspace --all-targets --all-features`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo +1.85.0 clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: passed all advisory, ban, license, and source checks.
- `git diff --check`: passed.

The authority ceiling is unchanged: this is repo-ready pure Snapshot V2 library evidence. Fixture provenance, source qualification, capture, runtime, deployment, execution, and trading authority remain outside scope and unverified.
