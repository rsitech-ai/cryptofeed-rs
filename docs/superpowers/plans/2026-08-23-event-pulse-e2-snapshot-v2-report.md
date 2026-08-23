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
