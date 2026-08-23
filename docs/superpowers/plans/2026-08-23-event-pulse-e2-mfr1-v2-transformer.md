# EventPulse E2 MFR1 MechanicsInputV2 Transformer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded, pure-offline Binance routed-v4 MFR1 transformer that authors strict `MechanicsInputV2` directly, including exact venue provenance and reserved V1 non-market drop inputs, without changing V1 behavior or claiming fixture/runtime authority.

**Architecture:** `marketfeed-event-pulse-mfr1` remains the offline orchestration leaf. A small internal validated-segment driver owns complete MFR1 framing, selected-session metadata/time/order/cap checks before machine mutation. The adapter exposes one additive immutable query that proves an owned routed-v4 machine is still pristine and identifies its exact route/connection/session/instrument. The V2 transformer checks that identity before prevalidation, observes every raw routed payload before replay, records bounded exact provenance, normalizes emitted envelopes to authoritative MFR action groups, and resolves buffered BOOK outputs by unique native coordinates. It stages every input and canonical JSONL byte in memory, rejects any missing/duplicate/ambiguous/unconsumed provenance or unsupported action, strict-reads the complete output, and returns only after all checks pass.

**Tech Stack:** Rust 1.85, `marketfeed-recording`, `marketfeed-adapter-api`, pure `marketfeed-adapter-binance`, `marketfeed-event-pulse`, canonical JSONL, existing model/dispatch types.

**Spec:** Root admission/wire bindings embedded by `marketfeed-event-pulse` at cryptofeed default `f8e1ecc1ecb13d150b9bbcf4313d103b7553855a`.

## Global Constraints

- Owner is `marketfeed-event-pulse-mfr1`; the only adapter change is an additive read-only pristine routed-v4 identity query and its state bit. Existing decoded/event APIs and legacy/default behavior remain unchanged. Recording, replay, engine, EventPulse V1, capture, runtime, and trading surfaces remain unchanged.
- `marketfeed-adapter-binance` is an allowed normal dependency only because its normal dependency graph is pure state-machine/model/book/Serde code and contains no transport, filesystem, socket, environment, credential, or runtime I/O dependency.
- V2 MARKET records are created only with `MechanicsInputV2::market`; they are never lowered through `MechanicsInputV1`.
- Non-market reserved ActionBuffer and MarketDispatch drops use the existing exact V1 system mapping and `MechanicsInputV2::from_v1_non_market`. SystemDispatch/category 2, ordinary `EmitSystem`, and `Reconnect` remain unsupported.
- The checked `ProspectiveCaptureAdmissionV2`, exact selected connection/session, `ReplayCatalogV1`, exact build/session metadata, capture start, decision bound, MFR1 v3 header, capacities, and immutable DropNewest/FailEngine policies are mandatory.
- Bounds remain: 256 MiB input, 65,536 raw records, 65,536 authored inputs, 16 MiB canonical output, bounded action/dispatch buffers, and bounded one-record provenance per selected raw market input.
- Buffered BOOK provenance is correlated only by the exact selected connection/session plus unique native `(kind,U,u,pu)` or snapshot `lastUpdateId`; duplicate, ambiguous, mismatched, or unconsumed ledger entries abort the entire transform. Buffered deltas become available only when emitted after snapshot; their E/T/U/u/pu remain exact.
- Output ceiling remains `blocked:fixture-provenance`, `evidence_authoring_allowed=false`; no snapshot, fixture, capture, source qualification, runtime, paper, canary, live, order, risk, or execution claim.

---

### Task 1: Freeze the V2 API and dependency boundary

**Files:**
- Modify: `crates/event-pulse-mfr1/Cargo.toml`
- Create: `crates/event-pulse-mfr1/src/v2.rs`
- Modify: `crates/event-pulse-mfr1/src/lib.rs`
- Test: `crates/event-pulse-mfr1/tests/transformer_v2.rs`

**Interfaces:**
- Consumes: `ProspectiveCaptureAdmissionV2`, `ReplayCatalogV1`, exact build/session metadata, selected `ConnectionKeyV1`/connection/session IDs, routed role, capacities, and owned `BinanceUsdmSession`.
- Produces: `Mfr1TransformContextV2`, `Mfr1SessionBindingV2`, `Mfr1MetadataBindingV2`, `Mfr1TransformerV2`, `Mfr1TransformOutputV2`, frame views, and typed `Mfr1TransformErrorV2`.

- [x] Add compile-time REDs for the absent V2 types, exact checked context, owned Binance machine, false authority, and V1 API compatibility.
- [x] Run the focused V2 target and capture the unresolved-import RED.
- [x] Add the pure adapter dependency and minimal public types without modifying V1 signatures or output bytes.
- [x] Validate exact admission topology, route/source/connection/session/catalog/metadata identity and supported overflow policy before constructing a transformer.
- [x] Re-run the API/context tests GREEN.

### Task 2: Add complete prevalidation and bounded provenance staging

**Files:**
- Create/Modify: `crates/event-pulse-mfr1/src/v2.rs`
- Test: `crates/event-pulse-mfr1/tests/transformer_v2.rs`

**Interfaces:**
- Consumes: complete MFR1 bytes, connect timestamp, final `Rfc3339Time`, and the immutable context.
- Produces: a fully validated selected-session record vector and bounded provenance ledger without calling the session machine.

- [x] Add REDs for MFR1 version/header/tail/tamper/size/count, missing/duplicate/conflicting metadata, wrong session/catalog/admission/role/source/symbol, pre-start/future/nonmonotonic time, frame zero/regression, and unsupported controls.
- [x] Add REDs for exact quote `u64::MAX`, trade/native i64 ceiling, BOOK snapshot/delta E/T/U/u/pu, OI time, liquidation outer E/inner `o.T`, duplicate ledger key, ambiguity, and leftover ledger entry.
- [x] Implement complete segment validation with checked length accounting before any machine call.
- [x] Decode each selected routed market payload through `decode_usdm_routed_v4_text`, validate its admitted family/route/symbol/timestamps/native bounds, and insert one bounded provenance entry keyed by exact session and family/native identity.
- [x] Re-run prevalidation and provenance tests GREEN.

### Task 3: Replay, map, and author strict V2 atomically

**Files:**
- Create/Modify: `crates/event-pulse-mfr1/src/v2.rs`
- Test: `crates/event-pulse-mfr1/tests/transformer_v2.rs`

**Interfaces:**
- Consumes: owned routed Binance session plus the prevalidated selected record sequence and ledger.
- Produces: ordered frames, strict `MechanicsInputV2` values, canonical V2 JSONL, frame/drop counts, false authority, and blocker string.

- [x] Add RED end-to-end records for PUBLIC quote, snapshot, buffered bridge delta, later contiguous delta and MARKET trade, OI, liquidation, plus reserved action/market drops.
- [x] Add REDs proving raw group/action/item normalization, native vs derived cursor choice, exact provenance, malformed response rollback, unsupported System/Reconnect, missing/duplicate/mismatched/unconsumed provenance, caps, and no partial bytes on every error.
- [x] Replay each transport frame with the existing ActionBuffer/EventDispatcher policies, rejecting unsupported action kinds before bounded dropping can hide them.
- [x] Map retained emitted market events directly to V2 using the exact raw group and ledger; resolve buffered BOOK emissions uniquely by native coordinates and consume each ledger entry once.
- [x] Stage canonical JSONL with `MechanicsInputV2JsonlWriter`, strict-read with `MechanicsInputV2JsonlReader`, compare exact typed values, and return only after all ledgers are empty and counts/caps pass.
- [x] Feed the returned sequence to a fresh `SourceStateMachineV2` and compare exact per-family cursor/outcome parity with an independently reconstructed direct sequence.
- [x] Re-run the focused V2 suite GREEN and the complete V1 transformer suite unchanged.

### Task 3A: Review hardening successor

- [x] Require the adapter-owned pristine routed identity before any MFR parsing or replay; reject legacy, advanced, wrong-route, and mismatched configured machines.
- [x] Apply admission/receive causality only to Binance T/time/o.T while retaining bounded E provenance even when E is before capture start or after receive.
- [x] Require all nonempty V1 build/session metadata fields and routed empty initial subscriptions.
- [x] Freeze an independent seven-record canonical oracle with exact hashes, strict route-local readback, typed equality, fresh-machine ingest outcomes, and per-family cursor equality.
- [x] Exercise exact/one-over aggregate limit predicates, a real 65,536-record boundary, both supported dispatcher policies, and late-error atomicity.
- [x] Bound ordinary actions to the wire-representable 65,535 count (`action_index` 0 through 65,534), retain a separate 65,536-action observation buffer so one-over is seen and rejected, and reject a derived 65,536 authoring capacity for both policies before replay.
- [x] Preserve the writer's existing explicit LF byte emission and bind the 7,736-byte routed V2 oracle fixture to `text eol=lf` so Windows checkout cannot rewrite its seven canonical line endings; keep byte/hash proof unconditional in source archives and guard only the Git-attribute proof behind successful repository discovery.

### Task 4: Full verification and report

**Files:**
- Modify: `Cargo.lock` only for the approved workspace-local dependency edge if Cargo requires it.
- Create: `docs/superpowers/plans/2026-08-23-event-pulse-e2-mfr1-v2-transformer-report.md`
- Modify: this plan's checkboxes/evidence only.

**Interfaces:**
- Consumes: the completed bounded implementation.
- Produces: exact RED/GREEN and compatibility evidence with residual holds.

- [x] Run focused V2/V1 MFR1, full EventPulse, full Binance, recording/replay/dispatch relevant tests on current Rust.
- [x] Run the same focused/full relevant checks on Rust 1.85 with incremental/debuginfo disabled if disk pressure requires it.
- [x] Run workspace check/test and clippy `-D warnings`, formatter, `cargo deny --offline --locked check`, docs/link scans, and `git diff --check`.
- [x] Confirm V1 public signatures/bytes and legacy adapter behavior are unchanged, the additive adapter identity query is the only adapter semantic addition, no forbidden dependency/I/O/runtime surface entered the diff, and worktree scope is intentional.
- [x] Write the report, commit only owned paths, and leave the branch clean without pushing.

## Test Plan

- `cargo test -p marketfeed-event-pulse-mfr1 --test transformer_v2`
- `cargo test -p marketfeed-event-pulse-mfr1`
- `cargo test -p marketfeed-event-pulse`
- `cargo test -p marketfeed-adapter-binance`
- `cargo test -p marketfeed-recording -p marketfeed-replay -p marketfeed-dispatch`
- `cargo test --workspace --all-targets --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Rust 1.85 equivalents through the pinned toolchain.
- `cargo fmt --all -- --check`
- `cargo deny --offline --locked check`
- `git diff --check && git status --short`

## Risks and Rollback

- Risk: buffered adapter outputs lose their original venue E/T/U/u/pu. Mitigation: raw payloads are decoded and bounded before replay; unique native-coordinate ledger entries are consumed exactly once when the adapter emits snapshot/delta events.
- Risk: output action time differs from buffered input receive time. Mitigation: envelope availability remains the raw MFR frame that actually emitted the event after snapshot; venue source times and sequence coordinates come from the original payload ledger.
- Risk: a dropped action hides an unsupported semantic action. Mitigation: inspect all ordinary action types before applying bounded retention/drop accounting.
- Risk: partial output escapes after a late failure. Mitigation: no output writer or caller-visible value exists until strict readback and empty-ledger validation succeed.
- Rollback: revert the single leaf-crate commit. No adapter, provider, filesystem artifact, capture, runtime, credential, or trading state changes.

## Memory Impact

- No external/root memory update. The task report records the durable offline correlation boundary and residual fixture-provenance hold.

## Progress

- [x] Isolated worktree created at exact hosted default `f8e1ecc1ecb13d150b9bbcf4313d103b7553855a`.
- [x] Dependency audit confirms `marketfeed-adapter-binance` has only pure normal dependencies; transport/engine/recording/replay remain dev-only.
- [x] Adapter inspection confirms routed snapshots and buffered deltas can be correlated deterministically by exact selected session and unique native BOOK coordinates; emitted availability is the snapshot/transport frame that releases them.
- [x] RED captured: the focused target failed with `E0432` for the absent routed V2 transformer types.
- [x] Initial GREEN complete: 11 routed V2 regressions plus all 29 existing V1 transformer regressions pass.
- [x] Review-hardening focused GREEN: 20 routed V2 regressions and 11 routed adapter regressions pass.
- [x] Re-run current and Rust 1.85 tests/clippy, full workspace tests/clippy, fmt, deny, and diff checks for the successor.
- [x] Commit the clean review-hardening successor.
