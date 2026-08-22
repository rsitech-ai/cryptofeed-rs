# EventPulse E2 Pure MFR1 Transformer Report

## Outcome

Implemented a new leaf crate, `marketfeed-event-pulse-mfr1`, for pure in-memory offline transformation of one selected MFR1 session into:

- raw-authoritative, strict MARKET `MechanicsInputV1` records;
- real ActionBuffer and market-dispatch loss records using the configured processor/DERIVED SYSTEM source and reserved frame-end coordinates;
- explicit frame groups, flat input order, exact loss counts, and canonical EPIN-JSON1 bytes.

The existing `marketfeed-event-pulse` mechanics crate has no source or dependency changes. The leaf crate performs no filesystem, network, environment, adapter implementation, capture, package/manifest, evidence, source qualification, snapshot, trading, paper, canary, or live work.

## Architecture correction

The initial RED targeted a production module inside `marketfeed-event-pulse`. Independent review correctly identified that normal recording/session/dispatch dependencies would weaken the pure mechanics boundary and that the prospective admission supplies only a processor-scoped derived SYSTEM source.

Before GREEN, the plan and test were moved to a new leaf crate. The shipped scope is deliberately limited:

- MARKET output is strict-authored from caller-supplied checked `ProspectiveCaptureAdmissionV1`, immutable `ReplayCatalogV1`, exact selected session binding, and bounded queue execution metadata.
- Ordinary `SessionAction::EmitSystem` rejects with `UnsupportedSystemAction` because contributor/connection target identity is not bound.
- `SessionAction::Reconnect` rejects with `UnsupportedReconnect`; a reconnect request is not proof that a disconnect occurred.
- Clock, Coverage, contributor/connection lifecycle faults, source qualification, and snapshot parity are not claimed.

## RED evidence

1. `cargo test -p marketfeed-event-pulse --test mfr1_transformer`
   - Failed to compile on missing `Mfr1TransformContextV1` and `Mfr1TransformerV1`.
   - This RED was intentionally superseded by the leaf-crate architecture correction before GREEN; EventPulse normal dependencies were restored unchanged.
2. `cargo test -p marketfeed-event-pulse-mfr1 --test transformer`
   - Failed to compile on missing `Mfr1SessionBindingV1`, `Mfr1TransformContextV1`, and `Mfr1TransformerV1`.
3. The authentic synthetic adapter initially returned `UnsupportedSystemAction` because its replay-start emits process-local connection state.
   - This proved the fail-closed boundary. The successful fixture now uses a purpose-built market-only `SessionMachine`; the unsupported-system counterexample remains.
4. `replay_start_owns_coordinate_zero_and_inbound_zero_fails_closed`
   - Failed because an action-producing inbound raw frame zero was accepted when replay-start was mechanics-empty.
   - GREEN reserves zero for replay-start unconditionally while retaining mechanics-empty raw control zero.
5. `cargo fmt --all -- --check`
   - Reported only rustfmt deltas; `cargo fmt --all` applied them and the final check is green.

## Implemented fail-closed boundaries

- The constructor consumes the checked prospective admission, not a caller-forgeable bare topology.
- Connect time, every selected-session raw receive time (including outbound/control records), and the explicit final decision bound must lie within the admitted prospective capture window.
- Only the exact bound `SessionId` is selected from a potentially multi-session MFR1 segment; other sessions are not delivered to the machine but their bytes remain part of whole-input validation.
- Full-input framing is checked as `HEADER_SIZE + sum(record_len)` with checked arithmetic and exact byte-length equality. CRC tamper, an incomplete framed record, and separate 1/2/3-byte trailing tails reject.
- Replay-start owns action-producing frame zero. Action-producing inbound zero, reuse, or regression rejects; mechanics-empty SubscriptionCommand/Metadata coordinates may use zero or reuse prior raw coordinates.
- Market envelope `frame_seq`, `receive_ts`, and checked zero-based `event_index` come from the authoritative raw group/action item. Exchange time and native source sequence remain adapter evidence.
- Batch session, catalog venue/instrument/epoch, stable source, configured contributor, and stable connection must agree exactly.
- Existing bounded `ActionBuffer` and `EventDispatcher` apply caller-bound capacities/policy. Ordinary MARKET inputs precede category-ordered reserved drops. Processor SYSTEM causal predecessors are chained exactly.
- Any error returns no partial output. The API documents that each attempt requires a fresh caller-owned stateful machine.
- Output reports `evidence_authoring_allowed() == false` and `blocker() == "blocked:fixture-provenance"`.

## Focused tests

The 13 focused tests cover:

- authentic SubscriptionCommand plus market raw groups and ReplayRunner frame/lane agreement;
- raw frame/receive normalization and strict canonical EPIN round-trip;
- two-item batches with adapter-invented coordinates replaced by raw action/item coordinates;
- replay-start coordinate zero and inbound zero/reuse/regression rules;
- mechanics-empty control zero/reuse behavior;
- Metadata validation and HttpResponse decode/application;
- real ActionBuffer plus market-dispatch overflow with exact reserved items 0/1 and causal chain;
- atomic rejection of ordinary system and reconnect actions;
- selected-session filtering and start/exact/post/future/connect/decision bounds;
- catalog/topology and execution-capacity rejection;
- CRC tamper, full-record truncation, and exact 1/2/3-byte tail rejection;
- strict EPIN reconstruction and step-by-step fresh `SourceStateMachine` result parity.

## Dependency and lock diff

- Root workspace/default members add only `crates/event-pulse-mfr1`.
- Cargo.lock adds one local package stanza with existing workspace dependencies:
  - normal: `marketfeed-adapter-api`, `marketfeed-dispatch`, `marketfeed-event-pulse`, `marketfeed-model`, `marketfeed-recording`, `thiserror`;
  - test-only: `marketfeed-replay`, `serde_json`.
- No version, source, or checksum changed.

## Validation

- `cargo test -p marketfeed-event-pulse-mfr1`: GREEN (13 integration tests; lib/doc tests green).
- `cargo test -p marketfeed-event-pulse`: GREEN (full crate, including 54 snapshot and 23 wire regressions).
- `cargo test -p marketfeed-engine --test record_replay`: GREEN (2).
- `cargo clippy -p marketfeed-event-pulse-mfr1 --all-targets -- -D warnings`: GREEN.
- `cargo +1.85.0 test -p marketfeed-event-pulse-mfr1`: GREEN.
- `cargo +1.85.0 clippy -p marketfeed-event-pulse-mfr1 --all-targets -- -D warnings`: GREEN.
- `cargo deny --offline --locked check`: GREEN (`advisories ok, bans ok, licenses ok, sources ok`).
- `cargo fmt --all -- --check`: GREEN.
- `git diff --check`: GREEN.

## Residual gates

- `blocked:fixture-provenance` remains exact.
- Full prospective E2 still lacks a Hyperliquid adapter, real post-admission public capture, independently persisted Clock/Coverage sidecars, stable contributor/connection system mappings, and provenance-bearing nine-role fixture packaging.
- This slice is repo-ready transformation only. It is not runtime-proven, package-ready, source-qualified, paper/canary/live-ready, or E2 complete.
