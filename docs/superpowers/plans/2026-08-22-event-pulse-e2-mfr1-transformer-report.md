# EventPulse E2 Pure MFR1 Transformer Report

## Outcome

Implemented a new leaf crate, `marketfeed-event-pulse-mfr1`, for pure in-memory offline transformation of one complete MFR1 segment, filtered to one selected session, into:

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
- Action capacity is derived from production dispatch metadata, while the bounded `EventDispatcher` applies the admitted caller-bound dispatch capacity/policy and drains after every transport frame. Ordinary MARKET inputs precede reserved ActionBuffer item `0` and MarketDispatch item `1` drops. SystemDispatch item `2` remains unsupported. Processor SYSTEM causal predecessors are chained exactly.
- Any error returns no partial output. The API consumes both transformer and machine ownership, never returns either, and therefore forces a fresh stateful machine for retry.
- Output reports `evidence_authoring_allowed() == false` and `blocker() == "blocked:fixture-provenance"`.

## Focused tests

The initial 13 focused tests cover:

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

## Review repair successor

The successor closes five fail-closed review findings without expanding the MARKET-plus-reserved-processor-drop scope:

- exactly one selected-session metadata record is mandatory and is bound to the immutable selected session, venue, replay catalog instrument identities, stable source, configured contributors, and configured connection; missing, duplicate, wrong, or conflicting metadata rejects;
- the entire MFR1 segment, exact byte tail, CRCs, selected-session time order/window, metadata, HTTP payloads, and subscription commands are validated before `on_replay_start`; `transform` consumes both transformer and concrete machine ownership and never returns either on success or error;
- raw records and authored inputs are each capped at 65,536, canonical EPIN output is capped at 16 MiB, and exact/one-over regressions cover all three bound implementations;
- only `DropNewest` and `FailEngine` are admitted; `BlockWithDeadline`, `DropOldest`, `LatestPerKey`, `SpillToDisk`, and `DisableSink` reject at construction;
- a 65,536-action observation buffer captures and inspects every allowed action before the smaller configured `DropNewest` capacity is emulated. Observation overflow rejects. Consequently an unsupported `EmitSystem` or `Reconnect` cannot be hidden behind a benign retained action.

Review RED evidence was captured with three focused failures: missing metadata returned success after calling replay-start; capacity-one DropNewest returned a successful reserved-drop output while hiding unsupported system/reconnect; and unsupported policies constructed successfully. The added full-tail no-mutation counterexample also proves replay-start is untouched when later MFR bytes are invalid.

Successor validation:

- `cargo test -p marketfeed-event-pulse-mfr1 --no-fail-fast`: GREEN (2 unit bound tests, 20 integration tests).
- `cargo test -p marketfeed-event-pulse --no-fail-fast`: GREEN (full crate: 6 lib, 12 contract, 19 cursor, 6 feature, 6 window, 6 offline preflight, 8 prospective, 18 Task 8 replay, 54 snapshot, and 23 wire tests).
- `cargo test -p marketfeed-engine --test record_replay --no-fail-fast`: GREEN (2).
- `cargo clippy -p marketfeed-event-pulse-mfr1 --all-targets -- -D warnings`: GREEN.
- `cargo +1.85.0 test -p marketfeed-event-pulse-mfr1 --locked --no-fail-fast`: GREEN (2 + 20).
- `cargo +1.85.0 clippy -p marketfeed-event-pulse-mfr1 --all-targets --locked -- -D warnings`: GREEN.
- `cargo deny --offline --locked check`: GREEN.

### Consumed-machine successor

The concrete-machine public signature is generic over an owned `M: SessionMachine`, and `transform_boxed` consumes canonical adapter-factory `Box<dyn SessionMachine>` output. Direct REDs failed to compile while the first API required `&mut dyn SessionMachine` and then while the generic owned API could not accept the box. The retained boxed regression drives a stateful machine through replay-start and an intentional post-start adapter error, proves that the failed box and machine are dropped, and proves the only successful retry uses a separately constructed fresh boxed machine.

### Segment, metadata, dispatcher, and strict-readback successor

- Dispatcher queues drain accepted contents after every replay-start/raw transport frame. Capacity-one accepts one batch in each of two frames without loss, while multiple same-frame batches still produce a truthful MarketDispatch drop.
- `Mfr1MetadataBindingV1` owns exact expected Build and selected Session metadata. Replay requires exactly one of each and compares all typed fields, including adapter, environment, endpoint, catalog version, and every catalog decoding field. ReplayCatalog/admission cross-checking is limited to selected EventPulse-relevant rows; 33+ exact unrelated session catalog rows are accepted without ReplayCatalog membership and remain non-authoring. Missing or semantically mismatched selected rows reject.
- One complete segment is capped at 256 MiB before reader construction and is read through a borrowed cursor without eagerly cloning the input. Only MFR1 v3 is accepted; header start equals connect time; admitted time bounds and selected receive/monotonic progression are checked.
- Action capacity is no longer caller-selected. It is derived as `max(dispatch_capacity * 4, DEFAULT_ACTION_BUFFER_CAPACITY)` with checked arithmetic and the reserved action-index ceiling.
- After bounded canonical EPIN authoring, the production strict `EpinJson1Reader` reads the bytes with `not_after`; decoded inputs must exactly equal staged inputs before output is returned.
- The focused suite is now 3 unit and 28 integration tests. Input is described truthfully as one complete segment, not a complete session; MFR1 has no session-completion receipt.

Final successor gates:

- `cargo test -p marketfeed-event-pulse-mfr1 --no-fail-fast`: GREEN (3 unit, 28 integration, doc tests).
- `cargo test -p marketfeed-event-pulse --locked --no-fail-fast`: GREEN (full crate, including 54 snapshot, 23 wire, and 18 Task 8 replay tests).
- `cargo test -p marketfeed-engine --test record_replay --locked --no-fail-fast`: GREEN (2).
- `cargo +1.85.0 test -p marketfeed-event-pulse-mfr1 --locked --no-fail-fast`: GREEN (3 unit, 28 integration).
- Current and Rust 1.85 `cargo clippy` with `--all-targets --locked -- -D warnings`: GREEN.
- `cargo deny --offline --locked check`, `cargo fmt --all -- --check`, and `git diff --check`: GREEN.
