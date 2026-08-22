# EventPulse E2 Pure MFR1 Transformer ExecPlan

## Objective and authority

Implement the smallest truthful pure offline boundary in a new leaf crate: complete MFR1 bytes plus a caller-owned `SessionMachine` become ordered strict MARKET `MechanicsInputV1` records and reserved processor-scoped `EventsDropped` inputs, with canonical EPIN-JSON1 bytes.

This is research-only, repo-ready transformation work. It grants no adapter, feed, capture, network, filesystem, credential, source qualification, evidence authorship, Clock/Coverage, snapshot, paper, canary, live, risk, order, or execution authority. The prospective fixture remains `blocked:fixture-provenance`.

## Outcome-changing constraints

- `marketfeed-event-pulse` remains a pure mechanics crate and must not gain normal dependencies on recording, adapter, dispatch, replay, or engine crates.
- The transformer lives in a new pure offline leaf crate depending on EventPulse, recording, adapter-api, dispatch, and model. Replay is test-only.
- Current prospective admission binds only one processor-scoped derived SYSTEM source. It cannot truthfully author contributor-scoped `SequenceGap`, `ChecksumMismatch`, `BookInvalidated`, `BookResynchronized`, or connection-scoped `Disconnected` inputs.
- `SessionAction::Reconnect` proves a requested reconnect action, not that a disconnect occurred. It must fail closed.
- MFR1 does not contain a stable `ReplayCatalogV1`, epoch/generation/OI conversion contract, or queue capacities/policy. The caller must supply those immutable checked values.
- There is no Hyperliquid adapter in the current repository. No complete prospective two-venue fixture or source-qualification claim is possible.
- Any ordinary `EmitSystem` or `Reconnect` is unsupported in this slice and rejects the entire transform with no returned partial output.

## Hard constraints

- Preserve authoritative raw `frame_seq` and `FrameStamp.receive_ts`; normalize derived MARKET `event_index` from the checked captured item index.
- Preserve zero-based action/item order. After each action-producing frame, append real queue losses at reserved action index `65_535` and items `0/1` in ActionBuffer/MarketDispatch order. SystemDispatch item `2` is unsupported because ordinary system actions are outside this leaf contract.
- Replay-start exclusively owns action-producing mechanics coordinate zero. Mechanics-empty SubscriptionCommand or Metadata may use zero or reuse a previous coordinate, matching `ReplayRunner` accounting.
- Reject availability regression, action-producing zero/reused/decreasing inbound frames, unsupported control/system actions, catalog/topology/session mismatch, bounds, tampering, and truncated MFR1.
- Do not call `RawSegmentReader::read_all`, because it intentionally tolerates a crash-truncated tail; iterate `read_record` and propagate truncation.
- Do not invent Clock/Coverage/System lifecycle inputs or claim MFR1-only snapshot parity.
- Use an observation `ActionBuffer` capped at the total authored-input ceiling so unsupported actions cannot hide behind the configured drop boundary. Reject observation overflow, inspect all actions, then deterministically emulate the admitted configured ActionBuffer capacity. Use the existing bounded `EventDispatcher`; dependencies are existing workspace crates only.
- Require exactly one exact `BuildMetadata` record and one selected-session `SessionRecordingMetadata` record. Bind every typed metadata field, including adapter/environment/endpoint/catalog version and complete decoding catalog rows, to immutable expected metadata before replay starts. Cross-check ReplayCatalog/admission topology only for the selected EventPulse-relevant configured instrument rows. Exact unrelated metadata rows need not fit ReplayCatalog's 32-row cap and do not become contributors.
- Prevalidate the complete MFR1 framing, CRC, exact tail, selected times, metadata, and control payloads before the first `SessionMachine` call. Consume both transformer and machine ownership per attempt; neither is returned on success or failure.
- Bound one complete MFR1 segment at 256 MiB before reader construction, raw records and authored mechanics inputs at 65,536 each, and canonical EPIN output at 16 MiB. Require MFR1 v3, the raw reserved/session-count header word at bytes 14..22 to be zero, exact header-start/connect binding, and selected-session receive/monotonic progression. The reserved word is checked from original bytes before any machine call because the recording reader does not retain it. Only `DropNewest` and `FailEngine` execution policies are admitted.

## Architecture and public interface

The new `marketfeed-event-pulse-mfr1` leaf crate preserves dependency direction. It uses public session/recording/dispatch contracts but contains no adapter or runtime implementation.

- `Mfr1TransformContextV1::new(admission, replay_catalog, session_binding, processor_system_source, expected_metadata, dispatch_capacity, overflow)` validates the exact catalog epoch, topology connection, unique configured processor/DERIVED system source, immutable Build/Session metadata, and bounded execution metadata. Action capacity is derived as `max(dispatch_capacity * 4, DEFAULT_ACTION_BUFFER_CAPACITY)`; callers cannot choose it independently.
- `Mfr1TransformerV1::transform(machine, bytes, connect_at)` takes a concrete `SessionMachine` by value. `transform_boxed` takes the canonical adapter-factory output `Box<dyn SessionMachine>` by value. Both drive the same private replay path; both retain ownership for the full attempt and drop the consumed machine on success or error, so retry requires constructing a fresh machine.
- `Mfr1TransformOutputV1` exposes frame groups, flat strict inputs, canonical EPIN bytes, frame counts, and exact loss counts. It exposes `evidence_authoring_allowed() == false` and `blocker() == "blocked:fixture-provenance"`.
- Accepted dispatcher contents are drained after each transport frame, so capacity is a within-frame lane bound rather than accidental cross-frame retention. Canonical EPIN is strict-read back with the immutable `not_after` bound and must decode exactly to staged inputs before return.
- MARKET actions are strict-authored through `MechanicsInputV1::market` after replacing only raw-authoritative frame/receive/item fields. Exchange time and native source sequence remain semantic adapter evidence.
- Reserved real queue losses map through the one processor SYSTEM source with a causal predecessor chain. Ordinary `EmitSystem` and `Reconnect` fail typed before any output is returned.

## Options considered

1. Add recording/session dependencies to `marketfeed-event-pulse`.
   - Rejected: violates the mechanics crate's pure dependency boundary.
2. Map all Task 8 system events from current prospective admission.
   - Rejected: contributor/connection targets and reporting sources are absent; reconnect is not disconnect proof.
3. New leaf crate, MARKET plus real reserved processor drops only.
   - Chosen: smallest lossless boundary that advances E2 without inventing stable identity or crossing E3.

## TDD execution

1. Move the authentic engine-shaped MARKET test to the new crate and capture missing-crate/API RED.
2. Add RED cases for multi-item raw coordinate normalization; real queue loss order; replay-start; zero/reuse control rules; unsupported EmitSystem/Reconnect atomic rejection; availability/session/catalog/topology mismatch; tamper/truncation; and EPIN reconstruction.
3. Implement immutable context validation and exact record-by-record replay/control switch.
4. Implement pre-lane capture, real bounded loss accounting, strict MARKET/drop mapping, and per-processor-source system chain.
5. Prove output EPIN round-trips exactly and a fresh `SourceStateMachine` sees the same step-by-step result sequence as direct inputs. Do not claim snapshot parity because MFR1 lacks Clock/Coverage.

## Validation

- `cargo test -p marketfeed-event-pulse-mfr1`
- `cargo test -p marketfeed-event-pulse`
- `cargo test -p marketfeed-engine --test record_replay`
- `cargo fmt --all -- --check`
- `cargo clippy -p marketfeed-event-pulse-mfr1 --all-targets -- -D warnings`
- `cargo +1.85.0 test -p marketfeed-event-pulse-mfr1`
- `cargo +1.85.0 clippy -p marketfeed-event-pulse-mfr1 --all-targets -- -D warnings`
- `cargo deny --offline --locked check`
- `git diff --check`; scoped `git status --short`; inspect Cargo.lock for workspace dependency-name-only changes.

## Risks and rollback

- A stateful machine can mutate before a post-start typed failure, so the API consumes and drops it without returning it. Retry requires a separately constructed fresh machine; no poisoned machine can be reused through this API.
- Raw action order may conflict with EPIN's strict total order. Preserve raw groups and let the existing writer fail typed; never reorder evidence silently.
- Full SYSTEM lifecycle, independent Clock/Coverage, Hyperliquid confirmation, real capture, and fixture provenance remain explicit future dependencies.
- Rollback is removal of the leaf crate/workspace member and task-local docs/tests. No persisted or external state is created.

## Completion bar

All focused/full/MSRV/lint/deny/hygiene gates are green; only owned leaf-crate/workspace/plan/report paths change; and the report explicitly retains `blocked:fixture-provenance` plus the unimplemented system/sidecar/Hyperliquid gaps.
