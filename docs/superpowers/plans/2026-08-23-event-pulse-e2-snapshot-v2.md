# EventPulse E2 Snapshot V2 Implementation ExecPlan

## Objective and authority

Implement an additive, pure `SnapshotProcessorV2` in `marketfeed-event-pulse` that consumes strict `MechanicsInputV2`, projects the exact E1-compatible per-family cursor surface frozen by the published root Snapshot V2 contract, and authors canonical transactional mechanics snapshots. This slice is repo-ready library work only. It does not capture data, qualify a source, author fixture evidence, implement a runtime adapter, or grant paper, canary, live, execution, or trading authority.

The implementation is bound independently to root merge `4d3e0f0398d3e113a79df7ac901f38912eaa8edd`, tree `273163e3d06578065f7327a90a1b9fbfcded3a6d`, and canonical Snapshot V2 contract SHA-256 `b9062e8e8bdc08e61f92b7890fe4d1dcebbb2eb975cc145c34ddf19f94be28af`. Existing V1 APIs, canonical bytes, tests, and behavior remain unchanged.

## Constraints

- Owner: `crates/event-pulse` plus this task-local plan/report and exact embedded contract assets.
- Capital impact: research-only contract implementation; all authority remains false.
- Consume MARKET input directly as V2. Never construct or lower through `MechanicsInputV1::Market`.
- Preserve `MechanicsProcessor`, `SourceStateMachine`, V1 replay, and all V1 contract bytes exactly.
- Use the existing family-keyed `SourceStateMachineV2`; no aggregate contributor cursor and no truncation.
- Project exactly the latest six MARKET family cursors, three CLOCK cursors, and six COVERAGE cursors after an arbitrary bounded complete prefix. Repeated family records are valid.
- Derived E1 display sequence is checked `frame * 2^32 + action * 2^16 + item`; raw frame above `2^31 - 1` is typed no-authorship and must not seal or mutate revision, predecessor, or cache.
- Convert nanoseconds to RFC3339 microseconds with Euclidean floor, including negative values.
- Preserve exact V2 payload hashes and exact sidecar source/epoch/cursor/availability authorship.
- SYSTEM remains truthful-empty for this topology.
- Processor record and JSONL replay bounds remain 65,536; failures are atomic.

## Options considered

1. Lower V2 MARKET into V1 and wrap the existing processor.
   - Rejected: it loses explicit V2 provenance/cursor semantics and violates the published no-lowering boundary.
2. Copy the entire V1 snapshot processor into a second implementation.
   - Rejected: it duplicates phase/feature/causal semantics and creates drift risk.
3. Add a narrow internal normalized-input boundary and reuse the mature V1 feature/phase/authorship core while retaining V2 records and cursor state separately.
   - Chosen: it preserves one mechanics implementation and lets V2 cursor/provenance authorship remain exact and family-keyed.

## Files and interfaces

- `crates/event-pulse/src/snapshot_v2.rs`: additive `SnapshotProcessorV2`, V2 record/checkpoint/cache state, exact cursor projection, contract binding constants, typed V2 errors.
- `crates/event-pulse/src/snapshot.rs`: only minimal crate-private normalized feature/phase/authorship accessors or shared helpers needed by V2; no public V1 behavior changes.
- `crates/event-pulse/src/cursor.rs`: only minimal immutable V2 family/sidecar state accessors if the accepted API lacks a required view.
- `crates/event-pulse/src/lib.rs`: additive exports.
- `crates/event-pulse/contracts/snapshot-v2/*`: exact LF-pinned published root amendment/contract bytes.
- `crates/event-pulse/tests/snapshot_v2.rs`: RED/GREEN contract, semantics, atomicity, parity, and boundary tests.
- `crates/event-pulse/tests/contract_vectors.rs`: independent embedded-byte/hash/root-pin assertions if needed.
- `docs/superpowers/plans/2026-08-23-event-pulse-e2-snapshot-v2-report.md`: final RED/GREEN and gate evidence.

## TDD execution

1. Add a compile-level RED for the absent `SnapshotProcessorV2` API, followed by semantic REDs for the published cursor projection and no-authorship boundary.
2. Embed and independently verify the exact root contract/amendment bytes, root merge/tree, schema, SHA, and authority ceiling.
3. Introduce the smallest normalized-input sharing seam required to reuse feature/phase/causal mechanics without V1 MARKET construction.
4. Implement bounded V2 ingest and replay records around `SourceStateMachineV2`; transactionally retain exact V2 cursor and payload provenance.
5. Implement exact E1 cursor projection for six MARKET families plus three CLOCK and six COVERAGE sources, including checked derived packing and Euclidean timestamp flooring.
6. Implement snapshot caching, sealing, revision, predecessor, causal maxima, and rollback semantics matching constitution section 10.
7. Prove arbitrary repeated prefixes, direct-vs-strict-JSONL byte/hash parity, same-time failure repair, and no mutation on every late failure.
8. Run focused, full EventPulse, workspace/current and Rust 1.85 tests/check/clippy, formatting, cargo-deny, documentation, and diff/status gates.

## Test matrix

- Exact root contract bytes/hash and independent merge/tree pins.
- Complete 15-record minimum and longer repeated-family prefix.
- Latest Quote and multi-snapshot/delta Book projection.
- Native and derived cursor boundaries, including raw `2^31 - 1`, raw `2^31`, full action/item bounds, and checked overflow.
- `+1001ns` and `-1ns` Euclidean floor goldens.
- Clock/Coverage own source identity, epoch, cursor range, availability, and hash even when contributor epochs differ.
- Missing/duplicate/wrong family/source/provenance/system input failures.
- Direct input and strict `MechanicsInputV2JsonlReader` replay produce byte-identical sequences and hashes.
- Failed snapshot authorship leaves seal/revision/predecessor/cache untouched; repaired same-time snapshot succeeds.
- All pre-existing V1 and V2 cursor/replay/contract tests remain exact.

## Risks and rollback

- Risk: accidental V1 semantic drift while extracting shared internal logic. Mitigation: keep public V1 types untouched and run all V1 snapshot/vector tests before commit.
- Risk: falsely claiming full V2 feature/snapshot parity while only cursor projection is correct. Mitigation: the processor must consume the whole bounded prefix through the same normalized feature/phase/causal core and tests compare canonical bytes, not only cursors.
- Risk: root contract ambiguity. Mitigation: stop before GREEN and report the exact ambiguous clause rather than invent behavior.
- Rollback: revert the single additive implementation commit; no persisted format or runtime state is changed.

## Progress

- [x] Isolated clean worktree at canonical `d069e59d5ee7ca71848fe4de004c3fdca43b1239`.
- [x] Full baseline `marketfeed-event-pulse` suite green.
- [x] Published root merge/tree and contract SHA identified.
- [x] RED captured: `unresolved import marketfeed_event_pulse::SnapshotProcessorV2`.
- [x] Minimal GREEN implementation complete.
- [x] Full current/Rust 1.85/quality gates green.
- [x] Final report prepared; clean commit is the remaining handoff step.

## Final notes

- Added `SnapshotProcessorV2` as a pure, bounded, family-keyed processor. MARKET records remain V2 from strict input through feature ingestion and cursor projection; no V2 MARKET record is lowered to V1.
- Reused the established feature, phase, causal, and canonical E1 authoring core through a private family-eligibility seam. The public V1 processor and its canonical contracts remain unchanged and its complete regression suite is green.
- Embedded the exact published root contract and amendment under LF-enforced contract paths and verify the independent merge, tree, and SHA pins at construction and in tests.
- The implementation accepts arbitrary ordered prefixes up to 65,536 records, projects the latest six MARKET family cursors plus three CLOCK and six COVERAGE cursors, and fails atomically before seal/revision/cache mutation when a derived cursor cannot be represented in E1.
- Validation completed with the focused and full EventPulse suites, full current and Rust 1.85 workspace tests and clippy, formatting, cargo-deny, LF attributes, and diff checks.
- Residual boundary: this is repo-ready Snapshot V2 library implementation only. No prospective fixture has been authored, no source has been qualified, and capture, MFR, adapter, runtime, deployment, execution, and trading authority remain outside this slice.
