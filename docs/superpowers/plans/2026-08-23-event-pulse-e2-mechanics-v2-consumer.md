# EventPulse E2 Mechanics V2 Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure, offline Rust consumer for the accepted EventPulse admission/2.0 and MechanicsInput/2.0 contracts, including explicit MARKET cursor/provenance semantics and a truthful-empty PreflightV4.

**Architecture:** Two exact root contract blobs are embedded as independent prospective pins, outside the historical EventPulse provenance manifest. `MechanicsInputV2` owns MARKET records directly so its explicit cursor and closed typed provenance remain in the V2 payload hash; byte-identical non-MARKET records wrap and delegate to `MechanicsInputV1`. A V2 JSONL reader/writer, separate family-keyed `SourceStateMachineV2`, checked admission capability, and in-memory PreflightV4 compose these types without adapters, MFR1, filesystem, network, capture, snapshot, or evidence authority. The V2 machine preserves shared connection/contributor lifecycle but exposes no aggregate V2 cursor and is not accepted by the snapshot processor.

**Tech Stack:** Rust 1.85, serde/serde_json, sha2, thiserror, existing marketfeed-model and EventPulse V1 domain types.

**Spec:** Root commit `44f3e091cb47c1b081f673e8bb09e8723a2090c6`: `docs/superpowers/specs/2026-08-23-event-pulse-e2-wire-v2-amendment.md`, canonical wire contract `docs/superpowers/specs/event-pulse-e2-wire-admission-v2-contract.json`, and topology freeze `docs/superpowers/specs/event-pulse-e2-producer-evidence-freeze-v2.json`.

## Global Constraints

- Base commit is exact hosted cryptofeed default `aab15d6717de106b25426ae0044265f463e5d9a1` on `feat/andrzej_event_pulse_e2_mechanics_v2_consumer`.
- Preserve every `MechanicsInputV1`, EPIN-JSON1, admission/1.0, OfflineArtifactPreflightV1/V3, fixture/1.0-3.0 API and byte result.
- Embed topology freeze exactly as 6,955 LF bytes / SHA-256 `7216d9c5bc4b5bcd463b644c53309594413608586288beb0d32623412a42f0d7` and wire contract exactly as 10,119 LF bytes / SHA-256 `dc79576062caf952be44e4808359c4328c2976282291838973d4884fadafa50b`; do not add either to the historical provenance manifest.
- V2 MARKET never lowers through an intermediate V1 MARKET. Its explicit cursor and typed provenance are validated and hashed before cursor-state mutation.
- V2 CLOCK, COVERAGE, and SYSTEM parse and serialize byte-identically through `MechanicsInputV1`; V1 MARKET is rejected by the non-market conversion.
- Admission/2.0 builds exactly three connections, three contributors, three clocks, six coverage keys, and one processor-scoped derived truthful-empty SYSTEM key.
- “12 non-System” is source-identity cardinality (3 contributors + 3 clocks + 6 coverage), not record cardinality. A complete minimum input has 15 records: 6 MARKET families + 3 CLOCK + 6 COVERAGE. No required evidence may be collapsed or omitted.
- PreflightV4 produces six nonempty MARKET-family artifacts, one nonempty CLOCK artifact, one nonempty COVERAGE artifact, and one canonical empty SYSTEM artifact.
- No adapter, MFR1, engine, snapshot, capture, file, environment, network, package/manifest writer, source qualification, evidence authorship, paper/canary/live, order, risk, execution, promotion, or capital authority.
- Public status remains `UNVERIFIED`, `blocked:fixture-provenance`, with `evidence_authoring_allowed() == false`.

---

### Task 1: Pin accepted root contracts independently

**Files:**
- Create: `crates/event-pulse/contracts/prospective/event-pulse-e2-producer-evidence-freeze-v2.json`
- Create: `crates/event-pulse/contracts/prospective/event-pulse-e2-wire-admission-v2-contract.json`
- Test: `crates/event-pulse/tests/prospective_v2.rs`

**Interfaces:**
- Consumes: exact Git blobs at root default `44f3e091cb47c1b081f673e8bb09e8723a2090c6`.
- Produces: private embedded bytes verified by independent byte-length and SHA-256 constants.

- [x] Add a failing test requiring both exact embedded lengths/hashes and proving the historical `contracts/provenance.json` bytes are unchanged.
- [x] Run `cargo test -p marketfeed-event-pulse --test prospective_v2 root_contract_pins_are_exact_and_independent -- --exact` and capture the missing-contract RED.
- [x] Add the two exact LF blobs and private verification helper.
- [x] Re-run the exact test and require GREEN.

### Task 2: Implement strict MechanicsInputV2 wire semantics

**Files:**
- Create: `crates/event-pulse/src/wire_v2.rs`
- Modify: `crates/event-pulse/src/wire.rs` only for minimal crate-private strict JSON/market-mapping reuse.
- Modify: `crates/event-pulse/src/lib.rs`
- Create: `crates/event-pulse/tests/wire_v2_regressions.rs`

**Interfaces:**
- Produces: `MechanicsInputV2`, `MechanicsInputRefV2`, `MarketCursorV2`, `SourceProvenanceV2`; methods `market`, `from_json_line`, `from_v1_non_market`, `validate_static`, `payload_hash`, and `view`.
- Consumes: exact V1 `EventEnvelope`, `ReplayCatalogV1`, strict unique canonical JSON parser, and catalog/action mapping.

- [x] Add RED for the root derived QUOTE golden and exact payload hash `3763341032b451fedc399d27b192ba2583dd0edb4d01e247a98d839db57cfa5e`.
- [x] Add RED for duplicate/unknown/noncanonical keys; rehashed envelope/catalog drift; V1 MARKET conversion; and byte-identical V1 CLOCK/COVERAGE/SYSTEM round trips.
- [x] Add RED for QUOTE `u64::MAX`, native `i64::MAX + 1`, timestamp negative/max/+1 and JSON fraction/exponent/string/bool/null forms.
- [x] Add table-driven RED for every exact family/source/provenance/cursor/time pairing: PUBLIC QUOTE derived bookTicker, PUBLIC BOOK native delta/snapshot, MARKET TRADE native aggregate trade, MARKET OI derived source time, MARKET LIQUIDATION derived force order, and Hyperliquid MarkPrice/IndexPrice derived NONE.
- [x] Implement the closed cursor/provenance unions, checked millisecond conversion, exact coordinate/native equality, family/source selection, canonical hashing, strict parsing, and V1 non-market delegation.
- [x] Re-run the focused wire V2 suite and all existing V1 wire/replay regressions.

### Task 3: Add V2 JSONL replay without EPIN2 claims

**Files:**
- Create: `crates/event-pulse/src/replay_v2.rs`
- Modify: `crates/event-pulse/src/lib.rs`
- Create: `crates/event-pulse/tests/replay_v2.rs`

**Interfaces:**
- Produces: `MechanicsInputV2JsonlWriter<W>` and `MechanicsInputV2JsonlReader<R>` using existing `ReplayInputError`.
- Consumes: V2 explicit MARKET cursor, V1 non-market cursor, canonical line parser, immutable `not_after`, 16 MiB line cap, and 65,536 record cap.

- [x] Add RED for canonical round trip, missing newline, duplicate/noncanonical/oversize input, future input, ordering regression, equal-time cursor ordering, and 65,536/65,537 record boundaries.
- [x] Implement streaming read/write and replay ordering from explicit MARKET cursor without reconstructing it from envelope provenance.
- [x] Strict-read every authored line back into the exact staged value.
- [x] Re-run V2 replay plus unchanged EPIN-JSON1 tests.

### Task 4: Ingest V2 cursor state transactionally

**Files:**
- Modify: `crates/event-pulse/src/cursor.rs`
- Create: `crates/event-pulse/tests/cursor_v2.rs`

**Interfaces:**
- Produces: `SourceStateMachineV2::ingest(&MechanicsInputV2) -> Result<IngestOutcome, CursorError>` and read-only family state/cursor/invalidity views keyed by exact contributor and family.
- Consumes: the exact V2 MARKET cursor and payload hash directly; delegates non-market records to the existing V1 transaction.

- [x] Add RED proving noncontiguous derived QUOTE succeeds while the same coordinate/hash duplicates, mutated duplicates, native overlap/gap/regression, and wrong mode fail with existing typed semantics.
- [x] Add RED proving rejected V2 static input never mutates state and retained cursor/hash equal the authored V2 record, not a V1-derived surrogate.
- [x] Implement clone-before-ingest transactional dispatch and direct V2 market ingestion using the existing checked slot machinery.
- [x] Re-run V2 cursor, existing cursor-state, mechanics, and snapshot suites.

### Task 5: Implement exact admission/2.0 capability and truthful-empty policy

**Files:**
- Create: `crates/event-pulse/src/prospective_v2.rs`
- Modify: `crates/event-pulse/src/lib.rs`
- Extend: `crates/event-pulse/tests/prospective_v2.rs`

**Interfaces:**
- Produces: `ProspectiveCaptureAdmissionV2` and `ProspectiveSystemArtifactPolicyV2` with immutable checked config/start/fingerprint accessors and false-authority/blocker accessors.
- Consumes: exact embedded topology/wire blobs and exact descriptor bindings.

- [x] Add RED for exact root bindings, all twelve false authority literals, canonical UTC start strictly after both merge times, coordinated rehash/binding drift, duplicate/unknown/noncanonical JSON, and JSON type aliases.
- [x] Add RED proving exact config cardinalities (3 connections, 3 contributors, 3 clocks, 6 coverage, 1 SYSTEM) and exact source/family ownership.
- [x] Implement strict admission parsing and fixed config construction from the embedded accepted topology, not caller-selected topology fields.
- [x] Bind `ProspectiveSystemArtifactPolicyV2` to the complete checked admission and exact truthful-empty processor policy.
- [x] Re-run admission V2 and unchanged admission V1/V3 policy tests.

### Task 6: Build atomic in-memory OfflineArtifactPreflightV4

**Files:**
- Create: `crates/event-pulse/src/preflight_v4.rs`
- Modify: `crates/event-pulse/src/lib.rs`
- Create: `crates/event-pulse/tests/preflight_v4.rs`

**Interfaces:**
- Produces: `OfflineArtifactPreflightV4` and `InMemoryArtifactV4` over exactly nine `ArtifactRoleV1` outputs.
- Consumes: checked admission/policy, immutable decision time, and a complete canonical V2 JSONL byte slice.

- [x] Build a literal 15-record valid fixture: six MARKET families, three clocks, six coverage records, with 12 unique non-System source identities.
- [x] Add RED for missing/extra/duplicate source or role, wrong family/source/provenance, before-start/future/order/newline/16 MiB/65,536 boundaries, and any nonempty SYSTEM record.
- [x] Add RED proving SYSTEM rejection and all classification/static checks occur before any staged state/result is committed.
- [x] Implement strict read, preclassification, full-source/full-role set checks, fresh `SourceStateMachineV2::ingest`, deterministic partitioning, canonical artifact bytes/count/SHA/first/last/record identities, strict readback, and canonical empty SYSTEM report.
- [x] Assert deterministic byte-identical nine-artifact outputs across fresh builds and exact strict readback of every nonempty artifact.
- [x] Re-run V4 tests and all existing V1/V3 preflight tests.

### Task 7: Verification and closeout

**Files:**
- Create: `docs/superpowers/plans/2026-08-23-event-pulse-e2-mechanics-v2-consumer-report.md`
- Modify: `docs/superpowers/plans/2026-08-23-event-pulse-e2-mechanics-v2-consumer.md`

**Interfaces:**
- Produces: one clean repo-ready candidate commit, not a fixture/completion/runtime claim.

- [x] Run focused V2 tests and full `cargo test -p marketfeed-event-pulse`.
- [x] Run explicit V1/V3 wire/replay/admission/preflight regressions.
- [x] Run current and Rust 1.85 EventPulse tests/check/clippy with `-D warnings`, relevant workspace tests/check, `cargo fmt --all -- --check`, and `cargo deny --offline --locked check`.
- [x] Verify embedded contract lengths/hashes/LF, unchanged historical V1 contract/provenance bytes, `git diff --check`, intentional diff, and clean status after commit.
- [x] Record exact RED/GREEN evidence and residual `blocked:fixture-provenance`, then commit only authorized EventPulse/plan/report paths.

## Risks and Rollback

- Risk: a V2 MARKET record could be reconstructed as V1 and silently lose provenance or select cursor mode from `source_sequence`. Mitigation: no V1 MARKET conversion path; cursor ingestion consumes the explicit V2 cursor/hash directly.
- Risk: a full-width derived V2 frame could be packed through the narrower V1 cursor display domain. Mitigation: V2 family slots, views, and replay ordering retain and compare `MarketCursorV2` directly; V1 cursor validation remains confined to V1.
- Risk: generic native `+1` continuity would reject Binance depth bootstrap overlap or accept a non-overlapping first delta. Mitigation: only the V2 BOOK family carries the frozen snapshot/first-overlap/subsequent-`pu` state and requires resnapshot after invalidity; every other native family keeps generic continuity.
- Risk: equal-time MARKET replay could order native before derived cursor variants instead of causal raw capture order. Mitigation: replay uses the payload-authenticated envelope frame/action/item tuple for every MARKET family and retains `MarketCursorV2` solely for family continuity.
- Risk: admission validates a coordinated caller-selected topology. Mitigation: caller supplies only the exact descriptor; topology and config are derived from independently pinned embedded root contracts.
- Risk: a nonempty or fabricated SYSTEM artifact false-greens completion. Mitigation: the policy is non-forgeable, processor-bound, and V4 rejects every SYSTEM input before staging.
- Risk: the shorthand “12 records” could omit required evidence. Mitigation: freeze 12 identities and 15 minimum records explicitly, with exact source and role set equality.
- Rollback: revert the additive V2 modules/tests/contracts/exports and the two minimal crate-private V1 helper visibility changes. Existing V1/V3 APIs and bytes remain untouched.

## Progress

- [x] Created an isolated clean worktree at exact hosted base `aab15d6717de106b25426ae0044265f463e5d9a1`.
- [x] Verified root topology and wire blobs independently at exact lengths and SHA-256 values.
- [x] Confirmed unchanged baseline `cargo test -p marketfeed-event-pulse` passes.
- [x] Resolved reviewer shorthand: 12 unique source identities require 15 minimum records.
- [x] Resolved the discovered mixed-mode cursor blocker with preallocated family-keyed V2 slots; V1 maps/APIs remain untouched, invalidating lifecycle effects fan out coherently, and no snapshot support is claimed.
- [x] Complete Tasks 1-7; the task checklists above remain the accepted review matrix and are evidenced in the companion report.

## Final Notes

- Added the independent topology/wire pins, strict V2 wire and JSONL boundary, family-keyed cursor state, fixed admission capability, and atomic truthful-empty PreflightV4.
- The successor repair removed the last V2 MARKET-to-V1 cursor lowering and proves the full root-authorized `u64` derived-frame domain plus exact replay/cap boundaries.
- The final repair applies frozen Binance BOOK bootstrap/`pu` semantics and raw-coordinate equal-time replay ordering without changing V1 or widening the authority ceiling.
- Preserved V1 state, APIs, contract bytes, provenance meaning, EPIN-JSON1, admission/1.0, and PreflightV1/V3 behavior. No V2 snapshot integration is claimed.
- Validation completed on current Rust and Rust 1.85, including the full EventPulse suite, focused V2 regressions, clippy with warnings denied, cargo-deny, formatting, and diff checks. Exact commands and RED/GREEN evidence are in the report.
- Residual status remains `UNVERIFIED` and `blocked:fixture-provenance`; this slice grants no capture, evidence, runtime, risk, or trading authority.
