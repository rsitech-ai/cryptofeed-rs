# EventPulse E2 Fixture V4 Assembler Implementation Plan

> **For agentic workers:** Execute inline in this sole-writer task. Do not
> delegate. Follow strict RED/GREEN cycles and keep every API pure and in-memory.

**Goal:** Add a deterministic, pure offline assembler and strict readback for an
EventPulse prospective Fixture V4 package without granting capture, evidence,
filesystem, network, runtime, or trading authority.

**Architecture:** The leaf `marketfeed-event-pulse-capture` crate consumes a
checked `ProspectiveCaptureAdmissionV2`, its matching truthful-empty policy,
immutable request metadata, and complete canonical MechanicsInputV2 JSONL. It
delegates record validation and nine-role partitioning to
`OfflineArtifactPreflightV4`, constructs exactly eleven named in-memory files,
then strictly reads the package back before returning it. The EventPulse crate
adds only a partitioned-artifact readback helper; all V1 APIs and bytes remain
unchanged.

**Tech Stack:** Rust 1.85+, serde/serde_json, SHA-256, existing EventPulse V2
wire/replay/preflight types.

**Spec:** Embedded byte-exact copies of root merge
`0a993f9e9e2a79be4afa0fe6037f5ee98fb6ec70`, tree
`9e2f40507d4522e4176c894470dcbce02d0d63f5`:
`event-pulse-e2-fixture-v4-contract.json` (5,527 bytes, SHA-256
`cb899211245fe039f30d9f0d595133365f36d28fff5b508c20e1bf52363a9f47`)
and its amendment (10,647 bytes, SHA-256
`2c19540bcc953700318a09738dfdbcf167c591827e8825adcad8003889fff965`).

## Global constraints

- Research-only; capital impact is none.
- Exactly eleven in-memory files: `manifest.json`, `admission.json`, and nine
  canonical role JSONL files under `inputs/`.
- Caller-variable request fields are only fixture ID, capture end, decision
  time, source-terms text, and complete JSONL. Admission and policy are checked
  immutable inputs; all authority/status/binding/path values are fixed.
- Admission and manifest are compact sorted canonical JSON plus exactly one LF.
  Artifact JSONL is preserved from strict V2 writer/preflight bytes.
- Eight roles are nonempty; SYSTEM is exactly empty. Manifest
  `record_identities` is always empty because identities are derived by strict
  readback, never caller-authored.
- Aggregate limits are 65,536 records and 16 MiB of artifact bytes. No partial
  package is returned on any failure.
- No filesystem production API, environment, transport, capture, credential,
  source qualification, evidence authorship, snapshot claim, runtime, risk,
  order, paper, canary, or live authority.
- V1 and existing V4 preflight APIs/bytes remain unchanged.

---

### Task 1: Freeze root contract inputs

**Files:**
- Create: `crates/event-pulse-capture/contracts/fixture-v4/event-pulse-e2-fixture-v4-contract.json`
- Create: `crates/event-pulse-capture/contracts/fixture-v4/2026-08-24-event-pulse-e2-fixture-v4-amendment.md`
- Create: `crates/event-pulse-capture/tests/fixtures/event-pulse-e2-fixture-v4-rust-writer.jsonl`
- Create: `crates/event-pulse-capture/.gitattributes`

**Interfaces:** `include_bytes!` binds immutable bytes; tests verify exact
length/SHA/LF and the 17-record structural oracle.

- [x] Add byte-binding RED assertions before the embedded files exist.
- [x] Copy only the exact Git-object bytes from root merge `0a993f9e...`.
- [x] Verify lengths, SHA-256 values, LF policy, and Git diff scope.

### Task 2: Strict partitioned V4 readback

**Files:**
- Modify: `crates/event-pulse/src/preflight_v4.rs`
- Modify: `crates/event-pulse/tests/preflight_v4.rs`

**Interfaces:** Add an additive `OfflineArtifactPreflightV4::readback` helper
that accepts the same checked admission/policy/decision plus the ordered nine
artifact byte slices, reconstructs complete JSONL, reruns strict build/state
validation, and requires exact artifact equality.

- [x] Add RED tests for successful 15/17-record readback and role/order/bytes/
  SYSTEM/canonical/order/timing/cap mutations.
- [x] Implement staging and equality checks with checked aggregate arithmetic.
- [x] Run the focused EventPulse V4 suite and preserve every existing API.

### Task 3: Pure eleven-file package assembler

**Files:**
- Create: `crates/event-pulse-capture/src/fixture_v4.rs`
- Modify: `crates/event-pulse-capture/src/lib.rs`
- Modify: `crates/event-pulse-capture/Cargo.toml`
- Modify: `Cargo.lock` only for the leaf crate's existing workspace dependencies
- Create: `crates/event-pulse-capture/tests/fixture_v4.rs`

**Interfaces:**
- `FixtureV4Assembler::new(admission, policy)` binds checked immutable topology.
- `FixtureV4Request` contains fixture ID, end/decision times, source terms, and
  complete JSONL.
- `InMemoryFixtureV4` exposes ordered immutable file views, manifest bytes,
  `STRUCTURAL_V4_CANDIDATE`, `blocked:fixture-provenance`, and all authority
  methods as false.
- `FixtureV4Error` is closed and typed; failures return no package.

- [x] Add RED tests for the absent API and exact eleven-file happy paths.
- [x] Implement canonical admission/manifest construction and fixed bindings.
- [x] Strictly read back all generated bytes before return.
- [x] Add deterministic, 15/17-record, identity-empty, LF/JSON/hash/path/role,
  admission/policy, capture/decision/source-terms, System, aggregate cap, and
  coordinated mutation regressions.
- [x] Add a test-only temp-directory smoke invoking the published root Python
  validator when its exact checkout is supplied; production remains I/O-free.

### Task 4: Verification and closeout

**Files:**
- Create: `docs/superpowers/plans/2026-08-24-event-pulse-e2-fixture-v4-assembler-report.md`
- Modify this ExecPlan checkboxes/final evidence only.

- [x] Run focused capture/EventPulse suites and the root-validator smoke.
- [x] Run full relevant workspace tests/check/clippy on current and Rust 1.85,
  `cargo fmt --check`, `cargo deny --offline --locked check`, docs, and diff.
- [x] Confirm no V1 byte/API drift and no production filesystem/network API.
- [x] Record exact RED/GREEN evidence and residual
  `blocked:fixture-provenance`; commit only intentional files.

## Risks and rollback

- A manifest assembled from mutable caller metadata could falsely claim
  authority. Fixed literals and private fields prevent that; only the five
  explicitly variable request values are accepted.
- Concatenating partitions in role order can differ from global replay order.
  Readback validates each partition and reconstructs strict complete input in
  authenticated order before equality checking.
- Cross-language drift remains possible. Exact embedded root byte pins plus the
  temp-directory Python smoke catch it without creating a production Python or
  filesystem dependency.
- Rollback is one additive commit; historical V1/V3 and existing V4 preflight
  behavior are untouched.

## Final notes

- Status: repo-ready pure/offline implementation; external fixture provenance
  remains unavailable.
- Authority ceiling: `STRUCTURAL_V4_CANDIDATE`; completion remains
  `blocked:fixture-provenance`.
