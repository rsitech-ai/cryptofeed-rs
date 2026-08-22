# Standalone Q1 RiskDecision Rust Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:test-driven-development` to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking. This task has one writer and
> must not be delegated.

**Goal:** Prove that `marketfeed-event-pulse` consumes the exact standalone Q1
`risk_decision_v1` success and rejection vectors in Rust.

**Architecture:** Keep the historical eight-artifact EventPulse provenance
manifest and its public verifier unchanged. Add a separate, compiled-in
standalone Q1 RiskDecision artifact set whose three exact bytes are independently
pinned to the already embedded Q1 contract lock. Load the existing
`ContractBundle` only after both the historical manifest and the standalone
RiskDecision artifact set verify, then exercise the published one-success and
nine-rejection suites through the existing strict Q1 consumer.

**Tech Stack:** Rust 2024, Rust 1.85 minimum, `serde_json`, `sha2`, existing
`marketfeed-event-pulse` contract and provenance modules.

**Spec:** The canonical source is root rsibot commit
`c32fa67c0e5921bd4a9c4da163daf5c8b6bae08d` and its embedded
`docs/superpowers/specs/quant-harness-contract-lock.json`.

## Global Constraints

- Research/schema-consumer proof only. No adapter, network, filesystem,
  runtime, Risk decision authorship, paper, canary, live, order, or capital
  authority.
- Preserve `marketfeed-event-pulse-provenance/1.0`, `PINNED_ARTIFACTS` length
  eight, and all historical E2 provenance/source-lock meanings byte-for-byte.
- Add no dependencies and do not change Cargo manifests or `Cargo.lock`.
- Vendor exact LF bytes only. The standalone paths, lengths, and SHA-256 values
  are:
  - schema: 6,037 bytes,
    `06a483c06d4186bc05979bcd9f232f0ccc67aee5fbe453ac0e2e9bf74462cf48`;
  - golden: 2,859 bytes,
    `97eb8772358470a9885797d19c24e9449245823ec42ec28cd8d62e8004bfe984`;
  - rejections: 13,288 bytes,
    `c706eec6777c9c4f7b6e99db555abf922d5907dc9a2767ade254a36d3c2365a0`.

---

### Task 1: Add a lock-bound standalone RiskDecision artifact verifier

**Files:**
- Create: `crates/event-pulse/contracts/quant-harness/risk_decision_v1.schema.json`
- Create: `crates/event-pulse/contracts/quant-harness/risk_decision_v1_golden.json`
- Create: `crates/event-pulse/contracts/quant-harness/risk_decision_v1_rejections.json`
- Modify: `crates/event-pulse/src/provenance.rs`
- Modify: `crates/event-pulse/src/contract.rs`
- Modify: `crates/event-pulse/tests/contract_vectors.rs`

**Interfaces:**
- Consumes: the exact embedded Q1 `contract-lock.json` and the three standalone
  RiskDecision artifact byte arrays.
- Produces: `verify_embedded_risk_decision_contracts() -> Result<Vec<VerifiedArtifact>, ProvenanceError>`;
  `ContractBundle::load_embedded()` requires both provenance families to pass.
- Preserves: `verify_embedded_contracts()` returns exactly the historical eight
  artifacts and its manifest semantics do not change.

- [x] **Step 1: Write RED tests**

  Add tests that require the standalone verifier to return exactly three
  nonempty artifacts, require the historical verifier to remain exactly eight,
  require one published RiskDecision success vector to round-trip canonical
  JSON/hash, and require all nine published rejection vectors to fail closed.
  Add a drift-injection test through a narrow hidden verification helper.

- [x] **Step 2: Capture RED**

  Run:

  ```bash
  cargo test --locked -p marketfeed-event-pulse --test contract_vectors risk_decision
  ```

  Expected: compilation failure because the standalone artifacts and verifier
  API do not exist.

- [x] **Step 3: Vendor exact canonical bytes**

  Copy the three files byte-for-byte from the immutable root source commit.
  Recompute byte lengths and SHA-256 values and compare them with the already
  embedded contract-lock row before implementation proceeds.

- [x] **Step 4: Implement the minimal independent verifier**

  Add a private fixed three-record table and exact path/length/hash checks. The
  verifier must reject lock drift, missing paths, duplicate paths, unknown
  paths, or byte drift before exposing bytes. Do not add these records to
  `ProvenanceManifest` or `PINNED_ARTIFACTS`.

- [x] **Step 5: Make contract loading require both verified sets**

  Extend only `ContractBundle::load_embedded()` so a consumer cannot validate
  Q1 contracts while the standalone RiskDecision proof bytes are absent or
  drifted. Keep the existing public validation methods unchanged.

- [x] **Step 6: Verify GREEN and all published semantics**

  Run the focused contract-vector and wire-regression suites. If a published
  rejection exposes a semantic gap, add its direct RED before the smallest
  `contract.rs` repair.

### Task 2: Verify and report the bounded proof

**Files:**
- Create: `docs/superpowers/plans/2026-08-22-risk-decision-rust-consumer-report.md`

**Interfaces:**
- Consumes: the unchanged implementation candidate after Task 1.
- Produces: exact RED/GREEN/gate evidence and residual integration boundary.

- [x] **Step 1: Run focused and full current-toolchain gates**

  ```bash
  cargo test --locked -p marketfeed-event-pulse --test contract_vectors
  cargo test --locked -p marketfeed-event-pulse --test wire_regressions
  cargo test --locked -p marketfeed-event-pulse
  cargo fmt --all -- --check
  cargo clippy --locked -p marketfeed-event-pulse --all-targets --all-features -- -D warnings
  ```

- [x] **Step 2: Run MSRV and dependency-policy gates**

  ```bash
  cargo +1.85.0 test --locked -p marketfeed-event-pulse
  cargo +1.85.0 clippy --locked -p marketfeed-event-pulse --all-targets --all-features -- -D warnings
  cargo deny --offline --locked check
  ```

- [x] **Step 3: Inspect the exact diff**

  Require no Cargo diff, exactly three new canonical artifacts, minimal
  provenance/contract tests/source changes, task-local plan/report only,
  `git diff --check`, and clean scoped status after commit.

- [x] **Step 4: Record the residual boundary and commit**

  The report must state that the Rust proof is repo-ready only. Updating the
  root cross-language receipt requires this candidate to become canonical
  default-reachable and remains a separate integration action. No authority or
  E2 completion claim follows.

## Risks and rollback

- Risk: expanding the historical E2 provenance set would invalidate its source
  lock. Mitigation: a separate three-artifact verifier whose lock root is the
  already pinned Q1 contract lock.
- Risk: tests could accept coordinated artifact and metadata drift. Mitigation:
  compiled-in independent pins plus direct drift counterexamples.
- Rollback: revert the single implementation commit; no external or persisted
  state is touched.
