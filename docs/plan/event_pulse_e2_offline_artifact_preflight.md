# EventPulse E2 Offline Artifact Preflight ExecPlan

## Task

- Objective: deterministically validate and partition one complete canonical
  EPIN-JSON1 stream into exactly nine nonempty in-memory role artifacts.
- Owner repository: `cryptofeed-rs`.
- Base: `17ca572872281cd301ac51ab504edbb67201c8e7`.
- Capital impact: research-only / SENSE-only.

## Context

The accepted prospective admission contract proves only that a proposed source
topology is compatible with EventPulse mechanics. It deliberately cannot author
evidence. This slice consumes that checked topology plus already-authored EPIN
bytes. It adds no producer and makes no statement about the truth of those
bytes.

## Assumptions

- `MechanicsInputV1::from_epin_json` remains the strict canonical line boundary.
- The checked admission topology is the only allowed source configuration.
- A later, separately authorized producer will be responsible for real MFR1,
  clock, coverage, and system observations.

## Constraints

- No MFR1, `SessionMachine`, adapter, daemon, transport, sink, filesystem,
  environment, network, manifest, package, snapshot, or evidence-authoring code.
- No new dependency.
- Rust 1.85 MSRV.
- At most 65,536 input records; all arithmetic is checked.
- Failure returns no partial artifact set.
- `blocked:fixture-provenance` remains active.

## Options Considered

1. Add a new capture crate and raw replay transformer.
   - Rejected for this slice: it would require producer and E3-adjacent
     decisions that are still blocked.
2. Add a pure module to the existing EventPulse crate.
   - Chosen: it reuses the accepted wire, cursor, and canonical replay types
     without expanding runtime dependencies or authority.

## Chosen Interface

- `OfflineArtifactPreflightV1::build(admission, decision_time, bytes)` consumes
  a complete canonical EPIN stream.
- Every line is strict-ingested, topology-validated, and routed by the checked
  contributor/source identity plus payload family.
- `OfflineArtifactPreflightV1` returns exactly nine artifacts with canonical JSONL
  bytes, record count, byte length, SHA-256, and first/last availability.
- Both admission and result keep evidence authorship false.

## File Scope

- Add `crates/event-pulse/src/preflight.rs`.
- Modify `crates/event-pulse/src/lib.rs` for minimal exports.
- Modify `crates/event-pulse/src/prospective.rs` only for a retained immutable
  mechanics configuration and narrow immutable identity accessors.
- Add `crates/event-pulse/tests/offline_artifact_preflight.rs`.
- Modify `crates/event-pulse/tests/prospective_capture.rs` only for accessor
  regression coverage.
- Add this plan and a final ignored report under `docs/plan/`.

## TDD Execution Plan

1. Add RED tests for exact nine-role output, strict canonical input, immutable
   topology, deterministic bytes/hashes, missing roles, future failures, and
   transactional topology failure. Retain the existing EPIN order/capacity
   regressions as the shared input-boundary coverage.
2. Capture the focused failing result before implementation.
3. Add the smallest immutable admission accessors and pure preflight module.
4. Run focused GREEN, then full EventPulse, MSRV, format, clippy, cargo-deny,
   diff, and status checks.

## Test Plan

```text
cargo test --locked -p marketfeed-event-pulse --test offline_artifact_preflight
cargo test --locked -p marketfeed-event-pulse --test prospective_capture
cargo test --locked -p marketfeed-event-pulse
cargo +1.85 test --locked -p marketfeed-event-pulse --lib --tests
cargo fmt --all --check
cargo clippy --locked -p marketfeed-event-pulse --all-targets -- -D warnings
cargo deny --offline --locked check
git diff --check
```

## Risks and Rollback

- Risk: typed test fixtures could be mistaken for source evidence. Mitigation:
  the API returns the blocker and never enables evidence authorship.
- Risk: role routing could accept a correct payload from the wrong contributor.
  Mitigation: validate every record through the admission-owned
  `SourceStateMachine` before routing and bind confirmation by source identity.
- Rollback: revert this additive module, tests, exports, accessors, and plan.
  There is no external or capital state to unwind.

## Memory Impact

No durable repository memory change is needed unless validation discovers a new
stable command or contract limitation.

## Final Notes

- Implemented a pure in-memory preflight in the existing EventPulse crate; no
  crate, dependency, producer, I/O, runtime, or authority surface was added.
- The admission now retains the exact `MechanicsConfigV1` that it already
  validated instead of rebuilding or duplicating admission logic downstream.
- Every canonical input is parsed by `EpinJson1Reader` (and therefore
  `MechanicsInputV1::from_epin_json`), ingested through a fresh
  `SourceStateMachine`, bound to a configured source, and then written with
  `EpinJson1Writer` into one of the nine fixed role artifacts.
- Successful output requires every configured contributor, clock, coverage,
  and system source and every role to be represented. Reports use checked
  count/length conversion and SHA-256 over the exact returned bytes.
- RED: the focused integration test failed to compile because the preflight
  types and immutable admission topology accessors did not exist.
- GREEN: focused tests, the full EventPulse crate, the full crate under Rust
  1.85.0, rustfmt, clippy with warnings denied, cargo-deny offline/locked, and
  Git diff checks passed. Exact evidence is recorded in the adjacent report.
- Rollback remains a single commit revert; there is no external or capital
  state and `blocked:fixture-provenance` remains active.
