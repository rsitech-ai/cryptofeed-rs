# PLAN

## Task

- Objective: Add one offline, synthetic cross-crate regression proving the routed Binance V4 adapter, MFR1 Transformer V2, strict MechanicsInputV2 readback, V4 preflight, and Snapshot V2 replay compose deterministically.
- Owner repo: root `cryptofeed-rs`; implementation ownership is limited to `crates/event-pulse-mfr1/tests/routed_v4_offline_pipeline.rs` and its minimal test dependency wiring.
- Capital impact: research-only. This task must not create fixture provenance, capture, evidence, paper, canary, live, order, or execution authority.

## Constraints

- Use only synthetic in-memory MFR1 inputs and already-exported APIs.
- Preserve the immutable authority ceiling: `blocked:fixture-provenance`, with all authority booleans false.
- Rehashed wrong source, family, cursor, and available-at records must fail at their semantic boundary before preflight or snapshot mutation.
- Do not change production behavior.

## Options Considered

1. Duplicate transformer or fixture internals in the regression.
   - Rejected: it would test copied logic instead of the cross-crate public boundary.
2. Compose the exported V4 adapter/transformer, strict readers, preflight, and snapshot APIs in one integration test.
   - Chosen: exercises the real boundary and keeps the change test-only.

## Execution Plan

1. Add the RED integration test using the Fixture V4 crate as a test-only dependency.
2. Record the missing dependency failure, then add the minimal dev-dependency.
3. Exercise synthetic PUBLIC and MARKET BNBUSDT MFR1 records; use the V4 sidecars only to complete the fixture preflight contract.
4. Assert strict readback, deterministic V4 package bytes, deterministic Snapshot V2 bytes/hash, semantic tamper rejection, and the authority ceiling.
5. Run the requested focused and workspace quality gates; inspect the final diff and status.

## Test Plan

- `cargo test -p marketfeed-event-pulse-mfr1 --test routed_v4_offline_pipeline`
- `cargo test -p marketfeed-event-pulse -p marketfeed-event-pulse-mfr1 -p marketfeed-event-pulse-capture`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo deny check`

## Risks and Rollback

- Risk: test fixture accidentally presents structural synthetic data as authentic evidence.
- Mitigation: assert explicit candidate status, blocker, and false authority flags; label the only source terms as synthetic.
- Rollback: revert the test and the test-only dependency together.

## Memory Impact

- None. This creates no durable runtime, fixture-provenance, or authority fact.

## Final Notes

- What changed: Added one synthetic integration regression and the minimal test-only `marketfeed-event-pulse-capture` dependency. It composes routed Binance V4 PUBLIC/MARKET MFR1 replay, V2 strict JSONL readback, V4 preflight/structural package readback, and two Snapshot V2 replays.
- RED evidence: the new test initially failed with `E0432` because the Fixture V4 assembler was not linked as a dev dependency.
- Validation: focused regression passed; requested three-crate suite passed with exit status 0; `cargo fmt --all -- --check`, workspace clippy with warnings denied, `cargo deny check`, and `git diff --check` passed.
- Tamper coverage: rehashed wrong source, family, and cursor fail strict V2 input construction; rehashed future `available_at` fails as `ReplayInputError::FutureInput` before preflight or snapshot mutation.
- Authority/result ceiling: synthetic output remains `STRUCTURAL_V4_CANDIDATE`, `UNVERIFIED`, all-false authority, and `blocked:fixture-provenance`. E2 remains IN_PROGRESS.
- Rollback: revert the test, its dev dependency, and lockfile entry together.
