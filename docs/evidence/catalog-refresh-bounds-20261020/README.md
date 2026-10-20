# Catalog-refresh index boundary — 2026-10-20

The actual runner cast an unbounded catalog row index to u16. A 65,537-row
refresh could repeat index zero and publish an ambiguous batch. The retained
behavioral regression on 244f fails because the invalid refresh returned success.
An earlier test-helper compilation error is retained separately, not counted as
a reproduction of the runtime defect.

The runner now rejects more than 65,536 instruments before allocating the event
batch or changing dispatch, normalization counters or the durable feed. Boundary
fixtures accept indices 0..65535 at exactly 65,536 rows, reject 65,537, permit a valid
follow-up and verify an unchanged committed watermark. All 98 engine tests pass;
all 3 boundary cases pass on Rust 1.85 and independently with unchanged source hashes.
Engine all-target/all-feature Clippy passes. Exact logs/hashes are in the receipt.

This is a representability guard, not full catalog/contract validity or a claim
that every maximum-sized serialized batch fits the archival byte budget. Complete
engine causal milestone B remains required. This engine-only repair does not
change hlscreen's five shared package trees or source pin. Full workspace/platform
CI for the follow-up is a separate gate; the previous 1,302 workspace result belongs
to 244f, not automatically to this changed engine.
