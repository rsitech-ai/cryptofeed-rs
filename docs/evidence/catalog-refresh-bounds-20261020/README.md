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
change hlscreen's five shared package trees or source pin. Follow-up source384 CI
39354936034 has now completed with all 17 jobs successful. Its actual merge
checkout has the identical complete source384 tree. The four full platform suites
pass: Linux x64/ARM 1,306 each, macOS 1,305 and Windows 1,303; each has zero failures,
56 ignored and 151 suites. Native hosted Windows durability passes 82 cases.
See `ci-qualification.json` for source/tree/log bindings. These results cover the
catalog guard and do not qualify later timing instrumentation or diagnostics.
