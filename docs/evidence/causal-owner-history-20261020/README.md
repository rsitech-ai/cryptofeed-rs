# MFCJ2 owner-history verification

The [contract](../../plan/causal_owner_history.md) describes the negotiated
format, independent journal/native ordinals, exact typed observations, source
and deadline fences, resource-budget meanings and explicit prefix uncertainty.
The runtime daemon owner still rejects MFCJ2 startup before factory construction;
this change does not implement its live recording path or actual-engine replay.

All 14 final local gates pass on the frozen source in [validation](validation.json):
1,460 workspace tests across 157 suites, zero failed and 56 existing ignored;
Rust 1.85 core 544 tests plus 34 bootstrap/owner unit, four public bootstrap/
profile and eight public owner cases; default-disabled, denied-warning workspace
Clippy/docs, cargo-deny 0.20.2, cargo-audit 0.22.2 with zero vulnerabilities/
warnings, formatting and diff checks. The workspace includes all 14 new engine
cases and 91 recording tests. All 13 source hashes stay unchanged through these
gates. Separate source review closes eight findings; independent artifact/source
inspection verifies the declared 525-file snapshot, exact embedded recipe and
preserved test file, without claiming loaded-image attestation or reproducible
builds. All 22 frozen dataset files retain exact bytes and recorded metadata.

Fresh own-source hosted CI and a new hlscreen pin/build/replay remain required.
Earlier published producer `8ba199a` and hlscreen `3cabbcb` qualifications remain
in the preceding operation-owner evidence and do not qualify this new source.

Separate source review found and closed actor V2 misclassification, native vs
encoded byte-budget confusion, execution-time source/nonexecution gaps, initial
runner-state binding, native failure/deadline termination, terminal revocation
at the same input ID, native reservations retained past commit, and missing
bootstrap capture-limit comparisons. A real regression fails before the
terminal-handoff fix and passes after it. Earlier test-helper compile failures,
the old unsupported-version-2 assertion (now version 3 with explicit V1 actor
rejection of valid V2), the zero-match filter and the lint failure remain in
retained development evidence; none is counted as a passing behavioral gate.

Tests use actual CaptureSession turns for this codec/validator scope. Exact
financial extrema, original bytes, real drains and eviction distinctions are
checked. These test callbacks are synthetic fixtures, not venue semantics or
actual factory replay evidence. The public boundary test uses the actual bound
bootstrap, writer actor, post-barrier receipt, saved reader and explicit memory
checkpoint/cursor continuation. It records unexecuted-prefix uncertainty and
verifies that factory startup for the new profile remains unavailable.

Runtime owner history, complete failure dispositions, deliberate compatibility
projection, external I/O intent/completion binding, actual-engine physical-order
replay/resume and daemon/live/control/supervisor migration remain required.
The registered failed-ingress workload was not rerun or relabeled. Native
semantics/continuity, strict datasets, rsibot context and calendar/hardware
qualification remain part of the active goal. No economics or promotion follows.
