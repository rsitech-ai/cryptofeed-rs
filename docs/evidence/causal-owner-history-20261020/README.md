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

[Own-source CI](ci-qualification.json) now passes all 17 jobs for producer
`7f383b2`. Independent raw-log review verifies all actual checkout identities,
the identical complete tested merge/source tree and the declared 525-file Git
source snapshot. Linux x64/ARM each pass 1,461 tests, macOS 1,460 and Windows
1,458, with zero failures and 56 ignored on each; Rust 1.85 core passes 545
plus the explicit bootstrap/owner cases, datasets 20 and Windows durability 126.
Passing ARM's 120s test watchdog does not establish a measured 10s bound.

Published hlscreen `6d5fb9e` pins this producer and passes fresh local 544 unique
Rust/12 Python cases and the byte-identical 1,055-event replay, with independent
source/lock/binary/evidence review. Its own main and all four
native artifact validations pass. The separate [CI receipt](hlscreen-ci-qualification.json)
binds raw checkout logs, identical source/merge trees, artifact ZIP digests,
native archives and their build/global manifests. Publication, announcement
and scheduled public-API smoke remain skipped. Earlier `8ba199a`
and `3cabbcb` qualifications retain their original source scope.

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

A separate [runtime migration review](runtime-migration-review.json) identifies
a prospective format gap: terminal fencing currently prevents retaining the
completion of an operation already in flight. The reviewed contract requires
an explicit execution permit, latched terminal state and withheld completion
evidence before runtime migration. It distinguishes nonterminal source fences
and actual versus future deadline-observer seams. This records required work;
no runtime fix or new test execution is claimed.

The subsequent [MFCJ3 runtime contract](../../plan/causal_owner_history_runtime.md)
implements the permit/late-completion path for the private disk/null owner.
This MFCJ2 receipt remains scoped to producer `7f383b2`; it does not qualify that
new runtime or a changed consumer pin.
