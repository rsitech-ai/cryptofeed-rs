# Hyperliquid projection watchdog diagnostics

The original `e812e44` producer CI 39462645500 Windows Hyperliquid target failed
47 passed / 1 failed at its existing 15s watchdog. Its phase and cause are unknown;
no backtrace was retained. The failure remains valid. Other platform passes and
these local checks cannot qualify it away.

Three test-only files add caller/market/phase markers, exact input settlement and
timeout owner status, plus projection poll counts, prepared groups, last
acknowledged cursor pair and time since ACK progress. Native payloads, order,
barriers, count/byte caps, 15s watchdog, 10s ingress age, 2s finalization age, 1ms
cadence and original timeout/abort boundary remain unchanged. Snapshot/logging
setup adds explicit overhead before the timed projection loop; this is not a
claim of identical timing. Dynamic status error strings may contain public
adapter error details; no raw CaptureInput payload is directly formatted.

All 48 named Hyperliquid integration cases pass locally on Rust 1.91.0 and 1.85.0,
with daemon all-target/all-feature Clippy denying warnings. This instruments
source `0db44c52cfa2499addb74491485bae055c541f620c826efb89c389306743f33f`
(548 build inputs), and is not a root-cause repair or current-source Windows
qualification. [Validation](validation.json) binds actual executed artifact-file
hashes, embedded declared recipes and exact test names. The source inventory
was collected after execution and matched those embedded recipes; no full
predeclared frozen qualification run, loaded-image proof, complete transitive
build attestation or reproducible-build claim is made.

The next hosted diagnostic run must retain the real failure phase and progress
without relaxing the workload or deadlines. The original run and raw failure
remain independently retained. HLS's newer consumer candidate stays pinned to
immutable `e812e44`; its separate local/hosted checks cannot inherit this successor's
results. The registered failed-ingress comparison is not rerun.
