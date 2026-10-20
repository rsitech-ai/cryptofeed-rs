# Actual engine operation capture — 2026-10-20

`CaptureSession` privately owns a real persistent `SessionRunner` and machine.
It executes typed public-evidence inputs with no runner/dispatcher/metrics/stop
handle escape. It captures original bytes before mutable parsers, actual callback
outcomes and buffered actions before clearing/draining, action-prefix results,
queue admission/drop/eviction results, produced records, partial post-state,
dynamic subscription preparation/reservation/commit, and suppressed diagnostics.
Produced records are explicitly distinct from admitted queue items. Exact native
FeedRecord JSON is retained without a floating-point JSON-value conversion.

Each accepted operation has an ordinal independent of raw frame sequence.
Connection operations inject a recorded nonzero identity, increment generation
and retain the same machine across reconnects. A timer poll is one input with its
actual ordered callback firings; cancellation/postponement and the native 64-fire
cap remain in effect. HTTP input is retained before response-encoding failure.
ActionBuffer overflow counters are observed without consuming them early; payloads
the native buffer already discarded are not invented or claimed preserved.

Capture completeness is separate from the native execution outcome. Entry/byte
limit failure does not turn a successful native operation into a fake engine
error. Partial mutation stays private, capture is marked incomplete and this
owner refuses subsequent execution. Oversized native error details are dropped
in favor of a bounded typed error class when their cost cannot be retained.
Callback panic also poisons the owner and explicitly loses completeness; neither
the panic payload nor unwound buffered actions are claimed captured. Explicit
sensitive actions are detected before cloning them; queue payload snapshots and
future execution are refused after that fault. All configuration/URL/header/body
inputs to this API must themselves be public reproduction material.

Limits account conservatively for logical trace retention, including input copies
and bounded state allowances. They are not allocator-capacity, peak-memory or RSS
limits. The actual machine and runner can allocate working memory independently;
serialization buffers may have spare capacity. An incomplete capture is never a
prepared transaction. This API has no journal, durable receipt or release method
and no live collector has migrated to it.

The 18 focused tests cover original mutable text/binary input, callback failure,
partial action failure/suffix, both drop policies, intentionally suppressed
diagnostics, live-then-failed-status mutation, separate capture faults, overflow
counter carry-over, dynamic refusal/commit, HTTP encoding failure, timer polls,
actual-runner reexecution across a failed prefix/reconnect, sensitive actions,
preflight rejection, oversized native error omission, panic, exact i128 extrema,
and post-mutation byte exhaustion. A separate reviewer ran all 18, inspected the
actual runner changes and closed the oversized-terminal-error retention defect.
Source and log hashes and broader check results are bound in `validation.json`.
The initial compile failure and the pre-fix reviewed defect remain in ignored
evidence; no capture/ingress workload or historical archive was rerun or rewritten.

The remaining repair still needs effect-parent attribution, a negotiated causal
schema/profile and single compatibility projection, metadata/bootstrap and drain
boundaries, writer submission and commit-gated state/readiness/dispatch/wire/HTTP
release, generation-safe revocation, and a durable actual-engine replay driver.
External I/O and raw/shared publication have separate uncertain crash windows.
Only after that integration should the unchanged registered failed-ingress
workload be compared. The bounded writer and this capture seam do not complete
D5 or establish live continuity, strict dataset admission or operational readiness.

Strategy-context features, models, ETF inputs and decision research remain in
existing rsibot owners. cryptofeed owns acquisition, exact semantics, quality,
storage and replay; hlscreen presents the same feed. rsibot consumes cryptofeed
directly. Its existing manifest inspector is not yet a live context implementation.

```sh
cargo test --locked -p marketfeed-engine --lib operation::tests
cargo test --locked --workspace --all-targets --all-features
cargo +1.85.0 test --locked --workspace --exclude marketfeed-datasets --lib
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps
```

Hosted CI for this original capture source `ba512ad` has now completed all 17
jobs successfully. Its tested merge has the identical complete source tree:
Linux 1,354 cases on each architecture, macOS 1,353 and Windows 1,351, with zero
failed/56 ignored across 153 suites per platform; hosted Windows durability
passes 104 cases across 11 suites. Exact trees, run and raw-log hashes are bound
in `ci-qualification.json`. These results qualify `ba512ad` only, not later
attribution/drain changes or the unfinished live owner and ingress repair.
