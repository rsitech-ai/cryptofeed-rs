# In-memory native observation carrier qualification

This qualifies the [native carrier](../../plan/causal_native_observation_carrier.md),
not an enriched durable feed or completed consumer integration.

The [portable qualification](qualification.json) binds 536 source inputs, their
declared source digest, 20 changed build inputs, 19 added regression names, seven
passing local gates and the retained native-test executable's embedded recipe.
It replaces local build-directory paths with descriptive directory classes and
records the exact original qualification digest. It is not loaded-image,
complete transitive-input, reproducible-build or deployed-system proof.

Stable and Rust 1.85.0 each pass 531 tests across 46 targets, including every new
regression. Full workspace compilation and Clippy with warnings denied pass.
Actual Hyperliquid/Bybit owner tests pass 40 regressions across two targets.
Formatting and diff checks pass. These are scoped checks, not a new full-workspace
test execution or consumer/operational qualification.

Independent source review found that a native-only BBO lost venue, physical
connection and receive/monotonic clocks. The original finding remains preserved.
The final source adds an outer source descriptor, collector-owned current input
stamping, canonical/source crosschecks and atomic encoded-size admission. A
separate independent source review closes that finding; source-bound receipt
digests are included. New tests cover forged adapter placeholders, zero-row
scope separation, reconnect preservation, signed-clock extrema and timer refusal.

Final independent evidence review also passes against the exact source, raw
logs, executable recipe, preserved captures and portable receipt mapping.
Its retained receipt SHA256 is
a95aea51f6070f6926f8f0bb539863f5be5a96a979605a580c933802887e9fd6.
No tests were rerun by either independent reviewer.

Original failures are retained. Earlier preflights exposed compatibility arms
and a reconnect fixture that incorrectly selected a canonical venue-status
queue item as a native observation. Validation06 was intentionally interrupted
for the independently identified clock-loss defect. Validation09 completed six
passing checks, then referenced a nonexistent Cargo target and ran no tests in
that final step. Corrected invocation10 ran only the native-owner gate; the six
passing checks were not rerun or relabeled. Both actual terminal receipts and
raw-log digests remain bound by the private qualification.

The original 22-file frozen dataset (37,276,008 bytes), two pilot files and 35
HLS evidence files retain their recorded bytes and metadata. HLS remains clean
at 735784b88b06e6f75a74081fe762cf5c0db131cd with producer pin95; this candidate
changes shared source and has not qualified that consumer combination.

All-mids actual admission, enriched causal persistence/replay, acknowledged
projection, consumer groups/readers, real daemon/live/control integration,
continuity, strict real datasets and native restart remain unfinished. The
registered ingress workload remains failed and has not been rerun. No economics,
trading activation, deployment, merge, release or operational promotion is
granted. The full goal remains active.
