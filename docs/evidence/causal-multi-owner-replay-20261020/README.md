# Bounded multi-owner actual-engine replay qualification

The opt-in `MultiHistoryReplay` retains independently persistent actual Bybit
and Hyperliquid spot/perpetual factories behind one physical MFCJ3 cursor.
It checks aggregate reservations before factory construction and retained
canonical/source budgets before callbacks. Every native completion must match
its complete typed recording; only committed observed handoffs expose values.
Unknown execution remains scoped; malformed or mismatched evidence globally
poisons the reader. Exact resume reconstructs the full prefix and discards old
handoffs. Existing single-owner behavior stays restrictive.

All sixteen frozen-source local checks finished with actual terminal zero and
completion markers. The [qualification](qualification.json) binds 532 declared
inputs at digest `a59fe698b7012225eb64b50fe41f7e5940003a03f42faf5bdcae306d0a7b09c5`,
five changed build inputs, raw logs, retained 0555 test executable and its embedded
recipe. The recipe truthfully records parent 95. This is not loaded-image
attestation, complete transitive-input closure or reproducible-build proof.

- Workspace: 1,518 passed, 0 failed, 56 existing ignored across 158 targets; focused 40.
- Rust 1.85: core 554; unfiltered Hyperliquid 29; bootstrap 51/public 4/owner 11.
- Disabled-feature 3; denied-warning Clippy/docs; formatting/diff.
- cargo-deny 0.20.2 and cargo-audit 0.22.2: zero vulnerabilities/warnings;
  Cargo.lock and dependency versions unchanged.
- Thirteen new regressions include every physical prefix/global resume/next
  append with three actual reconnecting factories, independent uncertainty,
  exact aggregate limits, pre-callback refusal and CRC-correct forged owners.

All 22 original frozen dataset files, both pilot files and 35 HLS evidence files
retain recorded bytes and metadata. Earlier expected compile and fixture
failures remain retained and unqualified. The complete source and raw evidence
are retained locally. Independent final evidence review passed with no material
findings, receipt SHA256
`eebd2886abc750bccb832a45be2a7237e6ac9f80cab5899ef07e99aa89fc0f74`.
This report does not yet qualify this new source's hosted checks.

[Producer95 hosted qualification](producer95-hosted.json) independently verifies
all 17 jobs and immutable 530-input source identity for the published parent.
[HLS735 hosted qualification](hls735-hosted.json) independently verifies its own
main/all-four-native checks, six workflow archives and exact 313-file source
export. Each retains its original source scope. All 52 source files actually
shared with HLS remain byte-identical between95, this frozen candidate and its
Cargo cache; that is source comparison, not a new consumer build. HLS still
uses the legacy accepted-frame reader.

The [native-observation projection contract](../../plan/causal_native_observation_projection.md)
is planned work: full native fields must share canonical action/queue/drop/drain
fate and publication must follow acknowledged `HandoffObserved`. Source
metadata/control/clocks, negotiated atomic target groups and bounded durable
backpressure remain required. Rsibot owns context/features/models and strategy
admission; hlscreen owns presentation.

Native checkpoints/restart, complete failure/admission retention, external I/O,
daemon/CLI/live/control/supervisor and consumer migration, continuity/strict
datasets, calendar/hardware qualification and rsibot's continuous reader remain
open. The registered ingress workload remains failed and was not rerun; its
original caps/schedule/thresholds/abort are preserved. No economics, trading
activation, deployment, merge, release or operational promotion is granted.
The full goal remains active.
