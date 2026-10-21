# Bounded acquisition timing diagnosis

The published `84aa635` repair passes required macOS and both Linux hosts,
including the original paused32 watchdog and finalization regression. Its shared
feed reconciles both consumers and passes the registered two-second final-file
tail in joint11 and joint12. Joint12 still ends with perps native-timestamp to
application-receipt differences near30 seconds (BBO maximum31.7249 seconds).
Freshness remains unqualified. A short projection tail cannot diagnose delays
before application acquisition.

This slice adds fixed-size local counters to the returned `DriverReport` and
terminal `report.json`. It changes no DriverPolicy, Bootstrap, journal, shared
wire schema, native permission, durability barrier, wake interval, capacity,
subscription, FrameStamp or physical read ordering. The private plan was recorded
before implementation; this document describes that bounded implementation.
The owning repository is cryptofeed-rs; authority is repository-process-only
public-data infrastructure. No orders, activation, deployment, root gitlink,
credentials or Razer access are included. Rollback is the separate reviewed
instrumentation diff; preserve captures, artifacts and failed evidence.

`read_diagnostics.diagnostics_version=1` contains:

- Existing connected owner-gated waits: count, total and maximum monotonic span,
  grouped by the owner state observed at wait entry. This is an observation of
  the existing poll/stop wait, not continuous phase residency.
- Observed gate runs from the first blocked gate observation to a subsequent
  ready observation or terminal boundary. Loop work between observations is
  included. These runs overlap individual waits; totals are non-additive.
- First-polled read futures: separate frame, returned error, returned transport
  timeout and polled-future-drop spans. A biased unpolled read branch counts zero.
  Constructing the underlying read future still occurs at the original point.
  Existing timer/poll/stop cancellation is a drop, not a new read timeout.
- A saturation flag rather than wrapped counts or nanoseconds. Storage is fixed
  size; there is no per-frame log, payload copy, extra queue, timer or task.

The existing owner-status/wire short-circuit decision supplies both readiness
and wait attribution. Diagnostics never authorize input. Read spans include
library buffering/copying and local scheduling. They cannot reveal kernel/TLS
arrival, native availability, network latency or the remote clock offset.
Metrics appear only when a terminal report is returned; dropping the entire
driver does not promise a surviving report. Panic/drop tests qualify the local
read wrapper's once-only accounting, not whole-process recovery.

The exact 568-input candidate recipe is
`347481b92b568c3bd57a1f971121ab216573c8f8092740be9dc62f2a4df7b4e4`.
Thirty focused tests pass, including the original driver/watchdog/replay cases,
actual buffered frames held behind a paused owner, original FrameStamp values,
the production JSON formatter, read outcome/drop/panic partitions, biased
unpolled branches and saturation. The original `live_tests.rs` bytes remain an
exact prefix; only a child-test module declaration is appended. The initial
formatter-module-path compile failure is retained and is not a behavioral red.
The old joint12 report's missing diagnostic field is an observability red, not
a claimed product capture failure or performance regression.

The exact candidate passes full stable checks (1687 passed, zero failed,
56 ignored), denied-warning Clippy/docs, formatting and default CLI checks.
MSRV default/core/native/public checks pass 111/314/131/80 tests; release
core/native/public checks pass 314/131/80. Fresh audit, deny and secret scans
pass. Independent source and final qualification audits pass. These are local
macOS results for this candidate, separate from hosted proof for `84aa635`.

The matching 120-second joint13 diagnostic completed with all four children
exiting zero: producer, Quants observer, hlscreen and retained native verifier.
Strict framing and eligible-handoff reconciliation finds 7684 source and target
records and 1613 released native observations. Both consumers reach the same
sealed cursor pair. One book observation is withheld at Stop; it is accounted
for separately from released observations. The original 155 files and new eight
capture files retain their recorded bytes and stat fields. The final-file tail
is 23,904,762ns, within the unchanged two-second diagnostic bound. The final
source-to-target interval was not sampled; its backlog remains unknown.

Perps observed gate runs total 119.445 seconds. Individual gate waits attributed
to AwaitingCommit at entry total 116.592 seconds; these spans overlap the gate
runs and must not be added. Completed perps reads total 3.909 milliseconds. Perps
BBO native-to-local-receipt differences end at 21.916 seconds, maximum 21.953
seconds. These unsynchronized timestamp differences do not identify network
latency, but the local counters establish substantial waiting before reads.
Instrumentation alone has not repaired or qualified acquisition freshness.

The next implementation slice is bounded Completion/Intent co-admission,
specified in `completion_intent_pipeline.md`. It requires per-record settlement
retention so a later Intent failure cannot erase an acknowledged Completion's
committed input. A generic aggregate-ACK shortcut is insufficient. The
original 480 ingress failure, strict dataset/context admission, external Quants
CI restriction and longer verification remain separate gates.
