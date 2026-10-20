# Local paced-ingress diagnostic — 2026-10-20

The synchronous shared writer fails this registered local workload. This is a
retained failure and a collection blocker, not an admitted dataset or an exchange
capacity/continuity result. Strategy-context implementation remains rsibot-owned.

The example runs the actual Bybit session machines and SessionRunner for linear,
spot and inverse BTC sessions. It uses historical native snapshot/ticker bytes,
synthetic subscription ACKs matched to actual request IDs, three independently
paced source threads, bounded count/byte queues and one shared raw pipeline.
Only the durable arm adds one global shared FeedHandle. Raw recording remains on
in both arms. The fixed ten-second schedule contains 4,800 regular frames plus
64 burst frames. Repeated full snapshots are deliberately a heavier workload
than a normal delta mix. This debug-build experiment does not measure live
exchange delay, production capacity, general filesystem performance or alpha.

| Registered arm | Observed result | Independent byte/accounting audit |
|---|---|---|
| Baseline, raw only | 4,864/4,864 load callbacks, 5,664 reported market rows; max schedule-to-read age 3.287ms; zero queued/dropped raw frames | CRC, ordered native payloads, startup ACKs, exact source/callback counts and native-derived output counts pass; canonical values were not journaled in this arm |
| Durable shared writer | Failed after 3.554s; 147 load callbacks, 171 market rows; max read age 3.169s; each ingress queue reached its 512-frame cap | All 159 saved raw frames match shared frames byte-for-byte per session; all 156 inbound frames have one acceptance marker; committed CRC, warmup/load native values, envelope bindings and output counts pass |
| Deliberately slow arm | Not run | Supervisor stopped after the unexpected durable-arm failure, as registered |

The failed arm accounts for all 4,864 planned inputs: 147 processed, 1,536 queued
but abandoned on stop, one rejected at the ingress bound and 3,180 not attempted.
Releasing queue reservations on shutdown does not erase abandoned-input counts.
The empty raw queue and zero raw-drop counter therefore do not establish lossless
source ingestion. The shared seal explicitly records failure.

The load plus final seal performs 295 durable transactions for 147 callbacks
(two per callback plus seal). Their aggregate append time is 3.542s, including
1.015s data sync, 1.207s watermark write/sync and 1.033s directory sync. These
three durability stages account for about 91.9% of append time. Aggregate mutex
wait across concurrent callers is 6.920s; it is not wall time and must not be
added to append time. Cumulative maxima cannot be subtracted to produce
load-only maxima. Timing counters include failures; durations are local
monotonic observations and do not modify durability barriers or source clocks.

This establishes a local writer/lock bottleneck under this fixed workload. It
does not uniquely attribute the earlier live Bybit lag: server/network buffering,
native mix, build mode, hardware and arm order remain uncontrolled there. The
original failed captures and immutable source identity remain unchanged.

Attempt 01 failed before setup/scheduling because its copied fixture metadata
included the enum's `kind` discriminator. The strict decoder rejected it. Both
its rejection and plan are retained. Plan revision 2 removes only that fixture
discriminator; all nine native payloads, schedule, thresholds, arm order and
abort policy are unchanged. The workload was not retried to obtain a passing
durable result. Early audit-helper mistakes (segment extension and automatic
build-metadata count) are retained in ignored logs; neither reran a workload.

`plan.json`, `execution.json`, `receipts.json`, `baseline.json`, `durable.json`
and `audit.json` bind the inputs, actual code/binary hashes and observed results.
The small public `fixture.json` preserves the exact registered payloads and
source metadata. Bulk raw/shared journals and full build/test/audit logs remain
under ignored `.local/evidence/hyperliquid-integration-review/`.

For a new explicitly bounded reproduction, use a fresh output path:

```sh
cargo build --locked -p marketfeed-daemon --example paced_shared_feed
python3 docs/evidence/paced-ingress-diagnostic-20261020/supervise.py \
  . docs/evidence/paced-ingress-diagnostic-20261020/fixture.json \
  target/debug/examples/paced_shared_feed .local/paced-ingress-new-attempt
python3 docs/evidence/paced-ingress-diagnostic-20261020/audit.py \
  .local/paced-ingress-new-attempt \
  docs/evidence/paced-ingress-diagnostic-20261020/fixture.json
```

The supervisor records each arm's exit code and stops on unexpected failure; its
own successful exit only means supervision completed. The audit reconciles the
stored prefix while retaining `workload_passed: false` for failed arms. No arm
is qualified by the harness's provisional `passed` field alone.

Next implementation must provide bounded batching/admission and explicit causal
operation boundaries while retaining durable acknowledgments, atomic outcomes,
failure visibility and ordered replay. Increasing queues, dropping sync barriers
or calling the saved prefix complete would not repair the failure. Full causal
milestone B, broader platform/runtime qualification, and native-unit/continuity
gates remain open.
