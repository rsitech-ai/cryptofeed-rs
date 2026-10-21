# Owned Hyperliquid public capture: local qualification

The opt-in `marketfeed-capture` command owns public spot/perpetual sockets,
journals native input and application-write dispositions in MFCJ5, and independently
reexecutes committed history into the shared MFOP feed. See the
[capture runbook](../../ops/hyperliquid-causal-capture.md) and
[wire contract](../../plan/causal_application_wire_owner.md).

The receipt-wake plus bounded-overlap successor, source recipe
`c576bfa5892daba8815f8bee2c7f07acedeb650b567a3b9ed5d68a59ece73ce1`,
passes two consecutive full macOS workspace runs: **1,668 passed, zero failed,
56 ignored** in each. The entire existing paused32 test file, its ten-second
watchdog and default test concurrency are unchanged. The owner now waits directly
for a durable receipt up to its existing polling interval, so ACK delivery can
wake it without waiting on an unrelated condition variable. This changes no
native permission, event ordering or durability barrier.

[Local validation](receipt-wake-validation.json) and the [566-input source
inventory](receipt-wake-source.json) bind Rust1.85 default108/core314/native112/
public80, the release build/core314/native112/public80, denied-warning checks,
and fresh audit/deny/gitleaks passes. Earlier failures remain retained below.
Live timing and the final coordinated consumer pins require their own evidence;
these local tests do not establish native availability or a delivery SLA.

The new [joint run10](joint10.json) passes the unchanged two-second final-file
mtime gate at **0.024975966 seconds**. All four producer/consumer/verifier children
exit0; 6,022 records, 1,173 projected items and 1,468 released native rows reconcile.
The 131 earlier originals and all eight new archive files remain unchanged during
verification and audits. This uses the qualified predecessor HLS5b9/dbad and
published Quantsa1d; final new-pin HLS qualification remains separate.

This closes this sample's source-to-projection tail, **not native acquisition
freshness**. Perps timestamp/receipt differences reach 46.628 seconds for BBO,
45.145 seconds for books and 46.053 seconds for subsequent trades. Owner ingress
to handoff is about 154ms at the median. Source6006 and target5988 physical
publications keep both writers occupied; no source cause or synchronized clock is
established. Source journal throughput needs further investigation before the
extended handoff. Source-finished/pre-target-finished intervals were not sampled
in this run, so their backlog stays unknown.

The earlier [joint run09](joint09.json) **fails** the unchanged two-second timing
gate at 3.473495950 seconds. The exact published producer `23859663` and rebuilt
hlscreen `5b9f89c8` reach matching 6,568 sealed records, 1,282 items and 1,551
released native rows; all four children exit0. Two completed BBO rows are withheld
at Stop. All 123 earlier originals and eight new files remain unchanged. Unlike
run08's unsampled final-source interval, run09 observes the final committed source
while the target is still 1,861 records behind. Perpetual BBO native/acquisition
deltas reach 42.105 seconds, with native timestamps advancing about 40.339 seconds
less than acquisition time during the sample. These unsynchronized diagnostics
do not isolate client, server, transport, filesystem or native last-change causes.
The earlier release run08 remains a short passing diagnostic, not sustained
performance qualification.

The earlier overlap-only candidate `0ff75eba` was implemented but **not fully
qualified**. It retains the existing N<=8 logical-record window, byte limits,
one-millisecond age checks and all durability barriers. A genuine held-ticket
regression reproduces the current wait-before-prepare behavior. The proposed
successor prepares within that same window while earlier writes are pending,
then validates every transaction receipt covering a whole physical publication
before advancing paired cursors. No source-owner or external-I/O barrier changes.
The exact candidate passes all 23 focused projection tests, formatting and denied-warning
Clippy. Its full stable run fails the existing paused32 10-second watchdog:
805 tests pass, one fails and 46 are ignored before Cargo stops early. The unchanged
retained executable passes that case alone in 5.54 seconds; the full-suite cause
remains unresolved. Rust1.85 default108/core311/native112/public80 and the existing
release build/core311/native112/public80 gates pass. Denied-warning docs and
default CLI validation also pass separately. See the [partial validation](projection-overlap-validation.json);
it explicitly retains the failed full stable gate. No timeout, thread count,
capacity or workload was changed.
One sampled closed09 native verification completes in 0.794 seconds with all 131
originals unchanged; it identifies shared replay work, not the live bottleneck.

The buffered-watermark successor has its own local
[qualification](watermark-buffering-validation.json) and
[source inventory](watermark-buffering-source.json), recipe
`002a2f5bc3c329c2979f6083ff431f207dea826c3d2b27cc9267cd6f1ff84ac0`.
All ten local gates pass: macOS 1,656/0/56; Rust 1.85 default 108, core 311,
native/bootstrap 103 and public integration 80. Actual dependency audit/deny09
passes apply to its byte-identical manifests, lockfile and dependency inputs;
this change adds no dependency or exception. The retained debug-profile capture
is `e6c69783…` and bridge `d96cfc49…`.
Its own published [hosted qualification](hosted238.json) passes macOS 1,656 and
both Linux architectures 1,657, with zero failures and 56 ignored per host;
MSRV/dataset checks pass 795 executions. All 17 jobs are terminal: 16 successful,
one secondary Windows paused-input watchdog failure, zero cancellations. These
results qualify source `002a2f5b`, not the new overlap candidate.

The real regular-file regression reproduces 111 backing reads for a 110-byte
watermark before the fix, then passes with at most two. A fresh bounded buffer
coalesces reads without caching across calls. The existing 4,097-byte cap,
short reads, I/O failures, JSON/schema checks and every-record watermark refresh
remain. This proves a read-amplification repair, not sole causality for backlog
or acquisition lag.

The subsequent debug-profile [joint run07](joint07.json) **also fails** the
unchanged two-second tail gate: 9.2871 seconds, with 1,413 groups pending at the
first post-source sample. All four children exit0 at matching 6,148 groups,
1,196 items and 1,384 released native rows; target has 4,987 transactions.
Two completed native rows are withheld at Stop. All 107 earlier originals and
eight new files remain unchanged. Unsynchronized BBO deltas reach 6.217 seconds
spot / 17.170 perps. These uncontrolled samples do not isolate why their measured
tails differ. The existing release profile is qualified separately below;
passing release results cannot repair or relabel a failed debug result.

The unchanged source also passes the existing portable Cargo release-profile
build and 311 core, 103 native/bootstrap and 80 public integration tests, with
denied warnings. See [release-profile validation](release-profile-validation.json).
Retained capture `a626feec…` and bridge `f447e6b3…` are fresh release-build outputs;
the capture embeds its recipe, while the bridge is bound by actual build-output
and retained-file hashes. The runtime request, one-millisecond grouping age and
all queue/byte/operation caps are unchanged. This is neither a full release
workspace test nor release MSRV, Linux runtime, SLA or data-admission qualification.

The subsequent release-profile [joint run08](joint08.json) passes the unchanged
two-second final-file-mtime diagnostic at 0.882445256 seconds. Capture, native
verification, Quants and the retained predecessor hlscreen consumer all exit0 at
the same sealed boundary: 5,554 groups, 1,079 projection items and 1,172 released
native rows. Independent audits reconcile CRCs, paired cursors, native routing
and all consumer counts; all 115 earlier originals and eight new files retain
their recorded hashes and metadata. The last physical projection publication
contains seven complete records. The final committed source boundary was not
sampled before target finalization, so backlog during that interval is unknown.
Unsynchronized BBO deltas reach 1.991 seconds spot / 5.090 perps; this remains
a development diagnostic, not clock, availability, continuity or SLA admission.
The request is byte-identical to runs06/07 and the existing build profile is the
only declared change. Uncontrolled traffic prevents a causal profile comparison.
Current-head hosted qualification and a freshly pinned hlscreen build remain
separate gates; the debug timing failures above remain failures.

The actual Quants checkpoint also passes [restart checks on run07](quants-checkpoint-joint07.json)
at groups 5, 11, 6,147 and 6,148, followed by sealed idle. Group 6,147 lies inside
the final two-record physical publication. Its 1,196-item state and paired
cursors agree after restart, and all 115 protected files remain unchanged.
This verifies a RAM-only structural reducer and durable checkpoint, not native
market-context admission or exactly-once external effects. The original receipt's
stale pre-execution sentence is retained with a prose-only correction receipt.

The preceding complete-group projection batch candidate has its own local
[qualification](projection-batching-validation.json) and
[566-input source inventory](projection-batching-source.json), recipe
`a80528023e75356be8e54cd07400031fef5f997fce8b8c6f7d37b9bdb3e74ef6`.
macOS passes 1,653 tests, 0 failures, 56 ignored; all ten local stable/MSRV checks
and fresh audit/deny checks pass. Rust 1.85 passes default 108, core 308,
native/bootstrap 103 and public integration 80. The retained capture artifact is
`ce7d3311…`; the bridge is `f27ab3b8…`. These are debug-profile local artifacts,
not release or Linux qualification. Its actual joint run06 **fails** the unchanged
two-second development tail diagnostic: 4.1458 seconds, with 552 groups pending
at the first post-source sample. All four children exit0 at matching 5,438 groups,
1,055 items and 1,231 released native observations; target has 4,400 physical
transactions. All 99 older originals and eight new capture files remain intact.
See [run06 reconciliation and failure evidence](joint06.json). Unsynchronized
BBO acquisition-minus-native-clock maxima are 4.740 seconds spot / 14.672 perps.
The buffered successor does not relabel this failed result.

The preceding combined source08 run failed the paused-input watchdog. Its exact
source, executable and raw log remain retained. Same-executable diagnostics
reproduced failure in the full suite and passed when the new heavy native
comparison fixture was omitted. Source09 shares a FIFO permit between these two
fixture lifetimes, acquired before fixture construction and their existing
watchdogs. Both cases, default test threads, assertions, caps, watchdogs and
within-case concurrency remain. This repairs fixture interference; it does not
establish the I/O cause of the timeout or repair production freshness.

The longer simultaneous producer/hlscreen/Quants run05 passes structural
reconciliation but **fails its timing gate**. All four processes exit0 at matching
6,203 groups, 1,209 projection items and 1,468 released native display observations.
Target publication ends 34.0466 seconds after the source file's final mtime;
2,288 groups remain outstanding in the first sample after source completion.
Unsynchronized native-to-acquisition BBO maxima are 7.753 seconds spot and
28.427 seconds perps. See [the independent run05 receipt](joint05.json).
Correct persistence and eventual synchronization do not establish timely delivery.
The bounded physical projection-batching candidate's local qualification above
does not replace this failed timing result.

The earlier 30-second policy-2 successor passes all ten local gates and a public capture:
1,415 groups / 271 items agree across native verification and Quants. BBO
acquisition-minus-native-clock peaks are 0.940 seconds spot / 1.445 seconds perps
in this short sample. See [the successor validation](timer-policy-v2-validation.json)
and [its exact source](timer-policy-v2-source.json). This is not a clock, latency
SLA, continuity or extended-soak qualification. Its own
[hosted result](hosted5479.json) independently binds source `5479d733` and all
566 compiled inputs: macOS passes 1,642 tests, both Linux architectures pass
1,643, and MSRV checks pass. The run has 16 successful jobs; the secondary Windows
paused-frame watchdog remains failed, with no cancellations. hlscreen's
revision `af6ff557` passes its own eleven local checks and the joint run05
structural check; its hosted/native artifact checks remain separate.

The earlier source05 slice passed its local correctness checks but **failed its
timing diagnostic**: in that 30-second public pilot, spot BBO acquisition-minus-native-clock
median/max was 6.378/11.163 seconds; perpetual BBO was 7.056/10.701 seconds.
Remote/local clock synchronization is unqualified, and context updates carry no
native timestamp. A clean storage seal establishes neither freshness nor complete
market coverage. Do not admit this pilot as decision-time data.

## Exact source and checks

[source.json](source.json) inventories 566 build inputs with recipe SHA256
`7fe5ea7c6c496997b4053acb5e3bb3711497cb95afe522aac2826f70b3ce3fd5`.
[validation.json](validation.json) binds raw-log hashes, commands, retained
artifact pins, independent audit receipts and the outcomes below. Local capture
artifacts were built before the publication commit; their recorded Git parent and
exact compiled recipe are both retained. They are not release artifacts.

- Stable macOS: workspace 1,637 passed, 0 failed, 56 ignored; formatting,
  denied-warning Clippy, documentation, default-CLI and diff checks passed.
- Rust 1.85: core 308, native/bootstrap 87 and public integration 80 passed.
- The exact unchanged lockfile/TLS implementation passes the retained audit and
  cargo-deny checks, with no advisory exceptions. Mozilla roots 1.0.10 and verifier
  0.103.16 replace affected versions; the obsolete roots compatibility wrapper is
  removed. Trust, hostname, Pong and close regressions cover the normal connector.
- A paused-storage regression reproduces the original driver's excessive reads
  and passes with bounded backpressure, preserving frame/queue/deadline bounds.

The source04 MSRV public run failed one existing 15-second projection watchdog.
Its source, executable and log remain retained; the same executable passed a
controlled one-case diagnostic. Source05 only serializes helper-created temporary
target-writer lifetimes in correctness fixtures. This explicitly reduces unrelated
projector concurrency in those fixtures. Inputs, cases, capacities, thread settings
and the active watchdog are unchanged. Resource waiting precedes that watchdog.
It is not a production performance repair or proof of the original timeout cause.

## Public pilot and retained failures

Pilot03 used the original 30-second request: HYPE/USDC native `@107` spot and BTC
perpetual, trades/BBO/L2/mark, plus perpetual open interest. It completed with:

- capture, retained-executable native verification and Quants observer exit 0;
- matching source/target/Quants paired positions at 1,599 groups and 157 items;
- real connections and requested native channels on both owners, committed clean
  Stop, five successful application writes per owner, zero abandoned inputs,
  unaccepted acquired frames or reported transport errors;
- independent physical CRC, native-channel routing, row census, cursor and writer
  counter reconciliation. Source writer publication groups (1,596) differ from
  committed operations (1,599); target has 1,599 of each.

Initial trade deliveries contain 30 historical rows per market and are excluded
from subsequent-trade timing diagnostics. Ingress-to-handoff observations were
below 176 ms, but exclude backlog before acquisition and final record ACK. They
cannot replace acquisition freshness or sink-delivery latency measurements.

Pilot01 failed because a valid 30-trade native payload exceeded a 4 KiB string
preset. The repair uses the existing 64 KiB frame ceiling; regressions accept
30/128 rows and still reject 129. Pilot02 failed closed on ingress saturation and
remains unsealed: 921 native-verified groups, only 842 Quants ACKs before its
declared failure abort. The successor driver waits for owner/wire disposition
before reading another frame. All failed captures, sources, executables and logs
are preserved without relabelling. Raw payloads and wallet associations remain
private; only hashes, counts and diagnostic summaries are published.

## Remaining gates

Finish the coordinated immutable hlscreen dependency upgrade and native artifact
smokes after the new producer qualifies. Quants RAM-only checkpoint restarts and
an opt-in adapter invoking the actual native context owner are locally verified;
the positive context profile is explicitly synthetic spot data. Real history,
availability, perpetual context and strategy admission remain separate. Quants
[draft PR98](https://github.com/s1korrrr/quants-lab/pull/98) now publishes the exact
tested `a1d45ac5` after user approval; the root gitlink is unchanged. Its own
[hosted run](https://github.com/s1korrrr/quants-lab/actions/runs/39561301014)
failed all four jobs before any steps because GitHub reports account payment or
spending-limit restrictions. See [publication evidence](quants-publication.json).
The account owner must resolve Billing & plans before the unchanged workflow can
run; no hosted Mac/Linux test result is claimed.
Published producer5479's own macOS/Linux results are in `hosted5479.json`;
they do not qualify this new source. Check longer acquisition
and full consumer-delivery timing, then rerun the
registered ingress comparison unchanged after actual consumer integration.
Extended Razer/24-hour/7-day checks remain deferred by the user. No dataset,
availability-time, feature/economic, strategy activation or release qualification
is granted by these results.

## Successor feature-gate repair

The source05 hosted run exposed a default-feature Rust 1.85 compile failure:
the paused-storage test helper compiled while its `causal-journal` tests were
disabled, producing three denied dead-code errors. The retained failure remains
part of run 39499150521. [msrv-feature-gate-fix.json](msrv-feature-gate-fix.json)
binds the successor's nine test-only feature gates, independent review and actual
denied-warning Rust 1.85 checks: default library 108 passed; opt-in causal library
87 passed; formatting/diff passed. All other source bytes are unchanged. This
narrow local check does not substitute for the successor's own hosted results.

The supplement also corrects a copied sentence in the original pilot03 timing
receipt: pilot03 has **zero unaccepted frames**. The original wording referred
to an unaccepted frame from failed pilot02. All numeric pilot03 diagnostics,
including the acquisition-lag failure, are unchanged; the original receipt is
retained.

## Policy-2 successor and latest public pilot

The original driver journaled 153 no-due timer polls in pilot03. Policy 2 uses
the latest observed committed native timer schedule to admit only due polls.
Version 1 remains supported with its periodic behavior. Wake frequency, native
64-fire cap, queue/frame bounds, quiescent admission, ACK barriers and Stop checks
are unchanged. Genuine legacy red evidence, deterministic due/read/Stop tests and
full native replay are retained. Unknown policy versions reject preflight.

The successor source recipe is `16a538c033fb37fa10112f66a6371ab7408512a7f991e95add1640af5a8afa95`.
All ten local gates pass with denied warnings: workspace 1,642/0/56; Rust 1.85
default library 108, core 308, native/bootstrap 92 and public integration 80 pass;
formatting, Clippy, docs and default CLI pass. Fresh audit and cargo-deny pass with
zero advisory exceptions. [The prior hosted run](hosted8de.json) passed the full
Mac/Linux suites but failed MSRV compilation and a secondary Windows watchdog;
it is not relabelled as a successor result.

Pilot04 used identical request bytes and bounds. Both markets have one actual due
timer callback, zero empty polls, five successful wire triplets, clean committed
Stop and zero abandoned/unaccepted inputs or reported transport errors. CRCs,
native routing/counts, final paired cursor and Quants ACK agree. Source publication
has 1,415 committed records / 1,347 transactions; target has 1,415 / 1,415. All 83
previous original files retain their recorded hashes and metadata.

Initial historical trade rows remain excluded only from the subsequent-delivery
timing proxy. Context has no native timestamp. These sequential public samples
do not isolate the cause of the lower observed acquisition deltas. Full external
delivery latency and decision-time availability remain unqualified.

The unchanged Quants checkpoint code also passes actual producer-backed restarts
at groups 5, 11 and 1,415, plus final sealed idle, with exact 1,415/271 state and
cursor agreement. It remains a locally committed RAM-only structural consumer;
no rsibot push, root gitlink or strategy admission follows. A harness bookkeeping
error caught normal `SystemExit(0)` after all three capture/observer/verify exits
were zero; the original script/log/diagnostic and a future-only correction are
retained. No capture was rerun or overwritten for that error.
