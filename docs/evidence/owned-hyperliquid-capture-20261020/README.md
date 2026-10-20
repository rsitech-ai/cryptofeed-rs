# Owned Hyperliquid public capture: local qualification

The opt-in `marketfeed-capture` command owns public spot/perpetual sockets,
journals native input and application-write dispositions in MFCJ5, and independently
reexecutes committed history into the shared MFOP feed. See the
[capture runbook](../../ops/hyperliquid-causal-capture.md) and
[wire contract](../../plan/causal_application_wire_owner.md).

The latest policy-2 successor passes all ten local gates and a new public capture:
1,415 groups / 271 items agree across native verification and Quants. BBO
acquisition-minus-native-clock peaks are 0.940 seconds spot / 1.445 seconds perps
in this short sample. See [the successor validation](timer-policy-v2-validation.json)
and [its exact source](timer-policy-v2-source.json). This is not a clock, latency
SLA, continuity or extended-soak qualification. hlscreen's coordinated revision
and the successor's own hosted checks remain separate gates.

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
smokes. Quants RAM-only checkpoint restarts are now locally verified; strategy
feature admission and the current native owner contract package remain separate.
The new producer requires its own hosted macOS/Linux results; predecessor hosted
passes are historical evidence only. Diagnose acquisition backlog, then rerun the
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
