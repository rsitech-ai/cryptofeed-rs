# Legacy streaming replay evidence — 2026-10-20

This receipt covers D5 milestone A and the strict MFR record-reader repair.
The source is bound by parent commit and exact file hashes in `validation.json`.
It does not establish complete engine causal replay or strict dataset admission.

The reader previously treated one to three trailing length bytes as clean EOF.
The retained baseline regression fails; `read_record` now rejects the torn word,
while the explicitly recovering `read_all` API still discards an incomplete tail.

The new metadata-directed runner streams one physical record at a time through
independent session machines and a synchronous whole-step sink. The 2,048-frame
fixture exceeds the old two-batch capacity without read-ahead or retained output
history. Exact values, backwards clocks, late/repeated/conflicting metadata,
subscription wire comparison, input/output quotas, callback/sink failures and
caught unwinds have focused challenges. Review found and fixed system-event
budget bypass, retained rejected actions and weak quota-error assertions.

Local current-toolchain and Rust 1.85 focused checks each pass 93 tests. The full
workspace/all-target/all-feature suite passes 1,302 (0 failed, 56 ignored). Those
ignored cases include opt-in live/corpus tests and remain unexecuted. Workspace
Clippy, warning-denied docs, formatting, exact cargo-deny 0.20.2 and gitleaks 8.30.1
checks pass. All 269 registry package records are unchanged. Independent
review runs 32 replay tests and 1 reader regression on matching source hashes.

Full-lock RustSec still exits 1 for the existing time 0.3.41/RUSTSEC-2026-0009
MSRV exception, with 0 warnings. No clean security clearance is claimed. Initial
red regression and lint failure logs are retained with hashes, alongside green
checks. Raw logs remain under the ignored evidence paths listed in the receipt.

MFR lacks observed startup, physical generations, timer-poll boundaries,
callback outcomes and queue drains. The required causal milestone B is specified
in the implementation plan, including actual-runner ownership and atomic typed
operation publication. That audit also identifies the existing catalog-refresh
u16 index overflow above 65,536 instruments as a required repair, not a completed
fix. No new live capture, soak, strategy/economic run, release or deployment occurs.
Consumer pin migration and platform CI are separately required; this initial
receipt does not claim their completion.

See [API contract](../../spec/legacy_streaming_replay.md) and
[D5 completion contract](../../plan/trade_data_integrity_implementation_plan.md#d5-delivery-milestones-and-remaining-causal-replay).
