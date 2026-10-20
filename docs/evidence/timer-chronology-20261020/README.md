# Timer admission and saved chronology repair

The replay-path audit found two scheduler defects in producer `a87d988`:

- An overdue signed deadline difference was cast to `u64`, so a timer overdue
  by one nanosecond returned a delay of `u64::MAX` nanoseconds. Positive
  differences crossing the signed range were also clamped prematurely.
- Due timers were snapshotted by ID. A callback could postpone another ID to
  a future deadline, but the old snapshot still removed and fired it early.
  The premature firing was also persisted as a `FeedRecord::Timer`.

`SessionRunner::delay_until_next_timer` now returns zero for due/overdue timers
and the exact unsigned difference for future timers, including the full signed
timestamp range. `poll_timers` rechecks the current deadline immediately before
removal and recording. Existing ascending-ID order and the 64-delivery per-poll
cap remain. The cap bounds one poll, not the rate of repeated overdue polls.

Independent review also identified a stale live-loop sample: the next wait used
the wall time taken before awaited write/HTTP flushing. The loop now resamples
after that work and updates clock-jump accounting before calculating its wait.
The existing 250 ms stop-poll cap remains. That cap already bounded the enormous
delay's effect in this live loop; it did not make the public delay API correct.

## Reproduction and verification

Raw local logs remain under `.local/evidence/hyperliquid-integration-review/`.
`validation.json` binds source, commands, results and log hashes. The initial
eight-test reproduction failed four cases: overdue delay, signed-range delay,
postponing a due timer, and immediate polling after bounded overdue rescheduling.
That log is retained as `timer-regressions-before-01.log`; it predates the final
test additions and is not a run of the final nine-test source.

The final nine scheduler tests cover future/due/overdue/backward-clock delays,
signed timestamp extremes, deterministic simultaneous timers, callback-driven
postponement/cancellation/already-due replacement, and two consecutive capped
polls. The postponement test opens the actual shared-feed reader and requires
exactly `(session=1, timer=1, now=100)` then `(session=1, timer=2, now=500)`.
An early or duplicate persisted firing fails that assertion.

Exact source `b72f173ed6d50d582203b64d66b35ff951e4ec94` passes 1,279 local
workspace/all-target/all-feature tests (0 failed, 56 ignored, 149 suites), all
nine scheduler cases on Rust 1.85, workspace Clippy, warning-denied rustdoc,
formatting and pinned cargo-deny 0.20.2. The initial candidate workspace run and
Clippy failure are retained separately; the full suite was rerun after the
lint-only predicate simplification. Ignored cases remain unexecuted live/corpus
checks, not verification of those scopes.

CI Linux passes 1,280 tests (0 failed, 56 ignored, 149 suites). The additional
case is `latency_runtime::tests::linux_rejects_core_beyond_setsize`. Checkout
`a5f08b9d6275544dfb0ac74c7bf3deea53462923` has exactly the same complete tree
`adb0cdbdd8d0ae9829de5ab36b9d1bc33f38a0e8` as source `b72f173`. The captured
CI snapshot has 16 successful jobs with the full Windows suite still running;
later job status must be inspected directly rather than inferred from this
historical receipt. `validation.json` records each job URL and disposition.
For its portable policy command, bind `${CARGO_DENY_0_20_2}` to the verified
0.20.2 binary; the older local 0.19.0 tool is not the qualification tool.

Independent reviewer `/root/trade_review` inspected both defects, the final code
and raw red/green logs. It requested callback cancellation coverage and found
the stale live-loop sample; both were addressed. It then independently executed
the final nine-test suite on source `b72f173` (9 passed, 0 failed/ignored), checked
the source hashes before/after, and found no residual actionable defect in this
patch. Its raw log is `timer-independent-focused-01.log`. It did not edit source.

## Scope and remaining gates

The nine offline regressions do not exercise delayed asynchronous HTTP/write
flushing or measure actual wake latency. The resampling change is source-reviewed;
end-to-end async timing qualification remains open. This patch adds no clock
injection, replay API, recording schema, automatic timer simulation or scheduling
guarantee across wall-clock jumps.

General mixed-session/generation causal replay (D5), late metadata availability,
continuity and strict research admission remain unfinished. This is a scheduler
prerequisite repair, not their completion. No capture, economic run, operational
promotion or new soak evidence is claimed. Hlscreen's five shared packages remain
pinned to producer `4c2bd15`; this patch changes engine implementation/tests only,
not those packages or their serialized contracts.

Hlscreen `6f13f02` subsequently reaches terminal CI: 12 checks pass, including
four target artifact builds and global artifact assembly. Scheduled public API
smoke, tag publication and release announcement are skipped. The receipt retains
all three skipped dispositions; these checks provide no new live/soak evidence
or release authority.

Rollback selects the previous producer revision. Existing saved feeds remain
unchanged; do not rewrite earlier timer history as if the defect had never
occurred. Historical data still requires its own qualification.
