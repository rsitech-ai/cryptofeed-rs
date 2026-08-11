# Reflection: release canary memory HOLD

## Task

- **ID / title:** UI release canary qualification
- **Date:** 2026-08-11
- **Scope:** Public, read-only multi-venue runtime and UI projections
- **Authority boundary:** Local code, tests, and canaries only; no push, PR, merge, trading, or deployment

## Success and Risk

- **Success criteria:** Exact clean one-hour GO with all fail-closed gates passing
- **Hypothesis 1:** Global projection locking caused API starvation and high CPU
- **Hypothesis 2:** Repeated adaptive calibration and rollover cloning caused excess CPU and transient allocation
- **Hypothesis 3:** Retained full price-level candle history causes the remaining RSS trend
- **Rollback path:** Revert isolated performance commits; evidence is ignored and runner-owned processes shut down cleanly

## Candidate Directions

| Candidate | Expected benefit | Main risk | Evidence before choice | Decision |
|---|---|---|---|---|
| Isolate projections and cache adaptive baselines | Remove contention and repeated work | Semantic drift in live analytics | Status timeouts and CPU p95 234.7% | Retained; API and CPU gates passed |
| Bound and share finalized candle history | Reduce retained duplicate state | Shorter calibration context | RSS growth remained above limit after earlier compaction | Retained; memory improved but did not pass |
| Waive or raise RSS slope | Immediate apparent GO | False production claim | Peak RSS was safe but slope repeatedly failed | Rejected |

## Evidence

- **First meaningful failure signal:** `/v1/status` timeouts during an exact canary under near-100% CPU
- **Commands or runtime checks:** full workspace tests/lint, UI tests/build, focused analytics tests, release canaries, log inspection, listener cleanup
- **What the evidence ruled in or out:** API serialization, queue pressure, sustained CPU, daemon warnings/errors, forced shutdown, and peak RSS are no longer blockers; retained/allocator memory growth remains unresolved

## Decision

- **Root cause or remaining unknown:** Full adaptive candle history is a material contributor, but the split between live retained heap, session-profile growth, and allocator-resident released memory is not yet measured.
- **Retained fix / direction:** Keep bounded/shared history and the four-candle UI calibration window; next instrument allocations before changing behavior further.
- **Why alternatives were rejected:** Repeated speculative reductions risk degrading signal semantics; threshold waivers would invalidate the release gate.
- **Residual risk:** RSS growth may continue during long volatile sessions; public venue reconnects can independently fail future runs.
- **Rollback trigger:** Any analytics parity regression, corrupt snapshot, missing UI signal, or worse long-run resource behavior.

## Reusable Lesson

- **Pattern to retain:** Treat short canaries as falsification gates and preserve exact evidence even when most metrics improve.
- **Pattern to avoid:** Inferring production stability from low peak memory or a clean UI smoke while the trend gate is failing.
- **Where it applies next:** Long-lived read-only analytics projections and future 24-hour beta qualification.
