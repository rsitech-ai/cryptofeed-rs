# Release canary qualification

## Goal

- User-visible outcome: a repeatable, read-only qualification gate for the UI-enabled release binary, followed by a supervised one-hour live run and an evidence-backed GO/HOLD recommendation for the next 24-hour beta gate.
- How to see it working: run the documented command, inspect the ignored evidence bundle, and read the checked-in qualification report.

## Current State

- Relevant paths: `scripts/laptop_soak.sh`, `scripts/live_ui_smoke.sh`, `scripts/parse_fixtures_gate.sh`, `docs/ops/`, `.local/live-ui/config.live.ui.toml`.
- Existing behavior: adapter canaries and a basic debug-build RSS soak exist; the live UI release binary has bounded runtime evidence, but no reusable threshold analyzer or comparative parser baseline.
- Constraints: public/read-only market data only; no audio, trading, order placement, credentials, CI remediation, or maturity promotion. Preserve ignored operator artifacts and unrelated work.

## Target State

- Desired behavior: a release-canary command builds or accepts the exact UI-enabled release binary, starts it on isolated ports, verifies readiness and UI/API contracts, samples process and venue health, shuts down cleanly, and emits machine-readable plus Markdown evidence with an explicit GO/HOLD verdict.
- Desired behavior: the parser benchmark has a host-local baseline and a fresh comparative gate result.
- Non-goals: scheduled automation, 24-hour execution in this slice, beta/stable promotion, authenticated feeds, external sinks, audio, or trading.

## Risks and Failure Modes

- Public venue outages or rate limits can produce transient failures that must be recorded rather than hidden.
- A sampler can accidentally mistake counter totals for run deltas or fail to distinguish optional venues from qualified L2 venues.
- A long-lived child can survive interruption unless cleanup and port ownership are explicit.
- Laptop RSS/CPU and benchmark measurements are local evidence, not portable SLOs.

## Milestones

### M1. Pin qualification semantics

- Goal: encode deterministic analysis of fixture samples before runtime orchestration.
- Files / systems: canary analyzer tests and implementation.
- Changes: fixtures for pass, counter regression, health loss, book loss, queue saturation, and memory growth.
- Verification: analyzer unit tests fail before implementation and pass after it.
- Expected result: a stable GO/HOLD contract independent of live network conditions.

### M2. Add release-canary runner

- Goal: collect bounded evidence from the exact release build and terminate safely.
- Files / systems: `scripts/`, `.local/evidence/release-canary/`.
- Changes: isolated config/ports, hashes and Git metadata, readiness wait, UI smoke, periodic samples, signal-safe cleanup, final analysis.
- Verification: self-check plus a short live qualification run.
- Expected result: a complete evidence directory and no surviving daemon/listener.

### M3. Establish comparative performance evidence

- Goal: close the missing local parser baseline without overstating it.
- Files / systems: ignored local baseline and `scripts/parse_fixtures_gate.sh`.
- Changes: record a multi-run host-local baseline, then independently rerun the comparison.
- Verification: baseline metadata exists and the subsequent gate passes within its documented tolerance.
- Expected result: performance-regression evidence for this machine and commit.

### M4. Run supervised one-hour canary and report

- Goal: make the first qualification decision on the release binary.
- Files / systems: public venue WebSockets/REST, local UI/API, `docs/ops/`.
- Changes: run one hour, review raw logs and summary, record exact result and limitations.
- Verification: clean startup, UI smoke, health samples, counter deltas, resource trend, graceful stop, log review, full project gates.
- Expected result: GO or HOLD for a separate 24-hour beta qualification, never an automatic maturity promotion.

## Verification

- `python3 -m unittest scripts/tests/test_release_canary.py`
- `./scripts/release_canary.sh --self-check`
- `RUNS=5 ./scripts/parse_fixtures_gate.sh --write-baseline --simd`
- `RUNS=5 ./scripts/parse_fixtures_gate.sh --simd`
- `DURATION=1h ./scripts/release_canary.sh`
- Project lint/test/build gates discovered from repository documentation.
- Manual smoke: open the live UI during the run, exercise primary chart/depth/tape/settings paths, inspect console/network, and verify shutdown leaves both ports free.

## Decision Log

- 2026-08-11: Reuse the repository's ignored evidence convention and existing UI smoke rather than introducing a second observability stack.
- 2026-08-11: Treat qualified L2 venues separately from optional public venues; public-network noise remains visible but does not silently redefine book-integrity gates.
- 2026-08-11: Use a one-hour first run because it meets the requested 1-2 hour qualification while leaving the 24-hour beta gate as a distinct next step.

## Progress Log

- 2026-08-11: Completed repository, metrics, existing canary/soak, and release topology inspection.
- 2026-08-11: Next: write analyzer tests first, implement the collector/runner, then execute short and one-hour qualifications.

## Rollback / Recovery

- If this fails: preserve the timestamped evidence bundle and classify the exact failed gate; do not promote readiness.
- Safe fallback: terminate only the runner-owned PID, verify isolated ports are free, and leave the existing live UI config and ignored artifacts untouched.
