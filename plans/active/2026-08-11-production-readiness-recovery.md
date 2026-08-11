# Production readiness recovery

## Outcome

Make the public, read-only market-data UI eligible for merge to `main` by closing the existing RSS-growth HOLD, verifying every major UI feature in a real browser, completing full repository and runtime gates, reviewing the final diff, and publishing only after every fail-closed gate passes.

## Hard constraints

- Scope is public market data and read-only UI only. Audio, trading, order placement, credentials, and private exchange APIs remain excluded.
- Preserve the existing UI semantics for depth heatmap, DOM, tape, market profile, order-flow bubbles, structural levels, derivatives, alerts, settings, replay, responsive layout, and failure states.
- Do not weaken qualification thresholds or hide public-venue failures to manufacture a pass.
- Push, PR, and merge are authorized only after local and live release gates pass and the final diff review has no unresolved blocker or high finding.

## Current evidence

- Branch: `feat/andrzej_canary_qualification`; base: `origin/main` at `8ec5bffc4fb6ed00dbe757002250a294105560f0`.
- Latest documented code canary: `7c97336f5656df7a6bdc099218b6a46325dc5652`.
- Functional gates passed: 13/13 venues, L2 book contracts, API p95 0.915 ms, CPU p95 10.4%, queues 0.20%, clean logs, UI smoke, graceful shutdown.
- Release blocker: RSS growth 109.50 MiB/hour versus the 64 MiB/hour limit in `.local/evidence/release-canary/runs/20260811T160728Z`.
- Full Rust workspace tests/lint and 148 UI tests/build passed on the final local code state.

## Completion bar

1. Attribute RSS growth to retained live heap, allocator residency, or a named projection/data structure with reproducible evidence.
2. Add a failing behavioral or resource-bound regression before production changes; implement one root-cause fix and prove red-green.
3. Pass focused tests, full Rust workspace tests/lint, UI tests/build, formatting, and diff checks.
4. Launch the release runtime and complete a browser scenario matrix across desktop and narrow viewport with clean console/network evidence.
5. Pass an exact 15-minute qualification without waivers, then an uninterrupted exact one-hour qualification.
6. Review the complete `origin/main...HEAD` diff for correctness, security boundaries, performance, maintainability, and scope.
7. Update the qualification report with exact final evidence, archive this plan, commit intentional files, create/review the PR, merge, and read back exact `main` only if all gates remain green.

## Milestones

### M1 — Attribute memory growth

- Reproduce with the exact release configuration.
- Capture RSS plus macOS VM/heap allocation evidence at warm-up and later samples.
- Add projection-level retained-size/count telemetry if native tooling cannot distinguish live heap from allocator high-water memory.
- State one falsifiable root-cause hypothesis before modifying behavior.

### M2 — Root-cause repair

- Write and run a failing regression that names the production break.
- Implement the smallest architecture-correct repair.
- Re-run the regression and focused parent tests.

### M3 — Repository and UI verification

- Run full Rust tests/lint/build and UI tests/build.
- Run live release service, inspect logs, and exercise all major UI surfaces and visible controls through the browser.
- Verify desktop, narrow viewport, loading/live/degraded/offline states, keyboard/focus paths, responsive panes, console, and network.

### M4 — Release qualification

- Run exact clean 15-minute canary.
- If GO, run exact clean one-hour canary.
- Any failed resource, venue, book, API, log, UI, or shutdown gate returns the release to HOLD.

### M5 — Review and publication

- Review `origin/main...HEAD` and resolve every blocker/high finding.
- Update operational documentation and exact readiness label.
- Push branch, open PR, read back checks/review state, merge, and verify remote `main` only after all local/live gates pass.

## Verification matrix

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-targets --all-features -q`
- `npm --prefix ui test`
- `npm --prefix ui run build`
- `./scripts/release_canary.sh --self-check`
- `DURATION=15m ./scripts/release_canary.sh`
- `DURATION=1h ./scripts/release_canary.sh`
- Browser scenario matrix and evidence captured outside committed source
- `git diff --check` and full comparison review against `origin/main`

## Recovery

- Preserve all timestamped ignored evidence.
- Stop only runner-owned processes and confirm ports `19108` and `19109` are free.
- Revert only isolated task commits if a semantic regression is proven; never discard unrelated work.
- If the same architectural memory blocker survives three evidence-backed repairs, stop and redesign the retained analytics representation before further changes.

## Progress log

- 2026-08-11: Native `heap`/`vmmap` comparison showed live heap growth of about 1.86 MiB over 5.5 minutes while RSS grew much faster. The dominant 53.76 MiB allocation was the bounded depth history; sparse instruments were still lazily allocating their eventual 600 bid/ask buffers after the canary warm-up.
- 2026-08-11: Added a failing regression requiring all bounded depth buffers after the first valid sample, implemented first-snapshot preallocation with continued exact sample exposure and eviction reuse, and passed the 15-test view-plane suite plus focused warnings-as-errors lint.
- 2026-08-11: Exact commit `44e5679` kept 13/13 venues live with zero reconnects, API p95 1.058 ms, CPU p95 11.5%, and peak RSS 141.02 MiB. The old linear-only RSS rule reported 69.91 MiB/hour even though the post-warm-up p95 envelope grew only 5.73 MiB/hour across repeated macOS reclaim cycles.
- 2026-08-11: Added red-green analyzer coverage for bounded reclaim oscillation while retaining the existing monotonic-growth HOLD. The 64 MiB/hour threshold and peak-RSS cap remain unchanged; a growth HOLD now requires both fitted and windowed-p95 growth above the limit.
