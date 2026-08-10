# CryptoFeed release audit, PR, and main integration

## Goal

- User-visible outcome: every reachable read-only CryptoFeed feature and UI
  surface is audited, defects are repaired, the real daemon/UI is restarted and
  exercised, and the resulting coherent change is reviewed through a pull
  request before integration to `main`.
- How to see it working: a clean local release build serves the production UI;
  public Binance USD-M, OKX Swap, and Bybit Linear market-data paths populate
  the dashboard; controls, responsive layouts, degraded states, and relaunch
  behavior work without relevant browser or daemon errors; the merged GitHub PR
  and `origin/main` resolve to the reviewed commit.

## Current State

- Relevant paths: the Git root `/Users/s1kor/dev/apps/cryptofeed`, Rust crates
  under `crates/`, the Svelte UI under `ui/`, project docs, scripts, and this
  plan.
- Existing behavior: the recovery branch contains the read-only market-profile,
  three-tier bubble, derivatives, depth/DOM, workspace, and structural-level
  implementation. Earlier bounded checks were positive, but the full worktree
  remains uncommitted and must be freshly audited rather than promoted from
  prior evidence.
- Constraints: preserve all intentional existing changes; no audio, account
  credentials, private streams, trading, or order placement; no live-money
  action; ignore GitHub Actions-only issues per user direction while keeping all
  local build/test/runtime failures actionable.
- Authority: local fixes, intentional staging/commit, branch push, PR creation
  and review, and non-force merge/push to `main` are authorized only after the
  local and PR gates below pass.

## Target State

- Desired behavior: one reviewable read-only application whose source semantics
  match current official exchange contracts, whose boundaries validate inputs,
  whose state is bounded and recoverable, and whose UI accurately communicates
  live, stale, partial, offline, and unavailable states.
- Non-goals: Codex Security scan; GitHub Actions repair; audio; authentication;
  balances, positions, PnL, orders, or order placement; claims of multi-day
  production soak or exchange-private-data coverage.

## Risks and Failure Modes

- Exchange stream sequencing or unit assumptions can create plausible but false
  books, funding, open-interest, or liquidation displays.
- A large mixed Rust/UI diff can hide dead code, unbounded state, stale timers,
  silent fallback, or serialization mismatches not covered by unit tests.
- Dense chart overlays and DOM updates can regress responsiveness, accessibility,
  memory, or main-thread latency despite a green build.
- Live public endpoints can be temporarily unavailable; source failure must be
  distinguished from an application defect and surfaced fail-closed.
- Publishing before exact diff and post-merge SHA verification can integrate an
  incomplete or different tree.

## Milestones

### M1. Establish authoritative scope and contracts

- Goal: inventory the full diff, project rules, feature surfaces, and current
  official public exchange contracts.
- Files / systems: Git history/worktree, Cargo/package configuration, project
  docs, Binance/Bybit/OKX adapter and API code, official documentation.
- Changes: documentation/audit notes only unless a concrete defect is proven.
- Verification: read-only Git comparison to `origin/main`, dependency/script
  inventory, route/subscription/parser mapping, official-source cross-check.
- Expected result: a traceable requirements and scenario matrix with no assumed
  feature or transport semantics.

### M2. Static and automated correctness audit

- Goal: find correctness, boundary, architecture, maintainability, security,
  performance, and regression defects across all changed and adjacent code.
- Files / systems: all changed Rust, Svelte/JavaScript, tests, configs, docs, and
  generated production assets.
- Changes: for each proven defect, record root cause, add a failing regression
  test first where behavior changes, apply the smallest repair, and re-run the
  focused path.
- Verification: formatting, clippy, workspace tests, UI tests/build, diff check,
  dependency and forbidden-surface scans, focused regression commands.
- Expected result: no unresolved blocker/high finding and every changed behavior
  backed by a direct check.

### M3. Real runtime and UI interaction audit

- Goal: prove the production daemon/UI and every reachable user-facing feature
  through interaction, responsive states, logs, and live public integrations.
- Files / systems: release daemon with embedded UI, browser, HTTP/SSE routes,
  three configured public venues, process lifecycle and logs.
- Changes: repair only reproduced runtime/UI defects using the M2 discipline.
- Verification: cold start, first meaningful render, navigation/control matrix,
  narrow/laptop/wide resize, keyboard/focus/tooltips, live/partial/offline/error
  states, endpoint invariants, console/network review, performance sample,
  clean SIGINT and restart.
- Expected result: interaction-clean and bounded runtime-proven evidence for the
  exercised public-data paths.

### M4. Independent final review and PR hardening

- Goal: review the exact candidate diff as if it were an external PR and close
  all actionable findings before publication.
- Files / systems: `origin/main...HEAD` plus intended worktree, audit report,
  generated assets, GitHub PR metadata and patch.
- Changes: documentation and narrow review fixes only, each re-verified.
- Verification: fresh full gate matrix, exact-file staging inspection, committed
  tree diff, PR patch/readback, mergeability check; GitHub Actions status is
  recorded but not used as the requested gate.
- Expected result: `ready` decision based on local evidence and reviewed patch.

### M5. Publish and verify `main`

- Goal: publish the reviewed branch, create the PR, merge without force, and
  independently confirm remote main.
- Files / systems: local Git repository, `origin`, GitHub PR.
- Changes: intentional commit(s), branch push, PR creation, merge, local main
  fast-forward/readback.
- Verification: clean candidate worktree, commit SHA, PR merged state and merge
  commit, `git ls-remote origin refs/heads/main`, clean local `main`, final
  smoke against the exact integrated source if the merge commit changes content.
- Expected result: `main-integrated`, with runtime/readiness labels limited to
  the evidence actually gathered.

## Verification

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo test -p marketfeed-daemon --features ui`
- `npm test -- --run` and `npm run build` from `ui/`
- project-provided smoke/check scripts discovered during M1
- production daemon restart plus HTTP/SSE and three-venue live assertions
- Browser checks for page identity, meaningful DOM, overlays, console/network,
  interaction, responsive geometry, accessibility basics, and screenshots
- bounded runtime/performance sample and clean shutdown/restart
- `git diff --check`, exact PR patch review, and remote `main` SHA readback

## Decision Log

- 2026-08-10: The user authorized fixes, PR creation, review, merge, and push to
  `main` after stability is demonstrated.
- 2026-08-10: Audio, trading, and order placement remain excluded from the
  product and verification scope.
- 2026-08-10: GitHub Actions-only issues remain outside the merge gate by prior
  user direction; local project gates, runtime behavior, and PR review remain
  mandatory.
- 2026-08-10: Ordinary security review is in scope; Codex Security is not
  invoked because it was not explicitly requested.

## Progress Log

- 2026-08-10: Session bootstrap completed, recovery branch/worktree preserved,
  and prior evidence classified as context rather than a fresh ship gate.
- 2026-08-10: Current: M1 authoritative inventory and documentation audit.
- 2026-08-10: Next: produce the scenario matrix and run clean baseline gates
  before changing behavior.
- 2026-08-10: M1 complete. Official Binance, Bybit, OKX, and Coinbase public
  contracts were cross-checked against adapter sequencing and heartbeat logic.
- 2026-08-10: M2 complete. Fixed multi-symbol L2 scales, exact server DOM,
  strict HTTP inputs, atomic Volume/TPO profiles, duplicate structural reactions,
  adaptive bubble defaults, and optional DOM layout. Full local gates pass.
- 2026-08-10: M3 complete. Release runtime reached 13/13 live venues; the live
  smoke passed 79 checks; responsive interaction, persistence, and frame pacing
  passed. Prior daemon shutdown joined all tasks and drained all sinks cleanly.
- 2026-08-10: Current: M4 exact candidate diff and PR review.
- 2026-08-10: Next: stage only the audited tree, push the candidate, review the
  PR patch/readback, and merge only if no actionable issue remains.
- 2026-08-10: M3 final restart superseded the earlier run. After removing
  profile hot-path cloning, the optimized daemon held 13/13 venues live for
  263 seconds with zero reconnects, invalidations, or drops, then joined all 13
  venue tasks and drained all sinks on SIGINT.

## Rollback / Recovery

- If a defect fix regresses another path, revert only that exact patch while
  preserving all pre-existing work; never reset, clean, or stash broadly.
- If a live venue is unavailable, retain the public-data feature, capture the
  exact transport/source error, prove deterministic fixtures and degraded UI,
  and label that venue runtime path `blocked:external` rather than weakening
  validation.
- If PR review fails, keep the branch open and do not merge. If merge succeeds
  but readback differs, stop without force-pushing and report the exact SHAs.
