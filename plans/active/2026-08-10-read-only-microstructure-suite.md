# Read-only crypto microstructure suite implementation

## Goal

- User-visible outcome: CryptoFeed exposes exact Market Profile, server-backed
  three-tier bubbles, truthful derivatives state, richer public-L2 depth/DOM,
  and structural bubble levels in one read-only interface.
- How to see it working: start the loopback daemon/UI, focus a qualified crypto
  venue/instrument, and observe revision-backed analytics, derivatives, depth
  history, DOM deltas, workspace controls, and structural levels without any
  private/trading/audio surface.

## Current State

- Relevant paths: `crates/analytics`, `crates/daemon/src/view`,
  `crates/daemon/src/run.rs`, adapter session/config code, `ui/src`.
- Existing behavior: exact profile, flow, and F1/F2/F3 detectors exist as a pure
  Rust crate, but the daemon does not depend on or project them. The view plane
  caches current books and sampled trade/quote tape only. The UI separately
  computes simplified bubbles/history in JavaScript and labels derivatives as
  not implemented.
- Existing recovery work: the current branch contains intentional uncommitted
  UI recovery changes and one OKX adapter edit. Preserve and build around them.
- Constraints: exact fixed-point inputs, bounded state, public read-only data,
  no MBO claims, no audio, no private API, no trading/order placement, no
  external publication or Git integration.
- Approved design:
  `docs/superpowers/specs/2026-08-10-read-only-microstructure-suite-design.md`.

## Target State

- Desired behavior: catalog-driven analytics run once in Rust, publish stable
  exact-string API/SSE snapshots with health/revisions, and render through a
  tested responsive UI.
- Non-goals: Audio; credentials/private streams; balances, positions, PnL,
  orders or order placement; CFTC/13F/dark-pool/options scanner integrations;
  automatic WebGL rewrite; MBO/L3 reconstruction claims.

## Risks and Failure Modes

- Resolved catalog IDs/scales may diverge from config-order IDs; analytics must
  register the exact filtered catalog before events and reset on grid changes.
- Analytics work inside the synchronous view sink may increase hot-path lock
  latency; all state must be bounded and benchmarked before richer history.
- Exchange derivative payloads have different units/semantics; cross-venue
  comparisons must require compatible, fresh data.
- Book reconnect/invalidation may create false pulling/stacking deltas across a
  discontinuity unless history epochs are explicit.
- Existing browser JS analytics can silently disagree with Rust unless removed
  from market-truth decisions in the same vertical slice.
- Large UI additions can flatten the current hierarchy or fail at laptop widths;
  each surface needs focused runtime proof before the next is added.

## Milestones

### M0. Baseline and contracts

- Goal: capture fresh starting evidence and define serializable projection DTOs.
- Files / systems: workspace manifests, analytics/daemon tests, UI tests, active
  plan progress log.
- Changes: add no behavior until baseline commands pass; define failing API/
  projection contract tests first.
- Verification: focused analytics tests, daemon view tests, full UI unit tests,
  `git diff --check`.
- Expected result: known-green baseline with failures only from newly written
  contract tests.

### M1. Catalog registration and Market Profile vertical slice

- Goal: show exact VAH, VAL, POC, range, volume, TPO count, and rotation factor
  for the focused session.
- Files / systems: daemon Cargo feature/dependency, `run.rs`, view plane and HTTP,
  analytics projection module, UI API/state and profile strip.
- Changes: register exact catalogs before sessions; construct bounded per-focus
  profile state; add profile route/SSE revision; replace browser approximation
  with server snapshot and explicit unavailable/stale states.
- Verification: profile arithmetic fixtures, catalog/grid/reset tests, route/SSE
  tests, UI state/component tests, synthetic/replay browser smoke.
- Expected result: one complete Rust→HTTP/SSE→UI profile path with no duplicated
  browser calculation.

### M2. Server-backed F1/F2/F3 bubbles

- Goal: render strict-priority Rust bubbles and expose focused controls/presets.
- Files / systems: analytics projection, daemon view/API/SSE, existing
  `orderflow.js` consumers, chart and settings components.
- Changes: feed trades into bounded candle flow; publish live/final bubble
  batches and health; implement approved presets; delete/retire JS tier decision
  logic; render exact server output.
- Verification: priority/adaptive/rollover/late-event tests, projection/revision
  tests, UI controls/error tests, live/final chart smoke.
- Expected result: visible bubbles exactly match Rust detector fixtures.

### M3. Derivatives state and qualified perpetual L2

- Goal: expose funding, OI, exchange-reported liquidations, and at least Binance
  USD-M BTCUSDT coherent L2; qualify OKX Swap and Bybit Linear sequentially.
- Files / systems: view derivative rings/DTOs, HTTP/SSE, adapter subscription
  plans/fixtures, live config, Flow dock/header copy.
- Changes: retain normalized derivative events; compute freshness, compatible
  funding divergence and OI sample change; show bounded liquidation overlay;
  enable one L2 venue only after snapshot/delta/reconnect proof.
- Verification: adapter fixtures and planner tests, derivative unit/route tests,
  live venue lifecycle smoke, UI stale/partial-source/empty states.
- Expected result: derivatives are truthfully first-class and every enabled perp
  book proves coherent L2 or remains explicitly unavailable.

### M4. Bounded depth history, MBP deltas, and DOM/workspace

- Goal: make heatmap/DOM richer without losing frame or ingest budgets.
- Files / systems: view plane depth sampler, new depth/DOM routes, telemetry,
  heatmap, DOM, layout/workspace modules.
- Changes: establish before metrics; add bounded sampled history and discontinuity
  epochs; compute MBP pulling/stacking and rolling executed volume/delta at
  price; add configurable DOM columns, keyboard/pointer divider, and versioned
  validated persistence; retain Canvas unless measured renderer budget fails.
- Verification: capacity/discontinuity/math tests, route size/limit tests,
  release-mode ingest benchmark, 60-second browser performance trace, responsive
  and persistence smokes.
- Expected result: history/DOM remain bounded and measured within the design
  budget with no MBO language.

### M5. Structural bubble levels

- Goal: add deterministic naked, reaction, and top-bubble levels.
- Files / systems: new pure analytics level module/tests, daemon projection/API,
  chart rendering/settings/legend.
- Changes: implement finalized-only line/zone state machines, touch/expiry and
  deterministic ranking; publish active/touched/expired state; render and
  configure without moving historical anchors.
- Verification: table-driven state-machine tests, ties/window/tolerance/capacity
  tests, projection/route tests, chart interaction smoke.
- Expected result: structural overlays reproduce fixture timelines exactly.

### M6. Full regression and runtime proof

- Goal: establish the strongest truthful completion layer.
- Files / systems: all changed Rust/UI paths, loopback daemon, browser, logs,
  active plan evidence.
- Changes: repair only regressions attributable to this program; rebuild derived
  UI assets; record remaining external/live-source limitations.
- Verification: format/lint/test/build suites, adapter fixture suites, UI tests,
  daemon restart and realistic browser interaction, console/network inspection,
  clean SIGINT/shutdown, `git diff --check`, exact final status/diff review.
- Expected result: `runtime-proven` for exercised local and qualified live paths;
  unexercised venues/features remain explicitly `unverified`.

## Verification

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --features ui -- -D warnings`
- `cargo test -p marketfeed-analytics`
- `cargo test -p marketfeed-daemon --features ui`
- relevant adapter fixture/planner tests for each enabled perp venue
- `npm test -- --run` from `ui/`
- `npm run build` from `ui/`
- release-mode projection/ingest benchmark introduced by M4
- loopback daemon + browser smoke at laptop and wide-desktop widths
- browser console/network review and clean daemon shutdown
- `git diff --check`

## Decision Log

- 2026-08-10: User approved the assessed roadmap and explicitly excluded Audio,
  Trading, and order placement.
- 2026-08-10: Regulatory/equities datasets remain separate because they differ
  in market, cadence, semantics, and product boundary from CryptoFeed's live
  crypto public-data plane.
- 2026-08-10: Server/Rust owns market truth; browser-only analytics are retired
  as each server vertical slice lands.
- 2026-08-10: Exact session defaults are UTC 24h, 30-minute TPO, 70% value area,
  volume basis default with a TPO option.
- 2026-08-10: Canvas remains until measurements justify a separate WebGL change.
- 2026-08-10: No Git integration or external writes without separate authority.

## Progress Log

- 2026-08-10: Completed repository/feature assessment and obtained scope
  approval.
- 2026-08-10: Recorded approved product/architecture design and implementation
  plan; M0 baseline is next.
- 2026-08-10: M0 passed (analytics 24/24, daemon view 19/19, UI 132/132).
- 2026-08-10: M1 implemented exact catalog registration, Market Profile API/SSE,
  and the seven-metric UI strip with unavailable/degraded states.
- 2026-08-10: M2 implemented server-owned Volume/Delta F1/F2/F3 batches,
  strict-priority rendering, exact labels, and persisted mode selection.
- 2026-08-10: M3 derivative projection/UI is implemented and Binance USD-M,
  OKX Swap, and Bybit Linear L2 are live-qualified together: 3/3 required
  venues ready with coherent books, zero reconnects/invalidations/dispatch
  drops, and positive OI during the bounded qualification run.
- 2026-08-10: M4 implemented bounded 100 ms/3,000-sample server MBP history,
  reconnect epochs, executed-flow and MBP-delta DOM columns, and a persisted
  accessible chart/DOM split. Performance/runtime proof remains in M6.
- 2026-08-10: M5 implemented finalized-only naked/reaction/top-day/top-week
  structural levels with deterministic ranking, touch state, bounded storage,
  API/SSE snapshots, and chart rendering.
- 2026-08-10: M6 local gates passed: Rust workspace and UI-feature daemon
  suites, 146 UI tests including the synthetic 1h buffer soak, all-feature
  clippy, formatting, production UI build, exact three-venue live endpoint
  assertions, responsive 1024x768 and 1440x900 browser checks, chart-mode and
  venue interactions, sustained Bybit Order Flow performance/heap sampling,
  and repeated clean coordinated shutdowns. This establishes repo-ready and
  bounded live-source runtime proof; a release commit/PR and long supervised
  canary remain outside this task's authority/evidence.

## Rollback / Recovery

- If a projection slice fails, keep book/tape ingestion live, mark only that
  projection unavailable, and disable its UI surface behind the existing
  read-only fallback.
- If an adapter L2 lifecycle cannot be proven, revert only its subscription
  enablement and retain trades/derivatives with an explicit no-book state.
- If performance budgets regress, first reduce/coalesce configured history and
  isolate the measured stage; do not discard existing user changes or rewrite
  the renderer speculatively.
- Preserve all pre-existing dirty files. Reversal uses exact-file patches only;
  never reset, stash, or clean the worktree broadly.
