# Read-only crypto microstructure suite

**Status:** Approved in conversation on 2026-08-10 and recorded before implementation.

## Outcome

CryptoFeed becomes a read-only crypto microstructure workstation whose visible
analytics are derived from the same normalized event stream as its tape and
book. The delivered surface includes:

- exact daily Market Profile statistics: VAH, VAL, POC, high-low range, traded
  volume, TPO count, and rotation factor;
- server-computed F1/F2/F3 order-flow bubbles with strict F3 > F2 > F1
  priority;
- first-class perpetual-futures state for funding, open interest, liquidation
  events, and qualified L2 feeds;
- bounded depth history, price-level pulling/stacking, configurable DOM
  execution columns, a resizable chart/DOM split, and persisted workspace
  preferences;
- deterministic naked-bubble lines, confirmed reaction lines, and rolling top
  bubble zones.

The product remains explicitly **L2 + public tape, not MBO/L3**.

## Non-goals and authority boundary

The following are excluded from this program:

- audio or sonification;
- private exchange APIs, credentials, balances, positions, PnL, working orders,
  trading, or order placement;
- one-click actions, brackets, stops, targets, or any live-money path;
- CFTC COT, SEC 13F, dark-pool, equities-options, or public-regulatory scanner
  integrations;
- claims to reconstruct individual orders, queue position, spoofing, hidden
  liquidity, or liquidation when the source event is not an exchange-provided
  liquidation event;
- WebGL as a predetermined rewrite. Canvas remains the renderer until measured
  frame-time or memory evidence misses the performance budget.

No commit, push, PR, merge, deployment, or external service mutation is part of
the implementation authority.

## Product semantics

### Market Profile

The existing exact `marketfeed-analytics` profile engine is authoritative.
Defaults are:

- UTC-aligned 24-hour session;
- 30-minute TPO period;
- 70% value area (`7000` basis points);
- volume-based value area by default, with a TPO-basis switch;
- venue + instrument scope matching the current focus instrument;
- exact fixed-point prices and quantities at the venue catalog grid.

The UI labels the session boundary and basis. Empty sessions remain empty. Late
events, incompatible scales, capacity overflow, or unavailable catalog metadata
produce an explicit unavailable/degraded state; they never trigger a browser
approximation.

### Order-flow bubbles

The existing Rust flow builder and detector own all signal decisions. Browser
code may select an approved configuration and format output, but does not
reclassify tiers or recompute thresholds.

- One-minute candles are the initial aggregation interval.
- Delta and Volume are mutually exclusive global signal modes in the first UI
  release.
- F1, F2, and F3 retain independent threshold mode, adaptive preset/manual
  value, maximum bubbles, size cap, shape, and enabled source segment.
- Strict priority is enforced once in Rust: a price bucket qualifying for F3 is
  absent from F2/F1, and one qualifying for F2 is absent from F1.
- Spot and perpetual sources remain separate economic segments. A visual view
  may show both, but the detector does not merge them into one bubble.
- Adaptive history is bounded and only finalized candles enter calibration.
- Live bubbles are provisional and visibly identified; finalized bubbles are
  immutable for that candle.

Configuration is validated server-side. The UI starts from named presets and
persists the selected presentation/configuration locally. Unsupported or
invalid configuration fails visibly and leaves the last valid configuration
active.

### Derivatives state

The view plane retains normalized `Funding`, `OpenInterest`, and `Liquidation`
events instead of dropping them.

- Funding shows the latest exchange-provided rate and event timestamp per
  venue/instrument.
- Funding divergence is a cross-venue comparison only when at least two fresh,
  semantically compatible rates are available. Otherwise it is unavailable,
  not zero.
- Open interest shows the latest exact value plus change from the previous
  retained sample and its time interval.
- Liquidations are a bounded event tape and overlay sourced only from normalized
  liquidation events. No liquidation is inferred from trades.
- Staleness is explicit for every derivative datum.

L2 enablement is staged and fixture/runtime-qualified one adapter at a time,
starting with Binance USD-M BTCUSDT, then OKX Swap, then Bybit Linear. A venue
that cannot prove a coherent snapshot/delta lifecycle remains tape/derivatives
only and is labeled accordingly.

### Depth and DOM

The server stores a bounded, sampled history of public L2 market-by-price
snapshots. The initial bounds are configurable but conservative: 100 ms minimum
sample interval, 300 price levels per sample, and 15 minutes of history. The
server may coalesce faster updates; it must expose sample interval and drop/
coalescing counters.

Pulling/stacking means signed change in displayed aggregate quantity at the
same price level between consecutive retained MBP samples:

- bid increase or ask decrease is positive pressure;
- bid decrease or ask increase is negative pressure.

This is an MBP delta, not an assertion about individual order intent.

DOM execution columns are computed from the normalized public trade tape in a
bounded rolling window: bid-executed volume, ask-executed volume, total volume,
and delta at price. Unknown aggressor volume remains separate and is not forced
to either side. Columns can be reordered/toggled; prices and quantities remain
exact strings across the API boundary.

The chart/DOM divider is keyboard-operable and pointer-draggable, constrained
to useful minimum pane widths, and persisted per browser workspace. Other view
preferences—depth, visible columns, analytics mode, and panel visibility—share
one versioned local workspace record with validated migration/default behavior.

### Structural bubble levels

Structural levels are a separate deterministic analytics module built on
finalized Rust bubbles and candle highs/lows.

- A naked bubble line is created from an eligible finalized bubble when the
  immediately following finalized candle does not trade through its anchor
  within configured tick tolerance. It remains active until a later candle
  touches it, or bounded history evicts it.
- A reaction high/low line requires an eligible bubble followed by a configured
  left/right swing confirmation. The line anchors to the confirmed candle
  extreme, not the bubble anchor.
- Top bubble zones retain the strongest eligible buy and sell events inside a
  UTC day or week window. Rank is deterministic by strength, then event time,
  then stable bubble id. A stronger event replaces the weakest retained zone.
- Historical lines never move after finalization. Live/provisional bubbles do
  not create structural levels.

Touch tolerance, source tiers, maximum active count, ranking count, and window
are bounded validated settings. The API distinguishes active, touched, and
expired states.

## Architecture

### Catalog registration

`run_live_ws` already resolves the exact `CatalogView` before creating
sessions. It registers that catalog with `ViewPlane` before any session event
can reach analytics. Synthetic/memory paths register their catalog or declare
analytics unavailable when their stub metadata is insufficient.

The view plane stores immutable per-instrument metadata needed by analytics:
venue/config mapping, instrument kind/segment, native symbol, price and quantity
scales, tick and quantity increments, and contract metadata needed for truthful
display. Catalog changes replace metadata atomically and reset incompatible
per-instrument analytics state rather than mixing grids.

### Stateful projection

The view plane remains a synchronous `EventSink`; it performs bounded pure
updates while holding its existing mutex and never awaits under the lock. Each
instrument state owns:

- live book plus bounded depth history and MBP deltas;
- trade/quote and derivative rings;
- session profile builder plus recent finalized profiles;
- candle flow builder, bubble detector, current live batch, and bounded
  finalized batches;
- structural-level builder and bounded state.

Events use exchange time when present and receive time otherwise, matching the
analytics input contract. Late analytics events fail only that analytic
projection, increment a reasoned health counter, and do not block book/tape
ingestion. A clean session/catalog reset recovers the projection.

### API and stream

The loopback API adds explicit read-only resources rather than overloading book
or tape payloads:

- `GET /v1/analytics/profile`
- `GET /v1/analytics/bubbles`
- `GET /v1/analytics/levels`
- `GET /v1/derivatives`
- `GET /v1/depth/history`
- `GET /v1/dom`

All focus routes require venue plus instrument/symbol, validate bounded limits,
and return a stable `schema_version`, source timestamps, freshness/health, and
exact decimal strings. `/v1/stream` includes compact current snapshots and
monotonic projection revisions so the UI can update without fetching unchanged
large history payloads. Historical arrays use their dedicated routes and are
refetched only when revisions change.

No mutating analytics endpoint is exposed in the first release. UI presets are
translated to server-owned validated configurations at startup/build time; a
future authenticated local configuration API would require a separate design.

### UI composition

The existing order-flow chart remains the primary surface. Changes extend its
current visual language rather than replacing the application shell:

- session statistics appear as a compact, inspectable strip tied to the focus
  chart;
- bubbles and structural lines render on the price/time chart with a focused
  settings drawer;
- funding, OI, and liquidation state occupy a derivatives section in the
  existing flow dock;
- depth history and MBP deltas feed the heatmap/DOM;
- the DOM adds a column picker and the chart/DOM divider becomes resizable;
- clear unavailable, stale, reconnecting, partial-source, and capacity-degraded
  states are present.

Keyboard focus, contrast, reduced motion, narrow laptop widths, wide desktops,
and browser zoom are verified. Copy uses `public L2`, `market-by-price`,
`exchange-reported`, and `provisional/final` precisely.

## Performance and capacity gates

Initial acceptance budgets on a release build and the existing live UI smoke
instrument are:

- view-plane ingest p99 below 2 ms for a normalized batch at target load;
- no unbounded vector/map/ring growth;
- 200–500 depth updates/s and 50–100 trades/s without queue growth attributable
  to analytics;
- chart animation p95 frame time below 16.7 ms and no recurring long tasks over
  50 ms during a 60-second interaction smoke;
- bounded history memory observable from configured capacities.

Benchmarks and browser measurements are captured before and after the depth
history work. Canvas stays in place if it meets the budget. WebGL becomes a
separate, evidence-backed project only if the renderer is the measured
bottleneck after data/coalescing fixes.

## Failure behavior

- Missing/placeholder catalog grid: analytics unavailable with reason.
- Late/out-of-order trade: projection health degraded; book/tape still live.
- Capacity reached: defined eviction or explicit capacity error; never silent
  widening.
- Stale derivative source: retain last value with age and stale state.
- Book invalidation/reconnect: clear book-derived MBP state and mark history
  discontinuity; do not draw changes across the gap.
- UI disconnect: preserve last snapshot as stale, show reconnecting state, and
  resynchronize revisions after reconnect.
- Invalid persisted workspace: validate/migrate known fields, discard only the
  invalid record, and use documented defaults.

## Verification strategy

Every vertical slice follows red-green-refactor and proves:

1. pure deterministic unit behavior, including exact arithmetic and boundaries;
2. view-plane event projection and reset/invalidation behavior;
3. HTTP contract plus SSE revision behavior;
4. UI component/state tests with loading, empty, stale, and error cases;
5. one running browser path against the loopback daemon;
6. relevant full Rust/UI regression suites.

The final result may be described as `runtime-proven` only after fresh daemon
restart, real interaction, console/network inspection, and clean shutdown.
Passing unit tests alone is `repo-ready`, not runtime proof.
