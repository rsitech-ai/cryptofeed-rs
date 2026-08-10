# marketfeed-analytics

`marketfeed-analytics` provides deterministic, rendering-neutral order-flow
analytics over canonical `marketfeed-model` trades.

The recovered alpha surface currently includes:

- exact price/quantity grid conversion with bounded bucket ranges;
- session volume profiles with buy, sell, and unknown-side totals;
- per-candle delta and cumulative-delta flow;
- configurable three-tier order-flow bubble detection;
- bounded, serializable snapshots suitable for downstream renderers; and
- deterministic naked-bubble, confirmed-reaction, and strongest day/week
  reference levels derived from finalized candles and bubbles.

The crate performs no network, storage, rendering, signaling, or order-routing
I/O. Its structural levels are descriptive order-flow references, not predictions
or trading signals, and the crate does not place orders. Callers remain
responsible for lifecycle, persistence, late-data policy selection, and any risk
controls around derived uses.

The API is alpha and may change before a stable release.
