# marketfeed-analytics

`marketfeed-analytics` provides deterministic, rendering-neutral order-flow
analytics over canonical `marketfeed-model` trades.

The recovered alpha surface currently includes:

- exact price/quantity grid conversion with bounded bucket ranges;
- session volume profiles with buy, sell, and unknown-side totals;
- per-candle delta and cumulative-delta flow;
- configurable three-tier order-flow bubble detection; and
- bounded, serializable snapshots suitable for downstream renderers.

The crate performs no network, storage, rendering, signaling, or order-routing
I/O. It does not identify structural support/resistance levels, produce trading
signals, or place orders. Callers remain responsible for lifecycle, persistence,
late-data policy selection, and any risk controls around derived uses.

The API is alpha and may change before a stable release.
