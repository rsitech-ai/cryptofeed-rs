# Native observation carrier

This implements the in-memory market unit required by the
[committed projection contract](causal_native_observation_projection.md).
The enriched durable profile, acknowledged consumer projection and consumer
migrations are still required. This carrier alone grants no live, durable or
economic publication authority.

## Source and data retained

`ObservedEventBatch` owns an unchanged canonical `EventBatch`, typed native
observations, explicit layout/encoded-byte limits and an outer source descriptor.
The batch header retains session/frame; the outer descriptor retains venue,
physical connection and the original input's receive/monotonic clocks. These
coordinates exist even when a one-sided BBO has no canonical price row.

Construction and deserialization validate a complete canonical-position
partition and crosscheck every canonical identity and receive timestamp against
the outer source. That structural check does not establish subscription,
instrument, financial or release validity by itself.

Collector stamping takes the current input receipt explicitly through the
runner. It replaces adapter placeholders in the outer source and canonical
events. Missing receipts are refused; a timer/control callback cannot borrow
the previous frame's clocks. Before mutation, exact JSON integer widths are
charged against the unit's encoded limit, including signed receive timestamps.
A bound refusal leaves the unit unchanged. Financial payloads do not cross a
float or buffered JSON-value representation.

`HyperliquidSession::new_with_observations` explicitly opts into this carrier.
The same decoded protocol value produces canonical rows and native attachments
for trades, BBO, book, candle and asset context. It retains trade identities and
participants, independent BBO sides and order counts, full native book depth,
original candle end/count, and optional context fields. One context associates
with all requested canonical outputs. Subscription/layout/byte admission occurs
before retained book and trade-delivery state advances.

All-mids has a typed representation only. Actual scope, subscription/catalog
admission and strict native duplicate-key handling remain unfinished. The
session continues to reject that channel.

## One queue fate

`SessionAction::EmitObservedBatch` and `MarketBatch::Observed` distinguish an
enriched unit from a canonical-only batch. The engine's bounded market queue
owns the entire unit. Action refusal, DropNewest, DropOldest, exact eviction,
partial sink forwarding and null drains retain or remove both representations
together. A native-only BBO consumes a real slot and never invents a zero quote.
Separately emitted quote-invalidated actions retain their original action order;
actual market/system drain order remains explicit capture evidence.

Capture accounts for full encoded attachments before trace/state copies and
retains observed units in action, queue, eviction, sink and drain evidence.
Incomplete capture poisons its owner. The bounds describe logical encoded
retention, not process RSS, parser allocations or a latency qualification.

`EventSink::push_observed_batch` requires explicit opt-in. Existing sinks reject
it, preserving the rejected source front and unattempted suffix. Existing
canonical dispatcher/sink APIs and `EventBatch` wire shape remain available.

## Compatibility and unfinished integration

Existing Hyperliquid factories remain canonical-only. The legacy shared-feed
runner refuses enriched units before dispatch or frame acceptance. Existing
causal codecs reject enriched actions and retained queues; both legacy raw
replay APIs reject the new action. The private-account pump also rejects it.
Frozen recordings are never reinterpreted as enriched histories.

The next step is a negotiated enriched bootstrap/profile and exact DTO codec,
followed by native replay equality and projection only from acknowledged,
matched `HandoffObserved`. The committed source view must carry original
catalog/subscriptions, controls, clocks and cursors. Target groups must commit
their values and source checkpoint atomically under bounded backpressure.

hlscreen must then use that projection through its existing feature code;
rsibot consumes the producer directly for its own time-qualified strategy work.
Neither consumer should decode accepted raw frames to reconstruct suppressed
market values. Their current readers have not completed that migration.

Real daemon/CLI/live/control/supervisor and external I/O migration must precede
any rerun of the registered failed-ingress workload. Its existing failure and
original schedule, limits, thresholds and abort rules remain unchanged.
Continuity, strict real datasets, native restart and operational qualification
are still required. No economics, trading activation, deployment, merge or
release is authorized by this implementation.
