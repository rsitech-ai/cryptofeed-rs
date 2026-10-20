# Exact native observations in MFCJ4

This extends the [in-memory carrier](causal_native_observation_carrier.md) into
receipt-gated disk/null owner history and actual-engine replay. It implements
the durable native-data layer of the
[projection contract](causal_native_observation_projection.md). It does not
implement a consumer projection or migrate the real daemon's legacy live loop.

## Explicit versions

| Layer | Native history version |
| --- | --- |
| Feed header and causal envelope | MFCJ4 / envelope 4 |
| Bootstrap | DTO 2 with both runtime-history and native-observation policies |
| Owner observation | DTO 3, preserving execution-permit semantics |
| Captured operation | DTO 2, including exact native leaves |
| Native attachment and policy | Attachment 1 / native-observation-policy-v1 |

Readers select the profile before watermark access or factory construction.
Default owner replay APIs still select MFCJ3; `open_profile` and `resume_profile`
require an explicit MFCJ4 selection. Prepared transactions and writers refuse
mixed profiles. Old envelopes cannot wrap native operation schemas, and old
operation schema 1 refuses native data even in retained queues or failed actions.
Existing absent-native canonical encodings remain in their original schemas.

## Bootstrap policy and factory ownership

The native policy is a bounded, canonical, content-hashed public reproduction
input. It is checked alongside the original catalog, subscriptions, selected
channels, runtime/capture limits and actual executable/source recipe. Missing,
duplicate, corrupt, noncanonical, oversized and wrong-factory policies fail.
Older profiles refuse this policy instead of silently ignoring it.

`HyperliquidV1` binds explicit native layout and encoded-unit limits to the actual
spot or perpetual session. `CanonicalOnlyV1` explicitly admits a Bybit owner in
the same physical MFCJ4 feed. A canonical-only policy cannot admit native leaves.
Hyperliquid V1 covers trades, BBO, book, candle and context. All-mids admission
remains unfinished and is rejected by this policy, including an empty map.

`PreparedBootstrap::new_observed_history` constructs only prepared evidence.
The original batch-writer commit receipt and current artifact check must succeed
before `instantiate` creates the private session. There is no new transport,
external sink, checkpoint, restart, deployment or trading permission.

## Financial leaves and complete queue fate

An `ObservedBatchJson` contains canonical JSON for a typed `ObservedEventBatch`
inside a string leaf. The surrounding tagged DTO buffers that string; financial
coefficients never pass through untyped JSON numbers or floats. Decoding checks
the typed unit, canonical bytes, source identities, positions, native layout and
encoded limits. Optional absence, exact coefficient/scale values, original
candle end/count, native order counts and complete context fields survive.

Operation DTO 2 retains observed actions, queue attempts and evictions, sink
attempts, completed drains and before/after queue snapshots. Every observed leaf
must use exactly the bootstrap's native limits. Whole-unit action/queue/drop
decisions remain those of the actual runner; persistence does not reconstruct
market values from raw-frame acceptance.

Capture bytes, record bytes, writer-group bytes and replay retention are separate
bounds. Native leaves are charged within canonical operation/state sizes and the
existing capture reservation. Temporary decoding, comparison and construction
copies are separate; these counters do not establish process RSS or latency.

## Owner settlement and replay

Owner DTO 3 retains the MFCJ3 execution-permit, completion, release-intent,
handoff-observation, revocation and uncertainty semantics. Missing or wrong
permits fail. Late completion after revocation remains evidence; it cannot regain
release authority. Encoding failure after a permitted callback records an
indeterminate outcome in the same negotiated profile and does not authorize a
fresh execution retry.

Actual single-owner and multi-owner replay use privately persistent factories
from the verified bootstrap. They compare the complete operation DTO, including
native-only fields and source clocks. A native-field mutation that leaves all
canonical values unchanged still fails actual replay. Structural history
validation alone does not establish native truth.

Only a committed `HandoffObserved` after a matching completion returns a recorded
handoff. A permit, completion, release intent or `HistoryJournalOwner::try_next`
alone cannot authorize consumer projection: that live internal handoff precedes
its observation's durable acknowledgement. Resuming reconstructs the original
physical prefix, discards prior handoffs and compares actual state; it is not a
native checkpoint or append-takeover protocol. A storage seal leaves execution
uncertainty unresolved.

## Remaining integration

The next layer must expose the verified original bootstrap/control/clock/source
cursor view and atomically publish bounded target groups, including each source
after-cursor. Healthy target backpressure must retain the same prepared group;
unknown settlement must stop publication. hlscreen and rsibot need negotiated
readers for that projection. Their existing integrations are not qualified by a
passing MFCJ4 history test.

All-mids scope/subscription/catalog admission, real daemon/CLI/live/control and
external I/O migration, native continuity, strict real datasets, native restart
and operational qualification remain open. Rerun the registered failed-ingress
comparison only after the actual integration repair, with its original limits,
schedule, thresholds and abort rules. No economics, trading activation,
deployment, merge, release or operational promotion is granted.
