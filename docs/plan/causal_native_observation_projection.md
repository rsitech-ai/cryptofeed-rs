# Committed native observations and consumer projection

This is the next implementation contract after bounded multi-owner replay.
It extends the [causal integration contract](causal_journal_integration_contract.md).
The [in-memory carrier](causal_native_observation_carrier.md) retains typed native
fields, outer source clocks and canonical queue fate. The explicit
[MFCJ4 history layer](causal_native_observation_history.md) now adds durable native
owner captures and exact actual-engine replay for five Hyperliquid families.
The immutable [verified source view](causal_verified_source_view.md) now exposes
the original physical metadata, controls, clocks, uncertainty and matched
handoffs. The local MFOP1 candidate now implements atomic target groups, a typed
structural reader and comparison with the verified source. Its exact-source
[local qualification](../evidence/atomic-observation-projection-20261020/README.md)
passes all ten commands; final independent evidence audit also passes and
consumer migration remains required. The current
HLS735 reader still uses legacy accepted raw frames. Passing disk/null
owner-history replay does not qualify this projection or its consumers.

## Ownership and publication authority

cryptofeed owns exact native observations, canonical associations, source
identity/clocks, queue dispositions, durable history and projection. hlscreen
owns its existing feature/display conversion, including the float boundary.
rsibot owns time-qualified strategy context, features, models and decisions.
Both consumers use the authoritative producer directly through their own
versioned readers; neither becomes a second collector.

Publish only from a committed, structurally verified `HandoffObserved` whose
native completion matched. `HistoryJournalOwner::try_next` returns before that
observation's durable acknowledgement and cannot authorize projection. A raw
frame, `FrameAccepted`, produced action, queue admission or `ReleaseIntent`
alone cannot authorize consumer market values.

## One native observation and one output fate

Define reusable typed observations in model/adapter-api, without an
engine-to-Hyperliquid dependency. The native protocol decoder validates once;
the adapter constructs canonical rows and their native attachment from that
same decoded value. The projector and consumer never decode the raw frame again
to recover market values.

Carry the attachment in the same bounded market dispatch unit as its canonical
rows. Each native observation names its canonical event indices, including an
explicit empty association when no canonical price row exists. Preserve native
order within a multi-trade message and associate one context observation with
all of its requested canonical outputs without duplicating it per channel.

A legitimate native-only observation is a real queue item. In particular, a
one-sided BBO must retain its independently optional sides and must not invent a
zero quote. Preserve the separately ordered quote-invalidated system effect.
Action-buffer refusal, market DropNewest/DropOldest, completed drains, capture
failure, owner fencing and withheld output must affect the attachment and its
canonical unit together. Never admit enrichment through a separate side queue.

Charge construction, canonical encoding, capture, queued/active state and
projection preparation against explicit logical bounds. Declare native rows,
book depth, strings/maps, associations and total encoded bytes. On oversize,
fail the complete unit rather than truncate enrichment. Temporary parser and
comparison copies remain distinct from retained-resource accounting.

## Required exact feature coverage

All financial fields use typed Fixed coefficient/scale values through native,
capture, durable codec, native comparison and projection. Do not route them
through `serde_json::Value`, internally buffered tagged content or floats.
Preserve optional absence separately from numeric zero.

| Observation | Required retained native fields |
| --- | --- |
| Trade | coin, native side, exact px/sz, exchange time, hash, tid; retain coin:time:tid identity |
| BBO | coin/time; independently optional bid/ask; exact px/sz and u64 order count per present level |
| Book | coin/time; ordered native bid/ask levels with exact px/sz and order counts; explicit requested/canonical depth association |
| Candle | original t and T, coin/interval, exact OHLCV and native count; never synthesize T from the nominal interval |
| Asset context | coin, day notional volume, previous-day price, mark, optional mid, circulating supply, funding, open interest, oracle price, base volume, premium and impact pair |
| All mids | exact symbol/value mapping with declared scope, subscription and catalog binding; add the actual adapter admission/queue path before claiming capability |

Native protocol decoding already represents these feature families. Canonical
rows alone lose some fields; all-mids decoding alone is not an admitted
canonical-session capability. The implementation must cover both gaps.

## Explicit version and verified source view

Negotiate the enriched causal contract and a separate consumer projection
profile before watermark access or factory construction. Old readers must fail
closed. Keep old absent-attachment encoding and existing capture bytes stable;
do not reinterpret frozen history as enriched history. Retain historical
artifact binding: a new executable does not gain permission to execute an old
artifact's history merely by supporting its schema.

Expose a narrow immutable verified source-step view: exact source cursor;
original bootstrap/catalog/subscriptions and source artifact identity;
owner/session/generation and physical input connection/frame reference;
original input receive/event clocks; owner observation stamp; ordered control
disposition; optional matched observed handoff; and exact seal details.
Do not label source data with the projector's build identity or supply later
metadata/clocks at an earlier cursor. Keep unknown execution explicit.

Use the single physical multi-owner order. Do not merge independent journals by
sorting wall clocks or exchange timestamps. Bind a versioned archive clock
policy, retaining Started origin, monotonic observation time and source clocks
as separate quantities. Any origin-plus-observation conversion is checked and
labeled as an approximation. Live wall-time health is separate from deterministic
archive feature time. A storage seal cannot advance time to projector execution.

## Atomic target groups and bounded backpressure

Encode one bounded typed group per verified source step: bootstrap metadata,
ordered released market/system values with attachments, or a clock/control-only
step. Embed source after-cursor, policy/schema identity and release lineage in
that same target transaction. Preserve actual drain/item order; writes/HTTP are
evidence and never market observations or inferred external delivery.

Stage at most one prepared group/ticket while awaiting target commit. Healthy
Full retains and retries that exact prepared group without native reexecution,
reencoding or an independently advanced source checkpoint. Accepted work remains
owned after caller cancellation. Unknown settlement or writer failure stops
publication; it cannot authorize a fresh retry as if nothing committed.

The first implementation creates a new explicit target directory/feed identity
when rebuilding. Existing writer APIs do not grant append takeover or native
checkpoint authority. Consumers rebuild state for a new feed identity and
advance their cursor only after a whole group validates and applies. A poll
budget cannot publish half a group. Persistent append recovery requires its own
verified protocol before existing target-cursor continuity is promised.

Keep projection outside owner execution. Slow consumers may lag a bounded
durable source; they cannot silently change producer queue/drop outcomes.

## Local MFOP1 implementation boundary

`ProjectionPump::create` consumes a fresh verified MFCJ3/MFCJ4 source reader and
creates a new MFOP1 target. One source record becomes one indivisible target
group, including bootstrap, controls, uncertainty and final source seal. The
final group itself seals the target; an independent legacy seal is refused
before actor admission. Policy identity and original source before/after cursors
travel with every group. Writes and HTTP requests retain their original output
positions as separately labeled evidence, without becoming market observations.

`ProjectionReader` returns one complete typed group and original per-session
metadata. It checks source ordering, owner/permit/release lineage, exact clock
policy, source reservation count/declared bytes, selected instrument and native
association scope, and unresolved execution at seal. Pending references retain
only fixed-size fields and the owner's Resync/Stop control subset. Connection
and frame checks distinguish the fresh runner's configured connection from the
physical transport connection, including valid pre-Connected timers/controls;
frame numbering persists across reconnects. Current-input market receipt checks
rely on this profile's fresh queues and successful-turn null dispatch. They do
not generalize to retained queues or external sinks.

This reader checks structure, not authenticity of rewritten financial values.
`compare_with_source` consumes fresh readers and deterministically compares the
complete logical target with actual native-verified source steps through the
target's initial observed durable bound. Later live growth cannot extend that
one-shot proof. The original source executable binding still applies. A target
checksum, policy hash or standalone reader does not replace that comparison.

Reader resume rebuilds metadata/validation state at an exact target/source cursor
pair. It does not restore consumer features or grant writer takeover. HLS and
rsibot must separately bind whole-group application and feature checkpoints to
that pair. Logical encoded/layout/count limits are not process RSS or latency
measurements; transient decoding/comparison and caller-owned returned groups are
additional memory. Consumer availability remains unknown where not measured.

The local candidate does not yet migrate HLS, implement rsibot strategy context,
admit all-mids, repair real daemon/CLI/live/external-I/O integration or qualify
restart/continuity. The original failed ingress workload has not been rerun.

## Required integration proof

Use independent native fixtures for every retained field, absent/zero/extreme
values, one-sided BBO, multi-trade and multi-channel context associations.
Exercise action capacity, both market overflow policies, eviction, partial/failed
drains, fences, revocation and incomplete capture. Raw-frame acceptance must not
resurrect any suppressed observation.

Inject crashes around completion, intent, handoff and target acknowledgements;
target Full/failure; accepted tickets with dropped waiters; and target commit
before local source bookkeeping. Test exact cursors, in-group cursor rejection,
foreign identities, corruption, uncommitted suffixes and sealed unknown owners.
Interleave spot/perps and Bybit metadata, reconnects, empty callbacks, controls
and regressing clocks; resumed/live-tail/archive feature states must agree at
the same committed boundary.

Migrate HLS through the negotiated typed reader and its existing feature code.
Compare complete feature values through trade/BBO/book/candle/context/all-mids,
CLI and server paths. Preserve the original 1,055-event legacy replay output and
all legacy profile refusals. Add rsibot's owned observation reader with explicit
unavailable timing/quality states; synthetic success cannot admit unsupported
real datasets.

Only after real daemon/CLI/live/control/supervisor and external-I/O integration
may the registered failed ingress comparison be rerun with its original caps,
schedule, thresholds and abort rules. Its present result remains failed.
Continuity, strict datasets, calendar/hardware qualification and native restart
remain required. No economics, trading activation, deployment, merge or release
is authorized by this contract.
