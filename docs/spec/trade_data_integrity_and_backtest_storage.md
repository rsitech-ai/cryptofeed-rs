# Trade integrity and backtest storage contract

Status: **specified and partially implemented; strict datasets/general causal
replay remain unqualified; no operational promotion**. Original audit date:
2026-10-19. Source baseline `1c668981ee2d5b456dfa6d085fbd558b1998937d`.
Architecture descriptions and proposed interfaces below preserve that baseline;
their present-tense wording is not a current source/API inventory. Subsequent
repairs, implemented subsets and their exact verification scopes are tracked in
the [active implementation ledger](../plan/hyperliquid_shared_feed_implementation.md)
and [goal scope](../plan/active_goal_scope.json). The requirements and unfinished
acceptance gates remain in force.
The [validation report](../ops/trade_data_validation_report.md) owns actual
results; the [implementation plan](../plan/trade_data_integrity_implementation_plan.md)
owns unfinished work. The [venue inventory](../plan/trade_venue_inventory.md)
and its JSON companion enumerate implemented and excluded scope.

## Authority, scope and assurance

This is a repository-process and public-data research audit. No economic run,
strategy promotion, capital authority, orders, private capture, deployment or
production migration is authorized. Preserve the frozen economic corpus for
`[2026-05-01T00:00:00Z,2026-09-01T00:00:00Z)` read-only. New captures belong in
fresh, separate directories. Missing frozen bytes block use; no replacement,
substitution, scoring-window change or manifest rewrite is allowed.

A successful fixture, socket, quote, candle or compiler check cannot establish
successful trade capture. Evidence states are independent: specified,
implemented, offline-tested, live-observed, independently-reconciled,
dataset-qualified, operationally-promoted. Scope includes venue, region/API
product, protocol/channel, instrument family, symbols and half-open UTC window.
Use `verified-for-stated-scope`, `failed`, `blocked`, `not-run`, `unsupported` per
dimension; each non-passing result needs a reason and next action.

The assurance is faithful attribution and preservation of exchange-reported
public executions, within a stated coverage proof. It cannot prove genuine
independent economic activity behind every exchange print. Authentic outliers
stay intact; cross-exchange price correlation is only a plausibility check.
Trades alone cannot validate fills, spread, queue position, fees, impact or
slippage. Future economic backtests need separately admitted quotes/books and
execution/latency assumptions, outside this assignment.

## Architecture at the audit baseline and extension points

Reuse the pure `adapter-api::SessionMachine` and `VenueFactory`, exact model,
transport, engine supervision, bounded dispatch, recording, replay and sinks.
No second exchange framework, bot-specific feeds, database or research platform.

Current path: daemon catalog discovery → concrete subscription expansion →
factory plan/session → transport reassembled application payload → engine
receipt stamp/raw enqueue → decoder → adapter envelope → dispatch → per-sink
mailbox/worker → file. `engine/src/runner.rs::on_text_frame` records before
mutable decode. This preserves parser input rather than IP/TLS/WebSocket wire
bytes or original fragment/compression boundaries. Oversized messages, transport
failures and remote omissions can occur before recording; these require visible
coverage uncertainty. Recorders cannot recover bytes the host never received.

Current MFR1 v3 preserves payload bytes with CRC32C, session, local raw frame
ordinal, wall receipt ns, monotonic duration, direction/opcode, HTTP responses,
initial build/session/catalog metadata and accepted subscription commands.
Readers support v1–v3. Metadata build SHA is optional. Dynamic catalog effective
time, reconnect/connection generations, actual timer firings, clock uncertainty,
commit checkpoints and canonical raw-row references are insufficient today.

Current raw enqueue is in-memory acceptance. `records_written` counts accepted
writer calls, not synced commits. Segment close/shutdown calls `flush`, without
syncing segment data or publishing immutable manifests. `File::create` at a
predictable segment name can truncate a collision. CRC protects payload damage,
not missing/reordered complete records or a dishonest source.

The current CLI `replay` only scans/validates records. Library `ReplayRunner`
feeds a supplied single session machine; it does not demultiplex process files,
reproduce engine timer/reconnect chronology or enforce research admission. It
retains output until end and can exceed dispatcher capacity. The audit example
explicitly demultiplexes OKX sessions for bounded adapter reconstruction; that
example is not a production reader.

The current four-field Trade lacks aggregation kind/count, quantity units and
native attributes. Factory default session IDs can differ from engine raw
session IDs. Adapter emission ordinals are not raw ordinals. These findings
must close before lineage or exact trade-count qualification.

Reuse the stricter explicit session/metadata/provenance and raw-ordinal binding
patterns in `event-pulse-mfr1/src/v2.rs`, retaining its routed-product scope and
prefix invariants. Do not replace its checked context with a generic fallback.
Analytics exact grid rescaling and Unknown-side accounting remain useful, but
cannot recover quantity units, constituent counts or missing source events.

## Ownership and source contracts

Adapter maintainers own official native semantics, independent expected fixtures
and channel identity. Engine/transport maintainers own receipt order, lifecycle,
backpressure, source health and raw capture. Recording/storage maintainers own
commit boundaries, recovery and manifests. Research-data maintainers own pinned
readers and use-specific admission; OKX consumer owners sign mapping parity.
Ops own time synchronization, capacity, backup/restore and calendar-spaced
promotion evidence. CODEOWNERS/release captain assign named people before work;
these role assignments do not invent a staffed operating service.

Instrument snapshots are immutable, content-hashed and effective-dated. A
reference resolves venue, region, API product, native symbol, kind, base/quote/
settlement, multiplier and its unit, inverse/linear formula, expiry, increments,
status, and effective/known-at times. Symbol reuse, delisting, redenomination and
metadata changes create revisions. Never resolve historical rows from today's
catalog. Discovery defaults (`BASE`, assumed quote/scales) cannot be admitted as
authoritative metadata. Local InstrumentId is scoped by venue/session/catalog.

Native IDs remain opaque strings: no float/JS Number conversion. Identity key is
`(venue, region, api_product, channel_granularity, native_symbol,
identity_epoch, native_trade_id)`, narrowed/widened only by documented venue
scope. A sequence is continuity evidence only with a proved scope and increment
contract. Repeated Bybit/OKX seq values do not imply duplicates. Never deduplicate
on timestamp/price/quantity/side. Missing identity retains ambiguity; it does not
create synthetic IDs that pretend venue uniqueness. Preserve each raw delivery;
canonical duplicate sightings retain all acquisition references. Same identity
with conflicting economic values is quarantined and blocks stricter admission.
Corrections/cancellations are append-only revisions with known-at times.

## Venue trade semantics

The inventory supplies per-product endpoints and official references with access
date. Each golden fixture must be independently interpreted from native payloads
and documentation, including opposite-side and unknown-side challenges.

- OKX current `trades` aggregates by taker/price/source; retain native count,
  last constituent ID, source and repeated seq. `trades-all` is a separate
  business-endpoint capability requiring explicit access/subscription validation.
  Never expand aggregates into invented constituent executions. Spot quantity
  is base, derivatives quantity contracts. Global endpoints do not prove EU/EEA.
- Binance individual `trade.t` and aggregate `aggTrade.a` have separate identity
  namespaces; preserve `f/l`, applicable counts, `T/E`, RPI/channel distinctions.
  Buyer-maker true means taker sell. Spot archive time resolution must come from
  archive product/date, including the documented microsecond change.
- Bybit publicTrade batches contain distinct executions; preserve each row,
  taker `S`, fill `T`, publication `ts`, opaque `i`, BT/RPI and repeated seq.
- Kraken v2 snapshots/updates and Futures trade_snapshot/live have distinct
  acquisition origin. Keep opaque IDs, native classifications and timestamp
  precision. A recent history snapshot is not forward-live coverage.
- Deribit amount and contracts are different fields. Preserve trade ID scope,
  instrument trade_seq, block/combo/liquidation attributes and native time fields.
  Inverse USD amount must not be called base volume. Catalog coverage excludes
  options and newly migrated USDC scope until explicitly implemented/tested.
- Coinbase Exchange maker side must be inverted; last_match is history, not a
  new arrival execution. Recovery needs heartbeat trade IDs plus matching REST
  semantics. Advanced batches/snapshots and aliases remain a separate product.
  Legacy INTX requires current-access/migration resolution, not an auth guess.
- Gemini uses native ns event time and buyer-maker inversion. Bitstamp type
  interpretation needs fresh authoritative confirmation before signed-flow
  qualification. Bitfinex signed amount denotes taker side; history snapshots,
  `te` execution and `tu` update must retain distinct provenance/correction rules.

All canonical prices/quantities use checked exact integers. Numeric JSON tokens
must be parsed from original lexical tokens before any binary float conversion.
Scientific notation is exact checked rescaling, with explicit overflow rejection.
Native precision is separate from normalized unit: ms multiplied by 10^6 remains
ms-precision. Invalid/missing/overflowing fields are rejected/quarantined with
raw lineage, never rounded, saturated or replaced silently.

## Delivery, health and accounting

Proposed stream states:

| State | Entry/effect | Exit |
|---|---|---|
| Discovered | validated immutable metadata and requested channel set | Subscribe or metadata-blocked |
| Subscribing | per-symbol/channel pending ack ledger | all required acks → Live; rejection → Degraded |
| Live | required channel health, capture path healthy; no continuity claim implied | transport/stale/drop/sink fault → GapSuspected |
| GapSuspected | mark interval uncertain from last verified boundary; keep collecting raw if safe | Recovering or Unrecoverable |
| Recovering | bounded live buffer + history overlap reconciliation | ContinuityVerified or unresolved Gap |
| ContinuityVerified | evidence proves declared source scope | new uncertainty → GapSuspected |
| Quarantined/Stopped | no publication/admission; retain evidence | explicit repaired revision/restart |

Readiness is the conjunction of required subscription acknowledgements, catalog
authority, recorder progress and required sinks. Socket open/MarkLive alone is
insufficient. Quiet symbols require heartbeat/subscription evidence and an
independent no-trade proof; missing trades alone cannot distinguish quiet from
broken. Track per-channel ack/rejection, last valid trade/heartbeat, stale age,
parse/unknown-trade failures, reconnect reason/attempt/generation and recovery.

Every queue has one owner, count AND byte capacity, policy and occupancy/drop
metrics. Proposed starting budgets (not measured guarantees): raw 8192 frames
and 256 MiB total; transport max4 MiB frame; dispatch 8192 batches/64 MiB;
per-required sink 8192 batches/64 MiB. Validate aggregate process reservation;
large messages must not turn a slot bound into unbounded memory. Tune after
measured capacity and retain the tested configuration hash.

Authoritative capture uses FailEngine/explicit degraded-stop on overflow or disk
failure. TCP backpressure can shift loss upstream; never call it end-to-end
losslessness. UDP and drop policies are observational paths, not archival truth.
Minimal Kafka/NATS TCP clients lack a proved archival acknowledgement contract;
file flush lacks crash durability. Standalone spill-WAL is rejected by daemon;
reuse any WAL only after its recovery consumer and commit semantics are tested.

Account received application messages; trade-bearing messages; decoded native
rows; known aggregate constituents (nullable); controls; intentional exclusions;
unknown trade-bearing records; rejected/quarantined records; duplicate sightings;
canonical unique rows; writer accepted rows; synced rows; manifest-committed
rows. Fanout duplicates delivery counts, not economic executions. Batching makes
frame and event counts differ. Every excluded row needs a machine-readable
reason; zero-price administrative USD-M messages are counted separately.

Proposed normal-load acceptance: zero unexplained missing rows, corruption,
conflicting identities or drops; all required subscriptions healthy; stage
balances reconciled. Injected failures must yield verified recovery or explicit
uncertain/gap/quarantine intervals. Counters cannot be reset into green by
restart. Threshold changes require versioned policy before judging a run.

## Recovery contract

Checkpoints hold separate received, parsed, synced, manifest-committed and
continuity-verified watermarks per stream, including raw ordinals and identity
sets/cursors. The maximum timestamp seen never proves complete prior coverage.

Resume from committed progress with configured overlap. Capture live raw while
requesting bounded backfill. Align venue/product/trade type/granularity before
comparing. Specify pagination direction and inclusive boundaries per endpoint;
retain cursors, request/response object hashes, rate-limit responses and page
bounds. Stop on no-progress/repeated cursor, row/time/storage/request cap,
retention truncation or second recovery failure. Never advance verified progress
past an unresolved boundary. Overlap dedup retains provenance and conflicts.
If the live buffer fills, mark the affected interval uncertain and fail closed.

Coverage interval states are `verified-continuity`, `recovered`, `suspected-gap`,
`confirmed-gap`, `unknown-continuity`, `verified-no-trades`. Union/split intervals
without discarding the strongest uncertainty; different channels/products have
separate ledgers. A source lacking a complete reference/sequence remains
unknown even when raw→normalized row accounting passes. Missing data is never
replaced by candles, interpolation, synthetic executions or rewritten timestamps.

## Storage decision and exact canonical schema (proposed v1)

Choose original historical objects + existing MFR1 application-payload capture,
with versioned immutable lineage sidecars, and partitioned Parquet/Zstandard as
the default canonical layer. Existing OKX corpus already uses ZSTD Parquet;
Python PyArrow/NumPy readers exist. Rust has no current Parquet writer/qualified
reader: adding one is planned, not a shipped capability. Measure a Rust/Python
cross-reader pilot before finalizing writer dependencies. JSONL is useful for
small evidence but larger/unsafe for unguarded large-number consumers; CSV loses
types/null semantics; database/distributed service has no demonstrated need.

Keep exact i128 through a Parquet struct `{coefficient: fixed_size_binary(16),
scale: uint8}`. Coefficient is big-endian two's-complement i128; scale0..255;
value = coefficient ×10^-scale. This avoids decimal128's 38-digit limit for
39-digit i128 extrema and allows different native scales. Do not silently cast
to float or fixed scale. Standard decimal128/256 analytics views are optional
validated exports with explicit bounds. Python uses int.from_bytes(signed=True)
and Decimal with sufficient precision; Rust uses i128::from_be_bytes. Canonical
semantic hashing uses this exact representation, never JavaScript Number.

`!` means required/non-null; `?` means nullable. All references are SHA256-bound.

| Field | Parquet type | Unit/origin and validation |
|---|---|---|
| schema_version! | uint16 | 1; reject unsupported major |
| dataset_id!, revision_id! | UTF8 | immutable manifest identity, may be partition metadata |
| venue!, region!, api_product!, protocol!, channel! | UTF8 | actual source identity; never relabel as OKX |
| native_symbol!, instrument_ref!, metadata_ref! | UTF8 | immutable effective instrument revision; ref hashes validated |
| native_trade_id? | UTF8 | lossless native ID; null explicitly limits identity assurance |
| identity_scope!, identity_epoch! | UTF8 | venue policy/version and reset boundary |
| native_sequence? | UTF8 | preserve original token; continuity role documented |
| granularity! | enum UTF8 | individual/aggregate/unknown |
| aggregate_id?, constituent_first_id?, constituent_last_id? | UTF8 | native only; no inferred reconstruction |
| constituent_count? | uint64 | native count; null differs from one |
| event_time_ns? | int64 | epoch UTC ns container; checked conversion |
| publication_time_ns? | int64 | venue time only where supplied |
| receipt_time_ns? | int64 | actual observed collector time; null on history without capture |
| retrieval_time_ns? | int64 | historical HTTP retrieval, never substituted for receipt |
| availability_time_ns? | int64 | genuinely known eligibility time; policy binds origin |
| native_time_value?, native_time_unit!, native_time_resolution! | UTF8 | source lexical time/unit/resolution; unknown explicit |
| timestamp_origin! | UTF8 | venue-field/collector/historical/unknown |
| price?, native_quantity? | exact struct | required positive on insert/correct; cancellation may omit; invalid economic rows quarantined |
| native_quantity_unit! | enum UTF8 | base/quote/contracts/lots/unknown |
| base_quantity?, quote_quantity? | exact struct | checked conversion with immutable formula/metadata; no guessed units |
| conversion_ref? | UTF8 | versioned formula/multiplier, rounding mode and bounds |
| native_side?, native_side_meaning! | UTF8 | original token and taker/maker/buyer-maker/unknown |
| aggressor! | enum UTF8 | buy/sell/unknown; side inversions independently tested |
| flags!, native_attributes_ref? | list UTF8 / UTF8 | aggregation/snapshot/correction/block/RPI etc, raw lineage |
| revision_operation! | enum UTF8 | insert/correct/cancel; cancels are actions, not fake zero-quantity trades |
| revision_target?, native_revision_id?, revision_order? | UTF8 | scoped native target identity/order; unresolved targets quarantine the action |
| revision_known_at_ns? | int64 | genuine availability of action; null disallows observed-availability application |
| collector_id!, acquisition_context_id! | UTF8 | globally scoped collector/run and live session or historical request/object context |
| session_id?, connection_generation? | UTF8 | required for live; historical requests must not invent a live connection |
| capture_ordinal! | uint64 | object-local acquisition order |
| lineage_kind! | enum UTF8 | mfr1/archive-member/http-object |
| raw_record_offset?, raw_row_index? | uint64 | required for MFR1; byte offset and row ordinal |
| archive_member?, decoded_record_index?, source_coordinate?, codec_ref? | UTF8 / uint64 / UTF8 / UTF8 | history: member path, decoded line/record index, format-specific coordinate, pinned decompressor/codec |
| raw_object_sha256!, acquisition_mode! | UTF8 | original MFR1/history bytes; live/backfill/archive/snapshot |
| validation_policy_ref!, quality_interval_ref! | UTF8 | interval state/admission reasons; cannot wash uncertainty away |

A field group may be stored once in immutable file/manifest metadata only if
readers check every reference and cannot reinterpret an old file using mutable
current metadata. Arrow schema/version and enum vocabulary are hash-bound.
Ordering/sighting sidecars retain all raw deliveries even when canonical rows
deduplicate. Unknown metadata prevents conversion/strict qualification, not raw
capture. Credentials, signed URLs and authentication payloads are forbidden in
shared recordings; authenticated sources need a reviewed redaction boundary.

Revision actions retain their original sighting and target identity. Correct
replaces only the targeted canonical version at its declared availability;
cancel withdraws that version without manufacturing an execution. Apply native
revision order within its documented scope; ambiguous order, unknown targets or
missing required availability are quarantined for strict replay. Event-time
reconstruction may apply historical revisions only under an explicit policy
that does not claim those revisions were originally known in real time.

Partition by venue/region/product/instrument-ref/event UTC date; unknown event
time uses explicit unknown-time quarantine. Receipt-day indices are secondary,
not rewritten event dates. Late arrivals create a new revision/fragment in the
correct event partition with availability preserved. Proposed target row group
64–128 MiB uncompressed, file128–512 MiB, ZSTD level3; bounded small pilots may
produce smaller files. Sort by event time, documented venue order where proved,
then collector/run, acquisition context, session/generation, capture ordinal and
row index, with a lexicographic immutable raw-object hash as final tie-break.
Observed-availability mode preserves per-stream receipt order and never uses
event-time sorting to erase arrival causality. A deterministic tie-break is not
cross-venue causality. Compaction preserves semantic hash, lineage and admission status;
never infer published data from directory contents.

## Raw sidecars, publication and crash consistency

Prefer a versioned sidecar over breaking MFR1 v1–v3. Sidecar v1 binds segment
SHA256, build/config SHA, feature/toolchain/dependency lock hashes, collector/run,
session/connection generations, lifecycle/timer inputs, catalog authority and
effective revisions, clock source/offset/uncertainty, raw record offsets, row
lineage, durable checkpoints and interval quality. Fields genuinely absent in
old capture stay unknown; migration cannot invent arrival or lifecycle history.
If embedded extensions are needed, version the opcode/reader and test current
plus required previous formats. Preserve original bytes unchanged.

Local filesystem publication protocol:

1. Exclusive dataset writer lease/lock and fresh UUID staging directory; use
   create_new, reject collisions/symlinks and path escape. Parallel writers own
   disjoint shards; coordinator alone publishes a dataset revision. An OS lease
   does not serialize threads sharing one writer instance: serialize complete
   mutations and CAS checks in process too. Sync the initialized directory and
   its ancestor chain; the immediate parent alone misses newly created ancestors.
2. Append raw and canonical staged files; verify CRC/shape/hash/row balances;
   flush and sync data. Proposed group-sync target250ms with progress watchdog;
   target is not an upper bound under I/O stalls. Accepted but unsynced bytes can
   be lost; commit only through successful sync. Fail readiness on stalled commit.
3. Finalize footer, re-read/count/hash, sync closed files, atomically rename on
   same filesystem, sync parent directories. Prepare immutable manifest containing
   exact files and quality decision. No checkpoint references uncommitted bytes.
4. Write the complete manifest to a new staged file, flush and sync that file,
   atomically publish it without clobbering an existing revision, then sync its
   parent directory. Test crashes between every step, including manifest fsync.
   A separate
   CAS pointer may name latest, but readers pin revision/hash. Interrupted writes
   cannot admit partial records; readers reject missing/hash-mismatched objects.
   Replacement followed by a failed namespace barrier is indeterminate: a complete
   new revision may be visible without a durability acknowledgment. Never claim
   that every failed publication leaves only an invisible tail. A shared-feed
   writer retains its last acknowledged cursor, returns an explicit indeterminate
   error after replacement/barrier failure, and permanently rejects further writes.

Dataset mutation failures poison that writer instance. Reopen under the exclusive
lease and recover from the intent; do not retry through the poisoned handle.
Matching immutable bytes alone do not establish durability after a failed install.
Writer reuse, publication and intent recovery must verify and sync the same opened
object plus its namespace before acknowledging authority. Recovered intent files
and reused immutable manifests need equivalent barriers. Read-only integrity
checks must remain independent of writer locks, write access and synchronization.

The implementation ledger records the tested portable subset of these rules.
Platform directory barriers must actually succeed; an ignored error or no-op on
an unsupported filesystem cannot qualify durable publication. A Windows API
compatibility probe is distinct from production integration and power-loss tests.

On restart, scan only own staged objects, identify complete raw CRC records and
quarantine truncated tails, preserve failed originals and publish recovery as a
new revision. Manifest idempotency keys prevent duplicate publication; a failed
write does not blindly retry a partially emitted record at its current offset.
Object stores require conditional put/CAS plus complete multipart objects and
immutable manifests; rename is not assumed atomic there. Backend support remains
blocked until its crash/concurrent-writer tests pass.

Backups copy manifests and every referenced raw/metadata/canonical object,
verify hashes, then restore to a fresh namespace and execute reader equality.
Proposed pre-promotion recovery objective: last committed revision must restore
without semantic loss; retention/RTO are ops decisions requiring capacity tests.
Delete staged/old data only through an explicit retention policy; no frozen
corpus deletion or production migration is part of this PR.

## Manifests and use-specific qualification

The proposed [manifest JSON Schema](dataset_manifest_v1.schema.json) fixes field
types, required groups and reason/state vocabularies. It is a specified format,
not an implemented publisher. Semantic validation additionally requires UTC-Z
start < end, matching object hashes/counts/lineage, no forward watermark over an
uncommitted object, and profile-specific coverage proof. `format` checks must be
enabled by the validator; JSON Schema alone cannot prove source completeness.
JSON manifest nanosecond bounds and uint64 counts/sizes use decimal strings
(nullable when genuinely unknown); semantic validators enforce signed64/unsigned64
ranges. This prevents ordinary JavaScript JSON parsing from rounding them.
Parquet uses the native integer/struct types above. Existing MFPE diagnostic JSON
contains numeric limbs/timestamps: read it with Rust/Python exact integer parsers,
never treat a browser Number roundtrip as exact persistence.
Published-but-not-qualified revisions remain inspectable and inadmissible to
strict readers. Zero-filled missing counts are forbidden; unknown constituent
counts use null. Immutable checkpoint objects describe absent cursors explicitly.

A revision manifest requires dataset/revision/parent IDs; half-open requested
UTC bounds and requested/observed symbols; event/receipt/availability bounds;
schema, metadata, decoder/build/source SHA, config/features/lock and validation
policy refs; exact file paths/sizes/SHA256/rows/time bounds/semantic hashes;
raw lineage and acquisition endpoint/channel/retrieval/session/cursors;
live/backfill boundaries; rejected/duplicate/conflicting counts; interval gaps,
uncertainty and reconciliation evidence; intended uses, decision and
machine-readable reasons; publication backend/commit record. Files outside the
manifest cannot enter the dataset. Raw-pilot capture manifests are evidence
inventories, not qualified dataset manifests.

Transitions: `raw-capture → quarantine OR validated-partition → qualified-revision
→ published-immutable`. Validation of values alone cannot bypass coverage gates.
Repairs/reconciliation create new lineage-linked revisions; originals remain.
A stale or weaker policy never upgrades itself through export or compaction.

| Profile | Minimum admission requirements |
|---|---|
| descriptive | faithful source identity/values; missing intervals visible; aggregates labelled; expressly accepted unknown coverage |
| execution-count | individual executions, native identity, complete admitted scope proof; no aggregate substitution/conflicts |
| signed-execution-flow | count-profile gates plus independently verified side and quantity units |
| confirmed-candle | exact interval/volume definition, true confirmation and eligible-trade coverage; no missing bars hidden by fill |
| observed-availability-replay | valid receipt/availability and lifecycle/clock evidence; no reconstructed unknown arrivals |

Readers require pinned manifest hash AND explicit requested profile. Reject absent
objects, unsupported schemas, metadata/hash mismatch, out-of-scope instruments,
uncertain intervals forbidden by profile and nonqualified decisions. Error
messages identify the blocking interval/reason. Coarse descriptive permission
never enters a strict experiment automatically. This audit admits no new data
into the economic corpus.

## Causal replay and candles

Two explicit modes: historical event-time reconstruction with unknown arrival
and declared delay assumptions; observed-availability replay using genuine
receipt/known-at times. Retrieval times are not historical arrival times.
Preserve receipt order and event order separately; cross-venue merge uses
clock uncertainty and deterministic policy, not a claim of proven global order.
Persist local monotonic durations, wall-clock source/jumps and offset evidence.
Parser latency is not exchange-to-host latency.

Replay sessions and connection generations independently; replay accepted
subscription changes, HTTP, timer firings and lifecycle inputs at their recorded
availability. Enforce a pinned catalog known-at policy. Backfills, corrections,
confirmed candles and late metadata become eligible only at declared availability.
Prefix invariance: adding future evidence cannot change earlier eligible outputs.
Pinned manifest/config replay must reproduce eligible event sequence and outputs
semantically, independent of file byte serialization/compaction order.

Candles are native or trade-derived, never interchangeable implicitly. Contract
includes UTC/venue timezone, half-open interval, opening/closing timestamp
meaning, incomplete/confirmed state, publication delay, late revisions, empty
interval policy, base/quote volumes and native/aggregate/constituent trade count.
Derived candles inherit source gaps; wall-clock closure does not establish
completeness. Empty/no-trade and unavailable are distinct. No silent forward-fill
of price/volume to manufacture a complete interval. Compare official and derived
candles only with aligned eligible trade set, source granularity, finality and
volume units; declare precision tolerance before evaluation.

## OKX consumer boundary and CLI design

See the report's actual field mapping. Existing frozen double-valued candles
cannot retrospectively become exact trades. Native USDC and USDT-proxy identity
must remain explicit. Keep EU/EEA spot1× cash/inventory context separate from
all derivative quantities and accounting. Keep legacy ok_rsi/rsi_v5 defaults,
persistence and lifecycle unchanged; adapters are additive/opt-in. The strict
meridian candle reader requires confirmation/quote volume/contiguous histories;
current normalization is missing essential fields. No current consumer reads
MFR1 directly. Use the smallest additive owner reader/bridge after contract
fixtures and source pins are agreed, not filename compatibility claims.

Proposed commands (not available today):
`marketfeed dataset validate --raw-manifest PATH --policy PATH --output STAGING`,
`dataset publish --validated PATH --expected-parent HASH`,
`dataset inspect --manifest PATH --profile PROFILE`,
`dataset replay --manifest PATH --mode event-time|observed-availability`.
All commands default offline, reject unknown config fields and unsupported
profiles, emit structured findings and nonzero exits for failed admission.
Acquisition requires an explicit bounded plan and separate public credentials
policy; no implicit network while reading or replaying datasets.

Proposed config adds `[dataset]` schema/policy/staging/backend/lease, `[capture]`
byte budgets/sync targets, per-stream channel-granularity/recovery capabilities,
and pinned metadata refs. Validate no Drop* archival path, no default region or
quantity conversion, complete bounds/request/storage caps and no frozen output
paths. Existing `run`, config schema and legacy behavior stay intact until an
opt-in migration has parity tests and a rollback disabling the new writer/bridge.

## Acceptance and operational limits

A scope passes only with independent native goldens, aligned same-venue
reference reconciliation where possible, raw→canonical→actual-reader round trip,
negative/fault tests and declared coverage proof. If a proof is unavailable,
retain a non-passing or weaker-use disposition. Full benchmark requirements:
trades/s and burst shape, bytes/trade by raw/canonical layer, CPU/RSS, write/sync
latency, scan throughput and daily storage projections for intended universe.
The report records measured pilot workload and missing canonical benchmarks;
short pilot estimates cannot size all instruments reliably.

Keep existing scheduled ≥7-run canaries and live ≥24h/≥7d soak gates, with exact
trade counts/scopes, failures, resource/clock/region evidence and owner sign-off.
A rapid loop is not calendar-spaced evidence. Operational promotion remains
separate from completion/review of this specification and plan.
