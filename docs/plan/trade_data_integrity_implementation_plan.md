# Trade integrity implementation plan

This document preserves the **original audit-stage plan** and its dependency
gates. Its description of a documentation-only deliverable applies to that
stage. Subsequent adapter, storage and engine repairs are tracked in the
[active implementation ledger](hyperliquid_shared_feed_implementation.md) and
[goal scope](active_goal_scope.json); work-package estimates below are not
current completion claims. In particular, the
[timer prerequisite repair](../evidence/timer-chronology-20261020/) does not
complete D5 or admit strict datasets.

Baseline: `1c668981ee2d5b456dfa6d085fbd558b1998937d`, 2026-10-19.
Scope/authority: repository-process/public research verification only. No economic
runs, orders, deployment, production migration or frozen-corpus mutation.
Completion of this PR means an evidence-backed specification and executable plan,
not implementation of every phase or qualification/promotion of a dataset.
See [contract](../spec/trade_data_integrity_and_backtest_storage.md),
[inventory](trade_venue_inventory.md), and [measured report](../ops/trade_data_validation_report.md).

## Original audit deliverable and working rules

This PR adds audit documentation, small public fixtures and bounded verification
tools. It does not repair adapters, add production Parquet storage, admit data,
or install a consumer bridge. Tool examples use current library paths and expose
failed semantic predicates; a successful tool process is not semantic acceptance.
The only Rust dependency change is the existing workspace replay crate as a
daemon **dev-dependency**. Cargo.lock adds that edge; runtime dependencies stay
isolated. Python PyArrow/NumPy are explicit audit-runtime prerequisites, not new
runtime feed dependencies.

Preserve unrelated work. Current cryptofeed checkout started clean; rsibot has
unrelated dirty work and stays read-only. Missing linked historical production
specification is recorded, not silently treated as inspected. Owner source
snapshots are pinned by commit/blob, not guessed from current submodule versions.
No existing open PR owned this audit on initial inspection; dependency PRs are
separate. Use a draft until independent review/PR checks resolve.

## Work packages and dependencies

Estimates assume one experienced engineer per package with existing test rigs,
public network access and no protocol change during work. They are engineering
days, not delivery promises; access/canary calendar time is excluded. Dependencies
are hard acceptance gates. Owners are responsibilities pending named assignment.

| ID / phase / estimate | Exact modules and reuse | Contract/change and failure semantics | Tests and measurable acceptance | Dependencies / owner |
|---|---|---|---|---|
| A0 / 0 / delivered | `docs/plan/trade_venue_inventory.{md,json}`, report/evidence, adapter factories/catalogs/configs | enumerate every shipped venue, segment/protocol and excluded family; evidence dimensions separate | every scope has evidence or reasoned non-passing disposition; source SHA/URLs/access dates resolve | none / adapter reviewer |
| A1 / 0 / delivered bounded | `scripts/audit_okx_consumer_cache.py`; actual quants/Hummingbot readers/snapshots | inspect cache read-only, hash/grid checks, instrument proxy semantics and source pins | five scored candle grids; BNB raw/derived check; exact missing trade bridge fields | none / consumer/data reviewer |
| S1 / 1 / 2–4d | `model/src/events.rs`, `ids.rs`, `instrument.rs`; `adapter-api/src/session.rs`; all factory session constructors | add channel/granularity, original side/unit/native attrs, scoped identity and runtime collector/session/connection context; preserve unknowns | two venues both instrument1; interleaved public/business; large IDs beyond2^53/u64 where native strings; no raw/session mismatch | A0,A1 / model+engine owners |
| S2 / 1 / 1–2d | `adapters/okx/src/{messages,session,factory}.rs`; shared serde/SIMD decode | decode all trade rows atomically with row indices; preserve count/source/seq; explicit aggregated and all-trades subscriptions | independently expected 0/1/many rows, repeated seq, large IDs, malformed late row, unknown side; no silent tail loss; native counts not invented | S1 / OKX owner |
| S3 / 1 / 2–4d | `adapters/binance/src/{usdm_messages,coinm_messages,usdm_session,coinm_session}.rs`, routed-v4 path | separate trade/aggTrade namespaces/ranges and endpoint contracts; classify administrative records explicitly | colliding t/a IDs remain distinct; f/l/count preserved; RPI/st classification; legacy+routed subscriptions acknowledged independently | S1 / Binance owner |
| S4 / 1 / 3–5d | Kraken/Deribit/Bitfinex `messages.rs`, shared exact model/parser boundary | parse original numeric JSON tokens before f64; exact exponent/scale/overflow; do not duplicate parser framework | independent numeric/string equality through serde/SIMD for 28+ digits, i128 bounds, negative/zero invalids, scientific notation; every rejection accounted | S1 / numeric+adapter owners |
| S5 / 1 / 3–5d | Bybit/Gemini/Bitstamp/Coinbase decoders; Kraken/Bitfinex snapshots | native side and block/RPI/classification; history/live origin and correction policy; OPENING_FILL stays unknown unless proved; preserve te/tu distinction | sign inversion fixtures, same-value distinct IDs, history overlap/conflicts, unknown side; official Bitstamp side evidence; no inferred constituents | S1 / venue owners |
| S6 / 1 / 3–6d | all `instruments.rs`, daemon `run.rs::resolve_live_catalog`, catalog metadata | immutable effective metadata and discovery authority; no stub qualification; current product discovery/roll policy | scale/multiplier changes, symbol reuse/delisting, linear/inverse quantities, expired contracts, unavailable catalog; no guessed conversion | A0,S1 / metadata owner |
| D1 / 2 / 2–4d | `adapter-api` acknowledgement contract; all sessions; daemon readiness; `engine/src/metrics.rs` | required channel/symbol ack ledger; partial rejection/staleness; trade-specific stage metrics | open socket and healthy quote cannot mask rejected trades; quiet vs unavailable distinguished; unknown trade-bearing messages visible | S1,S2,S5 / engine+venue owners |
| D2 / 2 / 3–5d | engine runner/live/reconnect, dispatch, recorder queue, daemon sink workers | count+byte budgets, archival FailEngine, source backpressure accounting, bounded shutdown | slow/full consumers, oversized/fragmented/compressed/binary/malformed frames; every loss/error leads to explicit interval state; no nominal unexplained drops | D1 / transport+engine owners |
| D3 / 2 / 3–5d | `recording/src/{pipeline,writer,reader,metadata,format}.rs`, `sinks` | create_new staging, synced progress, collision refusal; raw sidecar lifecycle/clock/offset linkage; commit watchdog | partial-write failure, disk full/blocked, rotation/shutdown failure, SIGKILL before/after sync; no overwritten prior segment or false committed watermark | S1,D2 / recording owner |
| D4 / 2 / 4–8d | factory capabilities + adapter HTTP; engine recovery supervisor/checkpoint; existing HTTP transport | venue-specific recovery, live buffer, overlapping pages and distinct received/parsed/committed/verified progress | inclusive/exclusive pages, wrong granularity, repeats/no progress/429/retention truncation, second disconnect, full buffer; unresolved gap persists | S2–S6,D3 / recovery owner |
| D5 / 2 / 3–5d | `replay/src/runner.rs`, recorded lifecycle sidecars, engine timers | bounded demultiplexed streaming replay; per-generation state; timer/HTTP/control chronology | >capacity+1 input, mixed sessions/generations, timer firing, clock jumps, late metadata; deterministic prefixes; retain v1–v3 limitations | D3,D4 / replay owner |
| P1 / 3 / 3–5d | `recording` exact schema/sidecars, proposed canonical writer module and schema fixtures; `model::Fixed` | implement specified binary-i128+scale Arrow mapping and ZSTD Parquet; resolve Rust crate choice through prototype | Rust↔PyArrow exact signed128 extrema/scales/nulls, CSV/JSON/protobuf actual boundaries, native units; compare semantic hashes, no JS truncation | S1,S6,D3 / storage owner |
| P2 / 3 / 3–5d | recording publication/checkpoint module, immutable manifests, CLI/config | transactional local publication, lease/CAS, immutable parent lineage, admission profiles | crash before/after footer/sync/rename/manifest, concurrent writers, missing/hash-mismatched files, restart idempotence, backup/restore; directory listing never admission | P1,D3,D4 / storage+ops owners |
| P3 / 3 / 2–4d | replay/recording reader; daemon CLI/config, metrics | pinned manifest/profile reader, quality state machine; no automatic weak→strict promotion | reject missing metadata, unsupported schema, uncertain strict interval, aggregate count profile, changed object, stale policy; preserve rejection reason after compaction | P2,D5 / research-data owner |
| C1 / 4 / 2–4d | OKX candle decoder/session, `model::Candle`, serialization; existing strict owner reader | preserve confirmation/quote volume and known-at; versioned additive export to wire-v1/owner candle format | unconfirmed/late/revised candles, native USDC vs USDT proxy, timestamp seconds/ms/ns, known-at and missing bars; real reader accepts only valid contract | S1,S2,S6,P3 / OKX+consumer owners |
| C2 / 4 / 2–4d | additive quants trade reader (owner approval), meridian_v3 boundary tests; no legacy edits | exact trade adapter with profile admission; explicit feature float boundary; dependency isolation | identical same-venue/instrument/window trade outputs; negative side/unit/order/gap cases; legacy ok_rsi/rsi_v5 persistence/default/lifecycle parity | C1,P3 / consumer owners |
| Q1 / 5 / 1–2d tooling + capture | existing daemon/harness, acquisition plan and manifests | resumable bounded live/history/archive acquisition; no implicit corpus writes | pilot complete through raw→validated→published→actual-reader with source-aligned reconciliation; request/byte/time/retry caps and stop evidence | D4,P3,C2 / data operator |
| Q2 / 5 / 2–4d | same components; no second downloader framework | expand one product/precision/parser family at a time; immutable new datasets | each row passes declared scope or remains failed/blocked/unsupported/not-run; no extrapolation from one liquid instrument | Q1 / venue owners+operator |
| O1 / 6 / 2–4d engineering + calendar | existing canary/soak scripts/workflows/runbooks/metrics | trade-specific acceptance, oncall, disk retention/restore and current endpoint region checks | ≥7 scheduled runs; ≥24h/≥7d requirements per existing runbook; RSS/CPU/disk/burst budgets, injected failure accounting and restore; ops sign-off | Q1,Q2 / release captain+ops |

## Cross-cutting decisions that must settle first

S1 is a shared ABI/schema decision: runtime identity, raw ordinal vs adapter
ordinal, aggregation and native quantity must be settled before parallel adapter
repairs. S6 owns effective metadata references; P1 cannot define conversion views
without them. D3 defines a real commit boundary before D4 can checkpoint or P2
publish it. P3 admission profiles precede consumer export. Keep proposed commands
out of existing configs until implemented; unknown fields must fail validation.

After those contracts settle, S2/S3/S4/S5 can proceed independently by venue;
S6 metadata can parallel them. Independent reviewers can split venue families,
recording/replay and actual consumer checks. They must not generate expected
values using the decoder under test. Record objections and dispositions in the
finding ledger. This PR's independent reviewers were separate source, consumer,
and venue-documentation agents; their execution scope is recorded in the report.

## Verification matrix and predeclared gates

| Risk | Existing harness to reuse | Required extension / predeclared pass condition |
|---|---|---|
| Native meaning | adapter fixtures, serde/SIMD parity, corpora | independent expected IDs/side/time/precision/units and aggregation; parity alone is not oracle |
| Source completeness | public canaries + official recent/history/archive | aligned source granularity, exact bounds/IDs/count/volume and retained discrepancies; no universal completeness inference |
| Durable delivery | engine chaos/foundation/shutdown tests; recording pipeline tests | slow sink, disk full/blocked/partial writes, process kill, rotation/checkpoint publication; no false committed rows |
| Recovery | transport-error/stable reconnect tests | trade recovery pagination + overlap/conflict/retention/second failure; distinguish reconnect from repaired gap |
| Representation | MFNE-JSON1/MFPE JSON+binary tests, Fixed tests | signed128/large-ID/time/null scale through every implemented boundary, not extension-only assumptions |
| Replay | adapter corpus replay, engine record_replay, EventPulse routed V4 | mixed sessions/timers/reconnects/late metadata; deterministic source/availability prefix and bounded memory |
| Consumers | actual pinned OKX reader and frozen cache audit | source identity, finality, exact decimal boundary, no gaps; reject proxies as native USDC and candles as individual tape |
| Sustained ops | canary_checklist, soak_runbook | calendar duration unchanged; explicit trade counts, config/source/region/resource/failure evidence |

The spec's zero unexplained loss/corruption requirement applies to normal archival
load. New byte capacities, 250ms sync target, file/row-group sizes and operational
budgets are proposed thresholds, not retrospectively selected passing criteria.
Before Q1, record intended universe and workload; select request concurrency per
venue published limits, retries and stop conditions. Large catalog responses are
bounded32 MiB each today but catalog requests are not raw-captured in the live
path. That provenance gap must close before metadata-qualified collection.

## Acquisition expansion plan

Actual 60-second public pilot and two bounded recent-reference batches are
recorded separately from this future plan in `docs/evidence/trade-audit-2026-10-19`.
Actual pilot was forward capture, not a history downloader or qualified release.
A JSON future plan in that directory defines per-request/time/storage caps and
qualification gates. Historical archive selection must vet source then validate
actual bytes/checksum/schema/time units before admission. No download of the
frozen May–August economic inputs is permitted.

Q1 pilot after implementation: OKX EU/EEA native-USDC spot shared consumer pairs,
plus one each linear/inverse product, a numeric-JSON source and a snapshot-heavy
source. Verify access/metadata first; discovery chooses active dated contracts by
product+expiry, not lexicographic symbol. Capture a new explicit future UTC
window, never silently substitute an instrument. Run recovery with a deliberate
bounded disconnect and capped overlapping reference acquisition. Block all
publication if required semantics/reference/reader gates fail.

Q2 expands symbols/precision/families only after Q1 and measured storage/throughput
budgets. Each dataset remains scoped. Unsupported options/margin/tokenized/new
protocol families need new inventory rows before code or claims. Legacy INTX
post-migration access is a separate blocker; do not activate credentials or
migrate existing production state as part of a planning PR.

## Migration and rollback

Proposed new owner files for C2 are
`quants-lab/core/data_sources/cryptofeed_manifest.py` and
`quants-lab/tests/test_cryptofeed_manifest.py`; they do not exist today and this PR
does not create them. Reuse owner candle validation at its existing feature-frame
boundary and add a distinct trade-returning API. Before any strict stream bridge,
bind the intended owner source/image and wire-v1 contract; do not silently select
an older current checkout instead of the pinned consumer snapshot.

All new fields/formats are versioned. Preserve MFR1 v1–v3 bytes/readers and legacy
consumer outputs. Unknown older fields remain unknown; no backfilled arrival
facts. New writers and bridges are opt-in. Run shadow publication to a separate
namespace and compare semantic hashes before reader adoption. Rollback disables
new capture/export or pins the previous admitted manifest; it never overwrites
original raw/frozen objects or mutates strategy ownership. Bad revisions stay
retained with withdrawal/replacement lineage; caches cannot silently point a
pinned reader at different bytes.

S1 runtime context changes need compatibility constructors or a versioned API;
S2/S3 enums require exhaustive-match migration tests; S4 exact parsing can reject
values previously rounded, which must surface as explicit quarantine. D3 file
naming must refuse existing paths. P1–P3 use a new namespace until cross-reader
and crash acceptance pass. C1/C2 must prove dependency isolation and legacy
parity before deployment proposals; deployment is not authorized here.

## Acceptance and handoff

This documentation PR passes when source/evidence inventory, actual schemas,
state transitions/durability/admission, executable file-level work packages,
actual OKX mapping, bounded evidence and honest gates/blockers are reviewable.
A draft may remain documentation-reviewable while cargo-deny/environment or
PR-level checks are unresolved; this does not grant operational readiness.

Trustworthy collection is blocked by S1–S6/D1–D4. Count/flow backtests are blocked
by granularity/side/coverage admission even where normalized values match.
Observed-availability replay is additionally blocked by D5. Qualified publication
needs P1–P3. Actual strict OKX compatibility needs C1/C2. Operational promotion
needs O1 and the source-specific calendar evidence. Every scope resolves to
passing evidence or an explicit non-passing disposition, not a single green box.
