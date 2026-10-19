# Trade data audit: measured evidence and limits

Date: **2026-10-19**. Baseline: `1c668981ee2d5b456dfa6d085fbd558b1998937d`
(`main`, initially clean). Work branch: `codex/trade-data-integrity-audit`.
Verdict: **bounded trade receipt/value accounting observed; no newly acquired
dataset qualified; strict OKX consumer compatibility blocked**.

This PR contains the [specification](../spec/trade_data_integrity_and_backtest_storage.md),
[file-level phased plan](../plan/trade_data_integrity_implementation_plan.md),
[complete source inventory](../plan/trade_venue_inventory.md), verification tools
and small original public fixtures. Production adapter/engine/storage behavior
is unchanged. The Rust replay crate is added only as a daemon dev-dependency.
Failed semantic challenges are findings, not repairs disguised as passing tests.

## Repository and contract inspection

No root/nested AGENTS.md was found in cryptofeed-rs or its ancestor directories.
README, CONTRIBUTING, Cargo workspace/lock/toolchain/features, factories, source
call paths, venue IDs/audit/maturity, ADRs, canary/soak/runbooks, recording/replay,
sinks, analytics, EventPulse capture/MFR1 and daemon consumers were inspected.
The previously linked `docs/spec/production_rust_multi_exchange_market_data_spec.md`
is absent at this source; the new spec does not pretend to reproduce it.
Open PRs at initial inspection were dependency updates #35/#47/#48; none owned
this audit. No pre-existing local edits were included or discarded.

Rust development toolchain is 1.91.0; workspace MSRV declaration 1.85. Workspace
has 28 members including nine real adapter families, synthetic, analytics,
EventPulse capture/MFR1, FFI and daemon/facade. Venue IDs2–20 are real source
identities, not19 independently qualified services. Inventory JSON contains 31
explicit product subfamilies and 37 protocol/channel scopes plus unsupported/excluded products. Optional SIMD,
Kafka/NATS, UI and private live code are included in offline all-feature gates;
no private session or credentials were used in acquisition.

Actual runtime source: discovery → plan → WebSocket transport → runner raw
recording enqueue → adapter decode/envelope → dispatch/sink workers → file.
Raw enqueue precedes mutable parser input but is not synced persistence. Catalog
REST responses are not retained in these raw files; embedded metadata contains
resolved catalog fields. All 18 configured pilot catalogs logged successful
resolution; this does not independently prove every metadata field correct.

Existing integrations have narrower contracts. `analytics/src/grid.rs` requires
exact rescaling, and `analytics/src/flow.rs` accumulates normalized quantity by
aggressor including Unknown; neither can restore upstream aggregation/unit facts.
`event-pulse-mfr1/src/v2.rs` already validates explicit routed Binance session/
metadata/provenance bindings, checks bounded order, and assigns raw frame ordinals
at its strict transformation boundary. That is a useful reuse pattern, not proof
that generic daemon envelopes have equivalent lineage. Its specialized BNBUSDT
routed aggregate path is outside this legacy BTC pilot. EventPulse capture is a
pure offline assembler; no CLOCK/COVERAGE/SYSTEM sidecars or prospective E2
package were created/admitted by this audit, and missing historical bytes remain
missing. Existing routed V4/prefix tests passed within the offline workspace gate.

The sibling rsibot instructions and cached-data policy were read. Its root is
dirty on `feat/andrzej_regime_forecast_sizing`, SHA
`90bd12ce5a76ffa952ed7fc079b781b595cacf1b`; it remains read-only. Current nested
quants SHA`c811cb1bfae7525df87e7ba15436879f8605d526` and Hummingbot
SHA`eb069054ff62aedaac52d2bb71fb8cb03738702b` differ from actual preserved
consumer-source snapshots. The old `${LEGACY_USER_ROOT}/.codex/scripts/session-bootstrap.sh`
path is unavailable here; no substitute bootstrap mutated the owner workspace.

## Evidence index and reproducibility

Tracked reports/configs use `${WORKSPACE_ROOT}`, `${QUANTS_ROOT}`,
`${HUMMINGBOT_ROOT}`, `${RSIBOT_ROOT}` and `${AUDIT_PYTHON}` aliases. Original
reports and actual local root bindings remain in ignored local evidence;
`portable-export-receipts.json` binds original and exported report hashes. Native
fixture/raw/data hashes are unchanged. Config templates must be materialized
with local absolute paths and fresh output destinations before use.

Small tracked evidence: [`docs/evidence/trade-audit-2026-10-19`](../evidence/trade-audit-2026-10-19/).
Bulk local evidence, excluded from Git:
`${WORKSPACE_ROOT}/.local/evidence/trade-audit/`.
Raw sample files and reference objects remain there, with exact SHA256/size
inventories. These are local accessible artifacts, not a claimed remote archive
or retention service. Moving them requires copying and checking their hashes.

| Artifact | Contents / assurance |
|---|---|
| `capture-manifest.json`, `pilot-config.toml`, `acquisition-plan.json` | original bounded pilot plan/config, launch/end/caps, actual output hashes; capture inventory, **not** qualified publication |
| `native-audit.json` | native row vs sink price/quantity/ID multiset; excludes side/time/order qualification; reports session/venue mismatch separately |
| `scope-bounds-and-counts.json` | exact per-venue event/receipt ns bounds, endpoint/session bounds, snapshots and aggregation counts |
| `fixture-provenance.json`, `fixtures/venue-*.json` |17 exact original public application payloads,38309 bytes total; raw object/offset/session/ordinal/receipt/payload hash |
| `reference*-manifest.json`, `reference-reconciliation.json` |19 original bounded HTTP responses; price/quantity/ID intersections and ineligible/no-overlap results |
| `semantics-challenges.json` | independently written synthetic OKX batching and Kraken high-precision challenges; both integrity predicates fail |
| `consumer-cache.json` | read-only five-pair frozen-corpus hashes/grids, BNB raw/derived/bundle consistency, pinned reader blob checks |
| `storage-probe.json` | exact Python/Arrow layout roundtrip and bounded timing; not a canonical or durable dataset |
| `build-provenance-addendum.json` | binary hash measured **after** original run; original runner did not hash binary at launch |
| `gate-results.json`, `findings.json`, `independent-review.md` | exact gate outcomes, issue closure requirements and reviewer dispositions |

Run from repository root, with fresh output paths when a command writes data:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo deny check
./scripts/check-oss-readiness.sh
python3 -B -m unittest discover -s scripts/tests -p test_trade_data_audit.py -v
cargo run --locked -q -p marketfeed-daemon --example audit_trade_semantics
python3 -B scripts/audit_trade_capture.py .local/evidence/trade-audit/pilot-001
# Reacquisition needs a fresh destination; original venue snapshots are immutable evidence.
# python3 -B scripts/acquire_trade_references.py --plan docs/evidence/trade-audit-2026-10-19/reference-manifest.json --output .local/evidence/trade-audit/reference-NEW
python3 -B scripts/reconcile_trade_reference.py .local/evidence/trade-audit/pilot-001 \
  .local/evidence/trade-audit/reference .local/evidence/trade-audit/reference-after
"${AUDIT_PYTHON}" -B scripts/audit_okx_consumer_cache.py \
  --quants-root "${QUANTS_ROOT}" --json
"${AUDIT_PYTHON}" -B scripts/benchmark_trade_storage.py --self-test
```

Original live invocation (already executed; existing output is never reused):

```sh
python3 scripts/trade_data_pilot.py --binary target/debug/marketfeed \
  --config .local/evidence/trade-audit/pilot-config.toml \
  --output .local/evidence/trade-audit/pilot-001 \
  --metrics-url http://127.0.0.1:19348/metrics --seconds 60 --max-bytes 134217728
```

For a new pilot, choose a fresh output directory and update **both** raw/sink
paths in the config. The hardened runner validates containment, forbids private/
INTX/network sinks, verifies telemetry bind, launches its captured config, hashes
the binary and records working-tree status. Those safeguards were strengthened
after review; the original run used the recorded reviewed config with contained
paths, but lacked launch-time binary hashing. Storage is an observed stop
threshold, not a filesystem hard quota; sampling and shutdown allow overshoot.
Current config must be revalidated against current instrument metadata; the
original lexicographic OKX futures selection failed and is not a recommendation.

For adapter-only reconstruction, run on each segment:

```sh
cargo run --locked -q -p marketfeed-daemon --example audit_okx_replay -- \
  .local/evidence/trade-audit/pilot-001/raw/seg-1792431649935378000-000000.mfr1
```

The example routes by raw session metadata, creates bounded OKX session machines,
and disables books. It does not execute a process-level causal scheduler or use
the actual research reader. Compare JSON output against live sink on venue,
instrument, exchange/receipt time and payload; exclude known-broken runtime
session/frame identity when reporting only semantic payload equality.

## Bounded live run

Host environment: macOS 26.6.2 arm64, Europe/Warsaw client; bounds below are UTC.
No VPN/geolocation or EU/EEA endpoint eligibility was independently established.
Forward observation requested60s; process interval:
`[2026-10-19T17:40:49.657390Z,2026-10-19T17:41:50.479755Z)`
(60.822s including graceful shutdown). Actual source event bounds differ and
include historical snapshots; exact ns bounds are in the scoped JSON.

18 venue configurations,19 WebSocket sessions (OKX public/business separate),
required raw recording and required file sink. Original config requested trades
on every source and candles on Binance/Bybit/OKX spot. Several factories also
subscribe tickers/reference channels even when not requested. This affects
resource counts and the OKX futures failure; generic totals cannot prove trades.

- Two MFR1 segments:12440343 and62601 bytes, total 12502944 bytes.
- Exact record accounting: **30838 records**, **30720 inbound text messages**,
  **3710 trade-bearing messages** as independently classified by the audit tool.
- **4853 eligible native rows and4853 normalized trades**. Four additional USD-M
  `e=trade,st=1,X=NA,p=0,q=0` messages are explicitly administrative exclusions.
- **33634 normalized events**, including **84 candle updates**. Counts include
  source history snapshots; they are not all new economic executions.
- Last pre-shutdown metrics snapshot: recording dropped0, per-venue dropped0;
  OKX futures reconnects10. This is a point-in-time metric observation; final raw
  file accounting is separate. No flush/fsync durability claim follows from it.
- SIGTERM shutdown completed with daemon exit0. No indefinite collector remains.

| Venue ID / source | Exact sample | Eligible raw / normalized trades | Actual scope and disposition |
|---|---|---:|---|
|2 Binance spot | BTCUSDT `@trade` |1373 /1373 | live receipt and value/ID accounting pass; full fidelity/continuity unqualified |
|3 Binance USD-M | BTCUSDT `@trade` |875 /875 | pass eligible rows;4 native administrative messages counted separately; routed aggregate path not sampled |
|4 OKX spot | BTC-USDT `trades` |70 /70 | aggregate rows match; native count sums130 constituents, which normalization discards |
|5 Bybit linear | BTCUSDT `publicTrade` |327 /327 | sampled perpetual only; dated futures not run |
|6 Bybit spot | BTCUSDT `publicTrade` |518 /518 | scoped value/ID accounting pass |
|7 Kraken spot WSv2 | BTC/USD `trade` |90 /90 | scoped sample passes; numeric exactness fails independent adversarial fixture |
|8 Deribit inverse perp | BTC-PERPETUAL `trades.*.100ms` |184 /184 | native USD amount preserved for sample; not base units; other families not run |
|9 OKX linear swap | BTC-USDT-SWAP `trades` |375 /375 | aggregate rows match; native count sums771 constituents, discarded by normalization |
|10 OKX futures | BTC-USD_UM_XPERP-310404 |0 /0 | **failed**: added index-tickers symbol rejected60018; no successful trade evidence |
|11 Bybit inverse | BTCUSD `publicTrade` |7 /7 | inverse perpetual only; small sample |
|12 Binance Coin-M | BTCUSD_PERP `@trade` |32 /32 | inverse perpetual only; quantity is native contracts |
|13 Kraken Futures | PI_XBTUSD `trade_snapshot` |100 /100 | **snapshot-only**; no forward trade observed, no live-update completeness proof |
|14 Bitstamp | btcusd `live_trades` |10 /10 | value/ID accounting; fresh authoritative side interpretation remains blocked |
|15 Gemini | BTCUSD `@trade` |6 /6 | small sample, native ns; precision metadata/retention limitations remain |
|16 Coinbase Exchange | BTC-USD `matches` |347 /347 | includes1 initial last_match; no heartbeat/REST gap-recovery proof |
|17 Bitfinex spot | tBTCUSD `trades` |48 /48 | includes30 history snapshot rows; te/tu policy not qualified |
|18 Coinbase Advanced | BTC-USD `market_trades` |448 /448 | includes100 snapshot and348 update rows; no constituent-count inference |
|19 Coinbase legacy Intl | no acquisition | not-run | **blocked**: auth/current service and documented migration; no credentials used |
|20 Bitfinex derivatives | tBTCF0:USTF0 `trades` |43 /43 | includes30 snapshot rows; conversion metadata unqualified |

Every value-multiset pass above is limited to ID/price/quantity and an unambiguous
venue-local symbol map. It does not establish side/time/order/finality/continuity
or raw lineage. All 4853 trade envelopes use session 1;4805 of those resolve through
that session to a different venue's metadata. Across all event kinds,33519 of
33634 events have that mismatch. Raw session metadata itself correctly names the
19 sessions. The audit records the mismatch and uses a unique venue-local symbol
fallback for value accounting; ambiguous mappings fail instead of guessing.

All dates/other symbols/precision regimes and unsampled product families remain
not-run. Options, margin economics, Advanced derivatives, new Deribit USDC
products and other excluded surfaces remain unsupported by the stated catalogs.
Synthetic is test infrastructure only. No venue gets a qualified green verdict.

## Three evidence paths

### A. Native messages and independent expectations

17 authentic fixtures retain exact payload bytes and provenance. Independently
reviewed documented meanings are in the inventory. Existing fixtures/corpus
and serde/SIMD tests ran in the workspace gate, but decoder-generated
expectations or decoder parity are not independent semantic truth.

The additional synthetic challenge is deliberately independent of existing
single-row fixtures. Two OKX rows with IDs9007199254740993/9007199254740994 and
exact18-place price produce **one** decoded row. Kraken native numeric price
`0.1234567890123456789012345678` becomes coefficient 12345678901234568 scale 17,
instead of coefficient 1234567890123456789012345678 scale 28. Both predicates
fail while the challenge executable exits0 to emit findings. These are reproduced
current defects, not production repairs or successful acceptance tests.

Aggregation has a measured research impact: the OKX spot sample has70 aggregate
rows representing native count130; swap375 rows represent771. Raw bytes retain
count; current normalized Trade does not. Single-row messages in this short
capture do not disprove the multirow decoder defect. Side/missing-field/overflow/
scale/correction acceptance remains partly fixture-tested and otherwise planned;
no universal field semantics claim follows from row totals.

### B. Same-venue reference acquisition and reconciliation

Two batches fetched original public recent-trade responses:10 requests before
(including one active-OKX-futures catalog) and9 later. One request at a time,
8s request timeout,1 MiB response cap, one attempt/no retry. All returned bounded
bytes; JSON/API meaning is checked separately from HTTP success. Total reference
bytes/hashes and exact retrieval timestamps are in the two manifests. These are
recent snapshots, not a downloaded historical archive or recovered interval.

| Source | Overlapping IDs with zero price/quantity mismatches | Limit |
|---|---:|---|
| Bitfinex spot |22 before,48 later | overlaps may share IDs; do not add them as70 unique independent executions |
| Bitstamp |7 later | limited recent window; side/time not independently compared |
| Coinbase Exchange |23 later | ID/value intersection only; no complete gap recovery |
| Deribit |57 later | same instrument/native amount, not conversion proof |
| Gemini |6 later | HTTP ms vs native ns needs separate time semantics; retention docs conflict |
| Kraken spot |67 later | recent REST result; source range not complete pilot coverage |
| Binance spot / Bybit spot |0 in bounded snapshots | **not-run-no-overlap**, not mismatch or completeness evidence |
| OKX spot | comparison blocked | WS aggregates vs REST individual rows are different granularities |

No cross-exchange prices were used as a correctness oracle. No pagination,
intentional disconnect backfill or historical retention boundary was exercised;
those remain D4/Q1 work. Reference acquisition and plans do not imply dataset
qualification. Requests after/pre capture were bounded but not scheduled canaries.

### C. Raw replay, storage and actual consumers

Bounded OKX raw-session reconstruction produced 445 trade payloads matching live
sink venue/instrument/exchange time/receipt time/price/quantity/side/ID exactly
as a multiset, with zero missing/extra payloads. This reproduces the existing
normalizer and its aggregation/session/lineage defects. It does not independently
prove original executions, deterministic arrival chronology, crash recovery or
compatibility with the actual research reader.

Python3.13.15/PyArrow25.0.1 candidate layout probe read 4853 normalized trades and
wrote 17 venue/receipt-date partitions with ZSTD3. Coefficients use signed
big-endian binary16 plus uint8 scale;18 signed-i128/scale boundary cases and4
invalid-input rejections pass. Parquet readback has exact row equality and
semantic hash`ecb31f923137082110799ec8efb8938ff5a7616d24beec59dd4c01f3faa0d6d1`.
Files total 236414 bytes,48.715 bytes/trade including small-file footer overhead.
Write0.02837s; scan plus Python materialization0.03422s; whole-process macOS
maxRSS153780224 bytes. These are one warm small Python sample, not sustained
Rust throughput, CPU capacity or fsync/transactional durability evidence. Probe
partitions use receipt date for diagnosis; proposed canonical event-date
partitioning is not implemented by the probe.

Illustrative scenario:1000 trades/s ×48.715 measured bytes/row ×86400 is about
4.21 GB/day canonical bytes before raw, metadata, replicas and other channels.
This linear calculation is not intended-universe sizing; dictionary compression,
snapshots, row groups and small files change the ratio. Actual pilot raw rate
including all factory-added channels is about0.206 MB/s over60.822s; extrapolation
to all symbols or sustained operation is unjustified. CPU time, Rust writer/reader
scan, sync latency, burst load and intended-universe day-size remain unmeasured.

The probe is staged unqualified evidence. There is no canonical Rust writer,
qualified immutable publication or new consumer-admitted trade dataset.

## Actual OKX consumer compatibility

Owner root: `${QUANTS_ROOT}`.
Frozen corpus root:
`output/data_backfills/spot_corpus_okx_usdt_2025_07_2026_08`.
Manifest SHA256`fe159bf4069198acc92280310f79fbdc578bf01bd7e753adf7caeee348d85133`.

All five BNB/ETH/BTC/SOL/XRP Parquet hashes match their manifest. Each scored grid
contains 177120 contiguous60s rows in `[1777593600,1788220800)` with no duplicate
timestamps. This is candles, not a trade corpus. Logical USDC pairs explicitly
use **USDT source instruments**; the proxy identity cannot be erased or called
native USDC parity. Existing corpus bytes and manifests were not changed.

BNB raw May–August files were exhaustively read:177120 unique confirmed scored
bars,120 cross-month overlapping timestamps,zero conflicts. Derived BNB OHLC,
base and quote volume equal float(raw fields1/2/3/4/5/7) on every scored row.
Its trade-count/taker-volume fields are entirely NaN. This is raw-cache→derived
consistency, not independent official trade reconciliation or exact-decimal proof.
BNB meridian bundle hash/NPZ receipt matches:227520 contiguous warmup+scored rows,
exactly 177120 scored rows. Other pairs' raw-value parity was not exhaustively
reconciled. All exact hashes, file sizes and reader-source blob checks are in
`consumer-cache.json`.

Actual recent consumers reside in pinned preserved snapshots:
`output/backtests/meridian_v3/laptop2/src/q-6c4ed9a`, `q-79069cc`, `hb-356b3da`.
Their full commit IDs and inspected paths match git-show blobs in the cache audit.
Do not infer their versions from current old nested checkouts. Snapshot Rust
candle producer pins rust_decimal1.43.0, reqwest0.12.28, tokio1.53.1, axum0.8.9,
and direct tokio-tungstenite0.28.0 (0.29.0 also transitive); no cryptofeed dependency.
Current ordinary candle cache directory is empty, but the frozen corpus is present.

| Field/behavior | Actual consumer/reference | Current cryptofeed status / mapping requirement |
|---|---|---|
| Instrument/venue/region/product | native strict USDC spot vs frozen USDT proxy; EU/EEA1× context | global endpoint sample does not verify EU/EEA or native USDC; explicit mapping blocked |
| Data type | flat ten-column candle Parquet; strict candle wire-v1; meridian minute bundle | MFR1/trade envelope not accepted; no trade bridge exists |
| Timestamp | corpus epoch seconds; REST open ms; strict publication/receipt/epoch and per-pair seq | ns envelope exists but native precision/availability origin incomplete; no implicit unit conversion |
| Decimal | frozen Parquet/NPZ doubles; strict wire Decimal strings, float only at feature frame | Fixed exact for string paths; numeric-token defect; new exact writer/bridge planned |
| Side | no side in candle corpus; RSI V5 buyer-maker inversion on aggregate rows | candle parity cannot validate trade side; explicit aggressor policy and negative tests required |
| Quantity/volume | raw spot base row5/quote row7; derivative units excluded | Candle lacks quote volume; Trade lacks explicit unit/conversion ref |
| Ordering/replay | strict histories contiguous; transport epochs/sequences; snapshots not entry events | per-session defaults/raw ordinal gap; causal lifecycle replay missing |
| Deduplication | frozen grids unique; raw BNB120 identical overlaps | venue-aware canonical identity/conflict policy absent |
| Candle finality | strict confirmed=true; raw BNB confirms1 | decoder sees confirm but normalized session discards it; **blocked** |
| Nulls/unavailable | frozen n_trades/taker fields NaN; legacy Hummingbot substitutes0 | unknown cannot become observed zero; additive reader preserves nulls |
| Coverage/admission | hash/grid/parent lineage, full fixed scoring window | no qualified manifest reader; timestamp grid alone not execution coverage |
| Legacy lifecycle | ok_rsi explicit Binance proxy; RSI V5 aggregate-row count and empty-second behavior | preserve defaults/persistence; no owner code changed; no legacy runtime promotion |

Current quants readers include `core/backtesting/engine.py:156` and
`core/data_sources/clob.py:647`. Strict snapshot `candle_stream.py:CandleStreamClient`
and `candle_data.py:Candle/CandleSnapshot` require confirmation, quote volume,
contiguous history, closure before publication and reject epoch/gap/stale state.
Legacy Hummingbot OKX parser uses row6 and discards confirmation; strict producer
uses row7. Inspected spot BNB row6/7 values agree, so this contract difference is
not labeled a demonstrated numeric spot bug. Neither reader accepts MFR1 today.

**Compatibility verdict: blocked/planned.** No overlapping newly captured
native-USDC/EU-spot interval was passed through the actual strict reader. There
is no individual-trade reader to establish trade parity. C1/C2 specify the
smallest additive bridge and negative/roundtrip tests. Frozen raw-cache consistency
is useful evidence but does not clear those blockers.

Exact open contract questions: which deployed owner commit/image is the target;
which current OKX EU/EEA public endpoint and native-USDC products are permitted;
which use profile accepts aggregates versus constituent executions; which
availability/clock evidence is required; and which prospective source interval
will be pinned before acquisition. Answering these cannot be replaced by a
shared Parquet suffix or an invented reader path. Proposed owner files are
explicitly new work in C2; existing files/snapshot hashes are recorded above.

## Findings, independent review and remaining gates

The structured ledger gives severity/scope/reproduction/evidence/impact/proposal/
regression/closure for every finding. Highest risks: OKX batched row loss and
aggregation metadata; numeric JSON precision; runtime session identity/raw lineage;
unsynced/colliding segments; partial causal replay; discarded candle finality;
source-proxy identities; current legacy INTX service access; futures subscription
selection; missing qualified publication/admission and trade-specific exclusions.

Separate source-semantics, venue-documentation and consumer reviewers inspected
independently. Their tests and non-executed suggestions are distinguished in
`independent-review.md`. Review identified manifest fsync, historical lineage,
revision actions, deterministic ties and pilot provenance/path safeguards;
those are incorporated into the specification/tooling. The conservative signed
flow profile was renamed signed-execution-flow to distinguish aggregate-volume
studies. No repeated self-review is represented as independent review.

Initial workspace gates: fmt/clippy/test/rustdoc passed; tests 1123 passed,
56 ignored. Ordinary ignored live tests were not silently counted as passes.
The explicit pilot provides separate bounded network evidence. Final daemon
all-targets/all-features and workspace Clippy were rerun for the new examples.
OSS readiness passed with **gitleaks unavailable, scan skipped**. Cargo-deny was
initially absent; local version 0.18.3 installed successfully but failed to parse
a current CVSS4 advisory. With cargo-deny 0.19.0, the final gate **fails advisories** for locked
`rustls 0.23.42`: public RUSTSEC-2026-0285, RUSTSEC-2026-0346,
RUSTSEC-2026-0348 and RUSTSEC-2026-0349. Bans, licenses and sources pass.
This is a dependency-version gate finding, not a claim that every advisory
configuration is exploitable here. Exact exit/log is in gate-results;
no dependency changes are smuggled into this audit to make a baseline gate green.

Current tests cover many malformed/slow-sink/disconnect/disk-pressure/rotation/
shutdown and EventPulse prefix cases; this run did not perform a new live disk
fill/process-crash/second-recovery-failure experiment. Existing tests cannot
prove missing sync or trade-history recovery contracts. Failure-injection
extensions are explicit D2–D5/P2 work, not claimed executed tests.

No scheduled canaries or multi-day soak were run. Existing ≥7 scheduled-run,
≥24h/≥7d requirements remain unchanged. Immediate next work is S1–S6 semantics
and identity, D1–D5 delivery/recovery/durability, P1–P3 storage/admission, C1/C2
actual OKX readers, then Q1/Q2 qualified acquisition and O1 calendar evidence.


## Bybit native integrity repair followup (2026-10-19)

Bybit public trades/candles now preserve batch rows, trade classification and
repeatable cross-sequence provenance, explicit candle finality and native
start/end/last-match/publication fields. Malformed late rows, negative financials,
timestamp overflow, mismatched symbols/intervals and missing catalog identity
fail before partial emission. Optional canonical candle provenance reaches JSON
and protobuf; existing absent fields remain unknown. This is a Rust alpha
constructor/decoded-variant migration, not source compatibility.

Independently checked offline:47 Bybit tests (4 live/regen ignored),6 explicit
native adversarial regressions,4 generated-protobuf exact/presence fixtures and
45 Python audit tests with false-green mutations. Full workspace testing and a
new source-pinned bounded live probe are still pending at this checkpoint.

Open Bybit findings: unsupported L2 u+1 inference, conflicting duplicate-book
discard, first-page-only derivative catalog, missing liquidation bankruptcy-price
provenance, exact per-channel ACK readiness, source continuity and WS quantity
unit qualification. These repairs do not admit a dataset or verify every product.
See adapter README and active implementation ledger for source contracts/evidence.
