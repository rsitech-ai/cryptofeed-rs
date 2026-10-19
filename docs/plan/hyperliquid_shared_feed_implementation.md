# Hyperliquid shared feed implementation and verification ledger

User-authorized implementation goal, 2026-10-19. Baseline cryptofeed
`8b186c33fa6b5cc99060a0efe20f4ff8d4b5801f`, hlscreen
`6deb7f506e90c238b63d9c057f9a477c8f65f662`. Work branches:
`codex/hyperliquid-shared-feed` and `codex/cryptofeed-shared-feed`.

## Acceptance

One public market-data implementation owns Hyperliquid spot and perpetual
metadata/parsing. One collector produces the exact authoritative stream;
hlscreen consumes committed records with resumable cursors, preserving its
screening/UI and explicitly separating float feature projections from exact
stored values. Save original payloads, versioned metadata, lifecycle and exact
normalized events. Refuse silent overwrite, silent drops, unverified continuity,
unsupported precision, missing metadata or false commit progress. Existing
recordings remain readable and are never relabelled as exact original evidence.

Implementation is not complete until offline contract/regression tests, shared
consumer tests, bounded public live/replay/reconciliation, injected failure and
recovery checks, dependency checks, and a fresh review have evidence. Long-running
operational gates retain their actual duration; short tests do not satisfy them.
No orders, credentials, production deployment, frozen corpus mutation or economic
strategy runs are part of this goal. New capture is bounded and stored separately.

## Work sequence / current state

1. IMPLEMENTED; continuing hardening: shared exact Hyperliquid protocol, spot/native-perp discovery,
   adapter state machine, daemon/CLI/config integration, protocol contract tests.
2. IMPLEMENTED initial local journal/UI path; continuing tests: complete committed shared feed, metadata/raw/lifecycle linkage,
   resumable reader and hlscreen shared consumer + projection parity.
3. IMPLEMENTED initial Hyperliquid-only exact dataset publisher, immutable
   manifests, transactional publication/recovery and backup/restore. Strict
   profile admission remains deliberately denied; full mixed-input replay and
   general venue policies remain pending.
4. PENDING: repair known integrity findings from the prior audit, retaining a
   per-finding test and disposition rather than declaring all venues verified.
5. PENDING: live source references, end-to-end hlscreen parity, failure injection,
   dependency/security gates, fresh review, cross-repository pinned delivery.

## Findings to repair and verify

- Runtime session/connection identity and normalized-to-original-frame linkage.
- OKX full trade-array decoding, aggregation/source attributes and candle finality.
- Binance trade/aggregate/admin classification and native ranges.
- Exact numeric JSON parsing in Kraken, Deribit and Bitfinex.
- Source snapshot/history vs forward events and side/unit semantics.
- Authoritative immutable metadata, product/quantity/scale mapping and provenance.
- Required subscription acknowledgements and channel-specific health.
- Recorder create-new collision refusal, actual file/directory sync and commit
  progress, queue byte/count limits, disk failure and bounded shutdown.
- Per-source gap recovery with overlap/conflict handling; unresolved gaps remain
  explicit and block strict admission.
- Bounded mixed-session lifecycle/timer/HTTP replay and deterministic prefixes.
- Exact signed-i128+scale storage, versioned manifest/profile reader and real
  consumer tests. No implicit qualification of old float/proxy data.
- Existing cargo-deny rustls advisories and missing OSS scanner gate.

## Initial measured checks

The first eight Hyperliquid contracts pass (exact values beyond 2^53, all batch
rows, atomic malformed-tail rejection, native scoped trade IDs, token-index
metadata mapping, product namespace, acknowledgement ledger, disconnect state,
book order/crossing and derivative fields). Existing model tests pass. This is
partial evidence, not qualification. Perpetual discovery now uses `allPerpMetas` and preserves builder DEX
namespaces and collateral tokens. Native and builder metadata contract tests pass.

## Sources

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- hlscreen protocol knowledge inspected at the pinned baseline above; Apache-2.0.

## Development checkpoint (goal remains active)

- Added `marketfeed-adapter-hyperliquid`, IDs 21/22, real POST discovery, exact
  lexical JSON decimal parsing through RawValue, public trades/BBO/snapshot books/
  candles/asset contexts, all requested acknowledgement ledger and heartbeat.
- Added MFSF1 in `recording::shared_feed`: fresh-directory ownership, original
  bytes, metadata HTTP evidence, exact batches, lifecycle/connection/timers,
  atomic synced commit watermark, CRC/sequence validation, bounded size, cursor
  replay, write poisoning, clean end seal. Each connection attempt gets a fresh
  engine-owned connection ID; envelopes use runner-owned session/raw ordinals.
- Repaired MFR raw writer collision truncation and added file/directory sync.
  This is not yet transactional qualified Parquet publication or full recovery.
- hlscreen changes live in sibling checkout on the branch above. Shared parser
  projections and `hls live|tui --shared-feed` consume committed accepted frames,
  rebuilding bounded feature windows on restart. Float conversion stays in the
  feature/display module. Development path dependencies still need immutable Git
  pins once the cryptofeed commit is pushed. Legacy recording readers remain.
- Pilot 01 failed closed on expanded native L2 acknowledgement defaults. Added
  regression and normalized only equivalent defaults. Pilot 02 then exited 0:
  45s, spot @107/PURR and perps BTC/ETH, 218 exact trade rows, no differences.
- Pilot 03 ran collector and hlscreen together on five markets, adding xyz:TSLA.
  Both exited 0 and hlscreen consumed the same sealed cursor. 262 native/canonical
  trade rows matched, zero original text frames lacking acceptance. hlscreen
  projected 1343 events:262 trades,782 BBO,40 books,55 candles,204 contexts. Its
  gap count 0 and seal=true. The independent Python audit passes framing,
  session/venue/ordinal/receipt/ID/side/time/price/quantity checks; exchange
  continuity remains unverified. Before/after recentTrades references each
  contain 10 rows per market, so they cannot certify the whole window.
- Pilot 03 readiness stayed 503 because snapshot books were not registered with
  engine health. Fixed by maintaining current session snapshots, emitting
  BookResynchronized and disconnect invalidation, and adding query tests. A new
  live health run is still required. Keep this failed readiness evidence.
- A global arbitrary_precision JSON feature initially changed unrelated
  Bitfinex scale handling and broke hlscreen alert decoding. Removed it entirely;
  exact decoding is now isolated using RawValue, including the feed record
  variant boundary. Signed i128 extrema roundtrip and both regressions pass.
- Initial complete hlscreen workspace rerun:538 passed,0 failed. Cryptofeed
  all-target/all-feature suite and Clippy are in progress (see ignored logs).
- Evidence: `.local/evidence/hyperliquid-integration-review/`; captures are local
  development evidence and have NOT been qualified or published as datasets.
  Pilot scripts are bounded to 45s and 64MiB shared data, public endpoints only.

Next: finish current test/lint/security gates, repeat health and shared replay
parity on the corrected adapter, pin/deliver both repos, implement publication/
backup/recovery and resolve the remaining prior-audit finding ledger. Scheduled
canary/soak durations and all unrun checks must stay explicit non-passing gates.

- Pilot04:305 native/canonical trade rows match, no unaccepted text frames;
  hlscreen live and restart replay both produce the identical sealed cursor and
  event counts (305 trades,838 BBO,45 books,69 candles,205 contexts). Readiness
  polling still returned503 in that busy run. Follow-up probes retained full
  metrics and confirmed /ready200 with two live sessions and five valid books
  after initial discovery. Code inspection plus an immediate-ready burst test
  exposed potential parent-future starvation; the engine now explicitly yields
  after consuming each frame. A post-fix live run remains required.
- Cryptofeed all-target/all-feature suite checkpoint:1144 passed,0 failed,
  56 ignored. Initial Clippy all-target/all-feature pass. Additional focused
  tests now cover connection generations, original raw ordinal assignment,
  poisoned archive acceptance and scheduler fairness. Later edits need final
  full gates again.
- rustls upgraded to0.23.46 and webpki0.103.15 in both lockfiles; cryptofeed
  cargo-deny now passes advisories/bans/licenses/sources. hlscreen audit also
  exposed Wasmtime vulnerabilities, upgraded within its existing major line
  to36.0.18; post-update tests/advisories remain due. Parquet59.1 depends on
  unmaintained paste (RUSTSEC-2024-0436); replacement investigation remains open.
- Installed checksum-verified Gitleaks8.30.1 locally. The source scan passes.
  Generated local public token metadata caused false positives; generated
  .local/.tools are excluded from the source scan, while the OSS guard now
  explicitly refuses either directory if tracked. No source secret rule was
  disabled. OSS readiness passes with the scanner actually present.


## Independent review corrections (2026-10-19)

Three separate reviewers examined source semantics, engine/storage boundaries,
and the hlscreen consumer at cryptofeed revision e52880c. Their findings are
acceptance gates, not cosmetic issues. Retained probes proved that two canonical
batches could be published before a later dispatch failure left the frame
unaccepted. Outputs now stage under a bounded transaction and publish with
FrameAccepted under one synced watermark; original bytes remain a separate
prior durable record. Failed actions expose no partial canonical transaction.
Added a failing-late-action regression and multi-record publication/cap tests.

Normal transport loss now journals connection boundaries; shared-only dynamic
subscription mutations, engine-owned clock/drop/system/status/catalog output
also enter the journal. Outbound records describe queued send attempts, not
confirmed writes; monotonic timestamps are zero where unavailable, rather than
mislabeling epoch wall time. Timer batches still need explicit causal references
before strict mixed-input replay qualification.

Raw recording validates maximum record size before enqueue, retains records
until successful write, poisons partial-I/O/close failures and bounds queue bytes.
Ingress and the single consumer each have a configurable byte budget (default
128MiB), plus at most one 16MiB temporary frame copy; repeated/concurrent flushes
cannot accumulate unbounded pending retries. Synchronous filesystem stalls are
not bounded by a hard realtime deadline and remain a performance gate.

Initial trade deliveries now carry explicit generation-local initial-delivery
and unverified-origin flags. Canonical quotes emit invalidation if a side
vanishes. Nonuniform executable ticks are represented as unsupported (zero
constant increment); decimal precision remains available. Readiness requires every subscription acknowledgement and initial/fresh
recurring book/context delivery. Change-driven trades/BBO/candles may be silent;
channel silence never proves trade continuity. Quiet trade continuity
remains unknown. The independent Python audit now checks native coin against
session instrument metadata and reports initial-delivery counts; a swapped
instrument regression verifies this check.

Pilot05 (source e52880c, prior to these review corrections): collector/hlscreen
exit0; /ready503 during startup, then200 on eight polls.280 native/canonical
trade deliveries match exactly, including150 initial-history deliveries and130
other deliveries; no text frame lacks acceptance. Bounded before/after reference
intersections agree on tokens, never whole-window completeness. This confirms
the fairness/readiness repair on that revision, not the later health changes.

Both hlscreen Git dependencies and all marketfeed lock entries are pinned to
one immutable cryptofeed revision; no sibling path dependency remains. Pinned
hlscreen workspace538 tests and Clippy passed. Wasmtime36.0.18, Parquet60.0.0 and
TLS upgrades pass advisory checks. New review fixes require another final run.
The consumer reviewer also proved event/cursor parity was insufficient: frozen
replay used invocation wall time and aged feature windows out. Shared replay
now uses the recorded archive clock for --once/sealed captures; live tails
retain wall-time staleness. Feature-clock regression verification is pending.

New captures embed the public canonical parsed configuration (comments omitted)
and build Cargo.lock. Catalog responses retain exact request bytes, URL/method,
headers and response receipt time. Old captures do not retroactively acquire
missing provenance. No capture is qualified; publication/recovery/restore and
remaining venue failures are still open work under the active goal.

## Dataset and consumer review checkpoint (goal remains active)

The offline `marketfeed-datasets` crate (Rust1.88; collector/core remain1.85)
implements bounded sealed Hyperliquid trade export. Original MFR1 bytes and
metadata are reconciled with accepted MFSF1 inputs, exact canonical rows and
connection generations. Publication uses immutable content hashes, parent CAS,
synced staged intents and atomic HEAD. Backup/restore copies and verifies pinned
ancestry plus every referenced object. Inspector validates hashes, schemas,
financial columns, original source rows and runtime provenance. Strict profiles
remain empty: no dataset is qualified by these integrity checks. See
`crates/datasets/README.md` for limits and reproducible commands.

Independent review reproduced partial-final-intent recovery failure, hidden
catalog backup dependencies, forged price, forged runtime generation and forged
effective catalog metadata. Regression tests now cover these cases. Footer
collection preflight precedes Parquet metadata allocation; nested dictionary/data
counts, page sizes, encodings and decoded row budgets are bounded. Remaining
allocation review is pending. Two-way PyArrow cross-reader checks preserve all
signed i128 coefficients, uint8 scales and signed nanosecond extrema. Parquet60
removes the unmaintained paste dependency. Cargo-deny passes with a narrowly
reviewed MIT-0 permissive license addition and versioned duplicate exceptions.

Pilot06 (source99516c):295 trade deliveries matched original native rows,
210 initial deliveries and85 subsequent deliveries; no unaccepted input. Collector
and hlscreen exited0. All /ready probes returned503: the new health rule required
an inactive PURR candle to deliver, triggering two spot reconnects. Corrected
health distinguishes recurring book/context liveness from change-driven channel
silence. Preserve this failed evidence; a corrected live pilot is still required.
The consumer reviewer proved full final feature screens, summary, cursor and
counts identical between live and saved replay on pilot06. Archive time is a
monotonic high-water across sessions. API shared mode was added and independently
checked with an invalid WS URL: no exchange connection, saved feature values and
cursor preserved, wall-time health correctly stale for an old archive.

A hlscreen PTY cleanup test failed once under integration load. The prior helper
ignored final wait/signal results and emitted stubborn-child readiness before
its traps were installed. The harness now reports fallible cleanup, signal and
remaining-process evidence, shares a bounded deadline and closes PTY handles
before final escalation. Focused and full PTY tests pass; the historical process
failure's precise cause remains unproven and its log is retained. Further review
and the final full hlscreen suite remain due.

Current passing checkpoints before the latest review fixes:1170 workspace tests
with56 intentionally ignored; pinned hlscreen540 tests; collector MSRV436 library
tests; dataset MSRV; full Clippy; dependency and source scans. Latest edits need
final gates and immutable pins again. The original venue decoder/source-origin
failures, full causal replay, actual external backtest readers and sustained
operational durations remain open. No claim of perfection or production
qualification follows from these development pilots.

## Pinned pilot07 and prior numeric repair checkpoint

Shared collector62a8fef and hlscreen8367b0b:270 native/canonical deliveries
(150 initial,120 other), zero mismatches/unaccepted frames. Collector, terminal
consumer and shared API exited0; /ready reached200 after startup. Final live and
saved output (summary plus feature screen,3354bytes) is byte-identical at
cursor2683/1983487, feed fadae1df2c473d021fa8b06b0947ed664507e04d,
as-of1792440727739ms;1055 projected events and zero reported gaps.
Journal SHA2569207b56e6cb1544f0d9d313b17824afb508a6d44d14c35ca740a7b9184a1a45a.

Manifest729b0b99d6c47767d3550f753e7440ce40d929643d644fad6bd7113b7a68ed4f
retains146 event-time rows, region unknown. Independent JSON Schema/PyArrow,
CLI inspect, fresh backup/restore and corruption rejection pass; five strict
profiles are denied. Decision remains validated-not-qualified. Development
artifacts live only in ignored evidence; old failed/partial artifacts are intact.

Final checkpoint gates: cryptofeed1175 passed/56 ignored; hlscreen542 passed;
core Rust1.85 library437 passed; dataset Rust1.88 thirteen tests passed. Clippy,
source scan and cryptofeed dependency gates pass. Hlscreen dependency gate needed
an explicit shared repository allowlist plus required immutable rev, now passing;
its generated licenses/notices were refreshed. The old binary soak report cannot
qualify the changed hlscreen binary: fresh standalone15min and shared collector+
terminal+API15min runs are in progress. Neither is an operational pass yet.
Draft delivery: cryptofeed PR50, hlscreen PR21. Old-venue fixes use a stacked
`codex/exchange-integrity-repairs` branch so the measured shared revision remains
pinned and reviewable.

The prior numeric JSON failures are repaired in Kraken spot/futures, Deribit
messages/catalogs and Bitfinex WS/REST financial inputs. Shared RawValue decoding
keeps decimal/exponent lexemes, integer IDs, bounded structure/work and duplicate
key rejection. Financial SIMD entrypoints intentionally share this lexical path;
fixture parity alone is not an independent numerical oracle. Explicit expected
38-digit, i128, scale/exponent, ID and malformed-value fixtures provide that
boundary check. Existing raw corpora are unchanged. One Bitfinex test expectation
now preserves native29001.00 scale, rather than its previous float-reformatted
29001.0 scale.

Review discovered and closed an obsolete Kraken Futures scientific fallback
that could allocate gigabytes or turn0.1e-255 into a value ten times larger.
Fixed::parse_str now rejects it directly. Bitfinex signed-absolute i128 overflow,
u64 timestamp wrapping and trade ms-to-ns overflow checks were repaired. Native
fractional liquidation milliseconds are converted exactly to ns. Deribit trade
ms bounds and lexical metadata were also repaired. Focused all-feature119 tests
pass (13 deliberately ignored); independent exact-number fixtures9/9 pass.
Non-trade timestamp saturation, source-origin semantics, OKX multirow/finality,
Binance classification/ranges and full causal replay remain open work.


## Additive native semantics and OKX admission repair

On stacked PR51, Trade now has optional native provenance: source channel,
individual/aggregate/unverified granularity, optional native constituent IDs,
optional native count and source attributes. A count is never inferred from a
range. Candle now preserves optional native confirmation, quote volume and
product-dependent volCcy. Absence remains unknown; explicit false remains false.
Serde/normalized JSON/protobuf use additive optional fields; existing events omit
absent fields. Every adapter constructor was migrated with unknown defaults.
Generated Google protobuf classes independently read four synthetic Rust binary
and normalized JSON records, preserving i128 maxima, scale255, uint64 maxima,
false/absent distinction and optional empty IDs/count0. This tests the wire
boundary, not a native exchange observation.

OKX validates every trade/candle row before producing output, preserving up to
4096 rows in one event batch with stable frame/event ordinals. Malformed late rows
fail atomically. Native trade IDs no longer impersonate source sequences. Trades
retains aggregate provenance and native count/source/seqId; explicit trades_all
(or trades-all) selects the individual trades-all business channel. Other venues
reject that configuration. Public/business sessions send only their actual
requested native arguments. Candle confirmation and both native volume variants
are retained exactly. Public/business/private WS use443; the announced8443
retirement was checked against the official notice.

Readiness now waits for the exact channel+instId/instType ACK set and every
requested book snapshot. Duplicate and unsubscribe responses cannot advance it;
malformed/wrong-scope ACKs cannot admit a session. The20s startup deadline stays
armed until admission and reconnects on missing ACKs or initial book snapshots.
Independent review found the first version canceled its timer too early after
ACKs; that defect is fixed and a missing-second-book timeout regression passes.
This does not establish trade continuity or sustained OKX operational readiness.

Current broad gates before this final ACK edit: full workspace all-targets/
all-features tests passed; full workspace Clippy passed. Final ACK-specific tests
and independent review pass; final repeated gates are recorded with the delivery
checkpoint. Source-origin semantics, Binance classification/ranges, non-trade
timestamp saturation, actual external backtest readers and mixed causal replay
remain open under the active goal.


## Bounded soak08, storage and release-gate checkpoint

Shared collector62a8fef captured2807 exact native/canonical deliveries (150
initial deliveries) across spot@107/PURR and perpsBTC/ETH/xyz:TSLA. No unaccepted
text frames, trade differences or source parse/drop/gap errors were found by the
independent Python audit. Readiness was503 on the startup poll and200 on all29
subsequent polls. Sampled peak RSS: collector31,244,288bytes; terminal19,808,256;
API21,528,576. Live and saved summary plus final screen match byte-for-byte
(3400bytes). The collector binary was copied and hashed before use. The hlscreen
process binary hash was not captured at launch; later builds prevent reliable
retrospective attribution. Source runtime is unchanged from8367b0b.

Collector and terminal exited0. API was SIGKILLed by the harness: its15s wait
started after sealing, while API had a separate later duration deadline. This
combined soak is not a clean-shutdown pass. A preserved distinct12s collector /
18s API probe with30s exit allowance exited0 for all processes; API shared mode
also exited0 after a separately measured2.018s replay run. The original failure
is retained; the short corrected probe does not replace a15min operational gate.

Exact export retained2666 event-time rows in manifest
`a146175ce1fa387e6cfe301626d28798d8136542ad723e9ccbc61dc732f68d39`.
JSON Schema and independent PyArrow checks, CLI inspection, fresh backup and
restore, corruption rejection and five denied strict admission profiles pass.
Decision is validated-not-qualified, region unknown. Tracked compact receipts
and the immutable manifest are in `docs/evidence/hyperliquid-integration-2026-10-19`;
bulk original captures and restored stores remain in ignored local evidence.
No failed or frozen original corpus was modified. Recent source references have
only10rows per instrument and cannot establish whole-window continuity.

Hlscreen's separate existing supervisor soak passed after902s with334symbols,
292159 projected events, zero reported parser drops/gaps and zero replay drift;
RSS peaked48,185,344bytes. That fallback plan used all-mids and asset contexts,
so it provides no sustained trade-tape coverage. Fresh report is committed in
hlscreen. Its canonical `scripts/check.sh pr` gate passed, including565 test
executions, Clippy, rustdoc, packaging/public scans, matching soak runtime hash
and local release artifact smoke. Immutable shared pin and dependency-source
checks pass. Hlscreen PR21 is pushed throughd5a26dd. No calendar-spaced canaries,
24h or7d operational soak was run, and the active goal remains unfinished.


## OKX production probe and readiness configuration correction

At source6478a65, a30s global-endpoint public/business probe selected BTC-USDT,
BTC-USDT-SWAP and catalog-resolved BTC-USD-261030. All6 socket ACK ledgers
completed,3 books synchronized and the daemon sealed/drained with exit0. The
independent audit compared139 native trade rows and38 native candle rows against
canonical exact values, IDs, side/time, channel granularity, native count/source/
seqId and candle finality/volumes: zero mismatches, zero unaccepted text frames,
zero parse/drop/gap/unknown source events. Spot/swap trades and trades-all were
observed; dated-futures trades were not observed and remain unverified live.

All readiness polls returned503 because the pilot incorrectly requested
min_live_sessions6 for3 venue groups. The legacy option counts venue groups;
each is live only after every planned child session is live. This was a pilot
configuration failure. Validation now rejects thresholds above venue count and
ADR0009 documents the semantics. A corrected separate probe is pending. The
independent oracle has mutation challenges for count/granularity/side/time/
price, duplicate or missing canonical ordinals, missing ACKs and fake sequences.
All36 Python audit tests pass. Independent review found two oracle omissions:
candle interval/envelope-time/sequence and incomplete earlier ACK generations.
Both now have comparisons and adversarial regressions; the frozen pilot still
passes all compared fields. Tracked receipts preserve the failed pilot.

Final pre-followup broad gate:1192 passed,56 intentionally ignored; full Clippy,
rustdoc, dependency advisories/bans/licenses/sources, OSS scan and Rust1.85
relevant library110 tests pass. The later readiness configuration followup has
its own daemon tests; it does not invalidate the prior compiled runtime probe.


Corrected OKX probe02 (source58f2683) exited0 and readiness reached200 after
startup (13 ready polls). All6 ACK ledgers completed and all3 required books
synchronized.422 native/canonical trades and21 candles matched exactly; no
unaccepted text frames, parse/unknown/gap/drop events or compared differences.
Spot and swap trades/trades-all were observed. Dated-futures trade channels again
had no rows, so their live trade delivery is not verified by this sample. The
prior503 configuration failure remains intact. Native granularity/count/source,
interval/finality/volumes and no fabricated candle sequences were checked by the
independently reviewed oracle. This is a bounded observation, not continuity or
operational promotion. Receipt and journal hash accompany pilot01 evidence.


## Accepted cross-venue observation extension

The2026-10-19 user follow-up expands this active goal with time-qualified
cross-venue observations for existing OKX spot entry research. The full accepted
completion contract is `docs/plan/cross_venue_observation_implementation.md` and
the machine-readable scope is `docs/plan/active_goal_scope.json`. Existing feed,
storage, integrity and operational work remains active. New scope covers data
feasibility, causal context/feature contracts, source alias/quote/unit semantics,
small qualified observation sets, owner bridge and separately gated model/ETF
hypotheses. No economic, model or deployment result is implied. Integrity repairs
remain the immediate prerequisite; Binance repair is committed at8fb517d, followed by the additive rsibot-owned
manifest observation boundary.


Placement clarification: the user asked that strategy context live in rsibot.
Quants Lab will own cross-venue feature combination and model/ETF research;
Hummingbot retains admitted decisions, order/inventory/exit ownership; Research
OS retains hypothesis/evidence truth. cryptofeed remains the data authority and
hlscreen its view. Existing owner implementations are further along than old
SPECIFIED_NOT_IMPLEMENTED banners: inspect the active ExecPlan and exact source
commits, not stale nested checkouts. Existing root rsibot dirty policy/docs/
gitlinks are unrelated and must be preserved. Continuous flow is a distinct
contract and must not masquerade as a listing/OTHER announcement event.


## Binance trade semantics repair checkpoint

Repair8fb517d retains source trade vs aggregate granularity and
namespaced aggregate IDs, f/l bounds, nq, st, X and E/T. No execution count is
inferred from ranges. Source trade IDs no longer impersonate sequence continuity.
Spot native trades use documented individual granularity; legacy futures trade
remains unverified because the current official futures contract only documents
aggregates. Product-type mismatches and trade timestamp overflow fail decoding.

Non-MARKET native X is a conservative tape exclusion policy. It emits an exact
native-attribute system record with engine-owned venue/session/connection/original
frame scope and exclusion counters, rather than silently Ignored. Unclassified
nonpositive prices/quantities fail. Routed-v4 exclusions require the correct
aggregate channel, symbol, role and valid E/T. Independent review found that the
first version rejected classified routed inputs before the exclusion handler;
that route is fixed and direct role/symbol/channel regressions pass.

Five explicit expected native fixtures/adversarial cases pass; six shared-engine
tests include committed exclusion/raw-frame linkage and counters. The focused
Binance/engine/daemon/v2 suite passes372 tests with7 deliberate ignores. Frozen
v2 serialization has an explicit validated compatibility projection from rich
canonical aggregate provenance into its historical aggregate-ID cursor encoding;
original MFR1 and frozen v2 bytes remain untouched. Independent20-case v2 review
and exact synthetic frozen-oracle tests pass. Rust decoded enum constructors now
require native provenance: this is an alpha source-API migration, documented in
the adapter README; it is not claimed source-compatible. Clippy and Rust1.85
relevant47 library tests pass. Full workspace all-targets/all-features testing passed1200 cases with56
deliberately ignored tests. The later read-only dataset-consumer followup has
separate package, real-boundary and review gates.

The first rsibot-owned slice is an isolated Quants Lab exact manifest/admission
boundary at origin/main72c71564 on feat/andrzej_cryptofeed_context_20261019.
It invokes the pinned trusted Rust dataset validator, rather than duplicating
its security/financial checks. It does not load controllers or run economics.
Existing data_sources eager imports require an unavailable Hummingbot package,
so an opt-in pure market_observations package preserves that legacy initializer.
The completed slice and its measured validation limits are recorded in the
following checkpoint.


## rsibot consumer boundary and read-only dataset checkpoint

The first opt-in Quants Lab slice validates exact manifest identity through a
SHA256-pinned Rust executable and distinguishes integrity inspection from strict
profile admission. It uses no exchange client, Hummingbot import, financial float
conversion, controller, model or economic run. Nine Python tests plus six
subprocess subcases pass; Black/isort and compile checks pass. Independent review
closed FIFO blocking, scalar identity, read-only-store and receipt-provenance
findings. A prior FIFO test pathname-transition failure remains in local logs
with its OS cause unproven; independent fresh fixtures preserve bounded rejection
checks and pass both verification runs.

Rust Store::open_read_only now requires existing regular store directories and
never creates files/locks, syncs, recovers or acquires the writer lock. CLI inspect,
admit and backup/restore sources use this path; mutation methods reject a reader.
Fifteen dataset tests pass, including reads under an existing writer lock and
missing/incomplete-store CLI failure without resource creation. Clippy and
Rust1.88 package checks passed.

A separate read-only copy of the existing saved capture passed real consumer
inspection (2666 rows), all five strict profiles denied, unchanged complete store
byte/path inventory, denied write probe and no writer.lock creation. Exact
manifest, executable, source-parent and patch hashes are retained in
docs/evidence/quants-manifest-boundary-2026-10-19/readonly-boundary-smoke.json.
The original capture and frozen research corpus were untouched. This is
Hyperliquid integrity evidence only; OKX trade-reader compatibility, continuous
context/features, models, economics and calendar canaries remain pending.
Quants work stays local in its isolated owner branch, without root gitlink changes.

The Quants CLI also passed the real read-only-copy boundary: default30s inspector
deadline, exit0 with2666 verified rows and exit2 for signed-execution-flow refusal;
complete store byte/path inventory unchanged and no writer.lock. CLI receipts
with canonical local paths stay in ignored task evidence.

Quants boundary is committed locally at97e034c on its isolated owner branch.
No Quants push/PR, root pointer update or publication was performed.


## Next integrity slice: Bybit native trade/candle semantics (in progress)

Current source f441d5f discards native BT/RPI/L/publication provenance, exposes
repeatable cross-sequence as generic trade sequence, retains only first candle
row, drops confirmation/turnover and uses last-match time as candle publication
time. Bounded atomic row validation, native provenance and honest product volume
semantics are being implemented with independent native expectations and review.
L2 update-ID consecutiveness and liquidation bankruptcy-price provenance are
separately under audit; no new live or qualified-data result is claimed yet.


Bybit trade/candle implementation checkpoint: independent reviewer ran47 offline
Bybit tests with4 live/regeneration ignores; root focused Bybit/recording/sinks/
event-pulse suite passed368 with4 ignores. Rust1.85 relevant104 library tests,
workspace Clippy, rustdoc and generated-Google-protobuf4-fixture checks pass.
Full workspace run is still in progress and is not included in these counts.

Native trade provenance preserves source channel, exact native IDs/financials,
T/ts/seq/BT/RPI/L and original side. Every documented batch (max1024) is validated
before one emitted batch; repeated seq is native provenance, not continuity.
Unknown side stays Unknown, absent flags stay absent. Candles validate all rows
(local cap4096) and retain explicit finality and native timing/turnover via new
optional CandleProvenance, propagated to serde/normalized JSON/protobuf11. Frame
ts is system generation; last matched order time remains separate. Native WS
turnover is never promoted to qualified quote volume by REST unit analogy.

Independent review found false-green paths in the first Bybit oracle: orphan
acceptance, bool/int/float aliasing, duplicate JSON keys, candle history flags,
dual enum payloads and unbounded whole-file hashing. These are fixed and covered
by mutations plus a real-framing sparse-tail regression. The new Bybit audit
reports a pinned committed cursor and streaming prefix hash within128MiB;
watermark changes fail.45 Python audit tests pass and independent review closed
all findings for the new tool. Existing older HL/OKX report whole-file digest
behavior is separate; their earlier finite sealed-capture evidence is preserved.

The next live pilot has an explicit three-symbol30s/45s-wall-cap acquisition plan
at docs/evidence/bybit-native-2026-10-19/acquisition-plan.json. No live repair
evidence is claimed yet. Bybit L2 uses an unsupported +1 assumption and silent
conflicting-repeat discard; this separate P1 finding is recorded in its README.
Metadata discovery also currently requests only the first1000 derivative rows
and does not follow nextPageCursor; whole-catalog coverage cannot be claimed.
Liquidation bankruptcy-price/position metadata and channel-specific readiness
remain open. No original/frozen corpus, strategy owner or rsibot pointer changed.
