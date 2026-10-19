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
3. PENDING: exact persistent datasets and manifest admission, transactional
   publication, streaming replay, backup/restore and bounded data acquisition.
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
