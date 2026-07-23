# Changelog

All notable changes to this workspace are documented here.
This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
style notes. Version tags are **not** a 1.0 claim — see the honesty bar in
[`docs/plan/production_drive.md`](docs/plan/production_drive.md).

## [Unreleased]

### Added

- **W7-P0a Bitfinex-deriv liquidations (VenueId 20):** public WS `status` /
  `liq:global` → `MarketEvent::Liquidation` (filter subscribed symbols; exact
  `Fixed` fixtures; `Capability::Liquidations`). Still **alpha** only.

- **Bitstamp/Gemini Stats24h REST timers (VenueIds 14/15):** SessionMachine
  `RequestHttp` + `STATS_TIMER_ID` poll — Bitstamp `GET /ticker/{pair}/`
  (open_24/high/low/last/volume); Gemini `GET /v2/ticker/{symbol}` (OHLC) +
  `GET /v1/pubticker/{symbol}` (volume). Capability + fixed fixtures. **alpha** only.

- **Bitfinex-deriv VenueId 20 peer-parity:** daemon `catalog --live` scripted
  (`pub:list:pair:futures`) + stub → `session_config_from_catalog`; R6 status
  coverage docs for **20**; L2 corpus `deriv_l2_book.mfr`; laptop
  `INCLUDE_ALPHA` runs `live_bitfinex_deriv_trade_or_mark`. Still **alpha** only.

- **W6-P1b Bitfinex derivatives (VenueId 20):** `bitfinex-deriv` alpha SessionMachine (WS T/Q/L2/candles/Stats24h + REST `status/deriv` mark/index/funding/OI); liq **N/A**; daemon `segment=deriv`. Not beta.

- **W6-P0d Gemini `catalog --live`:** `GET /v1/symbols` → instrument defs with
  default scales (2/8); optional capped N+1 `/v1/symbols/details/{symbol}` via
  `GEMINI_LIVE_DETAILS_MAX` (default `0`, no unbounded fan-out). Scripted daemon
  fixtures. Maturity **alpha** only.

- **Coinbase Advanced Trade public remainder (VenueId 18 / W6):** Adv L2 corpus
  (`adv_l2_book.mfr`); public WS `status` → `InstrumentUpdate`; WS `candles`
  decode (5m; no subscribe — REST preferred); `live_ignored` L2 + canary T/Q/L2/candle;
  daemon `catalog --live` scripted CLI parity. Still **alpha** only.

- **W2-R10 Coinbase International (VenueId 19) SKIP:** claim `coinbase-intl` in
  [`venue_ids.md`](docs/plan/venue_ids.md); no SessionMachine/daemon/fixtures.
  INTX MD WS (`wss://ws-md.international.coinbase.com`) requires HMAC subscribe
  (`TIMESTAMP + KEY + "CBINTLMD" + PASSPHRASE`); REST instruments/quote alone is
  not continuous public T/Q/L2. Re-open only with authenticated-MD scope or a
  future public feed. Classic **16** + Adv **18** unchanged.

- **Coinbase International auth MD (VenueId 19):** `COINBASE_INTL_VENUE_ID`,
  authenticated SessionMachine (`MATCH` / `LEVEL1` / `LEVEL2`), env-only credentials,
  offline fixtures, daemon `segment = "intl"`, public REST instruments catalog. Alpha only.

- **Coinbase Advanced Trade public T/Q/L2 (VenueId 18 / W5-P0c):** SessionMachine
  for `market_trades` / `ticker` / `level2` (wire `l2_data`) with exact `Fixed`;
  one subscribe message per channel; REST candles kept; offline fixtures + optional
  `live_ignored` T/Q smoke. Classic Exchange VenueId **16** remains. Maturity
  **alpha** only.

- **W5-P1d private live expand:** richer `marketfeed-private` `#[ignore]` smokes
  (extended idle + reauth/bootstrap probes), `scripts/laptop_private_canary.sh`
  (SKIP when keys missing; archives `docs/ops/private_canary_evidence/`), docs in
  private README / `.env.example`. **No order placement.** Not a maturity gate.

- **Catalog `--live` expand (W5-P0b):** Bitstamp (`trading-pairs-info`) + Bitfinex
  (`conf/pub:list:pair:exchange`) + Coinbase-adv public `/market/products`
  `parse_instruments` with scripted HTTP fixtures. **Gemini N/A** (no clean bulk
  instruments REST: `/v1/symbols` strings only; `/v1/symbols/details` 404; per-symbol
  detail is N+1). Laptop canary `INCLUDE_ALPHA` adds VenueId **17** Bitfinex +
  **18** Coinbase-adv (`live_ignored` smokes).

- **MFNE-JSON1** (§18.5 / W4-P0a): `NormalizedEventWriter` default = newline-delimited JSON
  aligned with `proto/marketfeed/v1` / MFPE-JSON1 body schema; shared
  `event_envelope_json`; `read_normalized_jsonl` tempfile round-trip tests.
  Legacy `DebugJsonl` retained. MFR1 unchanged (ADR-0008).

- **§21.4 config hot reload (partial):** Unix `SIGHUP` re-validates TOML; applies
  `telemetry.log_level` + `[readiness]` in-process; logs
  `config reload: restart required` for venues/sinks/recording/runtime/bind/etc.
  Ceiling documented in [`docs/plan/orchestrator_wave4.md`](docs/plan/orchestrator_wave4.md)
  (no shared `EngineControl` for subscription apply). **§23.3 OTel skipped** (same plan).
- **Facade crate `marketfeed` (R28 / §19 / §7.1):** workspace member at
  `crates/facade` re-exports embed surface (`Fixed`, market/system events,
  `EngineControl` / `EngineSupervisor`, `SessionMachine` / `VenueFactory`,
  subscription types, common `sinks::*`) without exposing every internal crate.
  See [`crates/facade/README.md`](crates/facade/README.md).

- **Coinbase Exchange spot (`VenueId(16)` / `coinbase-spot`):** public
  SessionMachine for `matches` / `ticker` / `level2` (exact `Fixed`); offline
  fixtures + L2 sync; daemon `adapter = "coinbase"` wire. Maturity **alpha**
  only (no candles — Advanced Trade is a separate protocol).
- **Private OKX/Bybit user-data (C6c expand):** env credentials (`OKX_*` /
  `BYBIT_*`, redacted `Debug`), HMAC login/auth payloads, live runners that
  flush `SendText` and null-drain `AccountEventSink`; daemon optional
  `[private.okx_spot]` / `[private.bybit_spot]` enable-only TOML; `#[ignore]`
  live tests; docs + `.env.example`. No order placement / no TOML secrets.
- **C4c+:** real optional Kafka/NATS TCP `EventSink` producers behind features
  `kafka` / `nats` (Produce v0 MessageSet + NATS `INFO`/`CONNECT`/`PUB`; no
  `rdkafka`/`async-nats`); bounded ingress + overflow policy; daemon
  `[[sinks]] type = "kafka"|"nats"` (requires matching daemon features);
  loopback mock tests; external broker = operator-only. See
  [`crates/sinks/README.md`](crates/sinks/README.md).
- **CODE plateau:** daemon `[[sinks]] type = "udp"` (`address = "host:port"`) wires
  existing `UdpSink`; CI job matrix `cargo test -p <adapter> --features simd-json`
  for Binance/OKX/Bybit/Kraken/Deribit; Binance `benches/parse_fixtures` Instant
  harness (serde ± simd; evidence only, no SLO / enablement claim). See
  [`docs/plan/production_drive.md`](docs/plan/production_drive.md) §0.1 and
  [`docs/ops/latency_runtime.md`](docs/ops/latency_runtime.md). C10 remains
  **PARTIAL** until operator profiles. prost-codegen = YAGNI (MFPE-PB1 ships).
- **C10 venues:** optional `simd-json` on Bybit (linear/spot/inverse shared
  `decode_text`), Kraken Spot, and Deribit public JSON `decode_text` (same
  per-crate `json` helper pattern); serde default; serde↔simd unit + fixture
  parity; no latency profiles / bench gate / insecure TLS — C10 stays
  **PARTIAL** ([`docs/ops/latency_runtime.md`](docs/ops/latency_runtime.md)).
- **C5d:** `ProtobufBinaryFileSink` — length-prefixed protobuf3 (`MFPE-PB1`) via
  hand wire encoder matching `proto/marketfeed/v1` tags (no prost); daemon
  `type = "protobuf-file-bin"`; `protobuf-file` / MFPE-JSON1 unchanged. Framing in
  [`crates/sinks/README.md`](crates/sinks/README.md). Prost codegen remains a
  future upgrade.
- **C10 remainder:** optional `simd-json` on Binance USD-M / Coin-M and OKX public
  `decode_text` (same shared `json` helper pattern as Spot); serde default;
  serde↔simd unit + recorded-fixture parity (`tests/decode_simd_parity.rs`);
  no latency profiles / bench gate / insecure TLS — C10 stays **PARTIAL**
  ([`docs/ops/latency_runtime.md`](docs/ops/latency_runtime.md)).
- **C6c (daemon):** optional `[private.binance_spot] enabled` in daemon TOML; credentials
  only from `BINANCE_API_KEY` / `BINANCE_API_SECRET` (TOML secrets rejected; redacted
  `Debug`); spawns `marketfeed-private` live helpers over engine transports with
  null-drain account sink; docs in `crates/daemon/README.md` + `.env.example`.
- **C5c:** `ProtobufFileSink` — length-prefixed JSON (`MFPE-JSON1`) using
  `proto/marketfeed/v1` field names (no prost); daemon `type = "protobuf-file"`;
  framing documented in [`crates/sinks/README.md`](crates/sinks/README.md).
  Connection→core shards: documented skip — session = bounded dispatch lane
  ([`docs/ops/latency_runtime.md`](docs/ops/latency_runtime.md)).
- **C6c (library):** Binance Spot private live auth scaffold — `BinanceApiCredentials`
  from `BINANCE_API_KEY`/`BINANCE_API_SECRET` (redacted `Debug`), feature `live`
  drives `PrivateSessionMachine` listenKey REST + user-stream WS via engine
  transports (`run_binance_spot_user_data_live_until` drains `AccountEventSink`);
  `#[ignore]` `live_ignored` test; `.env.example`. No order placement, secrets
  never logged/committed.
- **C9 CI matrix:** `windows-latest` + `ubuntu-24.04-arm` on the test job; `docs`
  job (`cargo doc --workspace --no-deps` with `RUSTDOCFLAGS=-D warnings`). YAML
  ready when Actions billing is unlocked.
- `marketfeed-ffi`: minimal C ABI (`marketfeed_version`, `marketfeed_fixed_parse` /
  `_cstr`) + hand-written `include/marketfeed.h` (no cbindgen; no full API).
- `marketfeed-sinks`: `UdpSink` bounded best-effort datagram `EventSink` with
  localhost UDP tests.
- **C10 lite:** optional `simd-json` feature on `marketfeed-adapter-binance` for
  Spot `decode_text` via shared `json` helpers; serde remains default; serde↔simd
  canonical event parity tests; enable criteria + ponytail in
  [`docs/ops/latency_runtime.md`](docs/ops/latency_runtime.md).
- Protobuf schema stub: `proto/marketfeed/v1/market_event.proto` + `proto/README.md`
  (MarketEvent / EventEnvelope core types; generation optional/ponytail — no prost).
- Linux latency affinity: `pin_worker_to_core(Some(n))` uses `sched_setaffinity`
  behind `cfg(linux)` (safe `Err` fallback); no-op elsewhere
  ([`docs/ops/latency_runtime.md`](docs/ops/latency_runtime.md)).
- **C9 lite (fuzz):** `candle_decode` / `coinm_decode` / `private_account` libFuzzer
  targets + Binance Coin-M / private no-panic smokes; exported `decode_coinm_text`.
- Latency runtime profile hooks (`marketfeed-engine::RuntimeProfile` /
  `pin_worker_to_core`; feature `latency-runtime` intent-only). Portable results
  unchanged.
- Kraken Spot `ohlc` + Deribit `chart.trades.*` → `MarketEvent::Candle` (C2f/C2g)
  with fixtures; ponytail: both venues lack closed-bar flags (partial bars ok).
- `marketfeed-sinks`: `KafkaSink` / `NatsSink` — feature-off stubs return
  `Unsupported`; feature-on = minimal TCP Produce/PUB clients (see C4c+).
- **C6b Phase 2:** OKX Spot + Bybit Spot private account fixture state machines
  (`SendText` login/auth only; balances/orders/fills; auth fail → `Reconnect`).
  No live keys, no order placement.
- Binance Coin-M native `@kline_*` → `MarketEvent::Candle` (C2e; reuses Spot
  interval helpers).
- **C6b Phase 1 (alpha scaffold):** `PrivateSessionMachine` + `PrivateActionBuffer`,
  expanded account events (`BalanceDelta`, order status fields, `ListenKeyExpired`),
  and Binance Spot user-data fixture state machine (listenKey `RequestHttp` create/PUT
  keepalive; `outboundAccountPosition` / `balanceUpdate` / `executionReport` → account
  events). No live keys, no order placement. `HttpMethod::{Put,Delete}` for listenKey.
- Bybit linear native `kline.{interval}.{symbol}` → `MarketEvent::Candle` (C2c) with
  fixtures; OKX SWAP `candle*` fixture coverage (C2d; shared session already supported).
- Bybit linear, Kraken Spot, and Deribit L2 book corpora (`linear_l2_book.mfr` /
  `spot_l2_book.mfr` / `perp_l2_book.mfr`) with replay identity tests (C7 done).
- §23.2 fixed-bucket histograms for parse duration, REST latency, and sink write
  latency on daemon `/metrics` (extends existing frame-to-event hist; removes
  parse sample-count stub).
- Binance Coin-M L2 book corpus (`coinm_l2_book.mfr`) with dapi REST depth snapshot
  sidecar and replay identity tests (C7 partial; Binance books complete).
- Binance Spot + USD-M L2 book corpora (`spot_l2_book.mfr` / `usdm_l2_book.mfr`)
  with REST depth snapshot sidecars and replay identity tests (C7 partial).
- Binance Coin-M L2: dapi `/dapi/v1/depth` snapshot + `@depth@100ms` with USD-M-style
  `pu` continuity (buffer/bridge/drain, gap→reconnect); fixtures + buffer-overflow tests.
- `marketfeed-private`: Phase 6 private-account milestone scaffold (traits/event
  stubs only; no auth, signing, or live trading paths).
- `marketfeed-recording::NormalizedEventWriter`: minimal line-delimited Debug
  writer for stamped `EventBatch` / `EventEnvelope` with `max_records` /
  `max_bytes` bounds (ponytail: not protobuf §18.5 yet).
- Daemon wires `binance-coinm` (`VenueKind::BinanceCoinm` / `BinanceCoinmFactory`)
  via `segment = "coinm"` (aliases `inverse`/`dapi`); example config + tests.
- `marketfeed-sinks::FileSink`: bounded append-only Debug line writer
  (not protobuf §18.5 normalized recorder yet); daemon wires `type = "file"`.
- Daemon optional `[[sinks]]` (`type = memory|logging|file`, `capacity`,
  `overflow`, `path` for file): live/synthetic paths `forward_dispatcher` into
  configured sinks, otherwise null-drain. Dispatch is always drained
  (FailEngine-safe). Offline tests: synthetic + `MemorySink` / `FileSink`.
- `marketfeed-sinks`: bounded `EventSink` trait + `MemorySink` / `LoggingSink`
  with `OverflowPolicy` tests. Kafka/NATS stubs ship as `Unsupported` placeholders.
- MFR1 reader unit coverage for truncated-tail crash recovery and schema
  compatibility (bad magic / unsupported version / CRC hard-fail).
- Tag release workflow (`.github/workflows/release.yml`) that builds a CycloneDX
  SBOM via `./scripts/generate-sbom.sh` and uploads it as a workflow artifact.
  May not run while Actions billing blocks jobs; merge anyway for when billing
  works.
- Release provenance runbook (`docs/runbooks/release_provenance.md`) documenting
  cosign / GitHub Artifact Attestations enable path, plus a commented
  non-blocking `attest` stub in `release.yml` (disabled until Actions billing +
  OIDC/attestation perms exist). Spec §29 still **IN_PROGRESS** — stub ≠ signed
  release.

### Changed

- Supply-chain docs (`CONTRIBUTING.md`, `docs/plan/chaos_supply_chain.md`,
  production drive) point at tag SBOM + attestation stub; do not claim 1.0.

## [0.0.0] - workspace bootstrap

Pre-tag baseline on `main` (library engine, multi-venue alpha adapters, daemon,
recording/replay, deny/SECURITY hygiene). No SemVer stability guarantees.
