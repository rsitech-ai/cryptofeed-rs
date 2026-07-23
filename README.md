# marketfeed

Rust-native multi-exchange market-data engine (library-first, optional daemon).

Authoritative design: [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](docs/spec/production_rust_multi_exchange_market_data_spec.md).

## Workspace

| Crate | Role |
|---|---|
| `marketfeed` | **Facade** (spec §19 / §7.1): thin re-exports — `Fixed`, events, `EngineControl`, key traits, common sinks |
| `marketfeed-model` | Domain types, exact `Fixed`, events |
| `marketfeed-adapter-api` | `SessionMachine` contracts |
| `marketfeed-book` | L2 book + sync |
| `marketfeed-dispatch` | Bounded queues + `OverflowPolicy` |
| `marketfeed-sinks` | External `EventSink` (`MemorySink` / `LoggingSink` / `FileSink` / `ProtobufFileSink` / `ProtobufBinaryFileSink` / `UdpSink`; Kafka/NATS stubs return `Unsupported`); daemon `[[sinks]]` wires `memory|logging|file|protobuf-file|protobuf-file-bin` |
| `marketfeed-ffi` | Minimal C ABI stub (`marketfeed_version`, `marketfeed_fixed_parse`); hand-written `include/marketfeed.h` |
| `marketfeed-transport` | `MemoryWebSocket` + `TungsteniteWebSocket` (Rustls/webpki) |
| `marketfeed-recording` / `marketfeed-replay` | Versioned raw MFR1 WebSocket/HTTP inputs + build/session/catalog metadata + deterministic replay |
| `marketfeed-engine` | Session runner, supervisor, reconnect/backoff loop |
| `marketfeed-private` | Phase 6 private-account (OKX/Bybit live user-data via `--features live`; Binance decoding scaffold is blocked pending authenticated WebSocket API migration) |
| `marketfeed-adapter-synthetic` | Mock venue |
| `marketfeed-adapter-binance` | Binance Spot + USD-M + Coin-M (trades/quote/L2/mark/funding/OI/liquidations; Coin-M also OI REST + `@forceOrder`) |
| `marketfeed-adapter-okx` | OKX Spot/SWAP/Futures trades/tickers/books L2 + mark/index/funding |
| `marketfeed-adapter-bybit` | Bybit linear/spot/inverse trades/quotes + L2 (`u` sync) |
| `marketfeed-adapter-kraken` | Kraken Spot WS v2 trades + ticker quotes + `book` L2 (CRC32) + opt-in `ohlc` candles |
| `marketfeed-adapter-deribit` | Deribit trades + ticker (quote/mark/index/funding/OI) + `book.*.100ms` L2 + opt-in `chart.trades` candles |
| `marketfeed-daemon` | Optional binary `marketfeed`: validate/run/replay/inspect-recording, `/live` `/ready` `/metrics`, optional `[[sinks]]`, JSON tracing; private sessions fail closed pending a durable account sink/readiness/reconnect path |

## Develop

```bash
cargo test --workspace
cargo run -p marketfeed-daemon -- validate --config crates/daemon/config.example.toml
```

### Host-opt profile (optional, non-portable)

Workspace profile `host-opt` inherits `release` with fat LTO and `codegen-units = 1`
(spec §30.3). Public/CI binaries stay on portable `release`. For a host-specific
binary, operators pass `target-cpu=native` via `RUSTFLAGS` (not a Cargo profile key):

```bash
RUSTFLAGS="-C target-cpu=native" cargo build -p marketfeed-daemon --profile host-opt
```

**Caveats:** non-portable across CPUs; fat LTO is slower to link; do not ship as the
default release artifact. Same tests/schema as portable release.

PR-quality local checks (fmt, clippy `-D warnings`, tests, `cargo deny`): see [`CONTRIBUTING.md`](CONTRIBUTING.md).

Ops runbooks: [`docs/runbooks/`](docs/runbooks/).

Live smokes (network; ignored by default):

```bash
cargo test -p marketfeed-adapter-binance --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-bybit --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-kraken --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-deribit --test live_ignored -- --ignored --nocapture
# Private OKX/Bybit (venue credentials; never commit .env — see .env.example):
cargo test -p marketfeed-private --features live --test live_ignored -- --ignored --nocapture
```

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) **OR** [MIT](LICENSE-MIT), at your option.

See [NOTICE](NOTICE) for copyright attribution and [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Fuzz / supply chain

- libFuzzer targets (nightly): `fuzz/` — see [`docs/plan/chaos_supply_chain.md`](docs/plan/chaos_supply_chain.md).
- SBOM: `./scripts/generate-sbom.sh` (`cargo-cyclonedx`, or `syft` fallback); tag workflow `.github/workflows/release.yml` uploads the artifact (see `CHANGELOG.md`).

## Status

**Not beta / not stable.** Binance Spot + OKX Spot are informal **alpha+** (beta-ready offline); other venues remain **alpha**.
Scheduled live canary + soak remain **OPS** — see the honesty bar in
[`docs/plan/maturity_matrix.md`](docs/plan/maturity_matrix.md), [`docs/ops/canary_checklist.md`](docs/ops/canary_checklist.md) and default review owner in
[`CODEOWNERS`](CODEOWNERS).

| Family | Status |
|---|---|
| Binance Spot | **alpha+** — fixtures + corpus + close-out docs; not beta |
| Binance USD-M | **alpha** — fixtures + OI timer; not beta |
| OKX Spot | **alpha+** — fixtures + corpus + close-out docs; not beta |
| OKX SWAP / Futures | **alpha** — fixtures; not beta |
| Bybit linear / spot / inverse | **alpha** — fixtures + L2 `u`; not beta |
| Kraken Spot | **alpha** — trades/quotes/L2 CRC32 + corpus; not beta |
| Deribit | **alpha** — trades/ticker/L2 `change_id` + corpus; not beta |

Candles (Binance Spot + OKX Spot native klines): [`docs/adr/0001-candles-deferred.md`](docs/adr/0001-candles-deferred.md).
Daemon wires config→sessions (synthetic memory offline; live venues optional).
Offline: `cargo test --workspace`. Live smokes: see above (`#[ignore]`).
