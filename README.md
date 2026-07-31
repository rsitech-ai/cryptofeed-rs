# cryptofeed-rs

Rust-native, library-first market-data ingestion for multiple cryptocurrency
exchanges, with an optional `marketfeed` daemon.

cryptofeed-rs normalizes public trades, quotes, order books, candles, statistics,
and derivatives reference data into exact fixed-point domain events. Venue
adapters are pure session state machines; networking, reconnects, bounded
dispatch, recording, replay, health, and metrics are separate layers.

This is an independent RSI Tech implementation. It is not affiliated with or
endorsed by any similarly named project.

> [!WARNING]
> This project is an **alpha prerelease**. Interfaces may change, venue behavior
> is not yet backed by scheduled credentialed canaries, and the project is not
> approved for unattended production use or trading.

Maintained by [RSI Tech](https://rsitech.ai). Public and confidential project
contact: [info@rsitech.ai](mailto:info@rsitech.ai).

## Capabilities

- Public market-data adapters for Binance, OKX, Bybit, Kraken, Deribit,
  Bitstamp, Gemini, Coinbase, and Bitfinex across supported spot and derivatives
  segments.
- Exact fixed-point price and quantity representation.
- Sequence-aware L2 order books with fail-closed validation and resynchronization.
- Bounded queues with explicit overflow policies and observable drop counters.
- Reconnect supervision with graceful cancellation and bounded shutdown.
- Raw MFR1 recording, deterministic adapter-level replay, normalized JSONL, and
  binary protobuf-compatible output.
- Optional file and UDP sinks plus experimental minimal Kafka and NATS TCP
  sinks; see
  [`crates/sinks/README.md`](crates/sinks/README.md) for the current limits.
- Protobuf-compatible MFPE-PB1 output uses a hand-maintained encoder; the
  `.proto` schema is not compiled and has no generated-stub or breaking-change
  CI gate yet.
- Daemon health endpoints (`/live`, `/ready`) and Prometheus-format `/metrics`.
- Optional loopback view API + SPA (`--features ui`): see
  [`docs/ops/ui.md`](docs/ops/ui.md). Run the live panel with
  `./scripts/run_live_ui.sh`. Grafana/Prometheus ops:
  [`docs/ops/grafana/README.md`](docs/ops/grafana/README.md).

The authoritative architecture and behavior specification is
[`docs/spec/production_rust_multi_exchange_market_data_spec.md`](docs/spec/production_rust_multi_exchange_market_data_spec.md).
The current adapter matrix and operational gaps are recorded in
[`docs/plan/maturity_matrix.md`](docs/plan/maturity_matrix.md).

## Install

### Download the alpha daemon

The `v0.1.0-alpha.1` release provides a locally built and tested Apple Silicon
macOS archive:

- [Release page](https://github.com/rsitech-ai/cryptofeed-rs/releases/tag/v0.1.0-alpha.1)
- [Direct archive download](https://github.com/rsitech-ai/cryptofeed-rs/releases/download/v0.1.0-alpha.1/marketfeed-v0.1.0-alpha.1-aarch64-apple-darwin.tar.gz)
- [CycloneDX SBOM](https://github.com/rsitech-ai/cryptofeed-rs/releases/download/v0.1.0-alpha.1/marketfeed-v0.1.0-alpha.1.cdx.json)
- [Checksums](https://github.com/rsitech-ai/cryptofeed-rs/releases/download/v0.1.0-alpha.1/SHA256SUMS)

Verify and run:

```bash
shasum -a 256 -c SHA256SUMS  # archive and SBOM must both be present
tar -xzf marketfeed-v0.1.0-alpha.1-aarch64-apple-darwin.tar.gz
./marketfeed-v0.1.0-alpha.1-aarch64-apple-darwin/marketfeed version
./marketfeed-v0.1.0-alpha.1-aarch64-apple-darwin/marketfeed --help
```

The published binary is ad hoc linker-signed, not Developer ID signed, and not
notarized. macOS may require an explicit local security decision before first
launch. Build from source if that is unsuitable for your environment.

### Build from source

The workspace MSRV is Rust 1.85; the pinned development toolchain is in
[`rust-toolchain.toml`](rust-toolchain.toml).

```bash
git clone https://github.com/rsitech-ai/cryptofeed-rs.git
cd cryptofeed-rs
cargo build --release -p marketfeed-daemon
./target/release/marketfeed version
```

## Quick start

Validate and run the offline synthetic configuration:

```bash
cargo run -p marketfeed-daemon -- validate --config crates/daemon/config.offline.toml
cargo run -p marketfeed-daemon -- run --config crates/daemon/config.offline.toml
```

In another terminal:

```bash
curl --fail http://127.0.0.1:19108/live
curl --fail http://127.0.0.1:19108/ready
curl --fail http://127.0.0.1:19108/metrics
```

For live public sessions, start from
[`crates/daemon/config.example.toml`](crates/daemon/config.example.toml) and
enable only the venues and channels you need. Exchange APIs are unreliable;
operators must monitor reconnects, book validity, drops, rate limits, and sink
health.

Live market panel (SPA + multi-venue public feeds):

```bash
./scripts/run_live_ui.sh
# → http://127.0.0.1:19109/?asset=BTC&mode=lines&dock=1
```

See [`docs/ops/ui.md`](docs/ops/ui.md) for flags, config, and manual commands.

Private feeds use environment variables only. Never place credentials in TOML,
logs, recordings, issues, or test fixtures. The private-account module does not
place orders.

## Workspace map

| Area | Purpose |
|---|---|
| `crates/model`, `book`, `dispatch` | Exact domain types, L2 state, bounded delivery |
| `crates/adapter-api`, `adapters/*` | Network-free adapter contracts and venue protocols |
| `crates/transport`, `engine` | Rustls transport, supervision, reconnects, lifecycle |
| `crates/recording`, `replay` | Versioned capture and deterministic replay |
| `crates/sinks` | Memory, logging, file, UDP, Kafka, and NATS delivery |
| `crates/daemon` | CLI, configuration, health, metrics, and orchestration |
| `crates/facade`, `ffi` | Embedding surface and minimal C ABI |

## Validation

Run the release-quality local gate:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo deny check
./scripts/check-oss-readiness.sh
```

Live tests perform real network I/O and are ignored by default. See
[`CONTRIBUTING.md`](CONTRIBUTING.md), the
[`canary checklist`](docs/ops/canary_checklist.md), and the
[`soak runbook`](docs/ops/soak_runbook.md) before running them.

The latest checked-in evidence is a 15-minute laptop run across every usable
public venue-segment: 22 concurrent WebSocket sessions, 1,187,432 frames,
1,260,589 normalized events, zero parse failures, zero sequence gaps, zero
drops, and clean shutdown. This is short-run evidence, not a stability or
production-readiness claim. See [`docs/ops/soak_results.md`](docs/ops/soak_results.md).

## Project policy

- [Contributing](CONTRIBUTING.md)
- [Governance](GOVERNANCE.md)
- [Support](SUPPORT.md)
- [Security](SECURITY.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Release process](RELEASING.md)
- [Changelog](CHANGELOG.md)

## License

Copyright 2026 Rafal Sikora.

Licensed under the [Apache License, Version 2.0](LICENSE). See
[`NOTICE`](NOTICE) for attribution. Unless explicitly stated otherwise,
contributions submitted to this repository are licensed under the same terms.
