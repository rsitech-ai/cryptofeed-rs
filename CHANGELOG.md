# Changelog

All notable changes to cryptofeed-rs are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) prerelease labels.

## [Unreleased]

No user-visible changes yet.

## [0.1.0-alpha.1] - 2026-07-23

First public alpha release under RSI Tech stewardship.

### Added

- Exact fixed-point market domain model and normalized event envelopes.
- Session-machine adapter API with network-free protocol state transitions.
- Public market-data adapters spanning Binance, OKX, Bybit, Kraken, Deribit,
  Bitstamp, Gemini, Coinbase, and Bitfinex venue-segments.
- Sequence-aware L2 books with snapshot/delta synchronization, checksum
  validation where venues support it, fail-closed invalidation, and recovery.
- Bounded dispatch, explicit overflow policies, drop metrics, and sink isolation.
- Rustls WebSocket/HTTP transport, reconnect supervision, interruptible backoff,
  venue-aware frame limits, and bounded graceful shutdown.
- Raw MFR1 recording, replay, normalized JSONL, and binary
  protobuf-compatible output.
- Memory, logging, file, UDP, Kafka, and NATS sink surfaces with documented
  support limits.
- Optional daemon with config validation, catalog/plan/replay commands,
  `/live`, `/ready`, `/metrics`, structured tracing, and partial SIGHUP reload.
- Environment-only private user-data scaffolding for supported venues; no order
  placement.
- Offline fixtures, replay corpora, live ignored tests, fuzz targets, CycloneDX
  SBOM generation, and supply-chain policy.
- Reproducible local release packaging with SHA-256 checksums.

### Changed

- Curated a clean public-source history under
  [`rsitech-ai/cryptofeed-rs`](https://github.com/rsitech-ai/cryptofeed-rs).
- Set RSI Tech as public maintainer and `info@rsitech.ai` as the public and
  confidential project contact.
- Adopted Apache License 2.0 as the sole project license.
- Replaced raw operator logs and internal orchestration boards with concise
  public evidence, governance, support, security, contribution, and release
  documentation.

### Known limitations

- Alpha APIs and configuration may change.
- Scheduled credentialed live canaries, multi-day chaos soaks, and external sink
  consumer proof are not complete.
- The downloadable binary is Apple Silicon macOS only, ad hoc linker-signed,
  not Developer ID signed, and not notarized.
- Private sessions remain alpha and do not place orders.
- Kafka/NATS implementations are intentionally minimal and require operator
  validation against the intended brokers.

[Unreleased]: https://github.com/rsitech-ai/cryptofeed-rs/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/rsitech-ai/cryptofeed-rs/releases/tag/v0.1.0-alpha.1
