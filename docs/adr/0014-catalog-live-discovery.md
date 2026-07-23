# ADR 0014: Catalog `--live` REST discovery

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §20 CLI catalog; instrument discovery  
**Package:** W4-P1a (#147)  
**Code:** `crates/daemon/src/catalog_discover.rs`, `crates/daemon/src/cli.rs`

## Decision

`marketfeed catalog --config … --venue … --live` performs a **one-shot REST**
discovery through `VenueFactory::instrument_requests` + `parse_instruments`,
executed by the daemon’s HTTP transport (`ReqwestHttpTransport` or scripted
test stub). Default (no `--live`) remains config-stub symbols.

Live discovery ships for factories with non-empty `instrument_requests`
(Binance / OKX / Bybit / Kraken / Deribit / Coinbase Exchange / Coinbase-adv /
Bitstamp / Bitfinex / Gemini). Gemini uses `GET /v1/symbols` (default scales)
plus optional capped N+1 `/v1/symbols/details/{symbol}` when
`GEMINI_LIVE_DETAILS_MAX` > 0 (default 0). Synthetic stays stub-only.

## Why

- Operators need real instrument lists without inventing a daemon-owned refresh
  loop yet.
- Keeps SessionMachine networking-free (ADR-0004): HTTP stays in the daemon CLI.

## Consequences

- **ponytail:** CLI one-shot + fixtures only — upgrade = engine-owned refresh
  + catalog versioning for continuous runtime discovery.
- Stub venues error clearly when `--live` is requested.
- Does not unlock maturity / beta by itself.
