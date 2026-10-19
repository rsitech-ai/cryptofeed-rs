# ADR 0014: Catalog `--live` REST discovery

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §20 CLI catalog; instrument discovery  
**Package:** W4-P1a (#147)  
**Code:** `crates/daemon/src/catalog_discover.rs`, `crates/daemon/src/cli.rs`

## Decision

`marketfeed catalog --config … --venue … --live` performs a **one-shot REST**
discovery through `VenueFactory::instrument_requests`, optional
`instrument_continuation_requests` and final `parse_instruments`,
executed by the daemon’s HTTP transport (`ReqwestHttpTransport` or scripted
test stub). Default (no `--live`) remains config-stub symbols.

Primary and continuation responses retain request order. The factory receives
all responses so far to validate the chain and choose another request or explicit
completion. The daemon collects at most 32 responses / 128 MiB within a 60-second
request phase, rejects duplicate request IDs, and never publishes a partial
catalog after a continuation, transport, byte or deadline failure. Every response
keeps its raw body and full request provenance in the configured shared journal.
The default continuation hook is empty, preserving existing factory behavior.
The separate optional detail phase retains its existing factory-owned bounds.

Bybit derivatives follow bounded opaque cursors, while spot omits pagination
parameters. See the adapter README for native schema, page/cursor limits and
scope. Completing a page chain does not prove an atomic exchange-universe
snapshot, historical completeness or dataset qualification.

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
