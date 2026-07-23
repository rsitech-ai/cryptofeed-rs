# Canonical VenueId registry

**Status:** authoritative
**Updated:** 2026-07-23
**Assigned:** 1–20; next free ID is 21

`VenueId` is a global `u16` in `marketfeed-model`. Each distinct venue-segment
must keep a stable, non-overlapping ID across configuration, events, recordings,
replay, metrics, and fixtures.

## Assigned map

| ID | Code / constant | Segment |
|---:|---|---|
| 0 | reserved | engine default / unset |
| 1 | `synthetic` / `SYNTHETIC_VENUE_ID` | test |
| 2 | `binance-spot` / `BINANCE_SPOT_VENUE_ID` | spot |
| 3 | `binance-usdm` / `BINANCE_USDM_VENUE_ID` | USD-M futures |
| 4 | `okx-spot` / `OKX_SPOT_VENUE_ID` | spot |
| 5 | `bybit-linear` / `BYBIT_LINEAR_VENUE_ID` | linear derivatives |
| 6 | `bybit-spot` / `BYBIT_SPOT_VENUE_ID` | spot |
| 7 | `kraken-spot` / `KRAKEN_SPOT_VENUE_ID` | spot |
| 8 | `deribit` / `DERIBIT_VENUE_ID` | perpetuals / futures |
| 9 | `okx-swap` / `OKX_SWAP_VENUE_ID` | swaps |
| 10 | `okx-futures` / `OKX_FUTURES_VENUE_ID` | dated futures |
| 11 | `bybit-inverse` / `BYBIT_INVERSE_VENUE_ID` | inverse derivatives |
| 12 | `binance-coinm` / `BINANCE_COINM_VENUE_ID` | COIN-M derivatives |
| 13 | `kraken-futures` / `KRAKEN_FUTURES_VENUE_ID` | perpetuals / futures |
| 14 | `bitstamp` / `BITSTAMP_VENUE_ID` | spot |
| 15 | `gemini` / `GEMINI_VENUE_ID` | spot |
| 16 | `coinbase-spot` / `COINBASE_SPOT_VENUE_ID` | Coinbase Exchange spot |
| 17 | `bitfinex` / `BITFINEX_VENUE_ID` | spot |
| 18 | `coinbase-adv` / `COINBASE_ADV_VENUE_ID` | Advanced Trade spot |
| 19 | `coinbase-intl` / `COINBASE_INTL_VENUE_ID` | authenticated international derivatives |
| 20 | `bitfinex-deriv` / `BITFINEX_DERIV_VENUE_ID` | derivatives |

## Authenticated market-data boundary

Coinbase International WebSocket market data requires an HMAC-authenticated
subscription. The catalog REST endpoint is public, but continuous T/Q/L2 is not.
Credentials are accepted only through:

- `COINBASE_INTL_API_KEY`
- `COINBASE_INTL_API_SECRET`
- `COINBASE_INTL_API_PASSPHRASE`

TOML credentials are rejected and the adapter does not place orders.

## Adding a venue

1. Reserve the next free ID in this file in the same pull request.
2. Use that ID in the adapter constant, daemon catalog, fixtures, metrics, and
   recordings.
3. Add a collision test or extend the existing registry coverage.
4. Never renumber a released ID. If a venue is retired, reserve its ID.

See the [maturity matrix](maturity_matrix.md) for capability and promotion
status, and the
[market-data specification](../spec/production_rust_multi_exchange_market_data_spec.md)
for event semantics.
