# ADR 0001: Candles deferred → implemented

**Status:** Superseded (implemented)  
**Date:** 2026-07-21 (deferred) / 2026-07-22 (implemented)  
**Spec:** §2.1 lists candles where venues supply them natively.

## Decision (original)

Do **not** implement candle / kline channels in adapters for the current 1.0 path.

## Decision (current)

Implement venue-native candles as deterministic `SessionMachine` paths (no networking):

| Venue | Stream / channel | Capability |
|---|---|---|
| Binance Spot (`VenueId(2)`) | `@kline_{interval}` | `Capability::Candles` |
| Binance USD-M (`VenueId(3)`) | `@kline_{interval}` | `Capability::Candles` |
| OKX Spot (`VenueId(4)`) | `candle1m` / `candle5m` / `candle15m` / `candle1H` / `candle1D` | `Capability::Candles` |
| Bitstamp (`VenueId(14)`) | REST `GET /ohlc/{pair}/` on `CANDLE_TIMER_ID` | `Capability::Candles` |
| Gemini (`VenueId(15)`) | REST `GET /v2/candles/{symbol}/{tf}` on `CANDLE_TIMER_ID` | `Capability::Candles` |
| Coinbase Exchange (`VenueId(16)`) | REST `GET /products/{id}/candles` on `CANDLE_TIMER_ID` | `Capability::Candles` |
| Bitfinex (`VenueId(17)`) | REST `GET /v2/candles/trade:{tf}:{symbol}/hist` on `CANDLE_TIMER_ID` | `Capability::Candles` |
| Coinbase Advanced Trade (`VenueId(18)`) | public REST `GET /api/v3/brokerage/market/products/{id}/candles` on `CANDLE_TIMER_ID` | `Capability::Candles` |
| Kraken Futures (`VenueId(13)`) | REST `GET /api/charts/v1/trade/{symbol}/{resolution}` on `CANDLE_TIMER_ID` | `Capability::Candles` |

Canonical intervals map from `CandleInterval::{M1,M5,M15,H1,D1}` to venue suffixes; OHLCV uses exact `Fixed` (no `f64`). Subscriptions opt in via `Channel::Candles` on the session plan / `*_session_config.candle_intervals`.

## Why (original deferral)

- Consumers can derive bars from trades when needed.
- Native candle streams add sync/gap/replay surface area without unblocking §3 stables.

## Why implement now

- Product surface §2.1 is claimed offline for Spot families that already ship native klines.
- Fixtures prove decode + emit without live I/O; consumers can subscribe when needed.

## Consequences

- `Capability::Candles` is live on Binance Spot, Binance USD-M, and OKX Spot specs.
- Maturity notes list candles as implemented (offline), not deferred.
- Advanced Trade public candles are VenueId **18** (`coinbase-adv`), not mixed into Exchange Classic.
- Kraken Futures WS candles remain N/A; REST charts timer is alpha (`Capability::Candles`).
