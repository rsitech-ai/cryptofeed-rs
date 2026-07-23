# ADR 0007: REST timer candles on some venues

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §2.1 candles where venues supply them; see also [0001](./0001-candles-deferred.md)

## Decision

Where a venue has **no native public WS candle channel**, expose `Capability::Candles` via engine **REST poll on `CANDLE_TIMER_ID`**, decoded by the same `SessionMachine` (no adapter networking). Exact `Fixed` OHLCV.

Applies today: Kraken Futures (13), Bitstamp (14), Gemini (15), Coinbase Exchange (16), Bitfinex (17), Coinbase Advanced Trade (18 — candles-only family). Native WS `@kline` / `candle*` remain preferred where the venue supplies them (Binance Spot/USD-M, OKX Spot, etc.).

## Why

- Spec §2.1 surface without inventing synthetic bars from trades.
- Keeps SessionMachine purity; fixtures prove decode without live I/O.

## Consequences

- MFR1 v2 records REST responses with request ID, status, headers, body, and receive stamp. Historical v1 corpora may retain their Text/sidecar inject harnesses.
- Maturity stays alpha until live canary/soak; REST timer ≠ WS native continuity.
- Prefer venue-native WS candles when available; do not dual-path the same venue without need.
