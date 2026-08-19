# Adapter maturity matrix

**Maintainer:** RSI Tech ([`CODEOWNERS`](../../CODEOWNERS))
**Canary checklist:** [`docs/ops/canary_checklist.md`](../ops/canary_checklist.md)  
**Laptop live evidence:** [`docs/ops/canary_results.md`](../ops/canary_results.md) (laptop **10/10**; scheduled **0**; reconnect **PASS** laptop — **not beta**)<br>
**Laptop soak evidence:** [`docs/ops/soak_results.md`](../ops/soak_results.md) (15m all-public-segment + ~31m live binance+okx + synthetic, not stable)<br>
**Updated:** 2026-07-23

Spec vocabulary: **experimental | beta | stable**. Informal labels:

| Label | Meaning |
|---|---|
| **experimental** | Scaffold / thin coverage |
| **alpha** | Primary channels + offline fixtures; short of beta docs |
| **alpha+** / **beta-ready offline** | Alpha plus README owner/limitations + canary checklist — **still not beta** |
| **beta** | Requires scheduled live canary (§11.8) — **OPS** |
| **stable** | Requires soak + ops ownership — **OPS** |

## Honesty bar

- Offline fixtures / corpora / L2 sync unit tests are **not** beta.
- Laptop one-shot live `#[ignore]` smoke is **not** beta (needs scheduled ≥7 greens).
- Laptop synthetic soak is **not** stable (needs multi-day live soak).
- **Beta** requires discovery + primary channels + replay corpus + **scheduled live canary** + documented limits (§11.8).
- **Stable** requires soak + ops ownership beyond this matrix.
- This document does **not** claim beta until the scheduled-canary gate passes,
  or stable until the separate multi-day soak and operations gates pass.
- Calling a venue `alpha+` / "beta-ready offline" means the **code + docs package** for P2 close-out is done; live promotion is still blocked.

## Matrix

| VenueId | Code | Segment | Capabilities (claimed) | Maturity | Offline proofs | Blocks beta |
|--------:|------|---------|------------------------|----------|----------------|-------------|
| 1 | `synthetic` | test | T, Q, L2, Candles | **alpha** | memory reconnect + QUOTE/CANDLE fixtures | N/A (test venue) |
| 2 | `binance-spot` | spot | T, Q, L2, Candles, Statistics24h | **alpha+** (beta-ready offline; laptop canary **10/10**, not scheduled) | fixtures + corpus + `@ticker` Stats24h + heartbeat + README/owner/limits + laptop canary + reconnect probe + ~31m live soak | scheduled canary ≥7 (**scheduled = 0**) |
| 3 | `binance-usdm` | linear | T, Q, L2, Candles, Statistics24h, mark, index, funding, OI, liq | **alpha** | fixtures + OI REST timer + `@kline_*` + `@ticker` Stats24h + dedicated `@indexPrice@1s` + L2 book corpus | live canary, soak |
| 12 | `binance-coinm` | inverse | T, Q, L2, Candles, Statistics24h, mark, index, funding, OI, liq | **alpha** | fixtures + OI REST timer + forceOrder + bookTicker + `@ticker` Stats24h + dedicated pair `@indexPrice@1s` + L2 `pu` + L2 book corpus (dapi) | live canary, soak |
| 4 | `okx-spot` | spot | T, Q, L2, Candles, Statistics24h | **alpha+** (beta-ready offline; laptop canary **10/10**, not scheduled) | fixtures + trade/quote/Stats24h + L2 book corpus + README/owner/limits + canary checklist + laptop canary + reconnect probe + ~31m live soak | scheduled canary ≥7 (**scheduled = 0**) |
| 9 | `okx-swap` | linear+inverse | T, Q, L2, Candles, Statistics24h, mark, index, funding, OI, liq | **alpha** | fixtures + open-interest + liquidation-orders + L2 book corpus; inverse instruments on same VenueId | live canary, soak, close-out docs |
| 10 | `okx-futures` | linear+inverse | T, Q, L2, Candles, Statistics24h, mark, index, funding, OI, liq | **alpha** | fixtures + open-interest + liquidation-orders + L2 book corpus; inverse instruments on same VenueId | live canary, soak, close-out docs |
| 5 | `bybit-linear` | linear | T, Q, L2, Candles, Statistics24h, mark, index, funding, OI, liq | **alpha** | fixtures + tickers (24h+der) + allLiquidation + L2 `u` + L2 book corpus + laptop `live_ignored` once | scheduled canary, soak |
| 6 | `bybit-spot` | spot | T, Q, L2, Candles, Statistics24h | **alpha** | fixtures + kline + tickers Stats24h | live canary, soak |
| 11 | `bybit-inverse` | inverse | T, Q, L2, Candles, Statistics24h, mark, index, funding, OI, liq | **alpha** | fixtures + tickers (24h+der) + allLiquidation + L2 book corpus + daemon `segment=inverse` | live canary, soak |
| 7 | `kraken-spot` | spot | T, Q, L2, Candles, Statistics24h | **alpha** | fixtures + trade/quote/Stats24h + ohlc + L2 book corpus + laptop `live_ignored` once | scheduled canary, soak |
| 13 | `kraken-futures` | linear+inverse | T, Q, L2, Statistics24h, mark, index, funding, OI, liq, Candles (REST) | **alpha** | fixtures + trade/ticker (mark/index/funding/OI/24h) + book (WS v1) + liq via trade `type=liquidation` + L2/ticker+liq corpora + laptop `live_ignored` once + REST charts candles timer| scheduled canary, soak |
| 8 | `deribit` | perp/futures | T, ticker (Q/mark/index/funding/OI/Statistics24h), L2, Candles, liq | **alpha** | fixtures + trade/ticker (incl. stats) + dedicated `deribit_price_index` + chart.trades + L2 book corpus + liq via trades `liquidation` field + laptop `live_ignored` once | scheduled canary, soak |
| 14 | `bitstamp` | spot | T, Q, L2, Candles (REST), Statistics24h (REST) | **alpha** | fixtures + live `live_trades` / continuous full `order_book`; `diff_order_book` is decode/replay-only because the independent full and diff streams expose no shared sequence; L2 book corpus + REST OHLC timer (#119) + REST ticker Stats24h + laptop `live_ignored` once | scheduled canary, soak |
| 15 | `gemini` | spot | T, Q, L2, Candles (REST), Statistics24h (REST) | **alpha** | fixtures + current multiplexed WebSocket `@trade` / `@bookTicker` / differential `@depth@100ms` with `snapshot=-1`, sequence-gap reconnect, L2 book corpus + REST candles timer (#119) + REST `/v2/ticker`+`/v1/pubticker` Stats24h; current-protocol ignored smoke exists but has no checked-in current-protocol evidence artifact; catalog `--live` via `/v1/symbols` (default scales; optional capped N+1 details `GEMINI_LIVE_DETAILS_MAX`) | current-protocol evidence, scheduled canary, L2 live canary, soak |
| 16 | `coinbase-spot` | spot | T, Q, L2 (env-auth), Candles (REST), Statistics24h | **alpha** | strict Exchange HMAC signing; signed subscribe/reconnect fixtures; subscription ack + snapshot/delta/replay corpus; REST candles timer; laptop public T/Q `live_ignored` once | credential-backed L2 canary, scheduled canary, soak |
| 17 | `bitfinex` | spot | T, Q, L2, Candles, Statistics24h | **alpha** | fixtures + WS v2 trades/ticker Stats24h/book + WS candles + L2 book corpus + R6 status/catalog | live canary, soak |
| 18 | `coinbase-adv` | spot | T, Q, L2, Candles (REST public market), status, Statistics24h | **alpha** | fixtures + Advanced Trade public `market_trades`/`ticker` Stats24h/`level2` + `status` → InstrumentUpdate + REST candles + heartbeats + Adv L2 corpus + catalog `--live` + laptop `live_ignored` | scheduled canary, soak |
| 19 | `coinbase-intl` | derivatives | T, Q, L2 (env-auth), status, catalog | **alpha** | HMAC subscribe/reconnect fixtures + INTX T/Q/L2 corpus + session-global sequence-gap invalidation + daemon `segment=intl` + REST catalog discovery | credential-backed canary, scheduled canary, soak |

| 20 | `bitfinex-deriv` | linear+inverse | T, Q, L2, Candles (WS), mark, index, funding, OI, Stats24h, liq | **alpha** | fixtures + REST `status/deriv` + WS `liq:global` + L2 corpus + catalog `--live` + `session_config_from_catalog` + R6 status + `INCLUDE_ALPHA` canary (#210); daemon `segment=deriv` | live canary, soak |

## Deferred product boundaries

| Topic | Decision | Doc |
|---|---|---|
| Candles | Native candles on Binance Spot/USD-M/Coin-M, OKX Spot/SWAP/Futures, Bybit linear/spot/inverse, Kraken Spot `ohlc`, Deribit `chart.trades`; REST timer OHLC/candles on Kraken Futures / Bitstamp / Gemini / Coinbase Exchange / Bitfinex / Coinbase Advanced Trade public market (offline SessionMachine) | [`docs/adr/0001-candles-deferred.md`](../adr/0001-candles-deferred.md) |

## Related

- VenueIds: [`venue_ids.md`](./venue_ids.md)
- Channel coverage audit: [`venue_channel_audit.md`](./venue_channel_audit.md)
- Runtime evidence: [`../ops/soak_results.md`](../ops/soak_results.md)
