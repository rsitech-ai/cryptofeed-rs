# Canonical VenueId registry

**Status:** authoritative for Phase 3 parallel adapters  
**Tip:** VenueIds **1–20** shipped (19 auth MD alpha); next free = **21**  
**Date:** 2026-07-22  
**Base:** `origin/main`

`VenueId` is a global `u16` in `marketfeed_model`. Parallel venue workers must not invent overlapping IDs. Claim new IDs here before coding.

## Assigned map

| VenueId | Code / constant | Segment notes | Owner / evidence |
|--------:|-----------------|---------------|--------------|
| 0 | _(reserved)_ | engine default / unset | — |
| 1 | `synthetic` / `SYNTHETIC_VENUE_ID` | test venue | domain / spot stack |
| 2 | `binance-spot` / `BINANCE_SPOT_VENUE_ID` | spot | `feat/andrzej_binance_spot` |
| 3 | `binance-usdm` / `BINANCE_USDM_VENUE_ID` | USD-M futures | `feat/andrzej_binance_derivatives` |
| 4 | `okx-spot` / `OKX_SPOT_VENUE_ID` | spot (v1) | `feat/andrzej_okx` |
| 5 | `bybit-linear` / `BYBIT_LINEAR_VENUE_ID` | linear | `feat/andrzej_bybit` |
| 6 | `bybit-spot` / `BYBIT_SPOT_VENUE_ID` | spot | `feat/andrzej_bybit` |
| 7 | `kraken-spot` / `KRAKEN_SPOT_VENUE_ID` | spot | `feat/andrzej_kraken_deribit` |
| 8 | `deribit` / `DERIBIT_VENUE_ID` | perp / futures | `feat/andrzej_kraken_deribit` |
| 9 | `okx-swap` / `OKX_SWAP_VENUE_ID` | linear perpetuals | `feat/andrzej_okx_swap_l2` |
| 10 | `okx-futures` / `OKX_FUTURES_VENUE_ID` | linear dated futures | `feat/andrzej_okx_swap_l2` |
| 11 | `bybit-inverse` / `BYBIT_INVERSE_VENUE_ID` | inverse (coin-margined) | `feat/andrzej_venue_beta_wave2` |
| 12 | `binance-coinm` / `BINANCE_COINM_VENUE_ID` | inverse (coin-margined) | `feat/andrzej_candles_coinm` |
| 13 | `kraken-futures` / `KRAKEN_FUTURES_VENUE_ID` | derivatives (perp + dated); status/catalog **HAVE** (#126) | `feat/andrzej_p1_venues_depth` |
| 14 | `bitstamp` / `BITSTAMP_VENUE_ID` | spot; candles REST #119; status/catalog **HAVE** (#126) | #111 |
| 15 | `gemini` / `GEMINI_VENUE_ID` | spot; candles REST #119; status/catalog **HAVE** (#126); catalog `--live` `/v1/symbols` (**W6-P0d**) | #111 |
| 16 | `coinbase-spot` / `COINBASE_SPOT_VENUE_ID` | spot (Exchange WS T/Q; env-authenticated L2 signing/subscribe + replay **HAVE**; credentials/live evidence gated; candles **HAVE** via REST timer #119; WS candle N/A; status/catalog **HAVE** #126) | #113/#119/AUTH-L2 |
| 17 | `bitfinex` / `BITFINEX_VENUE_ID` | spot (WS v2 chanId trades/ticker/book; REST candles; catalog/R6 peer-parity) | #127/#134 |
| 18 | `coinbase-adv` / `COINBASE_ADV_VENUE_ID` | spot (Advanced Trade public T/Q/L2 + REST candles; catalog/R6 peer-parity; Classic **16** remains dual protocol) | #132/#135/W5-P0c |
| 19 | `coinbase-intl` / `COINBASE_INTL_VENUE_ID` | INTX auth MD (HMAC `CBINTLMD` subscribe; env credentials; T/Q/L2 alpha) | W6-P1a alpha |
| 20 | `bitfinex-deriv` / `BITFINEX_DERIV_VENUE_ID` | derivatives (WS T/Q/L2/candles + REST status/deriv mark/index/funding/OI + WS `liq:global` liq; catalog/R6/L2 peer-parity #210) | W6-P1b / W7-P0a |

## Collision history (fixed)

Workers initially reused `3` / `4`:

| Branch | Was | Now |
|--------|-----|-----|
| OKX | 3 | **4** |
| Bybit linear / spot | 3 / 4 | **5 / 6** |
| Kraken / Deribit | 3 / 4 | **7 / 8** |
| Binance USD-M | 3 | **3** (kept) |

Synthetic `1` and Binance Spot `2` were already correct on the Spot base.

Wave-2 note: Coinbase was briefly planned as VenueId **14**; **#111** claimed **14/15** for bitstamp/gemini, so Coinbase Exchange landed as **16** (#113). Wave-3: Bitfinex claimed **17** (#127/#134); Coinbase Advanced Trade claimed **18** (#132/#135). Engine status/catalog tags cover **13–18** (#126/#135). **W2-R10** claimed **19** `coinbase-intl` (now auth MD alpha). Next free id = **21**.

**CODE plateau:** primary T/Q/L2 + der. mark/index/funding/OI/liq on ids **1–20** is **HAVE** where applicable; VenueIds **16** and **19** require env credentials for their authenticated MD paths and remain **alpha**; Stats24h **HAVE** (**W7-P0a/b/c**). Production readiness retains operations blockers.

## Coinbase International (VenueId **19**) — auth MD alpha

Shipped: `COINBASE_INTL_VENUE_ID`, SessionMachine (T/Q/L2), env-only credentials, offline fixtures, daemon `segment = "intl"`. No order placement.

| Surface | Public without credentials? | Notes |
|---|---|---|
| WS `wss://ws-md.international.coinbase.com` | **No** | HMAC `CBINTLMD` on first `SUBSCRIBE` |
| REST `GET /api/v1/instruments` | Yes | Catalog discovery |

Credentials: `COINBASE_INTL_API_KEY` / `COINBASE_INTL_API_SECRET` / `COINBASE_INTL_API_PASSPHRASE` (env only; TOML secrets rejected).

## How to claim the next ID

1. Append a row to the table above on `feat/andrzej_venue_ids` (or land a tiny PR against the merge tip that carries this file).
2. Use that `VenueId(N)` in `*_VENUE_ID` constants, fixtures, and live canaries.
3. Do **not** reuse IDs across segments of different venues; same exchange / different segments may share a crate but get distinct IDs when they are distinct `VenueSpecification`s (see Bybit).

## Related

- Orchestration: `docs/plan/multi_exchange_orchestration.md`
- Wave-3 plateau: `docs/plan/orchestrator_wave3.md`
- Spec SoT: `docs/spec/production_rust_multi_exchange_market_data_spec.md`
