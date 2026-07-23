# Venue channel audit (VenueIds 1–20)

**Role:** AUDITOR — per-exchange public channel surface vs Spec §2.1 + **full public MD available on each venue**  
**Tip audited:** `origin/main` @ ≥ `e8e6a0c` (#218 tip; #215 env-auth MD; **W7-P0** closed)  
**Date:** 2026-07-22  
**SoT:** [`production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md) §2.1 · [`maturity_matrix.md`](./maturity_matrix.md) · [`venue_ids.md`](./venue_ids.md)
**Related:** Wave-2–6 boards · continuous-improvement track **W7** (this audit)

## Verdict

**Not beta. Not stable. Not 1.0. Not production-ready.**

**Public CODE plateau (VenueIds 1–20):** applicable §2.1 channels are **HAVE** or segment **N/A**. VenueId **19** = **HAVE** (env-auth MD T/Q/L2 + catalog REST, #215). **W7-P0** closed. Implementable CODE gaps = **none open**. Maturity / §3 / §36 = **OPS-A…E only**.

Continuous-improvement bar (product goal): **every shipped exchange must implement every production-spec event type that is applicable and publicly available on that venue** — not only the Wave-6 “already-subscribed ticker” minimum. Catalog live, status, Stats24h, index, liquidations included when the venue exposes a public path. INTX MD WS requires env credentials — shipped as **HAVE** under that trust boundary (#215); still **alpha**, **not** beta.

| Track | Status |
|---|---|
| Spec §2.1 applicable channels on VenueIds **1–20** | **HAVE** or segment **N/A** — **CODE plateau** |
| VenueId **19** `coinbase-intl` | **HAVE** (env-auth MD T/Q/L2 + catalog REST; **alpha**) |
| Native `Statistics24h` | **HAVE** on **2–18, 20** (Bitstamp/Gemini REST timers **W7-P0b/c**); **19** — (not in INTX MD scope) |
| Liquidations on der. | **HAVE** on public der. venues incl. **20** (`liq:global`); **19** — (not shipped) |
| Catalog `--live` / engine status | **HAVE** on all engine-wired venues incl. **19** |
| Maturity (any venue) | **alpha** / Spot pair **alpha+** only — **0 beta / 0 stable** |
| Production blockers | **OPS-A…E** (excluded from CODE worker list below) |

**Legend**

| Tag | Meaning |
|---|---|
| **HAVE** / **Y** | Offline `SessionMachine` path emits the typed event (fixtures and/or corpus); daemon wire where applicable |
| **AVAILABLE** | Public venue API/WS offers it; not in production event model / §2.2 deferred / deliberate non-emit |
| **GAP** | Actionable implementable CODE (P0/P1) |
| **N/A** / **—** | Correctly out of scope for this segment / protocol / v1 boundary |
| **SKIP** | VenueId claimed; no adapter (auth-gated or product out) |

**Catalog `--live`:** REST one-shot via `instrument_requests` for Binance / OKX / Bybit / Kraken / Deribit / Coinbase Exchange / Coinbase-adv / Bitstamp / Bitfinex (+ bitfinex-deriv) / **Gemini** (`/v1/symbols`, default scales; optional capped N+1). Synthetic = stub **N/A**. Coinbase Intl **19** catalog REST `/instruments` **HAVE**.

**Status events:** engine-owned `VenueStatus` / `InstrumentUpdate` on connect/live/degrade + catalog refresh — **HAVE** for every engine-wired venue incl. **19**. Coinbase Classic (**16**) and Adv (**18**) also emit live `InstrumentUpdate` from public `status` channels (**HAVE**). Native `Statistics24h` **HAVE** on major tickers (**W6-P0a/b**) + Bitstamp/Gemini REST ticker timers (**W7-P0b/c**). Bitfinex-deriv liq **HAVE** via WS `liq:global` (**W7-P0a**).

**Maturity honesty:** all public venues below are **alpha** except Binance Spot + OKX Spot = **alpha+** (laptop canary ≠ scheduled). Offline fixtures ≠ beta. **W7-P0** closed + VenueId **19** env-auth MD **does not** unlock beta/stable/1.0.

---

## Summary matrix (§2.1 cells + Stats24h + catalog live; VenueIds 1–20)

| Id | Code | T | Q | L2 | Candles | Stats24h | Mark | Index | Funding | OI | Liq | Status | Catalog live | Maturity |
|---:|------|:-:|:-:|:--:|:-------:|:--------:|:----:|:-----:|:-------:|:--:|:---:|:------:|:------------:|----------|
| 1 | synthetic | Y | Y | Y | Y | Y | — | — | — | — | — | Y | N/A stub | alpha |
| 2 | binance-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y | Y | **alpha+** |
| 3 | binance-usdm | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 4 | okx-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y | Y | **alpha+** |
| 5 | bybit-linear | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 6 | bybit-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y | Y | alpha |
| 7 | kraken-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y | Y | alpha |
| 8 | deribit | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 9 | okx-swap | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 10 | okx-futures | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 11 | bybit-inverse | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 12 | binance-coinm | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 13 | kraken-futures | Y | Y | Y | Y REST | Y | Y | Y | Y | Y | Y | Y | Y | alpha |
| 14 | bitstamp | Y | Y | Y | Y REST | Y REST | — | — | — | — | — | Y | Y | alpha |
| 15 | gemini | Y | Y | Y | Y REST | Y REST | — | — | — | — | — | Y | Y | alpha |
| 16 | coinbase-spot | Y | Y | Y auth | Y REST | Y | — | — | — | — | — | Y | Y | alpha |
| 17 | bitfinex | Y | Y | Y | Y WS | Y | — | — | — | — | — | Y | Y | alpha |
| 18 | coinbase-adv | Y | Y | Y | Y REST | Y | — | — | — | — | — | Y | Y | alpha |
| 19 | coinbase-intl | Y auth | Y auth | Y auth | — | — | — | — | — | — | — | Y | Y | alpha |
| 20 | bitfinex-deriv | Y | Y | Y | Y WS | Y | Y | Y | Y | Y | Y | Y | Y | alpha |

Notes on matrix cells:

- **Bitstamp L2 (14):** live ingestion uses the continuous full `order_book`
  channel. `diff_order_book` remains decode/replay-tested but is not merged live
  because the independent streams expose no shared sequence for gap-free ordering.
- **Stats24h:** **HAVE** on major tickers (**W6-P0a/b**) + Bitstamp/Gemini REST ticker timers.
- **KF candles (13):** WS **N/A**; REST charts timer **HAVE** (**W6-P0c** / #198).
- **Index:** dedicated streams on Binance UM/CM + Deribit (#190); OKX/Bybit/KF/bitfinex-deriv index **HAVE** via peer paths.
- **19:** env-auth INTX MD **HAVE** — T/Q/L2 SessionMachine + catalog REST (**#215**); candles/Stats24h/mark/index/funding/OI/liq **—** (not in shipped INTX MD scope). Alpha only.
- **20:** `bitfinex-deriv` alpha (**W6-P1b** / #206); mark/index/funding/OI **HAVE** via REST `status/deriv`; liq **HAVE** via public WS `status` key `liq:global` (**W7-P0a**).

---

## Per-venue detail

### VenueId 1 — `synthetic` (test)

| Bucket | Content |
|---|---|
| **HAVE** | T / Q / L2 / Candles (`QUOTE` + `CANDLE` wire cmds, peer-parity #112); Stats24h (`STATS24H`); engine status; offline reconnect / gap fixtures |
| **AVAILABLE** | Nothing venue-real — synthetic only |
| **GAP** | None for §2.1 public MD |
| **N/A** | Mark / index / funding / OI / liq (spot test segment); catalog `--live` (test venue); private; maturity beta path |

---

### VenueId 2 — `binance-spot` (**alpha+**)

| Bucket | Content |
|---|---|
| **HAVE** | `@trade` T, `@bookTicker` Q, depth L2 (U/u buffer+snapshot), `@kline_*` candles; `@ticker` → `Statistics24h` (+ secondary Quote; bookTicker BBO kept); catalog `--live` (`exchangeInfo`); status; public laptop canary **9/9** + ~31m soak (≠ scheduled) |
| **AVAILABLE** | `@miniTicker`; aggTrade; partial book `@depth5/10/20`; `@avgPrice`; REST historical klines beyond live subscribe |
| **GAP** | **CODE:** private user-data is blocked pending authenticated WebSocket API migration; none for applicable public MD event types. **OPS:** scheduled canary ≥7 (**OPS-B**) to unlock public beta (not a CODE worker item) |
| **N/A** | Mark / index / funding / OI / liq (spot) |

---

### VenueId 3 — `binance-usdm` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | T / Q / L2 (`pu`); `@kline_*`; `@ticker` Stats24h; mark/index/funding streams; dedicated `@indexPrice@1s`; OI REST timer; `@forceOrder` liq; catalog live; status |
| **AVAILABLE** | Composite index; continuous kline variants; basis / premium index history REST; multi-asset mode metadata |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Spot-only channels; private USD-M (not shipped — private crate is Spot-only today) |

---

### VenueId 4 — `okx-spot` (**alpha+**)

| Bucket | Content |
|---|---|
| **HAVE** | trades / `tickers` → Quote + `Statistics24h`; books L2; `candle*`; catalog live; status; private Spot library **alpha** (explicit account sink; daemon gated); laptop canary **9/9** + soak |
| **AVAILABLE** | `trades-all`; `sprd-*` spread; REST history candles |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Mark / funding / OI / liq (spot); private SWAP (ponytail: Spot orders only) |

---

### VenueId 5 — `bybit-linear` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | publicTrade / orderbook / tickers (mark/index/funding/OI + Stats24h) / kline / `allLiquidation`; L2 corpus; catalog live; status; laptop `live_ignored` once |
| **AVAILABLE** | liquidation orderbook; ADL alerts; insurance fund; REST OI history |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Spot-only segment fields |

---

### VenueId 6 — `bybit-spot` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | T / Q / L2 / `kline.*` candles; tickers → Stats24h; catalog live; status; private Spot library **alpha** (explicit account sink; daemon gated) |
| **AVAILABLE** | RPI / LT channels; orderbook depth variants |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Mark / index / funding / OI / liq (spot) |

---

### VenueId 7 — `kraken-spot` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | trade / spread or book→Q / book L2 (CRC32) / `ohlc` candles; `ticker` → Quote + Stats24h; catalog live; status; laptop `live_ignored` |
| **AVAILABLE** | `ohlc` extra intervals; ownTrades / openOrders (auth) |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Derivatives fields on spot WS |

---

### VenueId 8 — `deribit` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | trades / ticker (Q+mark/index/funding/OI/Stats24h) / dedicated `deribit_price_index` / `book.*.100ms` L2 / `chart.trades` candles / liq via trades `liquidation` field; catalog live; status; laptop `live_ignored` |
| **AVAILABLE** | Options + Greeks (§2.2); `book.*.raw` (auth-only — public N/A); platform state; user portfolio (auth); index constituents |
| **GAP** | **CODE:** none for applicable public MD event types (perp/futures public) |
| **N/A** | Public `.raw` book (auth required); dedicated public liq channel (venue embeds in trades — **HAVE** via field); spot segment |

---

### VenueId 9 — `okx-swap` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | T / Q / L2 / candles; `tickers` → Quote + `Statistics24h`; mark-price / index-tickers / funding-rate; `open-interest`; `liquidation-orders`; linear **and** inverse instruments on same VenueId (`ctType`); L2 corpus; catalog live; status |
| **AVAILABLE** | estimated delivery / settlement; ADL; positions channel (auth) |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Spot-only fields |

---

### VenueId 10 — `okx-futures` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | Same public channel set as SWAP for dated futures (+ inverse kinds), including `tickers` → `Statistics24h`; catalog live; status; L2 corpus |
| **AVAILABLE** | auth account polish |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Spot-only; perpetual-only semantics |

---

### VenueId 11 — `bybit-inverse` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | Peer of linear: T/Q/L2/candles/mark/index/funding/OI/Stats24h/`allLiquidation`; daemon `segment=inverse`; L2 corpus; catalog live; status |
| **AVAILABLE** | Same extras as linear |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Spot-only fields |

---

### VenueId 12 — `binance-coinm` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | T / `@bookTicker` Q / L2 `pu` (dapi) / `@kline_*` / `@ticker` Stats24h / mark/index/funding / dedicated pair `@indexPrice@1s` / OI REST timer / `@forceOrder` liq; catalog live; status; L2 corpus (R2/R3 **DONE**) |
| **AVAILABLE** | continuous contract klines; basis history |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | Spot-only; private Coin-M (not shipped) |

---

### VenueId 13 — `kraken-futures` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | trade / ticker→Q+mark/index/funding/OI/Stats24h / book L2; liq via trade `type=liquidation` (#109); REST charts candles timer (**W6-P0c** / #198); catalog live; status; corpora |
| **AVAILABLE** | challenge / fills (auth); flexible futures extras |
| **GAP** | **CODE:** none for applicable public MD event types (WS candles correctly absent) |
| **N/A** | Native public WS candles (venue does not supply) — **correct N/A**; REST candles **HAVE** |

---

### VenueId 14 — `bitstamp` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | `live_trades` T; BBO from `order_book`; `diff_order_book` L2; REST OHLC candles (#119); REST `/ticker/{pair}/` Stats24h; catalog `--live`; status; L2 + candle corpora; laptop `live_ignored` |
| **AVAILABLE** | `live_orders` L3-ish; `live_full_order_book`; private account WS |
| **GAP** | **P0:** none §2.1. **P1:** live canary (**OPS**). **P2:** L3 (§2.2 deferred); private Spot |
| **N/A** | Mark / funding / OI / liq (spot); WS native candles / Stats24h (REST **HAVE**) |

---

### VenueId 15 — `gemini` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | Current multiplexed WebSocket `@trade` / `@bookTicker` / differential `@depth@100ms` with `snapshot=-1`, first-frame snapshot semantics, sequence-gap reconnect, and per-instrument request-scoped subscriptions; exact per-instrument REST candles (#119); exact per-instrument REST `/v2/ticker` + `/v1/pubticker` Stats24h; REST readiness waits for successful requested responses and degrades on later failure; status; current-format L2 + candle corpora; catalog `--live` via `/v1/symbols` (default scales; optional capped N+1 details `GEMINI_LIVE_DETAILS_MAX`, default 0). |
| **AVAILABLE** | Auction / block channels; private order events; unbounded details without cap (we refuse unbounded N+1) |
| **GAP** | **P0:** none §2.1 offline. **P1:** checked-in current-protocol live evidence, L2 live canary, scheduled canary, and soak (**OPS**). **P2:** private |
| **N/A** | Mark/funding/OI/liq (spot); WS candles / Stats24h (REST **HAVE**) |

---

### VenueId 16 — `coinbase-spot` Exchange Classic (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | `matches` T / `ticker` → Q + Stats24h / public `status` → `InstrumentUpdate`; authenticated `level2` HMAC subscribe with env-only credentials; snapshot/delta book + reconnect + replay; REST `/products/{id}/candles`; catalog live; engine status; laptop public T/Q `live_ignored` |
| **AVAILABLE** | Authenticated `full` / `level3`; `rfq_matches`; authenticated user channels |
| **GAP** | **CODE:** none for applicable §2.1. Credential-backed L2 canary, scheduled canary, and soak remain OPS. |
| **N/A** | WS candle channel (venue has none — REST **HAVE**); INTX uses VenueId **19** (env-auth MD **HAVE**, #215) |

---

### VenueId 17 — `bitfinex` (**alpha**, peer-parity)

| Bucket | Content |
|---|---|
| **HAVE** | WS v2 `trades` / `ticker` (Q+Stats24h) / `book` (chanId map); WS candles; catalog `--live`; R6 status/catalog (#127/#134); L2 corpus; canary hook `INCLUDE_ALPHA` |
| **AVAILABLE** | authenticated wallets/orders |
| **GAP** | **CODE:** none for **spot** applicable public MD |
| **N/A** | Spot mark/funding/OI/liq; claiming derivatives on VenueId 17 (use **20**) |

---

### VenueId 18 — `coinbase-adv` Advanced Trade (**alpha**, peer-parity)

| Bucket | Content |
|---|---|
| **HAVE** | public WS `market_trades` / `ticker` (Q+Stats24h) / `level2` (`l2_data`) / `status` → InstrumentUpdate (#174/#193); REST candles (M1–D1); heartbeats; catalog `--live` `/market/products`; L2 corpus; laptop T/Q/L2/candle smoke |
| **AVAILABLE** | authenticated Advanced Trade private |
| **GAP** | **CODE:** none for applicable public MD event types |
| **N/A** | WS `candles` subscribe (**W6-P1c**: REST preferred; 5m-only if received); Private/authenticated Advanced Trade; Classic Exchange channels (stay on **16**); INTX (**19** env-auth MD) |

---

### VenueId 19 — `coinbase-intl` (**alpha**, env-auth MD)

| Bucket | Content |
|---|---|
| **HAVE** | Env-auth WS MD (`MATCH`/`LEVEL1`/`LEVEL2`) with HMAC `CBINTLMD` subscribe → T/Q/L2; REST `/instruments` catalog `--live`; engine status; offline fixtures; daemon `segment=intl`; credentials env-only (`COINBASE_INTL_*`) — **#215** |
| **AVAILABLE** | Further INTX REST/WS surfaces beyond shipped T/Q/L2 (quote REST, funding/mark polish, …) |
| **GAP** | — (applicable env-auth MD T/Q/L2 + catalog closed **#215**) |
| **N/A** | Anonymous public WS (venue requires HMAC); candles/Stats24h/mark/index/funding/OI/liq not in shipped INTX MD scope; order placement |

---

### VenueId 20 — `bitfinex-deriv` (**alpha**)

| Bucket | Content |
|---|---|
| **HAVE** | WS v2 T/Q/L2 + candles; REST `status/deriv` → mark/index/funding/OI; ticker Stats24h; WS `status`/`liq:global` → `Liquidation` (**W7-P0a**); catalog live; R6 status; L2 corpus; `session_config_from_catalog`; `INCLUDE_ALPHA` canary; daemon `segment=deriv`; fixtures (**W6-P1b** / #206, peer-parity #210) |
| **AVAILABLE** | authenticated wallets/orders; funding/margin polish; WS `status`/`deriv:SYMBOL` live stream (REST poll already **HAVE** for mark/index/funding/OI — upgrade only); REST `GET /v2/liquidations/hist` (WS path preferred) |
| **GAP** | — (liq closed **W7-P0a**) |
| **N/A** | Claiming der. on VenueId **17** |

---

## Private venues status

Spec §2.2 deferred; Phase 6 / ADR-009 — **alpha only**. Public MD never requires this crate. **Not in CODE worker P0/P1 list.**

| Public VenueId | Private surface | Fixture SM | Live (`live` feature) | Daemon TOML | Laptop script | Maturity |
|---:|---|:-:|:-:|:-:|:-:|---|
| 2 `binance-spot` | authenticated WebSocket API migration scaffold | decoder only | **blocked** | rejected | **blocked** | **blocked: protocol migration** |
| 4 `okx-spot` | private WS account/orders/fills | Y | Y | `[private.okx_spot]` | Y | **alpha** |
| 6 `bybit-spot` | private WS auth + account | Y | Y | `[private.bybit_spot]` | Y | **alpha** |
| 1,3,5,7–20 | — | — | — | — | — | **not shipped** |

---

## Cross-cutting (closed Wave-6 + closed W7-P0)

| ID | Item | Spec | Status |
|---|---|---|---|
| **W2-R10** | Coinbase International / derivatives | Phase 3.5 stretch | **DONE** — VenueId **19** env-auth MD T/Q/L2 + catalog (**#215**; was W6-P1a **SKIP** #191) |
| **W2-R11** | Native per-venue `Statistics24h` | §8.7 / continuous improvement | **DONE** — majors **HAVE** (**W6-P0a/b**); Bitstamp/Gemini REST timers **HAVE** (**W7-P0b/c**) |
| **W2-R12** | Kraken Futures REST candles | candles convenience | **DONE** (**W6-P0c**, #198) |
| **W7-P0a** | Bitfinex-deriv liquidations | §2.1 liq | **DONE** — WS `liq:global` |
| **W7-P0b** | Bitstamp Stats24h REST | §8.7 / peer MD | **DONE** — `/api/v2/ticker/{pair}/` timer |
| **W7-P0c** | Gemini Stats24h REST | §8.7 / peer MD | **DONE** — `/v2/ticker` + `/v1/pubticker` timer |
| **W7-P0** | Continuous-improvement public MD track | §2.1 / peer MD | **CLOSED** — P0a/b/c **DONE**; **19** env-auth MD **HAVE** (#215) |
| **§2.2** | L3 books, options/Greeks, auth depth | deferred architecture | **P2** / out of v1 MUST |
| **R20** | Private live soak / more venues | Phase 6 | **P2** (OPS secrets) |
| Index | Dedicated index streams | peer-parity | **DONE** (#190) Binance UM/CM + Deribit |
| Bitfinex der. | VenueId **20** | segment | **DONE** (**W6-P1b**, #206) + liq **W7-P0a** |

---

## Top implementable CODE gaps for workers (ordered)

Exclude: **OPS-A…E**, §2.2 L3/options/private, AVAILABLE extras that do not map to production event types (`miniTicker`, partial books, ADL, insurance fund, …).

| Rank | ID | Work | Venue(s) | Endpoints | Acceptance |
|---:|---|---|---|---|---|
| — | — | **none open** — **W7-P0 closed**; public CODE plateau VenueIds **1–20** | — | — | — |

**Closed W7-P0a:** bitfinex-deriv `Liquidation` via WS `status`/`liq:global`.
**Closed W7-P0b/c:** Bitstamp/Gemini `Statistics24h` REST ticker timers (#213).
**Closed W2-R10 / #215:** Coinbase Intl VenueId **19** env-auth MD T/Q/L2 + catalog REST.

**P1 CODE:** none open — applicable channels on VenueIds **1–20** at **CODE plateau**.

Historical implementation waves closed the listed public channel gaps. This
audit tracks the resulting current coverage, not internal branch orchestration.

## OPS maturity (excluded from CODE worker list)

| Rank | ID | Work | Unlocks |
|---:|---|---|---|
| — | **OPS-A** | Scheduled live canary ≥7 for Spot pair (2+4) | **beta** |
| — | **OPS-B** | Multi-day live soak + live chaos inject | **stable** path |
| — | **OPS-C** | Publish tag attestation + SBOM | §3.9 |
| — | **OPS-D** | Explicit “1.0 allowed” after ≥2 stable | production-ready claim |

---

## Honesty bar

- Do **not** claim beta/stable/1.0/production-ready from this audit.
- Do **not** flip `maturity_matrix.md` without scheduled evidence.
- Do **not** treat KF WS candles as a gap — WS is **N/A**; REST **HAVE** (**W6-P0c**).
- Do **not** open “duplicate Advanced Trade T/Q/L2” or “per-adapter Status event” theater (Wave-3 / W6-P1c closed).
- Do **not** invent gaps from AVAILABLE extras that lack production event types.
- **Do** treat public REST/WS paths that fill existing `MarketEvent` types as CODE gaps (W7-P0a/b/c closed).
- VenueId **19** env-auth MD **HAVE** ≠ anonymous public WS; still **alpha**, **not** beta.
- **W7-P0** closed + public CODE plateau on VenueIds **1–20** does **not** unlock maturity; production readiness still **OPS-A…E**.

## Related

- Maturity: [`maturity_matrix.md`](./maturity_matrix.md)
- Venue IDs: [`venue_ids.md`](./venue_ids.md)
- Runtime evidence: [`../ops/soak_results.md`](../ops/soak_results.md)
