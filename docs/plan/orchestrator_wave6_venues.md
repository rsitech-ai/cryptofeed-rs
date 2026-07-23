# Orchestrator Wave-6 — public MD gaps per venue

**Role:** prioritized **worker packages** to close **public market-data** feature/endpoint gaps (channels, REST discovery, candles, liquidations, status, peer public endpoints).  
**Not OPS maturity.** Does **not** unlock beta / stable / 1.0.  
**Audit input:** [`venue_channel_audit.md`](./venue_channel_audit.md) (#184) — §2.1 primary channels **HAVE**/N/A; AVAILABLE rows + re-scope list **A–E** become Wave-6 packages.  
**Parent:** [`orchestrator_wave5.md`](./orchestrator_wave5.md) · [`orchestrator_remaining.md`](./orchestrator_remaining.md) · [`maturity_matrix.md`](./maturity_matrix.md)  
**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md) §2.1 / §8.7 / Phase 3.5  
**Tip audited:** `origin/main` @ `7d0261b` / #217
**Updated:** 2026-07-22  

**Verdict:** Wave-6 public MD packages **DONE / SKIP**. **W7** continuous-improvement public MD **DONE** → **public MD CODE plateau** (VenueIds **1–20**). Remaining for §3 / §36 = **OPS-A…E only**.

**Non-negotiable:**

- **Not production-ready. Not beta. Not stable. Not 1.0.**
- Spec §3 / §36 still **OPS-A…E only**. Wave-6 CODE **does not** flip maturity.
- Audit: **no open P0 §2.1 channel CODE** on ids **1–20** (19 **SKIP**). Wave-6 was **product expansion** of AVAILABLE public endpoints peers emit that we drop — not invented §2.1 debt.
- No maturity-flip PRs. No “scheduled = N” claims.

Branch prefix: `feat/andrzej_orch_w6_<package>` / worker `feat/andrzej_w6_<package>` / plateau `feat/andrzej_w6_plateau_*`  
Merge policy: **merge commits** (no squash). Ignore CI red while billing blocked.

---

## 0. Verdict — Wave-6 CODE plateau

[`venue_channel_audit.md`](./venue_channel_audit.md) ranks real maturity P0s as **OPS-A…E only**. Product-scoped public MD packages **A–E** (plus Gemini live / catalog status / P1 segments / docs) are **closed**:

| Audit ID | Item | Wave-6 package | Status |
|---|---|---|---|
| **A** / **W2-R11** | Native `Statistics24h` on major tickers | **W6-P0a**, **W6-P0b** | **DONE** (#196/#201/#207) |
| **B** / **W2-R12** | Kraken Futures REST candles | **W6-P0c** | **DONE** (#198) |
| **C** / **DOC-COINM** | Binance README Coin-M OI/liq lie | **W6-P2a** | **DONE** (#189) |
| **D** / **W2-R10** | Coinbase International VenueId **19** | **W6-P1a** | **SKIP** (#191) |
| Gemini `--live` | Audit: stub N/A was bulk details | **W6-P0d** | **DONE** (#202) |
| Catalog status map | Hardcoded `Active` after live filter | **W6-P0e** | **DONE** (#200) |
| Bitfinex der. segment | Spot **17** only | **W6-P1b** | **DONE** (#206) VenueId **20** |
| Adv remainder | L2 corpus / status / WS candles choice | **W6-P1c** | **DONE** (#193) |
| Index streams | Dedicated index on Binance UM/CM + Deribit | (peer-parity) | **DONE** (#190) |
| Matrix tip sync | Board + audit + drive honesty | **W6-P2b** | **DONE** (this PR) |

| Track | Status after Wave-6 plateau |
|---|---|
| Primary §2.1 T/Q/L2 + der. mark/index/funding/OI/liq on **1–18** + **20** | **HAVE** or segment **N/A** (19 **SKIP**) |
| Engine `VenueStatus` / `InstrumentUpdate` | **HAVE** (engine-owned) |
| Native `Statistics24h` from venue wires | **HAVE** on major tickers (**W6-P0a/b**) — VenueIds **2–13, 16–18, 20**; Bitstamp/Gemini REST ticker **HAVE** |
| Catalog instrument `status` field | **HAVE** (**W6-P0e**) |
| Kraken Futures candles | WS **N/A**; REST charts **HAVE** (**W6-P0c**) |
| Gemini `catalog --live` | Symbols list **HAVE** (**W6-P0d**); unbounded details **N/A** |
| Liquidations on shipped der. | **HAVE** (channel or trade tag). Spot **N/A**. Bitfinex-deriv liq **N/A** |
| Maturity / §3 / §36 | **OPS-A…E only** (unchanged) |

---

## 1. Gap inventory (public MD only) — closed

Legend:

| Mark | Meaning |
|---|---|
| **HAVE** | Offline SessionMachine emits typed event |
| **DROP** | Public wire/REST we already use (or trivial add) carries the field — ignored |
| **MISS** | ~~Public endpoint we do not subscribe or poll~~ — none open in Wave-6 scope |
| **N/A** | Segment or venue has no public path |

### 1.1 Matrix (product-expansion cells)

| VenueId | Code | Native `Statistics24h` | Catalog status map | Candles path | `catalog --live` | Liq |
|--------:|------|:----------------------:|:------------------:|:------------:|:----------------:|:---:|
| 2 | binance-spot | **HAVE** (`@ticker`) | **HAVE** (map) | HAVE | HAVE | N/A |
| 3 | binance-usdm | **HAVE** (`@ticker`) | **HAVE** | HAVE | HAVE | HAVE |
| 12 | binance-coinm | **HAVE** (`@ticker`) | **HAVE** | HAVE | HAVE | HAVE |
| 4 | okx-spot | **HAVE** (`tickers`) | **HAVE** | HAVE | HAVE | N/A |
| 9/10 | okx-swap/futures | **HAVE** | **HAVE** | HAVE | HAVE | HAVE |
| 5/11 | bybit-linear/inverse | **HAVE** (tickers 24h + der.) | **HAVE** | HAVE | HAVE | HAVE |
| 6 | bybit-spot | **HAVE** | **HAVE** | HAVE | HAVE | N/A |
| 7 | kraken-spot | **HAVE** (ticker) | **HAVE** | HAVE | HAVE | N/A |
| 13 | kraken-futures | **HAVE** (ticker) | **HAVE** | **HAVE** REST | HAVE | HAVE |
| 8 | deribit | **HAVE** (ticker.stats) | **HAVE** | HAVE | HAVE | HAVE |
| 14 | bitstamp | **N/A** | **HAVE** | HAVE REST | HAVE | N/A |
| 15 | gemini | **N/A** | **HAVE** | HAVE REST | **HAVE** (W6-P0d) | N/A |
| 16 | coinbase-spot | **HAVE** (`ticker`) | **HAVE** | HAVE REST | HAVE | N/A |
| 17 | bitfinex | **HAVE** (ticker) | **HAVE** | HAVE WS | HAVE | N/A (spot) |
| 18 | coinbase-adv | **HAVE**; WS candles **SKIP** (REST) | **HAVE** | HAVE REST | HAVE | N/A |
| 19 | coinbase-intl | **SKIP** | **SKIP** | **SKIP** | **SKIP** | **SKIP** |
| 20 | bitfinex-deriv | **HAVE** | **HAVE** | HAVE WS | HAVE | **N/A** |

Notes:

1. **Liquidations:** shipped derivatives **HAVE** except bitfinex-deriv (**N/A** — venue has no clean public liq path in scope).
2. **Engine status** remains **HAVE** — instrument status from discovery is **HAVE** (**W6-P0e**).
3. Binance Coin-M OI/liq docs honesty — **DONE** (**W6-P2a**, #189).
4. Dedicated index streams — **DONE** (#190) on Binance UM/CM + Deribit (OKX/Bybit already had index).

---

## 2. Worker packages — all **DONE** / **SKIP**

Acceptance for every package: offline SessionMachine fixtures (exact `Fixed`); matrix/README/venue_ids honesty; **no** maturity claim.

### P0 — close AVAILABLE public MD on shipped VenueIds **1–18**

| Package | Work | Venues | Endpoints / channels | Status |
|---|---|---|---|---|
| **W6-P0a** Stats24h from **already-subscribed** tickers | Emit `MarketEvent::Statistics24h` alongside `Quote` when ticker wire carries 24h OHLC/volume | **4–11, 13, 16–18** (14/15 = REST ticker timers) | OKX/Bybit `tickers`; Kraken Spot `ticker`; Deribit `ticker`; KF `ticker`; Coinbase Classic/Adv `ticker`; Bitfinex `ticker` | **DONE** (#196/#201/#207 + REST 14/15) |
| **W6-P0b** Binance `@ticker` (24hr) | Spot/USD-M/Coin-M `@ticker` → Quote + Statistics24h | **2, 3, 12** | Opt-in `@ticker` per symbol → Quote + Statistics24h (keep bookTicker BBO) | **DONE** (#196/#207) |
| **W6-P0c** KF REST candles | Only der. family without candles | **13** | Public REST charts/history on `CANDLE_TIMER_ID` | **DONE** (#198) |
| **W6-P0d** Gemini `catalog --live` | Peers have live discovery; audit N/A is **bulk details**, not symbols list | **15** | `GET /v1/symbols` → defs with **default scales**; optional capped N+1 (`GEMINI_LIVE_DETAILS_MAX`, default 0) | **DONE** (#202) |
| **W6-P0e** Catalog instrument status map | parsers map venue status → `InstrumentStatus` | **2–18** (+ **20**) | Map venue status → `InstrumentStatus` | **DONE** (#200) |

### P1 — peer segment expansion (new VenueIds)

| Package | Work | New VenueId | Public surface | Status |
|---|---|---|---|---|
| **W6-P1a** Coinbase International | Spec Phase 3.5; Classic **16** + Adv **18** are spot protocols | **19** `coinbase-intl` | INTX MD WS requires HMAC subscribe (`CBINTLMD`); REST instruments/quote only | **SKIP** (#191) |
| **W6-P1b** Bitfinex derivatives | Spot **17** only | **20** `bitfinex-deriv` | Public der. T/Q/L2 + REST status/deriv mark/index/funding/OI; liq **N/A** | **DONE** (#206) |
| **W6-P1c** Adv remainder / WS candles | L2 corpus + status; WS `candles` is 5m-only; REST covers M1–D1 | **18** | REST preferred; SM does **not** subscribe WS candles (decode if received) | **DONE** (#193) |

### P2 — honesty / polish

| Package | Work | Status |
|---|---|---|
| **W6-P2a** Docs drift | Binance README Coin-M OI/liq; stale Adv “REST-only” lines | **DONE** (#189) |
| **W6-P2b** Matrix tip sync | Refresh this board + matrix + remaining + audit + drive | **DONE** (this PR) |

### Explicitly out of Wave-6 (still open — maturity / platform)

| Item | Why |
|---|---|
| OPS-A…E, scheduled canary, multi-day soak | Maturity — audit P0 ranks 1–5 |
| Private/auth streams, order entry | §2.2 / Phase 6 |
| R18–R31 platform YAGNI | Not venue public MD |
| New families (KuCoin, Gate, BitMEX, …) | Only with explicit product + VenueId claim |
| KF WS candles | Venue has none — REST **HAVE** (**P0c**) |
| Per-adapter `VenueStatus` theater | Engine-owned; Wave-3 closed |

---

## 3. Worker order (closed)

```text
P0:
  W6-P0a  stats24h-from-tickers     (4–11,13,16–18) **DONE** (#196/#201/#207)
  W6-P0b  binance-ticker-24h        (2,3,12) **DONE** (#196/#207)
  W6-P0c  kf-rest-candles           (13) **DONE** (#198)
  W6-P0d  gemini-catalog-live       (15) **DONE** (#202)
  W6-P0e  catalog-instrument-status (2–18) **DONE** (#200)

P1:
  W6-P1a  coinbase-intl VenueId 19  **SKIP** (#191)
  W6-P1b  bitfinex-deriv VenueId 20 **DONE** (#206) + peer-parity (catalog/R6/L2/`INCLUDE_ALPHA`)
  W6-P1c  coinbase-adv remainder    **DONE** (#193)

P2:
  W6-P2a  docs honesty              **DONE** (#189)
  W6-P2b  matrix tip sync           **DONE** (this PR)

Peer-parity (shipped with W6):
  index streams Binance UM/CM + Deribit **DONE** (#190)

STOP for maturity: OPS-A → OPS-B → OPS-C → OPS-D → OPS-E
```

---

## 4. Per-venue package map

| VenueId | Code | Packages |
|--------:|------|----------|
| 2 | binance-spot | P0b **DONE**, P0e **DONE**, P2a **DONE** |
| 3 | binance-usdm | P0b **DONE**, P0e **DONE**, index **DONE** |
| 12 | binance-coinm | P0b **DONE**, P0e **DONE**, P2a **DONE**, index **DONE** |
| 4 | okx-spot | P0a **DONE**, P0e **DONE** |
| 9 | okx-swap | P0a **DONE**, P0e **DONE** |
| 10 | okx-futures | P0a **DONE**, P0e **DONE** |
| 5 | bybit-linear | P0a **DONE**, P0e **DONE** |
| 6 | bybit-spot | P0a **DONE**, P0e **DONE** |
| 11 | bybit-inverse | P0a **DONE**, P0e **DONE** |
| 7 | kraken-spot | P0a **DONE**, P0e **DONE** |
| 13 | kraken-futures | P0a **DONE**, P0c **DONE**, P0e **DONE** |
| 8 | deribit | P0a **DONE**, P0e **DONE**, index **DONE** |
| 14 | bitstamp | P0a **N/A**, P0e **DONE** |
| 15 | gemini | P0d **DONE**, P0a **N/A**, P0e **DONE** |
| 16 | coinbase-spot | P0a **DONE**, P0e **DONE** |
| 17 | bitfinex | P0a **DONE**, P0e **DONE** |
| 18 | coinbase-adv | P0a **DONE**, P0e **DONE**, P1c **DONE** |
| 19 | coinbase-intl | **P1a SKIP** |
| 20 | bitfinex-deriv | **P1b DONE** + peer-parity |

---

## 5. Honesty / non-claims

| Evidence | Allowed claim |
|---|---|
| Any W6-P0/P1 merge | Public MD surface progress; still **alpha / alpha+** |
| Native Statistics24h | still **not** scheduled beta |
| New VenueId **19** SKIP / **20** alpha | still **not** production-ready |
| All Wave-6 CODE done | still **not** beta / stable / 1.0 |

**Not production-ready without OPS-A…E.**

---

## 6. Delta vs Wave-5 / audit

| Item | Wave-5 / audit #184 | Wave-6 plateau |
|---|---|---|
| Focus | CODE plateau + laptop OPS; audit AVAILABLE = P2 | **Public MD packages closed** |
| §2.1 primary channels 1–18 | Exhausted | Unchanged baseline + **20** alpha |
| Statistics24h | Synthetic only / audit A | Native from venue wires (**P0a/b DONE**) |
| KF candles | WS N/A | REST path (**P0c DONE**) |
| Gemini `--live` | N/A (bulk details) | Symbols (+ capped details) (**P0d DONE**) |
| Catalog status | Always Active | Map venue fields (**P0e DONE**) |
| New families | Deferred | Coinbase Intl **19 SKIP** / Bitfinex der. **20 DONE** |
| Adv remainder | Peer-parity partial | L2/status/REST candles choice (**P1c DONE**) |
| Index streams | Partial (OKX/Bybit) | Binance UM/CM + Deribit dedicated (**#190 DONE**) |
| Maturity path | OPS-A…E | **Unchanged** |
