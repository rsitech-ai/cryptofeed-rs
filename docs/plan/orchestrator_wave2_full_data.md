# Orchestrator Wave 2 — full market-data channels

**Role:** CODE-only plan for *all public §2.1 channels on all exchange families*  
**Base tip:** `origin/main` @ `a03119c` (post-#122 tip sync)  
**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md) §2.1 + Phase 3.5  
**Prior manifest:** [`orchestrator_remaining.md`](./orchestrator_remaining.md) (R1–R17 closed; Wave-2 P0+P1 closed)  
**Drive:** [`production_drive.md`](./production_drive.md)  
**Updated:** 2026-07-22  

## Product override (this wave)

User scoped **all market data channels for all exchanges** + full Spec §2.1 CODE surface.  
That **reopens** formerly-P2 Coinbase (**R21**) and corrects Kraken Futures rows that were marked N/A but are **venue-supplied and unparsed**.

CI billing / Actions red = **ignored** (OPS-A). This wave does **not** claim beta/stable/1.0.

---

## Wave-2 landed (P0 honesty)

| Package | Status | PR | Notes |
|---|---|---|---|
| **W2-P0a** Kraken Futures ticker enrich + liq | ✅ **DONE** | **#109** | Mark/Index/Funding/OI + `type=liquidation` |
| **W2-P0b** Coinbase VenueId + scaffold | ✅ **DONE** | **#113** | VenueId **16** (`coinbase-spot`); **not** 14/15 |
| **W2-P0c** Coinbase T/Q/L2 | ✅ **DONE** | **#113** | Exchange `matches` / `ticker` / `level2` |
| **W2-P0d** Spot candles REST (14/15/16) | ✅ **DONE** | this PR | REST poll timer (Binance OI pattern) |
| Bitstamp + Gemini spot T/Q/L2 | ✅ **DONE** (adjacent) | **#111** | VenueId **14** / **15** — forced Coinbase off 14 |
| Docs tip sync | ✅ **DONE** | **#116** | Matrix + drive + audit honesty |
| Peer channel parity (synthetic QUOTE/CANDLE) | ✅ **DONE** (adjacent) | **#112** | Spot peer channels on test venue |
| **W2-P1** corpus / identity proofs | ✅ **DONE** | **#117** | KF ticker+liq + L2; Coinbase/Bitstamp/Gemini L2 `.mfr` |

---

## CODE plateau (implementable exchange-data CODE exhausted)

Wave-2 **P0 channel gaps** and **P1 offline corpora** are closed on shipped venues (ids 1–16). There is **no further implementable exchange-data CODE** in this wave: every matrix cell is **HAVE** or **N/A**. Production readiness (beta/stable/1.0) is **OPS-A…E only** — see [`production_drive.md`](./production_drive.md) §0.1 / USER OPS CHECKLIST. Remaining product CODE is **P2 YAGNI** only (R18–R31, W2-R10–R12, Advanced Trade candles).

**Not production-ready. Not beta. Not stable. Not 1.0.**

---

## Channel matrix @ `a03119c` (P0+P1 closed)

Legend:

| Mark | Meaning |
|---|---|
| **HAVE** | Offline SessionMachine path emits the typed `MarketEvent` (fixture and/or corpus) |
| **MISSING** | Venue supplies it on public WS (or required REST timer) and Spec §2.1 wants it — **not implemented** |
| **N/A** | Spot segment has no native mark/funding/OI/liq, or venue has no native public candle WS |

**Status** = engine-owned `MarketEvent::{VenueStatus,InstrumentUpdate}` (+ optional `Statistics24h` via synthetic `STATS24H`). Adapters stay I/O-free; status is HAVE whenever the venue is wired into the engine.

| VenueId | Code | Trades | Quote/BBO | L2 | Candles | Mark | Index | Funding | OI | Liq | Status |
|--------:|------|:------:|:---------:|:--:|:-------:|:----:|:-----:|:-------:|:--:|:---:|:------:|
| 1 | synthetic | HAVE | HAVE | HAVE | N/A | N/A | N/A | N/A | N/A | N/A | HAVE |
| 2 | binance-spot | HAVE | HAVE | HAVE | HAVE | N/A | N/A | N/A | N/A | N/A | HAVE |
| 3 | binance-usdm | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE |
| 12 | binance-coinm | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE |
| 4 | okx-spot | HAVE | HAVE | HAVE | HAVE | N/A | N/A | N/A | N/A | N/A | HAVE |
| 9 | okx-swap | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE |
| 10 | okx-futures | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE |
| 5 | bybit-linear | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE |
| 6 | bybit-spot | HAVE | HAVE | HAVE | HAVE | N/A | N/A | N/A | N/A | N/A | HAVE |
| 11 | bybit-inverse | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE |
| 7 | kraken-spot | HAVE | HAVE | HAVE | HAVE | N/A | N/A | N/A | N/A | N/A | HAVE |
| 13 | kraken-futures | HAVE | HAVE | HAVE | **N/A** | **HAVE** | **HAVE** | **HAVE** | **HAVE** | **HAVE** | HAVE |
| 8 | deribit | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE | HAVE |
| 14 | bitstamp | HAVE | HAVE | HAVE | **HAVE** | N/A | N/A | N/A | N/A | N/A | HAVE |
| 15 | gemini | HAVE | HAVE | HAVE | **HAVE** | N/A | N/A | N/A | N/A | N/A | HAVE |
| 16 | coinbase-spot | HAVE | HAVE | HAVE | **HAVE** | N/A | N/A | N/A | N/A | N/A | HAVE |
| 18 | coinbase-adv | N/A | N/A | N/A | **HAVE** | N/A | N/A | N/A | N/A | N/A | HAVE |

### Matrix notes

1. **Kraken Futures (`13`)** public `ticker` carries `markPrice`, `index`, `funding_rate` / `next_funding_rate_time`, `openInterest` — **HAVE** via W2-P0a **#109** (`FuturesDecoded::Ticker`). Trade `type=liquidation` → Trade + Liquidation — **HAVE**. No public candle WS → candles via REST charts timer (**HAVE** alpha, W2-R12).
2. **Coinbase (`16`)** = Exchange WS `matches` / `ticker` plus env-authenticated `level2` — **HAVE** (alpha; AUTH-L2 closes the current signing contract after the original #113 fixtures). VenueId **14**/**15** = bitstamp/gemini (**#111**); do **not** reuse. Exchange WS has no candle channel; candles **HAVE** via REST poll timer (Binance OI pattern). Advanced Trade = separate protocol. Spot derivatives fields = **N/A**. Coinbase International = VenueId **19** env-auth MD **HAVE**.
3. Spot rows correctly **N/A** for mark/index/funding/OI/liq.
4. Deribit liq = trades `liquidation` field (**HAVE** via R17); no dedicated liq channel.
5. Prior [`orchestrator_remaining.md`](./orchestrator_remaining.md) claimed Kraken Futures mark/OI/… as `—`; **that was wrong** relative to venue docs — corrected here.

---

## Missing CODE inventory (holes + P2)

### Must-have channel gaps (this wave = **P0**) — **closed**

| ID | Gap | Evidence | Spec |
|---|---|---|---|
| **W2-R1** ✅ | Kraken Futures ticker → Mark / Index / Funding / OI | DONE **#109**: `FuturesDecoded::Ticker` emits Quote + Mark/Index/Funding/OI | §2.1 |
| **W2-R2** ✅ | Kraken Futures trade `type=liquidation` → `MarketEvent::Liquidation` | DONE **#109**: trade tag → Trade + Liquidation (Deribit pattern) | §2.1 |
| **W2-R3** ✅ | Coinbase VenueId **16** + crate scaffold | DONE **#113**: `COINBASE_SPOT_VENUE_ID` / `crates/adapters/coinbase` (14/15 = bitstamp/gemini **#111**) | Phase 3.5 |
| **W2-R4** ✅ | Coinbase trades + Quote/BBO + L2 | DONE **#113**: Exchange `matches` / `ticker` / `level2` fixtures + L2 sync | §2.1 |
| **W2-R5** ✅ | Coinbase + Bitstamp + Gemini candles | DONE: REST poll timer (Binance OI pattern) | §2.1 |

### Should-have for this wave (**P1**) — optional leftovers

| ID | Work | Why |
|---|---|---|
| **W2-R6** ✅ | Kraken Futures capability flags + fixtures/corpus for ticker enrich + liq | DONE: caps assert + `futures_ticker_liq.mfr` + `futures_l2_book.mfr` |
| **W2-R7** ✅ | Coinbase VenueFactory + daemon wire | DONE **#113**/#114: `adapter = "coinbase"`; live canary still alpha gap |
| **W2-R8** ✅ | Sync `maturity_matrix.md` / `venue_ids.md` / `orchestrator_remaining.md` | DONE on tip (ids 14–16 honest) |
| **W2-R9** ✅ | Coinbase L2 book corpus + adapter-testkit assertions | DONE: `coinbase/tests/corpus/spot_l2_book.mfr` identity tests |
| **W2-R13** ✅ | Bitstamp + Gemini L2 corpus (cheap, same pattern) | DONE: `bitstamp`/`gemini` `tests/corpus/spot_l2_book.mfr` |

### Explicitly deferred this wave (**P2** — not channel must-haves)

Carry-forward from R18–R31 / non-channel Spec MAY items. Do **not** start unless product re-scopes:

| ID | Work | Spec |
|---|---|---|
| R18 | Kafka / NATS depth | §2.3 |
| R19 | FFI beyond stub | §2.2 |
| R20 | Private live soak / secrets ops | Phase 6 |
| R22 | prost codegen | ADR-010 |
| R23 | C10 profiles + bench gate | §24 |
| R24 | `fastwebsockets` alternate transport | §14.2 |
| R25 | gRPC / UDS streaming | §20.2 |
| R26 | Arrow / Parquet sink | §18.5 |
| R27 | Shared connection worker pool | §13.2 |
| R28 | Facade crate publish boundary | §7.1 |
| R29 | Config hot reload | §21.4 MAY |
| R30 | Remote control TLS/auth | §20.2 |
| R31 | OpenTelemetry feature | §23.3 |
| **W2-R10** ✅ **SKIP** | Coinbase International VenueId **19** | **SKIP**: INTX MD WS requires HMAC subscribe (`CBINTLMD`); no public T/Q/L2 SessionMachine. Claim + rationale in [`venue_ids.md`](./venue_ids.md). |
| **W2-R11** | Per-venue native `Statistics24h` (beyond synthetic STATS24H) | §8.7 optional depth |
| **W2-R12** ✅ | Kraken Futures REST candle backfill | DONE alpha: REST charts timer + `Capability::Candles` (WS still N/A) |
| **W2-R5b** ✅ | Coinbase Advanced Trade public candles | DONE: VenueId **18** `coinbase-adv`; public REST `/market/products/{id}/candles` timer; T/Q/L2 later on Adv via W5-P0c (Classic **16** remains dual protocol) |

### Unimplemented Spec § that is CODE (outside channel matrix)

Already closed for MUST surface on shipped venues: §10.4 dynamic subs, §17.5 SpillToDisk, §19 control, §20 CLI catalog/plan/benchmark, §11.7 testkit, §27 offline chaos. Wave-2 **P0+P1 closed** (#109/#111/#112/#113/#117). **Implementable exchange-data CODE exhausted.** Remaining product CODE = **P2 YAGNI** only. Maturity / production readiness = **OPS-A…E only**.

---

## Wave-2 priority board (THIS WAVE ONLY)

### P0 — must-have channel gaps — **all closed**

| Package | IDs | Owner crate(s) | Acceptance (offline) |
|---|---|---|---|
| **W2-P0a Kraken Futures ticker enrich** ✅ **DONE #109** | W2-R1, W2-R2 | `marketfeed-adapter-kraken` | Fixture ticker with mark/index/funding/OI → exact Fixed events; `type=liquidation` trade → Trade + Liquidation |
| **W2-P0b Coinbase claim + scaffold** ✅ **DONE #113** | W2-R3 | `venue_ids.md` + `crates/adapters/coinbase` | VenueId(16), `COINBASE_SPOT_VENUE_ID` (14/15 = bitstamp/gemini #111) |
| **W2-P0c Coinbase primary channels** ✅ **DONE #113** | W2-R4 | `marketfeed-adapter-coinbase` | Fixtures: trade, quote, L2 snapshot/delta sync |
| **W2-P0d Spot candles REST** ✅ **DONE** | W2-R5 | coinbase / bitstamp / gemini | REST timer fixtures: exact Fixed Candle |

### P1 — depth / wire / docs for P0 — **all closed**

| Package | IDs | Acceptance |
|---|---|---|
| **W2-P1a KF proofs** ✅ **DONE** | W2-R6 | Caps assert + `futures_ticker_liq.mfr` / `futures_l2_book.mfr` replay identity |
| **W2-P1b Coinbase daemon + live smoke** ✅ **DONE** (daemon) | W2-R7 | Daemon config segment; live canary remains alpha gap |
| **W2-P1c Docs sync** ✅ **DONE** | W2-R8 | Matrices + remaining manifest updated on tip |
| **W2-P1d Coinbase L2 corpus** ✅ **DONE** | W2-R9 | `spot_l2_book.mfr` + live-record identity |
| **W2-P1e Bitstamp/Gemini L2 corpus** ✅ **DONE** | W2-R13 | Same pattern as other spot venues |

### P2 — not this wave unless re-scoped

R18–R31, W2-R11–R12; **W2-R10** Coinbase Intl **SKIP** (VenueId **19** claimed).

---

## Recommended worker package order

```text
1. W2-P0a  kraken-futures-ticker-enrich   ✅ #109
2. W2-P0b  coinbase-venue-scaffold        ✅ #113 VenueId 16 (14=bitstamp, 15=gemini #111)
3. W2-P0c  coinbase-trades-quote-l2       ✅ #113
4. W2-P0d  spot-candles-rest              ✅ Coinbase/Bitstamp/Gemini REST timer
5. W2-P1a  kraken-futures-proofs          ✅
6. W2-P1b  coinbase-daemon-wire           ✅
7. W2-P1d  coinbase-l2-corpus             ✅
8. W2-P1c  docs-matrix-sync               ✅
9. W2-P1e  bitstamp-gemini-l2-corpus      ✅
```

Branch naming for workers: `feat/andrzej_orch_wave2_<package>` (e.g. `feat/andrzej_orch_wave2_kf_ticker`).

---

## Honesty / non-claims

- Wave-2 **P0 channel gaps are closed** offline (KF #109, Coinbase T/Q/L2 #113 @ VenueId 16, bitstamp/gemini #111; candles HAVE via REST timer on 14/15/16; Kraken Futures candles N/A).
- Wave-2 **P1 offline corpora / identity proofs are closed** (#117). Peer-parity **#112** closed. Maturity remains **alpha**.
- **CODE plateau:** implementable exchange-data CODE exhausted; production readiness = **OPS-A…E only**.
- Still **not beta / not stable / not 1.0 / not production-ready** without OPS-A…E.
- Do not invent Kraken Futures or Coinbase Exchange candles on WS.
- Do not claim Coinbase International without a new VenueId claim.
- Ignore CI red while billing is blocked.
