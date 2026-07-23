# Orchestrator remaining-work manifest (CODE only)

**Role:** prioritized, implementable CODE backlog for *spec surface completeness*  
**Tip audited:** AUTH-L2 implementation candidate based on `origin/main` @ `9e30f95` (VenueIds **1–20**)

**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md)  
**Drive / matrix / audit / wave2:** [`production_drive.md`](./production_drive.md), [`maturity_matrix.md`](./maturity_matrix.md), [`orchestrator_wave2_full_data.md`](./orchestrator_wave2_full_data.md), [`orchestrator_wave3.md`](./orchestrator_wave3.md), [`audit_production_readiness.md`](./audit_production_readiness.md), [`venue_ids.md`](./venue_ids.md), [`chaos_supply_chain.md`](./chaos_supply_chain.md)  
**Venue channel audit:** [`venue_channel_audit.md`](./venue_channel_audit.md) (VenueIds **1–20**; W7 families and Coinbase Classic AUTH-L2 closed in code)
**Wave-6 public MD gaps (product expansion, not maturity):** [`orchestrator_wave6_venues.md`](./orchestrator_wave6_venues.md) (**DONE / SKIP**; W7 closed mislabeled N/As)  
**Updated:** 2026-07-23

## Verdict

**Not beta. Not stable. Not 1.0.**

**Public MD status (VenueIds 1–20):** applicable §2.1 and continuous-improvement MD event types are **HAVE** / segment **N/A**. Coinbase Classic L2 now has env-only credential loading, strict HMAC signing, authenticated subscribe, snapshot/delta, reconnect, and deterministic replay code. Credential-backed live proof remains an operations gate. Maturity / production readiness also retains the operations gates tracked below.

**Wave-6** closed major tickers + catalog status + KF candles + Gemini live + bitfinex-deriv segment — see [`orchestrator_wave6_venues.md`](./orchestrator_wave6_venues.md). **W7** closed Bitstamp/Gemini Stats24h + bitfinex-deriv liq (#213/#214). #215 Coinbase Intl env-auth MD is auth-gated (not a public-MD reopen).

Do not open “beta” / “stable” / “1.0” PRs that only add fixtures. Do not claim production-ready without OPS-A…E.

---

## Explicitly out of scope (not CODE here)

| Item | Why excluded |
|---|---|
| Actions billing / remote CI green | **OPS-A** |
| Scheduled canary ≥7 / multi-day live soak / live chaos inject | **OPS-B/C**; unlocks beta/stable |
| Tag attestation publish | **OPS-D** after billing |
| Human “1.0 allowed” | **OPS-E** |
| Live private-key secrets | trust boundary; library fixtures/live helpers exist, while daemon enablement is gated pending a durable account sink/readiness/reconnect path |

---

## Channel matrix @ tip (claimed vs missing)

Legend: **Y** = offline SessionMachine path; **—** = N/A for segment; **gap** = venue supplies it / Spec §2.1 wants it / not implemented.

| VenueId | Code | T | Q | L2 | Candles | Stats24h | Mark | Index | Funding | OI | Liq | Status / catalog |
|--------:|------|---|---|----|---------|----------|------|-------|---------|----|-----|------------------|
| 2 | binance-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y |
| 3 | binance-usdm | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| 12 | binance-coinm | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| 4 | okx-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y |
| 9 | okx-swap | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| 10 | okx-futures | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| 5 | bybit-linear | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| 6 | bybit-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y |
| 11 | bybit-inverse | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| 1 | synthetic | Y | Y | Y | Y | Y | — | — | — | — | — | Y |
| 7 | kraken-spot | Y | Y | Y | Y | Y | — | — | — | — | — | Y |
| 13 | kraken-futures | Y | Y | Y | Y (REST) | Y | Y | Y | Y | Y | Y | Y |
| 8 | deribit | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| 14 | bitstamp | Y | Y | Y | Y (REST) | Y | — | — | — | — | — | Y |
| 15 | gemini | Y | Y | Y | Y (REST) | Y | — | — | — | — | — | Y |
| 16 | coinbase-spot | Y | Y | Y auth | Y (REST) | Y | — | — | — | — | — | Y |
| 17 | bitfinex | Y | Y | Y | Y | Y | — | — | — | — | — | Y |
| 18 | coinbase-adv | Y | Y | Y | Y | Y | — | — | — | — | — | Y |
| 19 | coinbase-intl | Y auth | Y auth | Y auth | — | — | — | — | — | — | — | Y |
| 20 | bitfinex-deriv | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |

Notes:

- **Status / catalog** = engine `VenueStatus` / `InstrumentUpdate` + catalog `--live`; Coinbase Classic/Adv also emit live `InstrumentUpdate` from public `status`. Native `Statistics24h` on major tickers (**W6-P0a/b**) + Bitstamp/Gemini REST (**W7-P0b/c**).
- **Bitstamp / Gemini Stats24h** = REST ticker timers **HAVE** (**W7-P0b/c** / #213).
- **Bitfinex-deriv liq** = public WS `status` / `liq:global` — **HAVE** (**W7-P0a**).
- OKX linear **and** inverse instruments share `okx-swap` / `okx-futures` VenueIds.
- **Kraken Futures** = VenueId **13**; REST charts candles **HAVE** (W2-R12).
- Deribit liquidations via trades `liquidation` field — **no dedicated public liq channel**.
- Private: §2.2 / Phase 6 — not W7 CODE.
- **Coinbase International** = VenueId **19** env-auth MD T/Q/L2 + catalog **HAVE**; credential-backed live evidence remains alpha-only.

---

## P0 — continuous-improvement public MD (**CODE complete; operations evidence remains**)

Ordered worker packages (see [`venue_channel_audit.md`](./venue_channel_audit.md)). Exclude OPS-A…E and VenueId **19** public path.

| Rank | ID | Work | Venue(s) | Status |
|---:|---|---|---|---|
| — | **W7-P0a** | `Liquidation` via WS `status`/`liq:global` | **20** bitfinex-deriv | **DONE** |
| — | **W7-P0b** | `Statistics24h` REST ticker timer | **14** bitstamp | **DONE** (#213) |
| — | **W7-P0c** | `Statistics24h` REST ticker timer (`/v2/ticker` + `/v1/pubticker`) | **15** gemini | **DONE** (#213) |
| — | **AUTH-L2** | Implement authenticated Exchange `level2` subscribe | **16** coinbase-spot | **DONE offline** |

**P1 CODE:** no required public-market-data code gap remains.

**Plateau boundary:** code-complete does not imply beta, stable, 1.0, or credential-backed live proof.

---

## P0 — prior Spec §2.1 surface on shipped venues — **historically completed; current Coinbase auth gap above**

| ID | Work | Spec | Status | Evidence |
|---|---|---|---|---|
| **R1** | Bybit linear mark / index / funding / OI / liquidations | §2.1 | **DONE** | `#94` tickers; liq via `allLiquidation.{symbol}` |
| **R2** | Binance Coin-M `@bookTicker` Quote | §2.1 BBO | **DONE** | `#94` |
| **R3** | Binance Coin-M OI + liquidations | §2.1 | **DONE** | `#94` OI REST timer + `@forceOrder` |
| **R4** | OKX SWAP/Futures open interest + liquidations | §2.1 | **DONE** | `#94` `open-interest` + `liquidation-orders` |
| **R5** | Dynamic subscriptions (`EngineControl::apply_subscriptions`) | §10.4 MUST | **DONE** | `#98` `SubscriptionPatch` + plan versioning |
| **R6** | Instrument / venue status (+ optional 24h stats) | §2.1, §8.7 | **DONE** | `#98`/`#99` engine emit + synthetic `STATS24H` |

---

## P1 — should-have CODE (spec depth / Phase 2–3 without OPS) — **COMPLETE**

| ID | Work | Spec | Status | Evidence |
|---|---|---|---|---|
| **R7** | Bybit inverse deepen (L2 corpus + daemon wire) | §2.1 inverse | **DONE** | `#104` `inverse_l2_book.mfr`; daemon `segment=inverse` / id infer |
| **R8** | Bybit spot candles | §2.1 | **DONE** | `#94` `kline.*` |
| **R9** | OKX inverse instruments (stop skipping) | §2.1 | **DONE** | `#104` `ctType=inverse` → `PerpetualInverse`/`FutureInverse` |
| **R10** | Kraken Futures segment | Phase 3.3 | **DONE** | `#104` VenueId **13** |
| **R11** | Real `SpillToDisk` / WAL | §17.5–17.6 | **DONE** | `#100` `SpillWalSink` |
| **R12** | CLI `catalog` / `plan` / `benchmark` | §20.1 | **DONE** | `#101` |
| **R13** | Chaos CODE leftovers | §27.5–27.9 | **DONE** | `#100` offline unit + fuzz |
| **R14** | `adapter-testkit` shared assertions | §11.7 | **DONE** | `#101` |
| **R15** | `EngineControl` book snapshot + recording rotate | §19.2 | **DONE** | `#101` |
| **R16** | OKX SWAP/Futures L2 book corpora | §3.3 | **DONE** | `#104` `swap_l2_book.mfr` + `futures_l2_book.mfr` |
| **R17** | Deribit liquidations | §2.1 | **DONE** | `#104` trades `liquidation` field |

---

## P2 — deferred / YAGNI for “spec complete” (honest backlog)

| ID | Work | Spec | Why YAGNI for code-complete |
|---|---|---|---|
| **R18** | Kafka / NATS depth (RecordBatch / JetStream / `rdkafka`) | §2.3 | Minimal TCP Produce/PUB **DONE** (#95); deeper clients only when scoped |
| **R19** | FFI beyond stub (sessions/events ABI) | §2.2 | `marketfeed-ffi` version + Fixed parse DONE (#76) |
| **R20** | Private live soak / secrets ops | Phase 6 | Daemon enable-only + lib live paths DONE (#96); needs secrets + soak — not public v1 |
| **R21** | Coinbase adapter | Phase 3.5 | **DONE offline / alpha** — VenueId **16** authenticated L2 signing + subscribe + book/reconnect/replay; credential-backed live canary remains OPS |
| **R22** | prost / protobuf codegen | ADR-010 | Hand MFPE-PB1 + MFPE-JSON1 ship |
| **R23** | C10 true finish (profiles + bench gate) | Phase 4 / §24 | simd-json parse paths DONE; no invented latency numbers |
| **R24** | `fastwebsockets` alternate transport | §14.2 | Behind conformance+bench gate |
| **R25** | gRPC / UDS streaming API | §20.2 optional | Daemon HTTP `/live`/`/ready`/`/metrics` sufficient |
| **R26** | Arrow / Parquet analytics sink | §18.5 optional | File/protobuf sinks cover recording |
| **R27** | Shared connection→core worker pool | §13.2 | Affinity hooks + session=shard DONE |
| **R28** | Facade crate / publish boundary | §7.1 / §19 | **DONE** | `marketfeed` facade at `crates/facade` |
| **R29** | Config hot reload | §21.4 MAY | **Partial** — SIGHUP validate + log_level/readiness; venues/sinks need restart ([wave4](./orchestrator_wave4.md)) |
| **R30** | Remote control API auth / TLS | §20.2 | Loopback-only today |
| **R31** | OpenTelemetry feature | §23.3 | **Skip** — Prometheus + tracing baseline DONE; OTel deps deferred ([wave4](./orchestrator_wave4.md)) |

---

## OPS-only blockers for production readiness (not CODE)

| ID | Blocker | Unlocks |
|---|---|---|
| **OPS-A** | GitHub Actions billing so CI / canary / soak / release jobs run | Prerequisite |
| **OPS-B** | Scheduled live canary ≥7 consecutive for ≥2 venues (Spot pair) | **beta** |
| **OPS-C** | Multi-day live soak + live chaos inject | **stable** path |
| **OPS-D** | Publish tag attestation + SBOM (`gh attestation verify`) | §3.9 |
| **OPS-E** | Explicit human “1.0 allowed” after ≥2 stable | **1.0** / production-ready |

**Explicit:** **NOT production-ready without OPS-A…E.**

---

## Spec § checklist (CODE-relevant, not maturity)

| Spec area | Status @ tip | Remaining CODE |
|---|---|---|
| §2.1 public channels | T/Q/L2/candles + der. mark/index/funding/OI/liq **HAVE** where applicable; Stats24h **HAVE** | — |
| §2.2 deferred | Intentionally thin | **R19–R20** optional |
| §2.3 non-goals | Honored (minimal Kafka/NATS; no strategy/SOR) | **R18** depth only if scoped |
| §3 success criteria | **FAIL** for production claim | **OPS-A…E** |
| §10.4 dynamic subscriptions | **DONE** | — |
| §11.7 adapter-testkit | **DONE** | — |
| §17.5 SpillToDisk | **DONE** | — |
| §19 embedded control API | **DONE** | — |
| §20 CLI | **DONE** for catalog/plan/benchmark | — |
| §27 chaos/fuzz | **DONE** (offline) | live inject **OPS-C** |
| Phase 4/5/6 extras | Stubs / skeletons / minimal brokers | **R18–R31** YAGNI |

---

## Recommended sequence

```text
P0 R1–R6        ─ DONE (#94, #98, #99)
P1 R7–R17       ─ DONE (#100, #101, #104; R8 from #94; Bybit inverse daemon wire)
Wave-2…6        ─ DONE / SKIP (see wave boards; W6 Stats24h majors + bitfinex-deriv segment)
W7-P0a          ─ DONE bitfinex-deriv liquidations (VenueId 20)
W7-P0b          ─ DONE bitstamp Stats24h REST (VenueId 14)
W7-P0c          ─ DONE gemini Stats24h REST (VenueId 15)
AUTH-L2          ─ DONE offline Coinbase Exchange signed level2 (VenueId 16)

**Public MD CODE plateau restored:** live credential proof and maturity remain separate.
**W2-R10** Coinbase International = VenueId **19** env-auth MD T/Q/L2 + catalog **HAVE**.
Do not start R18–R31 unless product explicitly scopes them.
Maturity (beta/stable/1.0) requires OPS-A…E.
```

---

## Honesty

- Closing **AUTH-L2** completes the current public code matrix; it is **still not beta** without credential-backed and scheduled operational evidence.
- **P2** items are backlog honesty, not a commitment.
- Production readiness = **OPS-A…E** after the local code gates pass.
- **Not production-ready without OPS-A…E.**
