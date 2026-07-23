# Production drive board

**Role:** living orchestrator checklist (docs-only)  
**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md)  
**Audit:** [`audit_spec_validation.md`](./audit_spec_validation.md) (full § audit @ tip) · prior [`audit_production_readiness.md`](./audit_production_readiness.md)  
**CODE surface backlog:** [`orchestrator_remaining.md`](./orchestrator_remaining.md) (P0 R1–R6 + P1 R7–R17 **DONE**; Wave-2 P0+P1 **DONE**; P2 YAGNI; Wave-3…6 **CODE plateau**; **W7-P0 closed**; VenueIds **1–20** applicable channels **HAVE** (incl. **19** env-auth MD) — does **not** grant beta)  
**Wave-3 (exchange-data CODE plateau):** [`orchestrator_wave3.md`](./orchestrator_wave3.md) (implementable exchange-data CODE exhausted on ids **1–18**; readiness = **OPS-A…E only**)  
**Wave-4 (PARTIAL-CODE plateau):** [`orchestrator_wave4_partials.md`](./orchestrator_wave4_partials.md) · [`orchestrator_wave4.md`](./orchestrator_wave4.md) (W4 packages **DONE**; R29 partial SIGHUP; R31 OTel **skip**; remaining = OPS-A…E)  
**Wave-5 (implementable CODE plateau):** [`orchestrator_wave5.md`](./orchestrator_wave5.md) (P0a–c + P1a–e **DONE**; readiness = **OPS-A…E only** — **not** beta/stable/1.0)  
**Wave-6 (public MD gaps per venue):** [`orchestrator_wave6_venues.md`](./orchestrator_wave6_venues.md) (P0a–e + P1a–c + P2a–b **DONE**; VenueId **19** env-auth MD **HAVE** #215 / **20** bitfinex-deriv — **not** OPS maturity; **not** beta/stable/1.0)  
**Wave-2 channels:** [`orchestrator_wave2_full_data.md`](./orchestrator_wave2_full_data.md) (KF **#109**, bitstamp/gemini **#111**, peer-parity **#112**, Coinbase VenueId **16** **#113**, corpora **#117**, REST candles 14/15/16 **#119**)  
**VenueIds / chaos plan:** [`venue_ids.md`](./venue_ids.md) (ids **1–20** shipped; **19** `coinbase-intl` env-auth MD alpha #215; next free **21**), [`chaos_supply_chain.md`](./chaos_supply_chain.md)  
**Provenance runbook:** [`../runbooks/release_provenance.md`](../runbooks/release_provenance.md)  
**Maturity:** [`maturity_matrix.md`](./maturity_matrix.md) + [`CODEOWNERS`](../../CODEOWNERS)  
**Canary:** [`docs/ops/canary_checklist.md`](../ops/canary_checklist.md) · [`canary_results.md`](../ops/canary_results.md) (laptop **10/10** cycle_10; scheduled **0**; reconnect **PASS** laptop)  
**Soak results (laptop):** [`docs/ops/soak_results.md`](../ops/soak_results.md) (30m synthetic #179 W5-P1e + optional 60m #181 + 15m/20m + ~31m live; **not** multi-day)  
**Main tip:** `origin/main` @ `e8e6a0c` (post-#218; #215 Coinbase Intl env-auth MD; **W7-P0** closed; plateau **1–20**)
**Venue channel audit:** [`venue_channel_audit.md`](./venue_channel_audit.md) (VenueIds **1–20** CODE plateau; **19** **HAVE** env-auth MD; **W7-P0** closed — **not** beta)

**Updated:** 2026-07-22  
**Local verify @ tip:** `cargo test --workspace` · clippy key crates `-D warnings` · Actions remote **BLOCKED** (billing — ignore)  
**Latency profile:** [`docs/ops/latency_runtime.md`](../ops/latency_runtime.md) (Linux affinity + C10 optional simd-json + fixture parse harness + local `parse_fixtures_gate`; session=shard)  
**Protobuf schema:** [`proto/`](../../proto/) + `ProtobufFileSink` MFPE-JSON1 + `ProtobufBinaryFileSink` MFPE-PB1 + MFNE-JSON1 normalized recording (hand wire; no prost)

Statuses: **DONE** | **IN_PROGRESS** | **TODO** | **OPS**

---

## 0. Verdict

**Not production-ready. Not beta. Not stable. Not 1.0.**

Offline CODE on `main` is substantial (multi-family adapters VenueIds **1–20**, daemon multi-venue, bounded buffers, §23.2 counters + frame/parse/REST/sink fixed-bucket histograms, chaos unit harness, recording crash-recovery tests, maturity matrix, Binance Spot + OKX Spot `alpha+`, release attest job enabled in YAML, `EventSink` + Memory/Logging/File/ProtobufFile/ProtobufBinaryFile/Udp sinks (daemon `type=udp` wired) + Kafka/NATS optional TCP producers (features; stubs when off), minimal `marketfeed-ffi` C ABI stub, Spot + USD-M + Coin-M + Bybit linear + OKX SWAP + Kraken Spot `ohlc` + Deribit `chart.trades` candles, Bitfinex **17** T/Q/L2 + WS candles, Coinbase-adv **18** T/Q/L2 + REST candles, Coinbase Intl **19** env-auth T/Q/L2 (#215), bitfinex-deriv **20** T/Q/L2 + der. mark/index/funding/OI, native `Statistics24h` on major tickers, KF REST candles, Gemini catalog `--live`, catalog instrument status map, dedicated index streams, status/catalog tags, private-account library support for OKX Spot + Bybit Spot with explicit caller-owned sinks, and fail-closed daemon/private Binance gates, L2 book corpora for major venues + Wave-2/3 extras, protobuf MarketEvent schema + MFPE-JSON1 + MFPE-PB1 file sinks + MFNE-JSON1 normalized recording, facade `marketfeed` crate, catalog `--live`, SIGHUP partial reload, ADRs `0001`…`0008`, local `parse_fixtures_gate`, Linux latency affinity, optional `simd-json` + CI feature matrix YAML + fixture `parse_fixtures` Instant harness). Daemon private sessions are **blocked** until a bounded durable account sink, readiness/liveness tracking, and reconnect supervision exist; Binance private streaming additionally requires authenticated WebSocket API subscriptions to replace the retired listen-key flow. Live canary evidence: laptop consecutive **9/9** `live_ignored` (#137 cycle_9; 9 public venues); scheduled **0**; reconnect probe **PASS**. 15m/20m/60m synthetic soak + ~31m live soak 0 drops (not multi-day). Spec §3 / §36 still **FAIL** for a production claim: **0 beta / 0 stable**, live chaos + multi-day soak **OPS**, Actions billing blocks remote CI, no published tag attestation yet.

**CODE plateau:** applicable channels VenueIds **1–20** **HAVE** (incl. **19** env-auth MD #215); Wave-4/5/6 + **W7-P0** closed. Human **OPS-A…E** are the **only** path to maturity. **Not beta.**
1. **Exchange-data** on VenueIds **1–18** — **exhausted** (Wave-3 through #139); **20** alpha shipped (Wave-6).
2. **PARTIAL-CODE** platform packages (Wave-4) — **DONE**.
3. **Wave-5 implementable CODE** — **DONE** plateau.
4. **Wave-6 public MD packages** — **DONE** plateau (P0a–e, P1a–c, P2a–b; #189–#207; W6-P1a superseded by #215).
5. **W7-P0** — **CLOSED** (#213/#214 + VenueId **19** env-auth MD #215); VenueIds **1–20** applicable channels **HAVE**.

**Public CODE plateau** VenueIds **1–20** (incl. **19** env-auth MD). Production readiness = **OPS-A…E only**. **Not beta.**

---

## 0.1 CODE plateau (exchange-data + PARTIAL-CODE + Wave-5 + Wave-6 + W7 public MD exhausted)

**Statement:** Applicable channels on VenueIds **1–20** are at **CODE plateau** @ tip `e8e6a0c` (post-#215/#218). VenueId **19** env-auth T/Q/L2 + catalog **HAVE** (alpha). **W7-P0** closed. Wave-4/5/6 implementable CODE **DONE**. Production readiness (beta/stable/1.0) = **OPS-A…E only**. **Not beta.**

| Bucket | Status | Notes |
|---|---|---|
| Adapters / L2 / candles / corpora | **DONE** | P1 venues (#104) + Wave-2 (#109/#111/#112/#113/#117/#119) + V17/V18 (#127/#132/#134/#135) + candle corpora (#131) |
| Daemon sinks (memory/logging/file/protobuf JSON+bin/**udp**/kafka/nats/spill-wal) | **DONE** | Kafka/NATS = optional TCP; SpillWalSink = SpillToDisk |
| Private alpha (fixture SM + live env paths lib+daemon) | **DONE** | No order entry; secrets env-only |
| C9 CI matrix / fuzz / FFI | **DONE** (YAML) | Remote runs billing-blocked |
| C10 simd-json parse paths | **DONE** (CODE) | All public JSON adapters |
| C10 CI `--features simd-json` + fixture Instant harness | **DONE** (evidence tools) | Not an enablement / SLO claim |
| C10 enablement under latency profile | **PARTIAL** | Needs operator `parse_*` profiles under load — **not** a CODE gap |
| **W4-P0a** MFNE-JSON1 | **DONE** | #149 |
| **W4-P0b** facade `marketfeed` | **DONE** | #144 |
| **W4-P0c/d** ADRs `0001`…`0008` + §24 criterion docs | **DONE** | #141 |
| **W4-P1a** catalog `--live` | **DONE** | #147 |
| **W4-P1b** SIGHUP partial; OTel | **DONE** / **SKIP** | #146 |
| **W4-P1c** `parse_fixtures_gate` | **DONE** | #148 |
| prost-codegen for protobuf | **YAGNI** | Hand MFPE-PB1 + MFPE-JSON1 + MFNE-JSON1 ship |
| proptest / host-opt / ADR `0009`…`0015` | **DONE** (W5) | #158 / #167; Loom **SKIP**; not §3/§36 gates |
| OTel | **YAGNI** (R31 SKIP) | Re-open only with named backend |
| **beta / stable / 1.0** | **OPS-only** | USER OPS CHECKLIST A…E |
| §2.1 surface CODE (R1–R17) | **DONE** | P0+P1 complete — does not unlock maturity |
| Wave-4 PARTIAL-CODE track | **DONE** (plateau) | Does not unlock maturity |
| Wave-5 implementable CODE | **DONE** (plateau) | P0a–c + P1a–e **DONE**; see [`orchestrator_wave5.md`](./orchestrator_wave5.md) |
| Wave-6 public MD packages | **DONE** (plateau) | P0a–e + P1a–c + P2a–b; **19** env-auth MD **HAVE** #215 / **20** alpha; see [`orchestrator_wave6_venues.md`](./orchestrator_wave6_venues.md) |
| **W7-P0** continuous-improvement | **CLOSED** | P0a/b/c **DONE** + **19** env-auth MD #215; plateau **1–20** — **not** beta |

**OPS-only for beta / stable / 1.0** (cannot be faked in CODE):

1. Actions billing → real CI greens  
2. Scheduled canary ≥7 for ≥2 venues → **beta**  
3. Multi-day live soak + live chaos → **stable** path  
4. Tag attestation publish → §3.9  
5. Explicit human “1.0 allowed” sign-off  

Do **not** claim beta/stable/1.0 from this CODE plateau. **Not production-ready without OPS-A…E.**



---

## 1. Spec §3 snapshot @ tip

| # | Gate | Status | Notes |
|---|---|---|---|
| 1 | ≥3 families, spot + derivatives | **DONE** (caveat) | Families present; candles on Binance Spot/USD-M/Coin-M, OKX Spot/SWAP, Bybit linear, Kraken Spot `ohlc`, Deribit `chart.trades`; Coin-M trades/quote/mark/funding/OI/liq/L2 |
| 2 | ≥2 adapters `stable` | **TODO** | **0 stable. 0 beta.** Spot pair is `alpha+` (laptop canary **9/9**, not scheduled) |
| 3 | L2 deterministic tests + corpora | **DONE** | OKX Spot/SWAP/Futures; Binance Spot/USD-M/Coin-M; Bybit linear+inverse; Kraken Spot; Deribit L2 book corpora |
| 4 | All buffers bounded | **DONE** | ActionBuffer + pending HTTP/writes + dispatch/books/timers |
| 5 | No silent drops | **DONE** | Drop* → `EventsDropped` |
| 6 | Metric + structured event | **DONE** | §23.2 counters + frame/parse/REST/sink fixed-bucket hists |
| 7 | Continuous soak, bounded memory | **OPS** | Offline 15m/20m synthetic RSS logged; multi-day live soak **OPS** |
| 8 | Chaos matrix | **IN_PROGRESS** | Unit harness + fuzz; live inject **OPS** |
| 9 | Dep/license/vuln/provenance | **IN_PROGRESS** | deny + SBOM + attest job enabled; publish blocked by billing |
| 10 | Public API / maturity docs | **DONE** (honesty) | Matrix + CODEOWNERS + canary checklist |

---

## 2. USER OPS CHECKLIST (humans only — blocks beta / stable / 1.0)

Nothing below is satisfiable by another CODE PR. Owner: `@s1korrrr` unless reassigned.

### A. Unblock GitHub Actions (prerequisite for scheduled canary/soak CI)

- [ ] Fix GitHub Actions spending / billing limit (jobs currently fail in ~3–6s without running)
- [ ] Re-run `CI` on `main` @ tip; confirm green (not just YAML present)
- [ ] Confirm `canary.yml` / `soak.yml` / `release.yml` can start jobs

Until A is done: merge on local `fmt` / `clippy` / `test` / `deny` only. Do **not** treat Actions as green.

### B. Live canary for ≥2 venues (required for **beta**)

Target venues for first beta pair: **Binance Spot** (VenueId 2) + **OKX Spot** (VenueId 4). Follow [`docs/ops/canary_checklist.md`](../ops/canary_checklist.md). Evidence so far: [`canary_results.md`](../ops/canary_results.md) (laptop **9/9** cycle_9; scheduled **0**; reconnect **PASS** laptop).

For **each** venue:

- [x] Network allowlist + secrets reviewed (public market data; no credentials in repo) — run 1
- [ ] Wire venue secrets into `canary.yml` (or documented operator runbook)
- [x] Manual session `/live` + `/ready` 200 (90s) — run 1; scheduled job still pending
- [x] Observe primary channels (trades + quote; frames advancing) — run 1
- [x] Metrics: frames advancing; zero unexplained `EventsDropped` under nominal load — run 1
- [x] Intentional reconnect recovers Live within reconnect policy — laptop probe **PASS** (still run on scheduled cadence)
- [x] Archive laptop consecutive live_ignored runs (**9/9** cycle_9)
- [ ] Archive evidence for **≥7 consecutive** **scheduled** runs (**scheduled = 0** today)
- [ ] Only then flip maturity matrix row → **beta** (not before)

### C. Multi-day soak (required for **stable** / Spec §3.7)

Follow [`docs/ops/soak_runbook.md`](../ops/soak_runbook.md). Laptop mini-soak evidence: [`soak_results.md`](../ops/soak_results.md).

- [x] Offline synthetic 15m/20m RSS plateau (laptop) — **not** multi-day live
- [ ] Calendar multi-day live soak (≥2 venues that already passed B)
- [ ] RSS / memory bound recorded and holds on **live** venues
- [ ] Disk-full / slow-sink / reconnect scenarios exercised live (not unit-only)
- [ ] Ops ownership named in CODEOWNERS / runbooks
- [ ] Only then flip maturity → **stable** for those venues

### D. Release provenance (Spec §3.9 / §29)

Follow [`docs/runbooks/release_provenance.md`](../runbooks/release_provenance.md).

- [x] Enable attest job in `release.yml` (`actions/attest-build-provenance@v2`; hard-fail if perms missing)
- [ ] After A: tag `v0.0.x-rc`; confirm SBOM artifact + attestation publish
- [ ] Document verify command output for consumers (`gh attestation verify`)

### E. Spec 1.0 claim gate (do not skip)

- [ ] ≥2 adapters marked **stable** in maturity matrix with B+C evidence linked
- [ ] §3 rows 2, 7, 8, 9 **DONE** or **OPS-signed** with artifacts
- [ ] Explicit human sign-off: “1.0 allowed” — not implied by merged PRs

**Honest promotion language**

| Evidence | Allowed claim |
|---|---|
| Local gates green + offline corpora | alpha / `alpha+` |
| Laptop consecutive live canary (9/9) + reconnect PASS | still `alpha+` — **scheduled = 0** |
| B complete (≥7 scheduled) for a venue | **beta** for that venue |
| B+C for a venue | path to **stable** |
| E complete | **1.0** / production-ready |

---

## 3. OPS-blocked (summary)

| Blocker | Impact | Unblock |
|---|---|---|
| GitHub Actions spending/billing limit | Remote CI / scheduled canary/soak / tag attest cannot run | Checklist A |
| Scheduled live canaries (**0**; laptop 9/9 ≠ scheduled) | Blocks alpha+ → beta | Checklist B (scheduled ≥7; reconnect done on laptop) |
| Multi-day soak + live chaos inject | Blocks beta → stable / §3.7–§3.8 | Checklist C |
| Published release attestations | §3.9 incomplete (YAML ready) | Checklist D after A |

---

## 4. Merged recently (through #72)

| PR | Status | Contents |
|---:|---|---|
| 16–30 | merged | bounds, metrics, multi-venue, Kraken/Deribit L2, audits, CI skeletons |
| 31–36 | merged | offline maturity, OKX daemon wire, A4 offline E2E |
| 37 | merged | ActionBuffer DropNewest → `EventsDropped` |
| 38–39 | merged | Binance USD-M corpus/README; MFR1 crash-recovery + tag SBOM |
| 40–45 | merged | attest stub; maturity matrix; `alpha+` close-out; L2 corpus; drive board; rustfmt |
| 46–48 | merged | live drain fix; canary series start; attest enable + soak RSS |
| 50–51 | merged | laptop canary **7/7** + ~31m soak; reconnect probe **PASS** |
| 52 | merged | bounded `EventSink` (`MemorySink` / `LoggingSink`) |
| 53 | merged | Spot candles (Binance+OKX) + minimal Binance Coin-M (`VenueId(12)`) |
| 54 | merged | Coin-M daemon wire + `FileSink` |
| 55 | merged | Binance USD-M `@kline_*` candles (C2b) |
| 56–57 | merged | daemon `[[sinks]]` + `type=file` |
| 58 | merged | private-account scaffold + `NormalizedEventWriter` |
| 59 | merged | Binance Coin-M L2 (`pu` / dapi) |
| 60–61 | merged | Binance Spot/USD-M + Coin-M L2 book corpora |
| 62 | merged | §23.2 parse/REST/sink latency histograms (C8) |
| 63 | merged | drive CODE status plateau through #62 |
| 64 | merged | **C7** Bybit/Kraken/Deribit L2 book corpora |
| 65 | merged | **C6b Phase 1** Binance Spot user-data fixture SM + Bybit linear / OKX SWAP candles |
| 66 | merged | **C6b Phase 2** OKX/Bybit private fixture SMs + Coin-M `@kline_*` candles |
| 67 | merged | drive tip board through #66 |
| 68 | merged | **C2f/C2g** Kraken/Deribit candles + **C4c** Kafka/NATS `Unsupported` stubs |
| 69 | merged | drive tip board through #68 |
| 70 | merged | **C9 lite** fuzz expand (candle/Coin-M/private) + latency runtime skeleton |
| 71 | merged | drive tip board through #70 |
| 72 | merged | **C5b** protobuf MarketEvent schema stub + **C10a** Linux `sched_setaffinity` affinity |
| 73–75 | merged | drive tips + **C10 lite** optional simd-json (#74) |
| 76 | merged | **C9** Windows/aarch64/docs CI + `marketfeed-ffi` + `UdpSink` |
| 77 | merged | **C6c** Binance Spot private live auth (lib + ignored test; no daemon) |
| 79 | merged | drive tip board through #77 |
| 80 | merged | **C5c** `ProtobufFileSink` MFPE-JSON1 + daemon `protobuf-file`; shards documented skip |
| 81 | merged | drive tip board through #80 |
| 82 | merged | **C6c** daemon private Binance Spot user-data path |
| 83 | merged | drive tip board through #82 |
| 84 | merged | **C10** remainder: optional simd-json USD-M/Coin-M/OKX + fixture parity |
| 85 | merged | drive tip board through #84 |
| 86 | merged | **C5d** `ProtobufBinaryFileSink` MFPE-PB1 + daemon `protobuf-file-bin` |
| 87 | merged | drive tip board through #86 |
| 88 | merged | **C10** venues: optional simd-json Bybit/Kraken Spot/Deribit + parity |
| 89 | merged | drive tip board through #88 |
| 90 | merged | daemon `type=udp`; CI `simd-json` matrix; Binance `parse_fixtures` Instant harness; CODE plateau docs |
| 91 | merged | drive tip board through #90 |
| 95 | merged | **C4c+** Kafka/NATS TCP Produce/PUB sinks (features + daemon types) |
| 92 | merged | orchestrator CODE remaining-work manifest (R1–R31) |
| 94 | merged | P0 venue channels (Bybit mark/OI/liq, Coin-M BBO/OI/liq, OKX OI; Bybit spot candles) |
| 95 | merged | **C4c+** Kafka/NATS TCP Produce/PUB sinks |
| 96 | merged | private OKX/Bybit live+daemon enable-only |
| 98–99 | merged | R5 dynamic subs + R6 status events; R1–R4 remainder |
| 100 | merged | R11 SpillWalSink + R13 offline chaos |
| 101 | merged | R12 CLI catalog/plan/benchmark, R14 testkit, R15 control |
| 104 | merged | R7/R9/R10/R16/R17 venue depth (inverse L2, OKX inverse, Kraken Futures, Deribit liq) |
| 106 | merged | P0/P1 plateau sync + Bybit inverse daemon wire |
| 109 | merged | **W2-P0a** Kraken Futures ticker mark/index/funding/OI + liquidation trades |
| 111 | merged | Bitstamp + Gemini spot T/Q/L2 (VenueId **14**/**15**) |
| 112 | merged | synthetic QUOTE/CANDLE peer-parity |
| 113–114 | merged | **W2-P0b/c** Coinbase Exchange spot VenueId **16** T/Q/L2 + docs |
| 115 | merged | drive tip board through #114 |
| 116 | merged | Wave-2 P0 honesty sync + Deribit corpus regen |
| 117 | merged | **W2-P1** KF ticker+liq/L2 + Coinbase/Bitstamp/Gemini L2 corpora |
| 119 | merged | REST candles Bitstamp/Gemini/Coinbase Exchange (14/15/16) |
| 124 | merged | Wave-3 plan (post-CODE plateau honesty) |
| 126 | merged | R6 status/catalog wire venues **13–16** (extended **13–18** by #135) |
| 127 | merged | Bitfinex VenueId **17** alpha T/Q/L2 + REST candles |
| 130–131 | merged | W3-P0 scripts + W3-P1 candle corpora / runbooks / alpha live expand |
| 132 | merged | Coinbase Advanced Trade VenueId **18** REST candles |
| 134–135 | merged | Bitfinex + Coinbase-adv catalog/R6 peer-parity; status **13–18** |
| 137 | merged | laptop cycle_9 canaries **9/9** + 15m synthetic soak (scheduled **0**) |
| 138 | merged | tip board through #137 |
| 139 | merged | Wave-3 CODE plateau — VenueIds 1–18; OPS-A…E only |
| 140 | merged | tip board through #139 |
| 141 | merged | **W4-P0c/d** §34 ADRs `0001`…`0008` + §24 Instant/comparison criterion docs |
| 142 | merged | orchestrator Wave-4 PARTIAL CODE packages plan |
| 144 | merged | **W4-P0b** facade `marketfeed` crate (R28 / §19) |
| 146 | merged | **W4-P1b** SIGHUP config reload (partial) + OTel **SKIP** |
| 147 | merged | **W4-P1a** catalog `--live` REST discovery |
| 148 | merged | **W4-P1c** local `parse_fixtures_gate` (>10%) |
| 149 | merged | **W4-P0a** MFNE-JSON1 normalized recording |
| 151–153 | merged | tip / audit board refreshes |
| 154–155 | merged | full-spec re-audit @ tip |
| 157 | merged | Wave-4 PARTIAL-CODE plateau — OPS-A…E only |
| 158 | merged | **W5-P0a** ADRs `0009`…`0015` |
| 163 | merged | Wave-5 orchestrator plan (`orchestrator_wave5.md`) |
| 167 | merged | **W5-P1a/b** host-opt profile + Fixed proptest smoke |
| 169 | merged | **W5-P0b** slice + **W5-P1c** catalog `--live` 14/17 + canary 17–18 |
| 172 | merged | **W5-P1d** private live scripts + richer ignored smokes |
| 174 | merged | **W5-P0c** coinbase-adv public T/Q/L2 |
| 176–177 | merged | **W5-P0b** catalog `--live` remainder + tip board |
| 181 | merged | **W5-P1e** `DURATION` presets + 60m laptop archive |
| 217 | merged | **public MD CODE plateau** boards — VenueIds **1–20**; W7-P0a/b/c **DONE**; tip sync (19 later **HAVE** #215/#this) |
| *this* | **this PR** | tip ≥ `7d0261b` + regen bitfinex-deriv L2 corpus for `liq:global` subscribe — **not** beta/stable/1.0 |

---

## 5. Next packages (recommended order)

**CODE plateau:** applicable channels VenueIds **1–20** **HAVE** (incl. **19** env-auth MD #215); Wave-4/5/6 + **W7-P0** closed. Human **OPS-A…E** are the **only** path to maturity. **Not beta.**

| ID | Package | Pri | Status | Notes |
|---|---|---|---|---|
| **OPS-A…E** | USER OPS CHECKLIST above | **P0** | open | **Only** path to beta / stable / 1.0 (§3 / §36) — **PLATEAU: only OPS-A…E remain** |
| **W5-P0a** | ADR docs `0009`…`0015` | **P0** | **DONE** | #158 |
| **W5-P0b** | catalog `--live` 14/15/17 + adv parse | **P0** | **DONE** | #169/#176; Gemini **HAVE** via W6-P0d #202 |
| **W5-P0c** | coinbase-adv public T/Q/L2 | **P0** | **DONE** | #174; still alpha |
| **W5-P1a/b** | host-opt / proptest (± Loom) | **P1** | **DONE** | #167; Loom **SKIP** |
| **W5-P1c** | canary 17–18 | **P1** | **DONE** | #169; laptop only; not beta |
| **W5-P1d** | private live expand (script + richer `live_ignored`) | **P1** | **DONE** | #172; env-only secrets; no orders |
| **W5-P1e** | longer laptop soak (`DURATION` presets + archive) | **P1** | **DONE** | #179 laptop 30m; optional #181 60m + `DURATION=7200`/2h; not multi-day / not stable |
| **C1–C10 / C4c+ / C6c** | Prior drive packages | — | **DONE** / C10 **PARTIAL** (profiles) | C10 profiles = OPS/laptop, not a CODE gap |
| **R1–R17** | Spec §2.1 + Phase 2–3 depth | **P0/P1** | **DONE** | Orchestrator complete |
| **W2-P0a–d** | Wave-2 channel gaps | **P0** | **DONE** | #109 / #111 / #113; P0d REST candles **HAVE** (#119) |
| **W2-P1** | KF proofs / Coinbase+Bitstamp/Gemini L2 corpora | **P1** | **DONE** | #117 offline `.mfr` identity; still **alpha** |
| **#112** | synthetic QUOTE/CANDLE peer-parity | **P1** | **DONE** | Spot channel parity on test venue |
| **W3-P0a–c** | Wave-3 docs honesty + laptop canary/soak scripts | **P0** | **DONE** | #130; cycle_9 **9/9** #137 — still not beta/stable |
| **W3-P1a–c** | candle corpora + alpha live + runbooks | **P1** | **DONE** | #131 |
| **W3-BFX / W2-R5b** | Bitfinex **17** + Coinbase-adv **18** | **P2 ship** | **DONE** alpha | #127/#134, #132/#135 — not maturity unlocks |
| **W4-P0a…d / W4-P1a–c** | Wave-4 PARTIAL→PASS packages | **P0/P1** | **DONE** | #141/#144/#146/#147/#148/#149; OTel **SKIP** |
| **W5 CODE plateau** | implementable Wave-5 CODE | — | **DONE** | P0a–c + P1a–e |
| **W6-P0a/b** | Native Statistics24h | **P0** | **DONE** | #196/#201/#207 |
| **W6-P0c** | KF REST candles | **P0** | **DONE** | #198 |
| **W6-P0d** | Gemini catalog `--live` | **P0** | **DONE** | #202 |
| **W6-P0e** | Catalog instrument status map | **P0** | **DONE** | #200 |
| **W6-P1a** | Coinbase Intl VenueId **19** | **P1** | **DONE** | #215 env-auth MD (supersedes #191 SKIP) |
| **W6-P1b** | Bitfinex-deriv VenueId **20** | **P1** | **DONE** | #206 alpha |
| **W6-P1c** | coinbase-adv remainder | **P1** | **DONE** | #193 REST candles preferred |
| **W6-P2a** | Docs honesty Coin-M OI/liq | **P2** | **DONE** | #189 |
| **W6-P2b** | Matrix tip sync / W6 plateau boards | **P2** | **DONE** | this PR |
| **#190** | Dedicated index streams | peer | **DONE** | Binance UM/CM + Deribit |
| **W6 CODE plateau** | implementable Wave-6 public MD | — | **DONE** | P0a–e + P1a–c + P2a–b |
| **W7-P0a/b/c** | bitfinex-deriv liq + Bitstamp/Gemini Stats24h | **P0** | **DONE** | #214 / #213 |
| **W7-P0** | continuous-improvement track | — | **CLOSED** | plateau VenueIds **1–20**; **not** beta |
| **OTel / criterion CI / R18–R30** | P2 YAGNI | **P2** | deferred | Not §3/§36 gates |
| **W2-R10** Coinbase Intl | VenueId **19** | **P2** | **DONE** | Env-auth INTX MD WS (`CBINTLMD`) T/Q/L2 + catalog #215 |

**Remaining for §3 / §36 = OPS-A…E only.** Public CODE plateau on VenueIds **1–20** **does not** unlock maturity.

**Recommended next:** human **OPS-A…E** (maturity). **W7-P0** closed — still **not** beta / stable / 1.0.

Do **not** open “beta” or “1.0” PRs that only add fixtures/docs — they cannot satisfy §11.8 without checklist B/C. **Not production-ready without OPS-A…E.**


---

## Honesty

Do not claim **1.0 / beta / stable / production-ready** until §3 rows are **DONE** or **OPS-signed** with checklist evidence. Offline L2 fixtures ≠ beta. Offline 15m/20m synthetic ≠ multi-day live soak. Laptop canary **9/9** + reconnect PASS ≠ scheduled beta gate (**scheduled = 0**). Attest YAML enabled ≠ published provenance. Do not treat Actions as green while the spending limit blocks job start. VenueIds **1–20** CODE plateau (incl. **19** env-auth MD) / Wave-4–6 / **W7-P0** closed / corpora / laptop evidence ≠ production-ready. **Not beta. Not production-ready without OPS-A…E.**
