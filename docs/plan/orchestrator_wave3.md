# Orchestrator Wave 3 — post-CODE plateau / production path honesty

**Role:** what agents can still ship after exchange-data CODE is exhausted  
**Base tip:** `origin/main` @ `1b8458b` (≥ `3e97921`) (Wave-3 delivery through #137)  
**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md)  
**Priors:** [`orchestrator_remaining.md`](./orchestrator_remaining.md), [`orchestrator_wave2_full_data.md`](./orchestrator_wave2_full_data.md), [`production_drive.md`](./production_drive.md), [`audit_spec_validation.md`](./audit_spec_validation.md), [`maturity_matrix.md`](./maturity_matrix.md), [`venue_ids.md`](./venue_ids.md)  
**Updated:** 2026-07-22  

**Wave-3 delivery (merged):**

| Package | PRs | Result |
|---|---|---|
| W3-P0/P1 scripts + corpora | #130, #131 | laptop canary/soak scripts; REST candle `.mfr` packs |
| Bitfinex VenueId **17** + peer-parity | #127, #134 | T/Q/L2 + REST candles + catalog/R6 + L2 corpus |
| Coinbase-adv VenueId **18** + peer-parity | #132, #135 | public REST candles + catalog/R6 |
| Status catalog VenueIds **13–18** | #126, #135 | engine R6 `VenueStatus` / `InstrumentUpdate` tagged for KF…Adv |
| Laptop cycle_9 canaries + 15m synthetic soak | #137 | laptop **9/9**; scheduled **0**; still **alpha+** only |

---

## Product ask vs reality

User still wants **100% production-ready**.

**Verdict (unchanged, non-negotiable):**

- **Not production-ready. Not beta. Not stable. Not 1.0.**
- Spec §3 **FAIL**s for any production claim: **0 beta / 0 stable**, scheduled canary **0**, multi-day live soak **absent**, published tag attestation **absent**, Actions **billing-blocked**.
- Wave-1 P0/P1 (R1–R17) + Wave-2 P0/P1 (#109/#111/#112/#113/#117/#119) + Wave-3 optional families (**17**, **18**) + status catalog (**13–18**) closed **implementable exchange-data CODE** on VenueIds **1–18**. Matrix cells are **HAVE** or **N/A**.
- **CODE plateau:** implementable exchange-data CODE is **exhausted**. Production readiness = **OPS-A…E only**. Agents cannot fake maturity by shipping more adapters.

This wave does **not** open “beta” / “stable” / “1.0” PRs. Remaining agent work is docs honesty + optional laptop OPS tooling only (already shipped). Human **OPS-A…E** is the only maturity path.

---

## 0. CODE plateau (stop inventing CODE)

**Statement:** Implementable **exchange-data CODE** on shipped VenueIds **1–18** is **exhausted** at tip `1b8458b` (≥ `3e97921`). There is **no** open P0/P1 adapter/fixture/corpus gap that moves Spec §3.

| Surface | Status |
|---|---|
| Spec §2.1 channels on ids **1–16** | **HAVE** or **N/A** (Wave-2) |
| Bitfinex **17** (optional family) | **HAVE** alpha (#127/#134) |
| Coinbase-adv **18** (T/Q/L2 + REST candles) | **HAVE** alpha (#132/#135/W5-P0c); Classic **16** remains dual protocol |
| Engine status/catalog tags **13–18** | **HAVE** (#126/#135) |
| W3-P0/P1 agent OPS scripts + candle corpora | **DONE** (#130/#131) |
| **beta / stable / 1.0 / production-ready** | **OPS-A…E only** |

Remaining product CODE = **P2 YAGNI** only (R18–R31, Coinbase Intl, KF REST candles, native per-venue `Statistics24h`). None unlocks maturity.

**Do not** invent holes (status-event adapters per venue, “Statistics24h on every REST ticker”) to look busy. That is YAGNI theater and does not move §3. Adv T/Q/L2 later shipped via W5-P0c (still **alpha**).

---

## 1. Real CODE holes (not YAGNI theater)

| Candidate | Hole? | Why |
|---|---|---|
| §2.1 public channels on VenueIds **1–18** | **No** | Every matrix cell **HAVE** or **N/A**. Offline SessionMachine + daemon wire + corpora for P0/P1; V17/V18 alpha families shipped. |
| Status events on venues **13–18** | **No** | Engine-owned `MarketEvent::{VenueStatus,InstrumentUpdate}` on connect/live/degrade + catalog refresh (#126/#135). Adapters stay I/O-free; status **HAVE** whenever a venue is engine-wired. Synthetic `STATS24H` proves `Statistics24h` (R6). |
| Coinbase Exchange candles | **No** | REST poll timer **HAVE** (#119). Exchange public WS has no candle channel (N/A on WS). |
| Coinbase Advanced Trade candles | **No** | VenueId **18** REST timer **HAVE** (#132/#135). |
| Kraken Futures candles on WS | **No** | Venue has no public candle WS → **N/A** (correct). |
| Bitstamp / Gemini / Bitfinex candles | **No** | REST timer **HAVE** (#119 / #127). |
| Docs drift (matrix / `venue_ids.md` / drive lag tip) | **Closed** | VenueIds **1–18** consistent @ `1b8458b` (≥ `3e97921`). **Not** missing adapter code. |
| Live maturity evidence | **OPS only** | Laptop canary **9/9** (cycle_9), scheduled **0**, 15m synthetic soak + ~31m live ≠ multi-day. Cannot be closed by fixtures. |

**Wave-3 CODE-hole inventory: empty** for implementable exchange-data on shipped venues.

---

## 2. P2 “all data” items — ship or skip

| ID | Item | Ship? | Rationale |
|---|---|---|---|
| **W2-R5b** | Coinbase **Advanced Trade** candles | **SHIPPED alpha** (peer-parity) | VenueId **18** `coinbase-adv` (#132) + `session_config_from_catalog` + R6 status/catalog (#135). Not beta. T/Q/L2 stay on Exchange Classic **16**. |
| **W3-BFX** | **Bitfinex** (VenueId **17**) | **SHIPPED alpha** (peer-parity) | WS v2 `chanId` T/Q/L2 (#127) + catalog/R6 (#134) + REST candles + L2 `.mfr` corpus. Not beta. |
| **Status events on new venues** | Engine status on **13–18** | **SHIPPED** (#126/#135) | Already **HAVE** for any engine-wired venue; catalog/status tests cover 13–18. |
| **W2-R10** | Coinbase International / derivatives | **SKIP** (claimed **19**) | INTX MD WS subscribe is auth-gated (`CBINTLMD`); REST instruments/quote only — no clean public T/Q/L2. Spot Exchange **16** + Adv **18** remain the shipped Coinbase claims. See [`venue_ids.md`](./venue_ids.md) § Coinbase International. |
| **W2-R11** | Per-venue native `Statistics24h` | **Skip** | §8.7 optional depth. Synthetic proves the type. Native per-venue 24h is polish, not production gate. |
| **W2-R12** | Kraken Futures REST candle backfill | **Low value** | Candles **N/A** on KF public WS (correct). REST backfill is convenience, not §2.1 MUST. |
| **W3-CORPUS-CANDLE** | REST candle identity corpora for 14/15/16 | **DONE** (#131) | Cheap confidence only; **does not** unlock maturity. |
| **R18–R31** | Kafka depth, FFI, prost, gRPC, OTel, facade, etc. | **Skip** | Explicit YAGNI for code-complete / production claim. Re-open only with product scope. |

**Honest “all data” answer:** on shipped VenueIds **1–18**, public §2.1 data is **code-complete**. Any *new* VenueId family is product expansion, **not** a production-readiness unlock.

---

## 3. OPS work — agents CAN vs CANNOT

Production readiness = OPS-A…E ([`production_drive.md`](./production_drive.md) USER OPS CHECKLIST). Split by what a coding agent on a laptop can actually deliver.

### Agents CAN (ship as CODE/docs/scripts)

| ID | Work | Unlocks | Ceiling (do not over-claim) | Status |
|---|---|---|---|---|
| **W3-OPS-DOC** | Tip/honesty sync: matrix, `venue_ids.md`, remaining/drive/audit tip → `1b8458b` (≥ `3e97921`) | Doc truth | **Does not** grant beta/stable/1.0 | **DONE** (W3-P0a + plateau refresh) |
| **W3-OPS-CANARY-SCRIPT** | `scripts/laptop_canary.sh` + cycle archives | Repeatable laptop evidence | Laptop **9/9** **≠** scheduled ≥7; **still alpha+** | **DONE** (#130; cycle_9 #137) |
| **W3-OPS-SOAK-SCRIPT** | `scripts/laptop_soak.sh` | Repeatable mini-soak | 15m synthetic **≠** multi-day; **not** stable | **DONE** (#130; #137) |
| **W3-OPS-RUNBOOK** | Canary/soak runbooks + allowed-claim table | Operator clarity | Still needs human calendar / billing | **DONE** (#131) |
| **W3-OPS-ALPHA-LIVE** | `live_ignored` for alpha venues (cycle_9: 9 public) | Broader laptop signal | **Not** beta | **DONE** (#131/#137) |
| **W3-OPS-C10-PROFILE** | Operator `parse_*` Instant profiles under load | C10 enablement evidence | **Not** published SLO / §3 pass | open (optional) |

### Agents CANNOT (human / calendar / billing)

| ID | Work | Why agent-blocked | Blocks |
|---|---|---|---|
| **OPS-A** | GitHub Actions billing / spending limit | Payment / org settings | Remote CI, scheduled jobs, release attest runs |
| **OPS-B** | **Scheduled** canary ≥7 consecutive (calendar-spaced `canary.yml`) | Requires OPS-A + GH schedule running for days | **beta** |
| **OPS-C** | Multi-day live soak + live chaos inject | Wall-clock calendar + ops ownership; not a single PR session | **stable** path / §3.7–§3.8 |
| **OPS-D** | Published tag attestation + `gh attestation verify` evidence | Needs OPS-A so `release.yml` actually runs | §3.9 |
| **OPS-E** | Explicit human “1.0 allowed” | Human sign-off only | **1.0** / production-ready |

**Explicit:** Agents may accumulate laptop evidence and scripts forever and the repo remains **not production-ready** until OPS-A…E. Do not open maturity-flip PRs from laptop cycles.

---

## 4. Prioritized worker packages (this wave)

Branch prefix: `feat/andrzej_orch_w3_<package>`  
Merge policy: **merge commits** (no squash). Ignore CI red while billing blocked; local `cargo test` / clippy / deny where CODE lands.

### P0 — honesty + agent-doable OPS leverage — **DONE**

| Package | IDs | Owner | Acceptance | Maturity claim allowed | Status |
|---|---|---|---|---|---|
| **W3-P0a docs tip honesty** | W3-OPS-DOC | `docs/plan/*` | VenueIds **1–18** consistent; tip ≥ `3e97921` | none (still not beta) | **DONE** |
| **W3-P0b laptop canary script** | W3-OPS-CANARY-SCRIPT | `scripts/laptop_canary.sh` + `docs/ops/canary_*` | cycle_N archives; **scheduled still 0** | still **alpha+** | **DONE** (#130/#137) |
| **W3-P0c laptop soak script** | W3-OPS-SOAK-SCRIPT | `scripts/laptop_soak.sh` + `docs/ops/soak_*` | bounded soak; duration ceiling documented | **not** multi-day / **not** stable | **DONE** (#130/#137) |

P0 does **not** include maturity matrix flips.

### P1 — optional depth — **DONE**

| Package | IDs | Owner | Acceptance | Status |
|---|---|---|---|---|
| **W3-P1a REST candle corpora** | W3-CORPUS-CANDLE | `adapters/{bitstamp,gemini,coinbase}/tests/corpus/` | Offline identity replay for REST candle fixtures | **DONE** (#131) |
| **W3-P1b alpha live_ignored expand** | W3-OPS-ALPHA-LIVE | adapter `live_ignored` + canary hooks | Laptop smokes for public venues without claiming beta | **DONE** (#131/#137 cycle_9 **9/9**) |
| **W3-P1c runbook tighten** | W3-OPS-RUNBOOK | `docs/ops/canary_checklist.md`, `soak_runbook.md` | Commands match scripts; allowed-claim table = drive | **DONE** (#131) |

**P1 status:** **DONE**. Still **alpha / alpha+ only** — no beta/stable/1.0.

### P2 — deferred / explicit skip unless product re-scopes

| Package | IDs | Default |
|---|---|---|
| Advanced Trade candles VenueId 18 | W2-R5b | **DONE alpha** (#132/#135) |
| Bitfinex VenueId 17 full adapter | W3-BFX | **DONE alpha** (#127/#134) |
| Coinbase International | W2-R10 | **SKIP** (VenueId **19** claimed; no adapter) |
| Native per-venue Statistics24h | W2-R11 | **Skip** |
| KF REST candles | W2-R12 | **Skip** |
| R18–R31 platform extras | R18–R31 | **Skip** |

---

## 5. Recommended worker order

```text
Wave-3 CODE + agent OPS leverage: DONE through #137.

STOP. Implementable exchange-data CODE exhausted on VenueIds 1–18.
Do not start R18–R31 / Coinbase Intl / status-event theater.

Human only:
  OPS-A → OPS-B scheduled ≥7 → OPS-C multi-day → OPS-D attest → OPS-E “1.0 allowed”
```

---

## 6. Channel matrix @ `1b8458b` (CODE plateau)

Legend: **HAVE** = offline SessionMachine emits typed `MarketEvent`; **N/A** = segment/venue has no native public supply for that channel.

| VenueId | Code | T | Q | L2 | Candles | Mark | Index | Funding | OI | Liq | Status |
|--------:|------|:-:|:-:|:--:|:-------:|:----:|:-----:|:-------:|:--:|:---:|:------:|
| 1–16 | (Wave-1/2 shipped) | HAVE | HAVE | HAVE | HAVE or N/A | HAVE or N/A | … | … | … | … | HAVE |
| 17 | `bitfinex` | HAVE | HAVE | HAVE | HAVE (REST) | N/A | N/A | N/A | N/A | N/A | HAVE |
| 18 | `coinbase-adv` | HAVE | HAVE | HAVE | HAVE (REST) | N/A | N/A | N/A | N/A | N/A | HAVE |

Candles **N/A** only where venue has no public candle path (Kraken Futures WS). Candles **HAVE** on Bitstamp/Gemini/Coinbase Exchange/Bitfinex/Coinbase-adv via REST timer. Status **HAVE** engine-wide for wired venues **13–18** (#126/#135). Registry: [`venue_ids.md`](./venue_ids.md). Full per-row matrix: [`orchestrator_wave2_full_data.md`](./orchestrator_wave2_full_data.md) / [`orchestrator_remaining.md`](./orchestrator_remaining.md).

---

## 7. Honesty / non-claims (copy for PRs)

| Evidence | Allowed claim |
|---|---|
| This plan + docs tip sync | planning honesty only |
| Laptop canary cycle_9 (**9/9**) | still **alpha+**; scheduled **= 0** |
| Laptop soak (15m synthetic / ~31m live) | **not** multi-day; **not** stable |
| REST candle corpora / V17 / V18 | alpha offline confidence |
| Merged CODE / fixtures alone | **never** beta / stable / 1.0 |

**Not production-ready without OPS-A…E.**
