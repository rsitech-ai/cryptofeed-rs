# Orchestrator Wave-5 — continuous CODE + agent OPS automation

**Role:** honesty board after Wave-3 exchange-data CODE plateau + Wave-4 PARTIAL-CODE plateau.  
**Parent:** [`orchestrator_wave4.md`](./orchestrator_wave4.md) · [`orchestrator_wave4_partials.md`](./orchestrator_wave4_partials.md)  
**Drive / matrix:** [`production_drive.md`](./production_drive.md) · [`maturity_matrix.md`](./maturity_matrix.md)  
**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md)  
**Tip:** `origin/main` @ `f9dfbb3` (≥ `f9dfbb3` / `954c110`; post-#208)
**Updated:** 2026-07-22  

**Non-negotiable:**

- **Not production-ready. Not beta. Not stable. Not 1.0.**
- Spec §3 / §36 **FAIL** until **OPS-A…E**. Wave-5 CODE / laptop OPS **do not** flip maturity.
- VenueIds **1–18** public §2.1 offline surface remains **exhausted** (Gemini catalog `--live` **N/A**).
- No maturity-flip PRs. No “scheduled = N” claims from laptop scripts.

Branch prefix: `feat/andrzej_orch_w5_<package>` / worker `feat/andrzej_w5_<package>`  
Merge policy: **merge commits** (no squash). Ignore CI red while billing blocked.

---

## 0. Verdict — Wave-5 implementable CODE plateau

**Implementable Wave-5 CODE packages are DONE** (including optional laptop OPS polish **W5-P1e**). Wave-5 is at **CODE plateau** on tip `e1a45b8` (≥ `e1a45b8` / `c1c16cc`; packages through #184).

| Package | PR | Status |
|---|---:|---|
| **W5-P0a** Spec ADR docs `0009`…`0015` | #158 | **DONE** |
| **W5-P0b** catalog `--live` 14/17/18; Gemini **N/A** | #169 / #176 | **DONE** |
| **W5-P0c** coinbase-adv public T/Q/L2 | #174 | **DONE** |
| **W5-P1a** host-opt profile + operator docs | #167 | **DONE** |
| **W5-P1b** proptest smoke (± Loom skip — no sync shared queue) | #167 | **DONE** |
| **W5-P1c** bitfinex `live_ignored` + canary **17–18** | #169 | **DONE** |
| **W5-P1d** private live scripts + richer ignored smokes | #172 | **DONE** |
| **W5-P1e** longer laptop soak (`DURATION` presets + archive) | #179 / #181 + *this* | **DONE** |

| Track | Status |
|---|---|
| Exchange-data §2.1 on ids **1–18** (offline) | **Exhausted** (catalog `--live`: 14/17/18 wired; Gemini **N/A**) |
| Wave-4 PARTIAL→PASS platform CODE | **DONE** |
| Wave-5 implementable CODE (this doc) | **CODE plateau** — P0 + P1a–e **DONE** |
| Maturity / §3 / §36 | **OPS-A…E only** (unchanged) |

**Remaining for §3 / §36 (production claim):** **OPS-A…E only.**

**Explicit:** closing every Wave-5 CODE package (including P1e) still leaves maturity at **alpha / alpha+**. **Not production-ready without OPS-A…E.** **NOT beta / stable / 1.0.**

---

## 1. Closed packages (final status)

### 1.1 Spec §34 ADR docs `0009`…`0015` — **DONE** (#158)

Shipped product ADRs (filenames claimed by product decisions; Spec ADR ids retained in headers where applicable):

| File | Topic |
|---|---|
| `docs/adr/0009-daemon-health-model.md` | daemon health model |
| `docs/adr/0010-spill-wal.md` | SpillWal / SpillToDisk |
| `docs/adr/0011-kafka-nats-minimal-tcp.md` | Kafka/NATS minimal TCP |
| `docs/adr/0012-simd-json-optional.md` | optional simd-json |
| `docs/adr/0013-private-env-only-secrets.md` | private env-only secrets |
| `docs/adr/0014-catalog-live-discovery.md` | catalog `--live` discovery |
| `docs/adr/0015-sighup-partial-reload.md` | SIGHUP partial reload |

**Acceptance met:** seven Accepted ADRs; **no** maturity claim.

### 1.2 Host-opt + proptest — **DONE** (#167)

| Piece | Outcome |
|---|---|
| `[profile.host-opt]` + operator docs | **DONE** — portable release stays thin LTO |
| Minimal `proptest` on `Fixed` | **DONE** |
| Loom | **SKIP** — no sync shared-mutable queue unit; upgrade path documented on `BoundedQueue` |

**Ceiling:** local/operator profile only. Published host-opt binaries / pinned SLO = **OPS-D**.

### 1.3 `catalog --live` Bitstamp / Gemini / Bitfinex (+ Coinbase-adv parse) — **DONE** (#169/#176)

| VenueId | Code | Outcome |
|--------:|------|---------|
| 14 | `bitstamp` | **DONE** — `/api/v2/trading-pairs-info/` + parse (#169) |
| 15 | `gemini` | **N/A** — no clean bulk instruments REST; stub `--live` kept |
| 17 | `bitfinex` | **DONE** — `GET /v2/conf/pub:list:pair:exchange` + parse (#169) |
| 18 | `coinbase-adv` | **DONE** — public `GET …/market/products` + `parse_instruments` (#176 remainder) |

### 1.4 Coinbase Advanced Trade public T/Q/L2 (VenueId **18**) — **DONE** (#174)

| Channel | Map | Status |
|---|---|---|
| `market_trades` | Trade | **DONE** |
| `ticker` | Quote | **DONE** |
| `level2` (wire `l2_data`) | BookSnapshot / BookDelta | **DONE** |
| candles | REST (kept) | **DONE** |
| Private / user channel | — | **OUT OF SCOPE** |

Classic **16** dual protocol remains intentional. Maturity stays **alpha**.

### 1.5 Canary 17–18 — **DONE** (#169)

`INCLUDE_ALPHA` / `INCLUDE_V17_18` paths for bitfinex + coinbase-adv; laptop only — **not** scheduled beta.

### 1.6 Private live expand — **DONE** (#172)

`scripts/laptop_private_canary.sh` + extended/reauth `live_ignored`; secrets env-only; no order entry.

### 1.7 Longer laptop soak — **DONE** (W5-P1e)

| Piece | Status |
|---|---|
| `DURATION` / `SOAK_SECS` presets (30m–8h soft-cap) + archive dirs | **DONE** (#179 + #181 `scripts/laptop_soak.sh`) |
| Optional `DURATION=7200` / `2h` operator run | **DONE** (documented; not required) |
| Closing evidence | **DONE** — #179 **30m** RSS plateau, **0** drops ([`soak_results.md`](../ops/soak_results.md) + [`soak_evidence/runs/synthetic_20260722T151457Z/`](../ops/soak_evidence/runs/synthetic_20260722T151457Z/)); canary cycle_10 |
| Extra laptop archive | **HAVE** — #181 **60m** ([`synthetic_20260722T154111Z/`](../ops/soak_evidence/runs/synthetic_20260722T154111Z/)); still laptop-only |

**Honesty:** #179 **30m** laptop soak ≠ Spec §3.7 multi-day / **not** stable. Optional `DURATION=2h` / `7200` is operator-only — **not** required.

---

## 2. OPS automation — agents CAN vs CANNOT

### 2.1 Agents CAN (laptop / repo automation)

| ID | Work | Honesty bar |
|---|---|---|
| **W5-OPS-a** | Longer laptop soak scripts (`DURATION` / `SOAK_SECS` 30m–8h; RSS CSV; evidence dirs) | **not** multi-day; **not** stable — package **W5-P1e DONE** (#179 30m) |
| **W5-OPS-b** | `laptop_canary.sh` VenueIds **17–18** | **DONE** (#169); laptop N/N ≠ scheduled |
| **W5-OPS-c** | Evidence archival helpers | archival ≠ beta |
| **W5-OPS-d** | Re-run cycle pattern and append evidence | scheduled remains **0** |
| **W5-OPS-e** | Local `cargo test` / clippy / deny / `parse_fixtures_gate` | ignore remote CI while OPS-A blocked |

### 2.2 Agents CANNOT (human / calendar / billing)

| ID | Work | Why |
|---|---|---|
| **OPS-A** | GitHub Actions billing / spending limit | Human account |
| **OPS-B** | Scheduled `canary.yml` ≥7 consecutive greens | Needs OPS-A + calendar; unlocks **beta** |
| **OPS-C** | Multi-day live soak + live chaos inject | Calendar + ops ownership; **stable** path |
| **OPS-D** | Publish tag attestation + SBOM verify | Needs OPS-A + release human |
| **OPS-E** | Explicit “1.0 allowed” | Human only |

Also **cannot:** flip maturity matrix to beta/stable; claim production-ready; invent scheduled canary counts; put API secrets in git.

---

## 3. Package board (final)

### P0 — **DONE**

| Package | Work | Status |
|---|---|---|
| **W5-P0a** | Spec ADR docs `0009`…`0015` | **DONE** #158 |
| **W5-P0b** | catalog `--live` 14/15/17 + adv parse | **DONE** #169/#176 (Gemini **N/A**) |
| **W5-P0c** | coinbase-adv public T/Q/L2 SessionMachine | **DONE** #174 |

### P1 — polish / OPS tooling

| Package | Work | Status |
|---|---|---|
| **W5-P1a** | host-opt profile + operator docs | **DONE** #167 |
| **W5-P1b** | proptest (± Loom) smoke | **DONE** #167 (Loom **SKIP**) |
| **W5-P1c** | bitfinex `live_ignored` + canary **17–18** | **DONE** #169 |
| **W5-P1d** | private live expand (script/archive + richer ignored smokes) | **DONE** #172 |
| **W5-P1e** | Longer laptop soak runner (`DURATION` presets + archive) | **DONE** — #179 laptop 30m synthetic (extra #181 60m); **not** multi-day / **not** stable |

### P2 — stay deferred unless product re-scopes

R18 Kafka/NATS depth, R19 FFI depth, R24 `fastwebsockets`, R25 gRPC/UDS, R26 Arrow/Parquet, R27 shared worker pool, R30 remote TLS/auth, R31 OTel (Wave-4 **SKIP** until named backend), W2-R10 Coinbase Intl (**SKIP** — VenueId **19** claim), W2-R11 native Stats24h, W2-R12 KF REST candles, full `criterion` CI gate.

---

## 4. Recommended sequence (historical)

```text
W5-P0a  ADR docs 0009…0015          ✅ #158
W5-P0b  catalog --live 14/15/17+adv ✅ #169/#176 (Gemini N/A)
W5-P0c  coinbase-adv public T/Q/L2  ✅ #174
W5-P1a  host-opt profile docs       ✅ #167
W5-P1b  proptest (/ Loom SKIP)       ✅ #167
W5-P1c  canary 17–18                ✅ #169
W5-P1d  private live expand         ✅ #172
W5-P1e  longer laptop soak archive  ✅ DONE (#179 30m laptop; not multi-day/stable)

Parallel anytime: W5-OPS-a…e laptop evidence (no maturity flips)

STOP for maturity:
  OPS-A → OPS-B → OPS-C → OPS-D → OPS-E
```

---

## 5. Honesty / non-claims

| Evidence | Allowed claim |
|---|---|
| Any W5-P0/P1 merge | Spec completeness progress; still **alpha / alpha+** |
| Laptop canary including 17–18 | still **not** scheduled beta |
| Multi-hour laptop soak | still **not** Spec §3.7 multi-day |
| All Wave-5 CODE done (+ P1e) | still **not** beta / stable / 1.0 / production-ready |

**Not production-ready without OPS-A…E.**

---

## 6. Delta vs Wave-4

| Item | Wave-4 | Wave-5 |
|---|---|---|
| PARTIAL→PASS CODE | **DONE** plateau | unchanged |
| Former YAGNI (ADR `0009`…`0015`, host-opt, proptest/Loom) | deferred | **DONE** plateau (#158/#167; Loom **SKIP**) |
| catalog `--live` stubs 14/15/17 (+ adv parse) | known ceiling | **DONE** (#169/#176; Gemini **N/A**) |
| coinbase-adv T/Q/L2 | deferred to Classic 16 | **DONE** (#174; still alpha) |
| Agent OPS | cycle_9 scripts | canary 17–18 **DONE**; longer soak **DONE** (laptop #179 30m) |
| Maturity path | OPS-A…E only | **Unchanged** |
