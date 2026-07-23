# Orchestrator Wave 4 — PARTIAL → PASS (platform CODE, not exchange-data)

**Role:** prioritize remaining **audit PARTIAL** sections that still move toward **PASS with CODE**, after exchange-data CODE on VenueIds **1–18** is exhausted  
**Base tip:** `origin/main` @ `0f4c88f` (≥ `011393a`; post-#176)  
**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md)  
**Priors:** [`audit_spec_validation.md`](./audit_spec_validation.md), [`production_drive.md`](./production_drive.md), [`orchestrator_wave3.md`](./orchestrator_wave3.md), [`orchestrator_remaining.md`](./orchestrator_remaining.md), [`orchestrator_wave4.md`](./orchestrator_wave4.md)  
**Updated:** 2026-07-22  

**Non-negotiable (unchanged from Wave-3):**

- **Not production-ready. Not beta. Not stable. Not 1.0.**
- Spec §3 / §36 **FAIL** until **OPS-A…E**. Closing Wave-4 CODE does **not** flip maturity.
- Exchange-data / §2.1 surface on VenueIds **1–18** remains **exhausted**.
- This wave was **platform / public-API / recording / docs** completeness toward “full spec,” not a maturity unlock.

Branch prefix: `feat/andrzej_orch_w4_<package>`  
Merge policy: **merge commits** (no squash). Ignore CI red while billing blocked.

---

## 0. Verdict — PARTIAL-CODE track **CODE plateau**

**Implementable Wave-4 PARTIAL→PASS CODE packages are DONE.** Platform PARTIAL-CODE track is at **CODE plateau** on tip `0f4c88f` (≥ `011393a`; packages through #155).

| Package | PR | Status |
|---|---:|---|
| **W4-P0a** MFNE-JSON1 normalized recording | #149 | **DONE** |
| **W4-P0b** facade `marketfeed` | #144 | **DONE** |
| **W4-P0c/d** ADRs `0001`…`0008` + §24 Instant/comparison criterion docs | #141 | **DONE** |
| **W4-P1a** catalog `--live` | #147 | **DONE** |
| **W4-P1b** SIGHUP partial reload; OTel **SKIP** | #146 | **DONE** (partial) / **SKIP** |
| **W4-P1c** `parse_fixtures_gate` | #148 | **DONE** |
| Audit tip honesty | #154 / #155 | **DONE** |

**Remaining for §3 / §36 (production claim):** **OPS-A…E only.**

**Optional YAGNI** (not required for PARTIAL-CODE plateau; do not assign unless product re-scopes):

| Item | Spec / backlog | Notes |
|---|---|---|
| proptest / Loom / differential suites | §27 | Polish; fixtures/corpora/fuzz already strong |
| host-opt / `target-cpu=native` published profiles | §13 / §30 | Needs OPS-D published artifacts + pinned host |
| ADR-009…015 | §34 | Key decisions covered by `0001`…`0008`; remainder optional |
| OpenTelemetry (`otel` feature) | §23.3 / R31 | Explicit Wave-4 **SKIP** — baseline logs+Prometheus DONE |
| Full `criterion` crate + pinned CI gate | §24 / R23 | Instant harness + local >10% gate ship; upgrade path documented |

**Explicit: NOT beta / stable / 1.0.** Wave-4 CODE cannot make the repo production-ready.

---

## 1. Audit PARTIAL inventory (final Wave-4 classification)

| § | Audit status | Wave-4 class | Why |
|---|---|---|---|
| **§3** success criteria | **FAIL** | **OPS-only** | Rows 2/7–9 need OPS-A…E |
| **§8** chaos | **PARTIAL** | **OPS-only** (live inject) | Offline harness DONE; live inject = **OPS-C** |
| **§9** provenance | **PARTIAL** | **OPS-only** | YAML ready; publish = **OPS-A + OPS-D** |
| **§11** adapters / maturity | **PARTIAL** | **OPS-only** (maturity) | Pattern PASS; 0 beta/0 stable = OPS-B/C |
| **§13** runtime profiles | **PARTIAL** | **YAGNI / OPS** | Affinity skeleton DONE; published SLO = laptop/OPS |
| **§18** recording | **PASS** raw / **PASS** MFNE-JSON1 | **DONE** (W4-P0a #149) | Proto-aligned JSONL + read; Debug retained |
| **§19** public Rust API | **PASS** | **DONE** (W4-P0b #144) | `EngineControl` + facade `marketfeed` |
| **§20** daemon | **PASS** core / stub+live catalog | **DONE** (W4-P1a #147) | catalog `--live` REST for venues with parsers |
| **§21** configuration | **PASS** / **PARTIAL** hot reload | **DONE** lite (W4-P1b #146) | SIGHUP apply safe knobs; unsafe → restart required |
| **§23** observability | **PARTIAL** (baseline PASS) | **DONE** baseline; OTel **SKIP** | Prometheus + tracing; R31 YAGNI |
| **§24** performance | **PARTIAL** | **DONE** tools (W4-P0d #141 + W4-P1c #148) | Instant harness + local >10% gate; criterion crate / CI = YAGNI |
| **§26** security | **PASS** / **PARTIAL** | **YAGNI** | Remote TLS/auth N/A until control plane exposed |
| **§27** testing | **PARTIAL** | **YAGNI** / **OPS** | proptest/Loom = polish; live = OPS |
| **§28** CI gates | **PARTIAL** | **OPS-only** | Workflows present; remote = **OPS-A** |
| **§29** deps | **PASS** / **PARTIAL** | **OPS** slice | deny DONE; remote vuln runs blocked |
| **§30** build/release | **PARTIAL** | **OPS-only** | host-opt / published artifacts need **OPS-D** |
| **§32** versioning | **PARTIAL** | **YAGNI** | Pre-1.0 intentional until OPS-E |
| **§33** roadmap | **PARTIAL** | **YAGNI** | Phases landed; Phase 4/5 stubs OK |
| **§34** ADRs | **PARTIAL** → key set **DONE** | **DONE** (W4-P0c #141) | `docs/adr/0001`…`0008`; ADR-009…015 optional YAGNI |
| **§35** adapter DoD | **PARTIAL** | **OPS-only** (live) | Offline DoD strong; live canary/soak = OPS-B/C |
| **§36** production engine | **FAIL** | **OPS-only** | Same as §3 |

---

## 2. Shipped packages (evidence)

### 2.1 W4-P0a — §18 MFNE-JSON1 — **DONE** (#149)

`NormalizedFormat::Jsonl` proto-aligned JSONL + `read_normalized_jsonl`; Debug retained; MFR1 untouched. ADR-0008.

### 2.2 W4-P0b — §19 facade `marketfeed` — **DONE** (#144)

Package `marketfeed` at `crates/facade` re-exports model / control / session / sinks. Smoke + doctest.

### 2.3 W4-P0c/d — §34 ADRs + §24 criterion docs — **DONE** (#141)

ADRs `0001`…`0008` for key decisions. §24 Instant `parse_fixtures` + documented comparison criterion (no `criterion` crate dep — upgrade path named).

### 2.4 W4-P1a — §20 catalog `--live` — **DONE** (#147)

Daemon `catalog --live` one-shot REST via factory parsers. Stub venues error on `--live`.

### 2.5 W4-P1b — §21 SIGHUP partial; §23 OTel — **DONE** / **SKIP** (#146)

SIGHUP validates TOML, applies `log_level` / readiness; unsafe keys log restart required. OTel feature **SKIP** (see [`orchestrator_wave4.md`](./orchestrator_wave4.md)).

### 2.6 W4-P1c — §24 local gate — **DONE** (#148)

`scripts/parse_fixtures_gate.sh` + `docs/ops/parse_fixtures_baseline.txt`; default **>10%** fail. Not Actions/maturity gate.

---

## 3. Explicit OPS-only (do **not** assign CODE workers)

| ID | Work | Unlocks | Agent action |
|---|---|---|---|
| **OPS-A** | GitHub Actions billing | Remote CI / scheduled jobs / attest runs | **None** — human billing |
| **OPS-B** | Scheduled canary ≥7 (Binance Spot + OKX Spot) | **beta** | No maturity-flip PRs |
| **OPS-C** | Multi-day live soak + live chaos inject | **stable** path / §3.7–§3.8 / §8 live | No chaos-inject theater |
| **OPS-D** | Publish tag attestation + SBOM verify | §3.9 / §30 artifacts | YAML already enabled |
| **OPS-E** | Human “1.0 allowed” | **1.0** / production-ready | Human only |

Also OPS-classified PARTIALs: §9 publish, §11 maturity, §28 remote CI, §29 remote deny runs, §30 published profiles, §35 live DoD, §36 production engine.

**Remaining for §3 / §36 = OPS-A…E only.**

---

## 4. YAGNI skips (closed wave + optional leftovers)

| Item | Spec / backlog | Why skip |
|---|---|---|
| New VenueId families / Coinbase Intl | W2-R10 | Exchange-data exhausted |
| Native per-venue `Statistics24h` | W2-R11 | Synthetic proves type |
| KF REST candle backfill | W2-R12 | WS N/A correct |
| Kafka/NATS depth (`rdkafka` / JetStream) | **R18** | TCP Produce/PUB DONE |
| FFI beyond stub | **R19** | Stub sufficient |
| Private live soak / secrets | **R20** | OPS + secrets |
| prost codegen | **R22** full | Hand MFPE-PB1 / MFNE-JSON1 ship |
| `fastwebsockets` alt transport | **R24** | Optional |
| gRPC / UDS streaming API | **R25** | HTTP health/metrics sufficient |
| Arrow / Parquet analytics sink | **R26** | §18.5 optional |
| Shared connection worker pool | **R27** | Affinity + session=shard DONE |
| Remote control TLS/auth | **R30** | Loopback-only |
| **proptest / Loom / differential** | §27 | **Optional YAGNI** |
| **host-opt published profiles** | §13 / §30 | **Optional YAGNI** (OPS-D) |
| **ADR-009…015** | §34 | **Optional YAGNI** (0001–0008 cover keys) |
| **OpenTelemetry** | §23.3 / R31 | **Optional YAGNI** (Wave-4 SKIP) |
| Full `criterion` + CI timing gate | §24 / R23 | Instant + local gate enough |
| Published latency SLO numbers | §13 / §24.2 | Needs pinned OPS runner |
| Maturity matrix flips | §11.8 | OPS-B/C only |

---

## 5. Worker packages — all **DONE**

### P0 — **DONE**

| Package | PR | Acceptance |
|---|---:|---|
| **W4-P0a** normalized recording | #149 | MFNE-JSON1 + tempfile round-trip |
| **W4-P0b** facade crate | #144 | `use marketfeed::…` + smoke/doctest |
| **W4-P0c** ADR pack | #141 | `docs/adr/0001`…`0008` |
| **W4-P0d** §24 criterion docs | #141 | Instant harness + comparison criterion docs |

### P1 — **DONE** / **SKIP**

| Package | PR | Acceptance |
|---|---:|---|
| **W4-P1a** catalog live discovery | #147 | REST one-shot for venues with parsers |
| **W4-P1b** hot reload lite | #146 | SIGHUP safe knobs; ceiling documented |
| **W4-P1c** bench regression script | #148 | Local >10% fail helper |
| **W4-P1d** OTel feature | #146 | **SKIP** (YAGNI) |

### P2 — deferred YAGNI (unchanged)

R18–R20, R24–R27, R30, W2-R10–R12, proptest, host-opt, ADR-009…015, OTel, full criterion CI.

---

## 6. STOP for CODE

```text
Wave-4 PARTIAL-CODE packages: ALL DONE (or OTel SKIP).
PARTIAL-CODE track: CODE plateau @ `0f4c88f`.

STOP for CODE. Maturity still:
  OPS-A → OPS-B → OPS-C → OPS-D → OPS-E

Optional YAGNI (not gates): proptest, host-opt, ADR-009…015, OTel, criterion CI.
```

---

## 7. Honesty / non-claims

| Evidence | Allowed claim |
|---|---|
| Wave-4 packages merged | platform PARTIAL → PASS (named §§); still **alpha / alpha+** |
| PARTIAL-CODE plateau docs | planning honesty only |
| Any Wave-4 CODE alone | **never** beta / stable / 1.0 / production-ready |

**Not production-ready without OPS-A…E.**

---

## 8. Delta vs Wave-3

| Item | Wave-3 | Wave-4 (closed) |
|---|---|---|
| Exchange-data VenueIds 1–18 | **Exhausted** | **Still exhausted** |
| Platform PARTIAL CODE | not scoped | **DONE** — plateau |
| Agent focus | Docs honesty + laptop OPS | Platform PARTIAL CODE (shipped) |
| Maturity path | OPS-A…E only | **Unchanged** — OPS-A…E only |
