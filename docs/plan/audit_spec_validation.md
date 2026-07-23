# Audit: Spec validation vs production drive

**Auditor role:** AUDITOR / VALIDATOR (ruthless; no false maturity claims)  
**Spec SoT:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md) v1.0  
**Drive board:** [`production_drive.md`](./production_drive.md)  
**Maturity matrix:** [`maturity_matrix.md`](./maturity_matrix.md)  
**CODE backlog:** [`orchestrator_remaining.md`](./orchestrator_remaining.md) · Wave-5 [`orchestrator_wave5.md`](./orchestrator_wave5.md)  
**Wave-2 / Wave-3 / Wave-4:** [`orchestrator_wave2_full_data.md`](./orchestrator_wave2_full_data.md) · [`orchestrator_wave3.md`](./orchestrator_wave3.md) · [`orchestrator_wave4_partials.md`](./orchestrator_wave4_partials.md) · [`orchestrator_wave4.md`](./orchestrator_wave4.md)  
**Prior readiness audit:** [`audit_production_readiness.md`](./audit_production_readiness.md) (stale tip through #50 era)  
**Audit date:** 2026-07-22  
**Audit branch:** `feat/andrzej_audit_w5_ed25f40` (refreshed onto tip `0c268d4`)  
**Tip audited:** `origin/main` @ `0c268d4` (post-#176; W5-P1d private scripts #172; catalog live bitstamp/bitfinex + alpha canary 17–18 #169; host-opt + Fixed proptest #167; Wave-5 plan #163; product ADRs `0009`…`0015` #158; Wave-4 plateau #157; VenueIds **1–18**)  
**Prior tip in this file:** `8858555` (post-#176/#155) — superseded by #156–#173.


---

## Verdict

**NOT production-ready. NOT beta. NOT stable. NOT 1.0.**

**Explicit: NOT production-ready without OPS-A…E.**

| Track | Status @ tip |
|---|---|
| Exchange-data §2.1 offline on VenueIds **1–18** | **Exhausted** offline; catalog `--live` 14/17/18 wired; Gemini **N/A** |
| Wave-4 PARTIAL→PASS platform CODE | **DONE** (#157) |
| Wave-5 continuous CODE | **DONE** plateau — closed: P0a (#158), P1a/b (#167), P0b (#169/#176), P0c (#174), P1c (#169), P1d (#172), P1e (#179/#181; 30m laptop ≠ multi-day) |
| Spec §3 / §36 production claim | **FAIL** — **OPS-A…E only** |

Spec §3 **fails** for any production claim: **0 beta / 0 stable**, scheduled canary **0**, multi-day live soak **absent**, release attestations **not published**, GitHub Actions remote runs **billing-blocked** (ignore CI red/green).

Maturity matrix Spot pair stays **alpha+** — laptop canary **9/9** ≠ scheduled. `INCLUDE_ALPHA` / private laptop scripts are still not scheduled beta.

---

## Local verification @ tip

| Check | Result | Evidence |
|---|---|---|
| `git fetch` + base | **PASS** | `origin/main` @ `0c268d4` (post-#176) |
| `cargo test --workspace` | **PASS** | this session (`CARGO_TARGET_DIR=/tmp/cryptofeed-audit-w5-target`) |
| Clippy key crates | **N/A this session** | Docs refresh; ignore Actions |
| `cargo deny` / remote Actions | **N/A this session** / **BLOCKED** | Billing limit |
| Live canary / soak | **Archived #137** (+ #169/#172 laptop hooks) | Laptop canary **9/9**, reconnect **PASS**; scheduled **0** |

---

## Spec §3 success criteria (production claim gate)

| # | Gate | Status | Evidence / gap |
|---|---|---|---|
| 1 | ≥3 families, spot + derivatives | **PASS** | VenueIds **1–18** alpha CODE present |
| 2 | ≥2 adapters `stable` | **FAIL** | **0 stable. 0 beta.** Spot pair `alpha+` only |
| 3 | L2 seq/gap/checksum/snapshot/replay | **PASS** (offline) | Corpora + identity tests; coinbase-adv L2 still Wave-5 CODE |
| 4 | All buffers bounded | **PASS** | ActionBuffer + pending/dispatch/books/timers/sinks |
| 5 | No silent drops | **PASS** (policy) | `Drop*` → `EventsDropped`. Not multi-day soak-proven |
| 6 | Failures → metric + diagnostic | **PASS** (core) | §23.2 counters + histograms on `/metrics` |
| 7 | Continuous soak, bounded memory | **FAIL** (for production) | Laptop soaks only; **multi-day live soak OPS** |
| 8 | Chaos matrix | **PARTIAL** | Unit/fuzz DONE; **live inject OPS** |
| 9 | Dep/license/vuln/provenance | **PARTIAL** | deny/LICENSE/SBOM YAML; **no published attestation** |
| 10 | Public API / maturity docs | **PASS** (honesty) | Matrix, drive, Wave-3/4/5, facade, runbooks |

**§3 overall: FAIL** — production claims are **disallowed**.

---

## Major spec sections

Statuses: **PASS** | **PARTIAL** | **FAIL** | **N/A**

| § | Topic | Status | Evidence |
|---|---|---|---|
| **1** | Purpose | **PASS** | Greenfield Rust engine; embedded + daemon |
| **2.1** | Included in v1 | **PASS** / **PARTIAL** | Strong on **1–18**; coinbase-adv public T/Q/L2 **open** (W5-P0c) |
| **2.2** | Deferred but supported | **PASS** | private/ffi scaffolds; private **alpha** + laptop private canary scripts (#172) |
| **2.3** | Explicit non-goals | **PASS** | No strategy/portfolio/risk/SOR |
| **3** | Success criteria | **FAIL** | See §3 table |
| **4** | Architecture alternatives | **PASS** | Library-first + optional daemon |
| **5** | Architecture principles | **PASS** | Engine owns I/O; Fixed; bounded queues |
| **6** | System architecture | **PASS** | Supervisor / session / transport / sinks |
| **7** | Workspace scaffold | **PASS** | Includes facade `marketfeed` (#144) |
| **8** | Domain model | **PASS** | Fixed model + VenueId map |
| **9** | Ordering guarantees | **PASS** | Per-session scopes |
| **10** | Subscription model | **PASS** | Static config + `EngineControl` (**R5**) |
| **11** | Adapter architecture | **PARTIAL** | Pattern **PASS**; **0 beta / 0 stable**; coinbase-adv channel depth open |
| **12** | Session runtime / supervision | **PASS** | Lifecycle, reconnect, timers |
| **13** | Runtime profiles | **PARTIAL** | Affinity + `[profile.host-opt]` (#167); published SLO = **OPS** |
| **14** | Transport | **PASS** | Tokio + rustls tungstenite |
| **15** | Parsing / frames | **PASS** | Serde + optional `simd-json` |
| **16** | Order-book subsystem | **PASS** | Snapshot/delta/gap/invalidate |
| **17** | Dispatch / backpressure | **PASS** | Bounded + `SpillWalSink` |
| **18** | Recording format | **PASS** | MFR1 + **MFNE-JSON1** (#149); Arrow/Parquet YAGNI |
| **19** | Public Rust API | **PASS** | `EngineControl` + facade `marketfeed` (#144) |
| **20** | Standalone daemon | **PASS** / **PARTIAL** | `catalog --live` for bitstamp/bitfinex/coinbase-adv/gemini + prior venues; synthetic still stub |
| **21** | Configuration | **PASS** (MAY subset) | SIGHUP applies `log_level` + `[readiness]` (#146) |
| **22** | Error model | **PASS** | Typed errors; fail-loud vs drop explicit |
| **23** | Observability | **PASS** (baseline) | Tracing + Prometheus; OTel **SKIP** (#146) |
| **24** | Performance / capacity | **PARTIAL** | Instant harness + `parse_fixtures_gate` + host-opt; criterion crate / published budgets open or OPS |
| **25** | Reliability / data quality | **PASS** (core) | Book invalidation, R6 status/catalog **13–18** |
| **26** | Security | **PASS** / **PARTIAL** | Secrets env-only; remote TLS/auth N/A (**R30** YAGNI) |
| **27** | Testing strategy | **PARTIAL** | Fixtures/corpora/fuzz + Fixed **proptest** (#167); private richer `live_ignored` (#172); live scheduled = OPS |
| **28** | CI / quality gates | **PARTIAL** | Workflows present; remote **billing-blocked** |
| **29** | Dependency baseline | **PASS** / **PARTIAL** | deny/LICENSE **PASS**; remote advisory blocked |
| **30** | Build / release profiles | **PARTIAL** | `[profile.host-opt]` local (#167); published artifacts = **OPS-D** |
| **31** | Open-source readiness | **PASS** | Dual-license, NOTICE, CONTRIBUTING, SECURITY |
| **32** | Versioning / compatibility | **PARTIAL** | Pre-1.0 (`0.1.0`); MSRV `1.85` |
| **33** | Adapter roadmap | **PARTIAL** | Wave-5 leftovers named |
| **34** | ADRs | **PARTIAL** | Product ADRs `0001`…`0015` (#141/#149/#158). **Spec §34 ADR-009…015** still missing as dedicated Spec ADRs (**W5-P0a**; next free file ids) |
| **35** | DoD per adapter | **PARTIAL** | Offline DoD strong; live canary/soak incomplete |
| **36** | Production-ready engine | **FAIL** | Same blockers as §3 + OPS-E |
| **37** | First implementation slice | **PASS** | Synthetic + Binance Spot proven |
| **38–39** | Ecosystem / recommendation | **N/A** | Reference only |

---

## PARTIAL inventory — CODE-fixable vs OPS-only

### CODE-fixable (Wave-5 — **no** maturity unlock)

| § / package | What's missing | Status @ tip | Notes |
|---|---|---|---|
| **W5-P0a** / §34 | Spec table ADR-009…015 docs | **OPEN** | Product `0009`…`0015` already used → next free ids |
| **W5-P0b** / §20 | catalog `--live` remainder | **DONE** | 14/17 #169; adv parse; Gemini **N/A** (N+1) |
| **W5-P0c** / §2.1 | coinbase-adv public T/Q/L2 | **DONE** (#174) | Public WS; maturity stays **alpha** |
| **W5-P1a** / §13/§30 | host-opt profile + docs | **DONE** (#167) | Local/operator only |
| **W5-P1b** / §27 | proptest smoke | **DONE** (#167 Fixed) | Loom optional / YAGNI |
| **W5-P1c** | bitfinex `live_ignored` + canary 17–18 | **DONE** (#169) | Laptop `INCLUDE_ALPHA`; **not** scheduled |
| **W5-P1d** | private live expand scripts | **DONE** (#172) | `laptop_private_canary.sh`; secrets env-only |
| **W5-P1e** | longer laptop soak archive | **DONE** (#179/#181) | Closing evidence #179 **30m**; still **not** multi-day / **not** stable |
| **R30** / §26 | Remote TLS/auth | **YAGNI** | N/A until exposed |
| **§24** criterion crate | Full criterion + pinned CI | **YAGNI** | Instant + local gate ship |

### OPS-only (cannot be closed by more CODE PRs)

| § | What's missing | OPS id | Unlocks |
|---|---|---|---|
| **§3.8** / chaos live | Live chaos inject | **OPS-C** | stable path |
| **§3.9** / **28** / **30** publish | Remote Actions + tag attestation + SBOM | **OPS-A** + **OPS-D** | §3.9 |
| **11** maturity | any adapter `beta` / `stable` | **OPS-B** / **OPS-C** | maturity flip |
| **13** / **24** published | Live SLO / capacity budgets | **OPS** | ops claim only |
| **27** scheduled live | Scheduled evidence beyond laptop | **OPS-B** / **OPS-C** | beta / stable |
| **28** | Workflows run on `main` | **OPS-A** | prerequisite |
| **29** remote | Remote deny / advisory CI | **OPS-A** | evidence only |
| **32** | Leave pre-1.0 | **OPS-E** after ≥2 stable | production claim |
| **35** | Live canary / soak DoD | **OPS-B** / **OPS-C** | beta / stable |
| **§3.2 / §3.7 / §36** | ≥2 `stable`; multi-day soak; production engine | **OPS-B…E** | beta / stable / production |

**Agent laptop OPS (W5-OPS-a…e)** can extend soaks/canaries/archives but **cannot** satisfy OPS-A…E or flip maturity.

### Split summary

| Bucket | Items | Maturity effect |
|---|---|---|
| **CODE remaining (Wave-5 P0)** | W5-P0a | **None** |
| **CODE remaining (P1 optional)** | Loom; criterion; R30 | **None** (P1e **DONE**) |
| **CODE closed this tip wave** | W5-P0a (#158); W5-P1a/b (#167); W5-P0b (#169/#176); W5-P0c (#174); W5-P1c (#169); W5-P1d (#172); W5-P1e (#179/#181) | **None** |
| **OPS-only** | OPS-A…E | **Only** path to beta / stable / production claim |

---

## Drive board alignment

| Board claim | Audit judgment |
|---|---|
| Not production-ready / not beta / not stable / not 1.0 | **Agree** |
| Wave-4 PARTIAL-CODE plateau **DONE** | **Agree** |
| Wave-5 continuous CODE plateau **DONE** (P1e laptop-only); maturity still OPS-A…E | **Agree** — P1e closed on #179 30m; still not multi-day |
| Product ADRs `0009`…`0015` close Spec §34 ADR-009…015 | **Disagree** — still **W5-P0a** |
| Laptop / `INCLUDE_ALPHA` / private scripts ≠ scheduled beta | **Agree** (`scheduled = 0`) |

---

## Per-venue maturity (§11.8)

| Adapter | Maturity | Blocks beta |
|---|---|---|
| Binance Spot / OKX Spot | **alpha+** | scheduled canary ≥7 (**scheduled = 0**) |
| Bitfinex (17) | **alpha** | scheduled canary (laptop hook exists) |
| Coinbase-adv (18) | **alpha** | T/Q/L2 CODE open + live |
| All other public | **alpha** | live canary + soak |
| Private | **alpha** | secrets + scheduled soak (laptop private script ≠ beta) |

**Stable: 0. Beta: 0.**

---

## Must-fix (ranked)

### P0 — maturity / production claim (**OPS only**)

| Rank | ID | Work | Unlocks |
|---:|---|---|---|
| 1 | **OPS-A** | Unblock GitHub Actions billing | Prerequisite |
| 2 | **OPS-B** | Scheduled live canary ≥7 for ≥2 venues (Spot pair) | **beta** |
| 3 | **OPS-C** | Multi-day live soak + live chaos inject | **stable** path |
| 4 | **OPS-D** | Publish tag attestation + SBOM after OPS-A | §3.9 |
| 5 | **OPS-E** | Explicit human “production claim allowed” after ≥2 **stable** + §3 | production-ready claim |

### CODE — Wave-5 remaining (does **not** unlock maturity)

| Rank | ID | Work | Status |
|---:|---|---|---|
| 6 | **W5-P0a** | Spec §34 ADR-009…015 docs (next free file ids) | **OPEN** |
| 7 | **W5-P1e** | longer laptop soak archive | **DONE** (#179 30m laptop; not multi-day) |
| — | W5-P1a/b; W5-P0b (incl. Gemini N/A + adv parse); W5-P0c; W5-P1c; W5-P1d | **DONE** | #167 / #169 / #172 / #174 + remainder |
| — | Wave-3/4 plateaus | **DONE** | unchanged |
| — | P2 / R18–R31 depth | **YAGNI** | unless re-scoped |

### Explicit non-claims

- Do **not** flip maturity from fixtures, corpora, laptop canary / `INCLUDE_ALPHA` / private scripts, facade, catalog `--live`, SIGHUP, MFNE-JSON1, product ADRs, host-opt, proptest, or Wave-5 merges.
- Do **not** treat Actions as green while billing blocks job start.
- Do **not** claim production-ready without OPS-A…E.
- **Not production-ready without OPS-A…E.**

---

## Sequencing

```text
OPS-A  Actions billing
  └─► OPS-B scheduled canary ≥7  ─► beta (per venue)
        └─► OPS-C multi-day soak + live chaos ─► stable path
              └─► OPS-D attest publish + OPS-E sign-off ─► production claim allowed

Parallel (no maturity):
  W5 CODE plateau DONE (incl. P1e laptop)
  W5-OPS-a…e laptop evidence only
```

---

## Delta vs prior audits

| Item | Prior @ `8858555` | This tip @ `0c268d4` |
|---|---|---|
| Tip | through #154/#155 | through #173 |
| Wave-4 PARTIAL-CODE | packages landed | **DONE** (#157) |
| Wave-5 | absent | **DONE** plateau (#163 + #158/#167/#169/#172/#174/#176/#179/#181); P1e laptop-only |
| §34 Spec ADR-009…015 | open | still **OPEN** (product ADRs ≠ Spec rows) |
| catalog `--live` 14/17/18 | stubs / partial | bitstamp/bitfinex/adv **DONE**; Gemini **N/A** |
| host-opt / proptest / private scripts | YAGNI | **DONE** (#167/#172) |
| Beta / stable / scheduled | 0 / 0 / 0 | **Still 0 / 0 / 0** |
| `cargo test --workspace` | PASS | **PASS** this session |

---

## Honesty bar (copy for PR / release notes)

| Evidence | Allowed claim |
|---|---|
| Local `cargo test` / clippy / deny | alpha engineering quality |
| Offline corpora + facade + catalog `--live` + MFNE + SIGHUP + ADRs + host-opt + proptest | still alpha — **not** beta |
| Any Wave-5 merge | spec completeness progress — still **alpha / alpha+** |
| Laptop canary **9/9** / `INCLUDE_ALPHA` / private laptop scripts | still `alpha+` / alpha — **scheduled = 0** |
| Scheduled canary ≥7 | **beta** (per venue) |
| Multi-day live soak + live chaos | path to **stable** |
| ≥2 stable + OPS-E | production-ready claim |
