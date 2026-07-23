# Audit: Production Readiness vs Spec 1.0

**Auditor role:** continuous validator (ruthless)  
**Spec:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md) v1.0  
**Prior audits:** PR #14 era; #27 / #34 follow-ups; handoff @ `e1de92c` / #44; canary #46–#47; #50 laptop 7/7 + soak  
**Audit date:** 2026-07-22  
**Audit branch:** `feat/andrzej_reconnect_probe` (base `origin/main` @ `e13ab87`)  
**Observed tip:** `origin/main` @ `e13ab87` — Merge PR **#50** (canary 7/7 + ~31m live soak); this PR adds reconnect probe  
**Drive board:** [`production_drive.md`](./production_drive.md)

### Verdict

**NOT production-ready. NOT beta. NOT stable. NOT 1.0.**

Do not ship, market, or operate as a production market-data engine. Offline CODE quality on tip is high; live canary is laptop **7/7** / scheduled **0**; reconnect probe **PASS** (laptop). Spec §3 still fails because maturity promotion requires **scheduled** canary ≥7 and multi-day soak/live chaos/provenance publish — **human OPS**, not another adapter PR.

---

## 0. Merge-aware tip map (through #50 + reconnect probe)

| PR range | State | Contents |
|---|---|---|
| 1–30 | **merged** | domain → engine → venues → bounds → metrics → multi-venue → L2 families → audits |
| 31–36 | **merged** | offline maturity wave; OKX daemon wire; offline `marketfeed run` E2E |
| 37 | **merged** | ActionBuffer overflow → `EventsDropped` |
| 38–39 | **merged** | Binance USD-M corpus; recording crash-recovery + tag SBOM provenance |
| 40 | **merged** | release attestation **stub** (commented) |
| 41 | **merged** | maturity matrix + CODEOWNERS; chaos unit harness; USD-M OI timer |
| 42 | **merged** | Binance Spot + OKX Spot `alpha+` docs; frame-to-event histogram buckets |
| 43 | **merged** | OKX Spot L2 book corpus + daemon histogram scrape |
| 44 | **merged** | production-drive handoff docs |
| 45 | **merged** | rustfmt so `cargo fmt --check` passes on `main` |
| 46–47 | **merged** | live `drain_dispatch` fix; Binance/OKX canary evidence (series start, not beta) |
| 48 | **merged** | enable `release.yml` attest job; offline 20m soak RSS log; audit refresh |
| 50 | **merged** | laptop canary **7/7** + ~31m live soak (still not beta) |
| *this* | **this PR** | laptop reconnect probe **PASS** (still not beta; **scheduled = 0**) |

**Honesty on PR titles:** “beta” / “offline beta proofs” / `alpha+` ≠ §11.8 **beta** (needs scheduled live canary ≥7).

---

## 1. Spec §3 success criteria — gate-by-gate @ tip

| # | Gate | Status | Remaining production gap |
|---|---|---|---|
| 1 | ≥3 families, spot + derivatives | **PASS** (caveat) | Spot candles (Binance/OKX). Binance Coin-M trades/quote/mark/funding/OI/liq/L2/candles (still **alpha**). |
| 2 | ≥2 adapters `stable` | **FAIL** | **0 stable. 0 beta.** Spot pair is `alpha+` only (laptop canary **7/7**, not scheduled). |
| 3 | L2 seq/gap/checksum/snapshot/replay | **Partial↑** | Strong fixtures; OKX Spot L2 corpus on tip; not all venues have book corpora. |
| 4 | All buffers bounded | **PASS** | ActionBuffer + pending + dispatch/books/timers. |
| 5 | No silent drops | **PASS** (policy) | Drop* → `EventsDropped`. Not multi-day soak-proven. |
| 6 | Failures → metric + diagnostic | **PASS** (core) | Frame/parse/REST/sink §23.2 fixed-bucket hists on `/metrics`. |
| 7 | Continuous soak, bounded RSS | **FAIL** | Offline 20m synthetic logged ([`soak_results.md`](../ops/soak_results.md)); **multi-day live still required**. |
| 8 | Chaos matrix | **Partial** | Unit/fuzz harness landed; live inject **OPS**. |
| 9 | Dep/license/vuln/API/provenance | **Partial↑** | Local deny green; attest job **enabled** in YAML (hard-fail); Actions billing still blocks runners; no published tag attestation yet. |
| 10 | Public API / maturity matrix docs | **PASS** (honesty) | Matrix + CODEOWNERS + canary checklist present. |

**§3 overall: FAIL (production claim disallowed).**

---

## 2. Offline verification @ tip

| Check | Result |
|---|---|
| `cargo test --workspace` | **PASS** (this session) |
| `cargo clippy --workspace --all-targets -- -D warnings` | not re-run this session (prior tip green; script/docs + YAML only) |
| `cargo deny check` | not re-run this session (no dep changes) |
| Offline soak `SOAK_SECS=1200` | **PASS** — see [`docs/ops/soak_results.md`](../ops/soak_results.md) |
| Live canary (laptop) | **7/7** consecutive `live_ignored`; scheduled **0**; reconnect **PASS** |
| Live soak (laptop) | ~31 min binance+okx; RSS flat; 0 drops — not multi-day |
| GitHub Actions on `main` | **BLOCKED** — spending/billing limit; runs fail in ~3–6s without executing jobs |

Local merge bar remains: fmt / clippy / test / deny. Remote CI is **not** a green signal.

---

## 3. Per-exchange maturity (§11.8)

Spec vocabulary: **experimental | beta | stable**. Informal `alpha` / `alpha+` are short of beta.

| Adapter | VenueId | Maturity @ tip | Blocks beta |
|---|---:|---|---|
| synthetic | 1 | experimental (test) | N/A |
| Binance Spot | 2 | **alpha+** (beta-ready offline; laptop canary **7/7**; reconnect PASS) | scheduled canary ≥7 (**scheduled = 0**) |
| Binance USD-M | 3 | **alpha** | canary + soak + deeper L2 corpus optional |
| OKX Spot | 4 | **alpha+** (beta-ready offline; laptop canary **7/7**; reconnect PASS) | scheduled canary ≥7 (**scheduled = 0**) |
| OKX SWAP / Futures | 9 / 10 | **alpha** | close-out docs + canary |
| Bybit linear / spot / inverse | 5 / 6 / 11 | **alpha** (inverse thin) | canary + soak |
| Kraken Spot | 7 | **alpha** | canary + soak |
| Deribit | 8 | **alpha** | canary + soak |

**Stable: 0. Beta: 0.** Canary progress: laptop **7/7**; scheduled **0**; reconnect **PASS** (laptop).

---

## 4. Delta vs prior audits

| Blocker | Status @ tip |
|---|---|
| Silent Drop* / ActionBuffer honesty | **Closed** |
| §23.2 scrape + frame/parse/REST/sink hists | **Closed** (C8) |
| Daemon multi-venue + enable_l2 | **Closed in code** — Coinbase Classic L2 uses env-only credentials and a signed subscribe; credential-backed live evidence remains OPS |
| Maturity matrix + CODEOWNERS | **Closed** |
| Chaos **unit** harness | **Closed**; live inject **OPS** |
| Recording crash-recovery / schema compat | **Closed** (unit); disk WAL chaos live **OPS** |
| Live `Dispatch(FailEngine)` on hot venues | **Closed** (#46 null-sink drain) |
| Spot laptop canary | **Partial** — laptop **7/7** archived; reconnect **PASS**; scheduled **0** |
| Offline laptop soak RSS | **Partial** — 20m synthetic logged; multi-day live **OPS** |
| Release attest job in YAML | **Closed** (#48); publish still blocked by billing |
| Two stables / soak / live chaos / billing | **Open — OPS** |

---

## 5. Remaining work

### 5.1 USER OPS CHECKLIST (required — see drive board §2)

Humans must complete for beta / stable / 1.0:

1. **Actions billing** — unblock spending limit; prove CI jobs actually run on `main`.
2. **Live canary ≥2 venues** — laptop **7/7** + reconnect **PASS** today; still need **scheduled** ≥7 before **beta**.
3. **Multi-day soak** — laptop 20m synthetic done; live multi-day + chaos still required for **stable**.
4. **Release attestations** — job enabled in YAML; tag RC after billing; publish verifyable provenance.
5. **Explicit 1.0 sign-off** — only after ≥2 **stable** + §3.7–§3.9 evidence.

### 5.2 CODE items — **CODE plateau** (cheap)

No remaining **cheap** CODE packages that move Spec §3 toward a production claim without OPS.

Optional / non-blocking CODE (do not prioritize over OPS):

| ID | Package | Why optional |
|---|---|---|
| M1 | parse/REST/sink histogram buckets | **DONE** (C8) |
| M2 | Deeper L2 corpora (USD-M / Bybit / Kraken books) | Improves §3.3 depth; does not grant beta |
| M3 | Fuzz expand / Windows·aarch64 CI | Bandwidth; needs Actions billing for CI value |
| M4 | Normalized event recording path | Spec depth; not a maturity unlock |
| M5 | Inverse/coin-M ADR or deepen Bybit inverse | Product boundary honesty |

**Do not** open fixture-only “beta” PRs.

---

## 6. Sequencing

```text
OPS-A  Actions billing          ─► remote CI real
OPS-B  Live canary ≥2 venues    ─► beta (per venue; laptop 7/7 + reconnect PASS, scheduled 0)
OPS-C  Multi-day soak           ─► stable path
OPS-D  Release attestations     ─► §3.9 (YAML ready; publish after A)
OPS-E  Human 1.0 sign-off       ─► only after ≥2 stable
Optional M1–M5                  ─► never ahead of OPS-A/B
```

---

## 7. Method

- Spec §§3, 11.8, 23, 26–29, 36 vs tree at `e13ab87` (#50) + reconnect probe PR.
- Local: reconnect probes live; `KillSwitchWebSocket` unit test.
- Remote: Actions billing-blocked; ignore red for merge decisions.
- Production / beta / stable / 1.0 claims: **denied.**

**Next auditor:** re-run §3 only after USER OPS CHECKLIST evidence (canary archives **scheduled ≥7** + multi-day live soak RSS + live CI greens + tag attestation verify) — never promote on laptop canary/reconnect alone (**scheduled = 0**).
