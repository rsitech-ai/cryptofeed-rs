# Orchestrator Wave-4 — MAY leftovers + PARTIAL-CODE plateau

**Role:** honesty board for post–Wave-3 MAY items and Wave-4 platform PARTIAL→PASS packages.  
**Parent:** [`orchestrator_wave3.md`](./orchestrator_wave3.md) (exchange-data CODE plateau on VenueIds **1–18**).  
**Packages plan:** [`orchestrator_wave4_partials.md`](./orchestrator_wave4_partials.md)  
**Spec:** §21.4 hot reload (MAY), §23.3 OpenTelemetry (optional), plus platform PARTIALs §18/§19/§20/§24/§34.

**Tip:** `origin/main` @ `6ba48da` (≥ `011393a`; post-#156)  
**Updated:** 2026-07-22

---

## Verdict — PARTIAL-CODE track **CODE plateau**

Implementable Wave-4 PARTIAL→PASS CODE is **DONE**. Remaining for §3 / §36 = **OPS-A…E only**.

| ID | Spec | Status | PR |
|---|---|---|---:|
| **W4-P0a** MFNE-JSON1 | §18.5 | **DONE** | #149 |
| **W4-P0b** facade `marketfeed` | §19 / R28 | **DONE** | #144 |
| **W4-P0c/d** ADRs + §24 criterion docs | §34 / §24 | **DONE** | #141 |
| **W4-P1a** catalog `--live` | §20 | **DONE** | #147 |
| **R29** / **W4-P1b** config hot reload | §21.4 MAY | **Partial ship** | #146 |
| **W4-P1c** `parse_fixtures_gate` | §24.2 | **DONE** | #148 |
| **R31** / **W4-P1d** OpenTelemetry | §23.3 | **SKIP** (YAGNI) | #146 |

Neither Wave-4 CODE nor this plateau unlocks beta / stable / 1.0. **Not production-ready without OPS-A…E.**

---

## Remaining (only)

### OPS-A…E (required for §3 / §36)

See [`production_drive.md`](./production_drive.md) USER OPS CHECKLIST.

1. **OPS-A** — GitHub Actions billing  
2. **OPS-B** — scheduled canary ≥7 → **beta**  
3. **OPS-C** — multi-day soak + live chaos → **stable** path  
4. **OPS-D** — tag attestation + SBOM publish  
5. **OPS-E** — human “1.0 allowed”

### Optional YAGNI (not gates)

| Item | Notes |
|---|---|
| proptest / Loom / differential | §27 polish |
| host-opt / published profiles | §13 / §30; needs OPS-D |
| ADR-009…015 | key decisions in `0001`…`0008` |
| OpenTelemetry | R31 **SKIP** — re-open only with named backend + deny-clean deps |
| Full `criterion` + pinned CI timing | Instant harness + local >10% gate already ship |

---

## R29 — Config hot reload (shipped ceiling)

**Shipped (#146):** Unix `SIGHUP` → re-load + validate TOML → classify diff → apply safe knobs → warn `config reload: restart required` for unsafe keys.

**Applied in-process:**
- `telemetry.log_level` (tracing `reload::Layer`)
- `[readiness]` policy (`DaemonState.reloadable`)

**Restart required (validated, not applied):**
- `engine.runtime_profile`, `engine.shutdown_deadline_secs`
- `telemetry.log_format`, `telemetry.bind`
- `recording`, `private`, `venues`, `sinks`

**ponytail ceiling:** daemon venue tasks own private `EngineSupervisor`s; no shared `EngineControl` to map venue symbol diffs → `SubscriptionPatch` / sink rebuild. Upgrade = control-plane wiring, then subscription-safe apply. File-watch not added (SIGHUP is enough for ops).

---

## R31 — OpenTelemetry (**SKIP** / optional YAGNI)

**Decision: skip** adding an `otel` Cargo feature in Wave-4 (and leave as optional YAGNI after PARTIAL-CODE plateau).

**Why deps are not acceptable right now:**
- Spec §23.3 + ADR-011 already treat OTel as optional; baseline is structured logs + Prometheus (§23.1–23.2 **DONE**).
- A minimal `opentelemetry` + SDK + OTLP + `tracing-opentelemetry` stack is a large, fast-moving transitive graph; `deny.toml` runs with `all-features = true`.
- No product consumer asked for traces yet.
- Session-connect span would touch engine/live I/O paths for little operator value vs existing connect/reconnect counters + tracing fields.

**Re-open when:** a concrete exporter/backend is named, deps pin stably under `cargo deny`, and the feature stays off-by-default with zero cost on the default build.

**Until then:** no `otel` feature flag, no stub that claims export.

---

## Explicit non-goals

- Remote control auth / TLS (R30)
- gRPC/UDS streaming (R25)
- Maturity from this doc (OPS-A…E only)
- Claiming beta / stable / 1.0 from Wave-4 CODE
