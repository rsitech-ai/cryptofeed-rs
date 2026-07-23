# ADR 0009: Daemon health / readiness model

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §19–§20 / §23.1–23.2  
**Code:** `crates/daemon/src/http.rs`, `crates/daemon/src/state.rs`

## Decision

The daemon exposes a minimal HTTP/1.1 surface on `telemetry.bind`:

| Path | Meaning |
|---|---|
| `/live` | Process supervisor is `Running` (`is_live`) |
| `/ready` | `[readiness]` policy satisfied (`evaluate_readiness`) |
| `/metrics` | Prometheus text (counters + fixed-bucket histograms) |

Readiness knobs: `require_running`, `require_required_venues`, `min_live_sessions`,
`require_recording_healthy`. Impossible combinations (e.g. require recording
healthy with raw recording off) fail config validation.

## Why

- Spec requires separate liveness vs readiness for orchestration.
- Plain HTTP avoids a framework; probe bodies stay trivial for k8s/systemd.

## Consequences

- `/live` ≠ “all venues healthy”; operators must configure `[readiness]`.
- Hot-reload may change readiness policy in-process (ADR-0015); bind restart-required.
- OTel remains optional / skipped (see Wave-4 R31); Prometheus is the baseline.
