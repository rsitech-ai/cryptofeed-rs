# ADR 0015: SIGHUP partial config reload

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §21.4 MAY hot reload  
**Package:** W4-P1b / R29 (#146)  
**Code:** `crates/daemon/src/reload.rs`, `crates/daemon/src/main.rs`  

## Decision

On Unix, **SIGHUP** re-loads + validates TOML, classifies a `ReloadPlan`, then:

**Applied in-process:** `telemetry.log_level`, `[readiness]`.

**Validated but restart-required:** `engine.runtime_profile`,
`engine.shutdown_deadline_secs`, `telemetry.log_format`, `telemetry.bind`,
`recording`, `private`, `venues`, `sinks`.

Windows has no SIGHUP — restart for config changes.

## Why

- Spec MAY allows a safe subset without full control-plane wiring.
- Operators can flip log level / readiness without bouncing the process.

## Consequences

- **ponytail:** venue tasks own private `EngineSupervisor`s; no shared
  `EngineControl` to map symbol diffs → `SubscriptionPatch` / sink rebuild.
  Upgrade = control-plane wiring, then subscription-safe apply.
- File-watch not added; SIGHUP is enough for ops.
- Full hot reload of venues/sinks remains future work, not a Wave-4 gate.
