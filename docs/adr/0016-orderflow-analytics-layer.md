# ADR 0016: Pure order-flow analytics layer

**Status:** Accepted
**Date:** 2026-08-10
**Code:** `crates/analytics`, `crates/facade`

## Decision

Keep exact market-profile and order-flow aggregation in a dedicated
`marketfeed-analytics` library crate. The crate consumes canonical model values,
maintains bounded deterministic state, and emits serializable snapshots without
owning transport, persistence, rendering, or execution.

The initial alpha surface is deliberately limited to grid conversion, session
profiles, candle flow, and three-tier bubble detection. The facade re-exports
that surface under `marketfeed::analytics`.

## Why

- Exact fixed-point inputs remain exact through analytics aggregation.
- Pure builders can be tested with deterministic add/remove and late-data
  cases, independently of daemon and UI lifecycle.
- Bounded maps and explicit overflow/late-data policies prevent hidden,
  unbounded state growth.
- Rendering and trading policy remain downstream concerns rather than implicit
  behavior in a data library.

## Consequences

- This layer does not claim structural-level detection, trading signals, or
  order execution.
- Callers choose and enforce snapshot cadence, persistence, and lifecycle.
- Any daemon or UI integration is a separate change with its own runtime and
  interaction evidence.
