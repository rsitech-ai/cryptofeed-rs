# ADR 0003: Fixed-point exact arithmetic

**Status:** Accepted  
**Date:** 2026-07-21  
**Spec:** §5.7 / §8.4 / §34 ADR-004

## Decision

Canonical prices, quantities, rates, and notionals use `Fixed { coefficient: i128, scale: u8 }` (and newtypes `Price` / `Quantity` / `Rate`). `f64` is never the source of truth; convenience conversions only.

Rules: parse from decimal bytes where possible; reject overflow; no silent rounding; rescaling uses an explicit rounding mode.

## Why

- Exchange decimals must round-trip without binary float drift.
- Book sync, funding rates, and candle OHLCV need exact equality in fixtures and replay.

## Consequences

- Adapters MUST emit `Fixed` (no `f64` in normalized events).
- REST/WS candle paths use exact `Fixed` OHLCV.
- Spec change needs RFC + migration analysis (§34).
