# ADR 0012: Optional simd-json parse path

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §15 / §24 / Phase 4 C10  
**Ops:** [`docs/ops/latency_runtime.md`](../ops/latency_runtime.md)

## Decision

Default JSON decode is **`serde_json`**. Public adapters expose an optional
Cargo feature `simd-json` (Binance Spot/USD-M/Coin-M, OKX, Bybit V5 shared
decode, Kraken Spot, Deribit) with **parity tests**
(`decode_text_serde` vs `decode_text_simd`; fixture corpora, no live network).

Evidence tools: CI matrix YAML for `--features simd-json`, Instant
`parse_fixtures` harness / local `parse_fixtures_gate`. These are **not**
enablement or SLO claims.

## Why

- Spec marks simd-json optional: mutates input, contains unsafe impl code.
- Portable defaults stay serde; latency binaries opt in after profiling shows
  parse as the bottleneck.

## Consequences

- Do not enable in public portable defaults.
- No invented latency numbers from harness timings alone.
- Feature-off builds remain the correctness baseline.
