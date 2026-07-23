# ADR 0013: Private credentials env-only

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §21 / §26 security  
**Code:** `crates/private/src/credentials.rs`, `crates/daemon/src/private.rs`  
**Config:** `crates/daemon/config.example.toml` `[private.*]`

## Decision

Private user-data library sessions (OKX / Bybit private WS)
load credentials **only from process environment** via
`*_session_from_env` / `*ApiCredentials::from_env`. TOML may set `enabled`
flags only — **never** API keys/secrets.

Debug/`Display` redacts key material. Library callers must provide an explicit
`AccountEventSink`; operational `SystemEvent` values use the same sink contract.
**No order placement.**

Daemon private enablement fails closed. Binance requires migration from its
retired listen-key flow to authenticated WebSocket API subscriptions. OKX and
Bybit require a bounded durable account sink, private readiness/liveness
tracking, and reconnect supervision before the daemon may load credentials.

## Why

- Trust boundary: secrets must not land in committed config or logs.
- Spec: resolve secrets from env (or a secret provider), not files in git.

## Consequences

- Live private tests are `#[ignore]` and require env (`BINANCE_API_KEY`,
  `OKX_*`, `BYBIT_*`).
- Changing `[private]` on SIGHUP is restart-required (ADR-0015).
- A private remote close is an error requiring caller-owned reconnect
  supervision; it is not reported as a clean completed session.
- Remote TLS/auth for control plane remains out of scope (**R30**).
