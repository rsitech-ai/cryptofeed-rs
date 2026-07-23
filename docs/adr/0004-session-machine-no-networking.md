# ADR 0004: SessionMachine has no networking

**Status:** Accepted  
**Date:** 2026-07-21  
**Spec:** §5.1–5.2 / §34 ADR-002, ADR-003

## Decision

Adapters implement deterministic `SessionMachine`: `SessionInput → on_input → SessionAction`. They MUST NOT create sockets, spawn tasks, sleep, log, or select runtimes. The **engine** owns all network I/O and task lifecycle.

## Why

- Same machine drives live, replay, and offline fixtures.
- Protocol bugs are unit-testable without I/O.
- Networking policy (TLS, reconnect, timers) stays in one place.

## Consequences

- REST candle polls are engine timers + `SessionAction` HTTP requests, not adapter sockets.
- Fixtures inject frames/HTTP bodies; no live network in adapter unit tests.
- Spec change needs RFC + migration analysis (§34).
