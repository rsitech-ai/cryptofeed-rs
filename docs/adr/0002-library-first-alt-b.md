# ADR 0002: Library-first engine (Alternative B)

**Status:** Accepted  
**Date:** 2026-07-21  
**Spec:** §4.2 / §34 ADR-001

## Decision

Ship a **library-first** workspace (engine, model, adapter API, transport, books, recording, replay, sinks). The daemon is an optional application that composes the same public engine API. Broker sinks remain optional.

Rejected: daemon-first monolith (Alt A), one microservice per venue with mandatory broker (Alt C).

## Why

- Embed and service deployments share one core.
- Adapters and sinks stay independently testable.
- Daemon is replaceable without changing engine semantics.

## Consequences

- Public API design is required from the start.
- More workspace crates and release discipline than a single binary.
- Spec change needs RFC + migration analysis (§34).
