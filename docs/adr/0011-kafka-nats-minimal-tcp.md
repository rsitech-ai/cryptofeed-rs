# ADR 0011: Kafka / NATS minimal TCP producers

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §2.3 non-goals / optional broker sinks  
**Package:** C4c+ (#95)  
**Code:** `crates/sinks/src/kafka.rs`, `crates/sinks/src/nats.rs`

## Decision

Ship optional feature-gated TCP producers — **not** `rdkafka` / `async-nats`:

| Feature | Client | Protocol |
|---|---|---|
| `kafka` | `KafkaSink` | Produce API key 0 / v0 MessageSet, `acks=0` |
| `nats` | `NatsSink` | Text `INFO`/`CONNECT` + `PUB` |

Feature **off** → every push returns `SinkError::Unsupported`. Payload shape
matches `FileSink` Debug UTF-8 lines. Daemon types `kafka` / `nats` need the
matching Cargo features.

## Why

- Spec forbids a mandatory broker dependency; optional sinks still need a
  real wire path for operators who want one.
- Minimal TCP avoids large transitive graphs under `cargo deny` / default builds.

## Consequences

- No compression, idempotent producer, JetStream, TLS, or credentials.
- RecordBatch-only Kafka clusters may reject Produce v0 — upgrade = ApiVersions
  + Produce v3+ / `rdkafka` when scoped (**R18** depth).
- Sync `write_all` on the push path can stall under a full TCP window.
