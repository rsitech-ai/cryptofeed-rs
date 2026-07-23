# ADR 0005: MFR1 raw recording format

**Status:** Accepted  
**Date:** 2026-07-21  
**Spec:** §5.8–5.9 / §18 / §34 ADR-008

## Decision

Raw inputs are recorded in **MFR1** segments (`marketfeed-recording`): magic `MFR1`, little-endian layout, per-record CRC32C, session / frame_seq / receive + monotonic timestamps, direction, and opcode. MFR1 v1 records WebSocket Text/Binary/Ping/Pong/Close frames. MFR1 v2 adds bounded HTTP responses and JSON metadata records. MFR1 v3 adds one bounded record for each accepted dynamic-subscription mutation, including both the engine command and exact prepared wire action, so replay restores adapter-local subscription state before applying later inputs and rejects adapter/wire drift. HTTP records contain request ID, status, non-secret headers, and binary body; sensitive response-header values are redacted before persistence. Replay feeds the same `SessionMachine` used live.

Production segments begin with build metadata and one record per registered session. Session metadata contains the exact endpoint, environment, initial concrete subscription plan, catalog version, and process-local `InstrumentId` mapping with fixed-point scales and constraints. Rotation repeats the complete metadata registry. Replay exposes metadata separately and never delivers it as an adapter frame.

Normalized events use **MFNE-JSON1** (`NormalizedEventWriter`); they are not MFR1
(see ADR-0008).

## Why

- Raw-before-normalize enables deterministic crash recovery and corpus replay.
- WebSocket and HTTP adapter inputs must both be replayable with their original receive stamps.

## Consequences

- Crash-recovery tests and `.mfr` corpora depend on MFR1 stability.
- Readers must support current + previous two stable major formats (spec).
- Readers accept MFR1 v1, v2, and v3. New recordings use v3.
- Historical v1 REST corpora may retain their Text/sidecar harnesses; new recordings use the HTTP-response opcode.
- Metadata is bounded, schema-versioned, and built only from non-secret runtime planning data.
- Spec / format major change needs RFC + migration analysis (§34).
