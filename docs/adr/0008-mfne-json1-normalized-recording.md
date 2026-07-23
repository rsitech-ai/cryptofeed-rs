# ADR 0008: MFNE-JSON1 normalized recording

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §18.5 Normalized recording  
**Package:** Wave-4 **W4-P0a**

## Decision

Normalized market events are persisted as **MFNE-JSON1**: newline-delimited JSON
objects whose field names match `proto/marketfeed/v1/market_event.proto`
(`EventEnvelope` + `MarketEvent` oneof). The body schema is shared with
**MFPE-JSON1** (length-prefixed framing in `ProtobufFileSink`) via
`marketfeed_recording::event_envelope_json`.

Raw frames remain **MFR1** and are unchanged.

Default writer format is `NormalizedFormat::Jsonl`. Legacy `DebugJsonl` is
retained for grepping only.

## Why

- Spec §18.5 requires a separately versioned schema; Debug text is not it.
- Reusing the proto field map keeps sinks and recording aligned without prost.
- JSONL is enough for fixtures / offline inspect; protobuf length-prefix stays
  available as MFPE-PB1 / future MFNE-PB1.

## Consequences

- Readers use `read_normalized_jsonl` (cheap Value replay; full Rust decode YAGNI).
- MFR1 crash-recovery / corpora paths must not depend on MFNE.
- Breaking JSON field renames require a new MFNE major (or dual-read).
