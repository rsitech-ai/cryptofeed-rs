# ADR 0010: SpillWalSink for SpillToDisk

**Status:** Accepted  
**Date:** 2026-07-22  
**Spec:** §17.5–17.6  
**Package:** R11 (#100)  
**Code:** `crates/sinks/src/spill.rs` (`SpillWalSink`, MFSPILL2)

## Decision

`OverflowPolicy::SpillToDisk` is implemented by **`SpillWalSink`**: memory queues
first, then append to a length-prefixed **MFSPILL2** WAL until
`wal_limit_bytes`. At the limit the sink **fails closed**
(`SinkError::FailEngine`) and surfaces `EventsDropped` / `DiskPressure` via
`take_system_events`.

The current complete-event format is **MFSPILL2**:
`["MFSPILL2\n"][u8 tag][u32 little-endian body length][JSON SpillItem]`.
Opening validates the file as a bounded stream, preserves a valid prefix when
only the final append is torn, and rejects malformed complete records.
Checkpointing writes and syncs a same-directory replacement before an atomic
rename and directory sync.

Daemon wires `[[sinks]] type = "spill-wal"` and fails startup if the file
contains unacknowledged recovery records. Recovery is therefore explicit:
consume `pop_recovered()` in append order and call `checkpoint_recovery()`;
the daemon never silently discards or skips a recovery prefix.

## Why

- Lossless-oriented claims need a real SpillToDisk path, not Drop*/Fail alone.
- Local file WAL keeps broker deps out of the default build.

## Consequences

- MFSPILL1 metadata-only files are not replay-compatible. Startup returns
  actionable quarantine/migration guidance rather than interpreting them as
  complete events.
- Recovery is at-least-once: a crash before checkpoint may replay records.
- A torn final append is truncated to the last validated record. Corruption in
  a complete record fails closed.
- Disk-full / WAL-cap behavior must keep readiness / fail policy coherent with
  recording (see runbooks under `docs/runbooks/`).
