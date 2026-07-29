# ADR 0010: SpillWalSink for SpillToDisk

**Status:** Amended
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

The library exposes this WAL primitive, but the daemon rejects standalone
`[[sinks]] type = "spill-wal"`. The primitive retains an in-memory prefix and
only writes overflow records, so using it as a terminal daemon sink would leave
that prefix without a delivery or recovery consumer. Daemon integration may be
restored only as a wrapper around a real sink with explicit ordered recovery:
consume `pop_recovered()` and checkpoint only after downstream acknowledgement.

## Why

- Lossless-oriented claims need a real SpillToDisk path, not Drop*/Fail alone.
- Local file WAL keeps broker deps out of the default build.

## Consequences

- MFSPILL1 metadata-only files are not replay-compatible. Startup returns
  actionable quarantine/migration guidance rather than interpreting them as
  complete events.
- Recovery is at-least-once: a crash before checkpoint may replay records.
- Standalone daemon configuration fails validation until downstream
  acknowledgement and recovery ownership exist.
- A torn final append is truncated to the last validated record. Corruption in
  a complete record fails closed.
- Disk-full / WAL-cap behavior must keep readiness / fail policy coherent with
  recording (see runbooks under `docs/runbooks/`).
