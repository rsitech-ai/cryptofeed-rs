# Bounded legacy adapter replay

`marketfeed_replay::LegacyStreamingReplay` reconstructs adapter actions from MFR
segments without loading all input or retaining an output history. It is D5
milestone A; [complete engine causal replay](../plan/trade_data_integrity_implementation_plan.md#d5-delivery-milestones-and-remaining-causal-replay)
remains required. Existing `ReplayRunner` behavior is unchanged.

The caller supplies `LegacyReplayLimits`, a factory accepting
`SessionRecordingMetadata`, an explicitly ordered sequence of readers, and a
synchronous sink accepting `LegacyReplayStep`. The factory returns a
`LegacyReplaySession` containing a machine and an **assumed** startup time. It
must reconstruct from locally supplied venue artifacts or reject missing native
context. Neither the runner nor metadata alone establishes discovery completeness.

## Ordering and delivery

Metadata takes effect only when physically read. A frame before its session
metadata fails; future metadata is never scanned ahead. Equal repeated metadata
across rotations keeps the machine and its state. Conflicting metadata, schema
versions other than 1, session/header mismatches and reserved session zero fail.
The factory is called once per unique accepted session binding.

Each complete record is processed and validated before one borrowed sink call.
The next record is not read until the sink returns success. The original raw
record is preserved even when an adapter mutates its parsing buffer. Interleaved
sessions have separate machines, even when native instrument/frame IDs collide.
Wall-clock timestamps do not reorder the physical input sequence.

Text, binary, HTTP and Pong records invoke the corresponding adapter input.
Recorded subscription mutations require exact prepared-wire equality before
commit. Outbound, Ping and Close records are observations only. Startup invokes
the adapter's existing `on_replay_start` hook. No network action, timer firing or
disconnect callback is synthesized. Output actions retain adapter identities;
the runner does not fabricate engine connection IDs or rewrite ordinals.

## Budgets and failure boundary

Limits bound session count, unique encoded metadata bytes, actions per callback,
combined market rows and system events, output payload bytes, processed records
and cumulative input bytes across segments. Batches/system events are counted
through JSON serialization without allocating a serialized copy; HTTP includes
URL, headers with separators and body. Sensitive wire actions are rejected.
Raw records and HTTP headers/body remain visible to the sink: this API is not a
redaction boundary.

Input bytes include segment headers. At exact quota exhaustion, at most one
additional byte probes EOF. The record quota permits one additional complete
CRC-checked record, bounded by the reader's existing 16 MiB record cap, but never
delivers it to an adapter or sink. Input-byte limits apply independently. Partial
record lengths/payloads and CRC errors fail; `RawSegmentReader::read_all` retains
its separately documented incomplete-tail recovery behavior.

Factory/adapter/sink internals remain responsible for their own allocations,
blocking and external effects. Output budgets are checked after the callback;
they cannot prevent temporary callback allocations. Ordinary errors release
buffered actions. Any error or caught unwind leaves the runner terminal.

`LegacyReplayFailure` retains the failing physical position and progress with
separate last-read, callback-invoked, callback-completed and sink-acknowledged
positions. A failed callback may already have mutated state. A failed sink may
have partially written externally. No rollback, transparent retry or exactly-once
delivery is promised. Restart requires reconstructing from the original prefix
and a caller-owned sink recovery/deduplication policy.

## Qualification limits

MFR does not establish physical generations, connection times, timer-poll
boundaries, callback outcomes or queue-drain decisions. Explicitly ordering files
does not prove common collector identity, gap-free continuity or observed
availability. Legacy MFR without preceding session metadata is rejected by this
API; the existing supplied-machine replay API retains its compatibility scope.
This API grants no strict dataset admission or research/economic qualification.

Focused fixtures challenge more than 2,048 inputs with a two-action buffer,
interleaving, rotation, late metadata, mutable parsers, exact values, byte/event/
action limits, partial failures, torn tails, CRC corruption and caught unwinds.
Those checks establish the legacy contract above, not the missing causal facts.
