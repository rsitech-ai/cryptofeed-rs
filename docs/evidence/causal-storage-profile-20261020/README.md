# Negotiated causal storage profile

The recording layer now has an explicitly negotiated `MFCJ1` causal profile,
bounded opaque envelopes and persistent per-session routing. Legacy `MFSF1`
APIs reject causal headers before reading records. The writer validates the
entire group before I/O and installs routing only after durable acknowledgment.
An indeterminate post-replacement failure leaves the writer poisoned at its
previous acknowledged cursor and routes, even if readers see the whole group.
See the [format contract](../../plan/causal_storage_profile.md).

Independent review identified and closed an inherited reader defect: known
disagreement between final byte offset and sequence could expose a record
before the following read failed. Both live and immutable readers now reject
either mismatch direction before exposing the offending record or advancing
state, for both profiles.

All **98 recording tests pass** on the final reviewed implementation, including
14 causal unit cases and two separate public actor regression cases. The
independent run binds five unchanged source hashes and the raw execution log.
The receipt separately identifies the broad workspace/MSRV checks before the
late boundary fix and the final implementation's recording/MSRV/static checks;
it does not relabel the earlier broad run as testing the later fix.

All 22 frozen dataset files retain their original hashes, sizes, modes and
modification/change timestamps. No dependencies or lock records changed.
The preceding engine drain-capture revision's completed 17-job hosted run is
qualified separately in its own [receipt](../engine-drain-capture-20261020/ci-qualification.json).
That hosted result does not qualify this later storage implementation.

This is storage infrastructure. Engine-owned exact payload decoding, verified
lossless bootstrap/factory creation, the private commit-gated owner, sink-intent
binding, committed-release projection, actual-engine replay and live migration
remain open. The registered ingress failure remains failed; it was not rerun
or repaired by these storage checks. Native semantics/continuity, strict data
admission, rsibot consumers/features, calendar and hardware gates also remain.
