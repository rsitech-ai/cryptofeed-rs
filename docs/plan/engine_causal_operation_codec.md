# Engine operation codec v1

The engine-owned codec implements the exact operation payload prerequisite in
the [causal journal contract](causal_journal_integration_contract.md). Recording
retains the negotiated bounded opaque envelope and routing; it does not depend
on engine types. The codec creates no factory, verifies no bootstrap, performs
no replay or external delivery, and has no commit/release or live caller.

## Identity and completeness

`CapturedOperation` now includes the owning venue/session from its private
`CaptureSession` configuration. `OperationDto::from_capture` checks those fields
against the supplied `OperationIdentity` before conversion. Adapter-supplied
batch identities are preserved independently, including malformed values in
buffered actions before runner stamping. They cannot replace the runner identity.
The bootstrap identifier is declared here; the future owner must obtain it from
a separately verified lossless bootstrap and bind it for its entire lifetime.

Only complete captures with a full native returned outcome and both complete
queue snapshots can encode. Incomplete/panicked/omitted-detail captures and
explicit sensitive actions fail. Unfinished queue/sink attempts, incomplete
drains and started-but-unfinished actions also fail, even if a caller labels a
capture complete. A completed failed action may leave a legitimate buffered
suffix; that suffix remains evidence and is not inferred to have executed.

The payload repeats its envelope route. Decode checks the expected outer
venue/session/bootstrap identifier before decoding payload bytes, then checks
payload schema and exact inner/outer route agreement, including ordinal and
generation. Queue lanes/counts, ordered timer identities, action/callback origin
references and trace-entry counts are validated. Historical continuity and
whether a generation change corresponds to an actual connection operation are
still responsibilities of the owner and actual-engine replayer.

## Exact owned data

The DTO retains every captured input, drain policy, runner/callback/action
origin, buffered action/disposition, produced record, queue decision/eviction,
actual drain, sink attempt, subscription mutation, native outcome and pre/post
state. Binary payloads use canonical standard base64; ordered HTTP headers,
control symbol lists and protocol lists keep their order and duplicates.

Batch/system/produced records are JSON string leaves validated through the
existing exact typed `FeedRecord` decoder, with canonical stream comparison.
Money never passes through a generic JSON value or buffered financial enum.
The full catalog uses explicit canonical i128 coefficient strings plus u8
scales and typed instrument kinds. Both key and row settlement/expiry, venue,
IDs, status, inverse flag, every increment/bound and per-row/view catalog versions
are retained. No sanitized endpoint or Debug-formatted factory configuration is
substituted for the forthcoming bootstrap.

Native error variants and fields become owned typed error DTOs. Static-string
native fields retain their text without leaking/interning untrusted strings.
Decode returns owned DTOs; this is not an API for reconstructing native static
error values or reexecuting a machine. `matches_capture` compares the full DTO
against a fresh conversion of an actual capture. It is useful for the future
replayer but does not itself prove that the expected operation was executed.

The complete payload is canonical JSON v1. Unknown/duplicate fields, unknown
variants, wrong leaf kinds, noncanonical coefficients/base64/record JSON and
noncanonical operation bytes are rejected. Canonical comparison streams against
the source bytes without allocating a second complete encoded payload.

## Bounds and failure handling

The complete encoded operation is capped at the recording envelope's decoded
payload limit (`MAX_CAUSAL_ENCODED_PAYLOAD / 4 * 3`), or a smaller explicit caller
limit. A capped writer rejects overflow without returning a truncated operation;
the envelope and actor preparation still apply their complete framed bounds.
Top-level trace deserialization stops at 65,536 entries. Capture metadata,
native queue/timer bounds and structural checks remain separate limits.

This is an encoded-output bound. Bounded native capture storage, conversion
copies, base64/financial strings, Vec capacity and deserialization working memory
are additional; it is not an allocation or process-RSS guarantee. Encoding can
fail after a native operation executed. The future private owner must treat that
as terminal, retain uncertainty and never execute the same input again as a retry.

## Remaining required path

Implement and commit the lossless bootstrap before first execution, bind the
actual persistent machine and immutable identity in a private owner, retain the
same prepared operation across full actor admission, and wait for its true
post-barrier receipt before releasing snapshots/readiness/dispatch/wire/HTTP.
Bind external sink completions to committed delivery intents. Implement one
deliberate committed-release consumer projection, actual-engine replay in
physical order and actual live/daemon/control/supervisor migration. Then compare
the unchanged registered failed-ingress workload. The failure remains failed.
Native semantics/continuity, strict datasets, rsibot context/features, calendar
and hardware gates remain part of the full goal.
