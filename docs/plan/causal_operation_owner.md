# Private causal operation owner

This continues the [integration contract](causal_journal_integration_contract.md).
The opt-in daemon owner consumes the actual bootstrap-created persistent
CaptureSession and its original writer handle. It does not open transports,
project a compatibility feed, forward external sinks, replay a journal or repair
the registered ingress workload by itself.

## Bound configuration and input

Bootstrap V2 explicitly binds JournalOwnerPolicyDto: pending operation count,
owned ingress capacity bytes, single input bound, canonical operation encoding
bound, maximum ingress age and receipt polling interval. V1 with no owner policy
remains canonically inspectable; V1 with a policy and V2 without one are rejected.
There are no silent owner defaults for a V1 bootstrap. Factory construction still
requires the verified bootstrap's actual post-barrier receipt.

The worker retains that original actor handle and checks its feed ID, covering
bootstrap cursor and immutable byte caps before the first callback. The maximum
declared canonical operation must fit the actor's wire/group admission caps,
including base64, routing and framing. Actor grouping and filesystem latency are
not guaranteed to fit an input deadline.

Ingress accepts public native connection/frame/HTTP/timer inputs, Stop and
Resync/Stop control only. Arbitrary Live/Lifecycle/System/Catalog injections,
unbound external sink completions and explicit drain bypasses are rejected.
Every input names its physical source connection. Real connection IDs are
nonzero, monotonic and never reused; the initial runner placeholder is not a
previous physical connection. Closed or superseded sources cannot supply new
frames or HTTP callbacks. Rejected input remains with its caller.

Ingress accounting includes queue plus active input, fixed storage overhead and
owned vector/string capacities. HTTP Bytes slices are copied only after budget
checks so a tiny slice cannot retain an unbounded backing allocation. Capture,
codec/transaction preparation and one committed release are separate bounded
retention scopes. These are not allocator metadata, caller allocation or RSS
measurements.

Count limits also bound queued entries and their notification channels. These
logical retention budgets do not claim an aggregate allocator or process peak.

## Execution, admission and settlement

A dedicated worker owns the machine exclusively. There is one executed,
unacknowledged turn and one backpressured committed-release slot. The callback
and actual null/write/HTTP drains execute in the same captured operation. Full
admission returns the exact prepared transaction to the owner: it retries
admission without executing or encoding again.

One absolute monotonic deadline starts at ingress acceptance and covers queue
wait, execution, preparation, admission retries and observed commit/handoff.
Expiry before admission terminates this owner without retrying execution. After
admission it revokes visibility but retains and polls the ticket until its real
durable/failure settlement. Nonblocking ticket observations distinguish Empty
from disconnection and cache terminal results. A disconnected waiter is Unknown,
never cancellation. Filesystem stalls can still prevent eventual join.

The original writer's closure or failure revokes visibility even while no input
is arriving. The idle worker polls its status at the bound receipt interval;
ingress, status, snapshot and handoff accessors observe it as well. An already
accepted storage ticket is still reaped after this revocation.

Each accepted input has an independent settlement ticket. Dropping it does not
cancel work. Settlements distinguish NotExecuted, ExecutedNotAdmitted,
Committed, CommitFailed and Unknown; a committed settlement is not an output
permit. Capture preflight Err means no callback ran. Complete native turn errors
are journaled and then terminate with outputs withheld. Incomplete capture,
panic, sensitive actions or post-execution encoding failure terminate without
pretending rollback occurred. Queued suffix inputs settle as never executed.

## Safety and release

Observed connect/close/disconnect/transport-loss/stop events immediately fence
older snapshots and release slots under the ingress safety lock, while their
own transition remains eligible for journaling. Execution-discovered failures,
new reconnect/stop requests and readiness/book loss also revoke prior visibility
before waiting for storage. A monotonic input-ID watermark fences older turns;
valid later queued recovery inputs do not inherit an obsolete enqueue epoch.
Persistent Degraded state alone does not repeatedly cancel a valid backlog.

Matching ACKs alone cannot restore a fenced older turn or a terminal owner.
Snapshots expose only committed lifecycle/counts/timers/queues, with a checked
deadline; they are not full book snapshots or universal freshness/readiness
certificates. Degraded startup/recovery can still release its committed protocol
requests. Closed/reconnecting sources expose safety evidence with no eligible
output items. A new actual connection clears the old runner reconnect request
before fresh callback actions can request another one.

Only actual complete QueueDrain entries are candidates for released items;
ProducedRecord and QueueAttempt are not promoted into availability. Handoff and
revocation serialize under one lock. The handoff's local monotonic observation
is sampled at that boundary; it is not exchange time or a value retroactively
inserted into the earlier journal. Already returned data cannot be unsent.
External I/O still needs bound intents/permits and recorded completions.

## Required remaining integration

Owner visibility watermarks, withholding and actual handoff observations are not
yet durable operation DTO fields. Therefore the journal alone cannot reproduce
observed owner availability, and consumers must not project every committed
drain as delivered. Implement the deliberate committed-release projection and
its timing/uncertainty contract before enabling a live reader.

Ingress settlement tickets are in-memory notifications, not a durable admission
ledger. Rejected and never-executed inputs need the actual acquisition/raw path
and its explicit loss accounting when live integration is implemented. A worker
panic can lose its receipt waiter and reports Unknown; actor acceptance still
does not mean cancellation. No exact-delivery or complete-input-history claim
follows from the owner in isolation.

Actual engine physical-order replay/resume, complete snapshots/checkpoints,
external sink intent/completion binding, transport control-reply disposition,
daemon/live/control/supervisor migration and seal-after-settlement remain open.
Only then compare the unchanged registered failed-ingress workload. Native
semantics/continuity, strict datasets, rsibot context/features, calendar and
hardware qualification remain required. No economics or operational promotion
follows from this owner.

The preceding published bootstrap source's ARM watchdog failure remains failed.
The updated test logs the receipt phase before artifact/factory verification and
uses a bounded 120-second test watchdog. This changes a test-harness liveness
bound; production deadlines and registered workload thresholds are unchanged.
Fresh source-specific ARM evidence is required, and a pass under that watchdog
does not establish a ten-second artifact-verification performance guarantee.
