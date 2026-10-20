# Durable owner observations and physical-order validation

This continues the [operation owner](causal_operation_owner.md) and
[integration contract](causal_journal_integration_contract.md). The MFCJ2 codec,
writer/reader negotiation and bounded physical-order validator are implemented.
The daemon factory explicitly rejects MFCJ2 before construction. A separate
[MFCJ3 private runtime](causal_owner_history_runtime.md) now adds the permit and
late-completion semantics required by the migration review below. No live availability or registered-ingress
repair follows from this format change.

## Negotiated records

MFCJ2 has a distinct eight-byte header and version-2 causal envelopes. Legacy
MFSF1 and MFCJ1 readers reject it before watermark/record access. Preparation,
actor admission, writing, live reading, immutable reading and cursor resume
retain the selected profile; mixed envelope versions are rejected.

The first envelope is the canonical, hash-bound schema-2 BootstrapDto with its
explicit JournalOwnerPolicyDto. Subsequent OwnerHistoryDto records contain
schema version, complete route, owner-local monotonic observed_ns and one
externally tagged event. Route.ordinal counts every journal record. An Executed
record separately retains the full typed OperationDto and its native callback
ordinal. Owner observations never advance that native counter.

| Event | Meaning |
|---|---|
| Started | Establish the owner clock origin at observed_ns zero and retain its sampled Unix time |
| Ingress | Record input ID, physical connection, exact typed input, deadline and source-reported owned capacity |
| Executed | Preserve the complete native operation associated with that exact accepted input |
| ReleaseIntent | Bind a possible internal handoff to a prior operation journal ordinal and output eligibility |
| HandoffObserved | Record the actual internal handoff observation, naming its exact prior intent |
| Withheld | Settle an executed operation without exposing its drained values |
| NotExecuted | Settle an accepted input as stale-source, deadline, preflight or terminal nonexecution |
| Fence | Record terminal revocation of visibility; previously returned values cannot be unsent |

All financial values remain in the existing exact typed leaves. Codec checks
bind inner/outer identity, phases and canonical bytes and enforce an encoding
cap. Recording retains opaque payloads and does not depend on engine types.

## Physical-order checks and budgets

The validator starts at the verified canonical bootstrap and processes records
in physical order, without metadata prescan. It checks journal and native
ordinal continuity, generation changes, clock order, exact accepted input,
prior/native state linkage, bound fresh runner state, capture byte/entry caps,
queue/timer caps and declared null-dispatch drain policy. These structural
checks do not execute the adapter or establish the truth of a producer's report.

Latest observed ingress source and actually executed physical source are
separate. A queued old frame can become ineligible at execution after a native
reconnect request. Its NotExecuted record removes the input without inventing a
callback. Native errors, stop requests, deadline expiry and explicit terminal
fences cannot admit later output. A late complete operation remains evidence
and requires withholding rather than being erased or retried.

Source-reported owned_bytes_bound is checked against the bound native input and
pending budgets and a minimum payload-size check. It is not a reconstruction or
independent measurement of original vector capacities. Native count/byte
reservations retire when Executed is committed, before a release is consumed.
The reader may retain that input and operation until handoff or withholding.
Explicit required OwnerHistoryLimits bound the reader's canonical input and
record retention independently. Neither budget claims allocator overhead or RSS;
one retained operation and its prior state are additional bounded scopes.

An invalid record leaves the validator prefix unchanged. Memory checkpoints can
be paired with an exact reader cursor to continue validation. This is not a
persisted engine checkpoint or actual-engine replay/resume.

## Availability and crash uncertainty

Ingress alone reports ExecutionUnknown: a crash could occur after native
mutation but before its operation is recorded. A committed native operation
without a release intent remains CommittedWithoutRelease. An intent without its
handoff observation remains HandoffUnconfirmed. None exposes values as delivered.
Only a valid HandoffObserved produces an internal handoff containing complete
actual QueueDrain items. Produced and queue-attempt records are not promoted.
A false output-eligibility flag yields no released items.

Handoff observations describe the owner's internal API boundary, not an external
socket, broker or application acknowledgement. A crash between handoff and its
observation record leaves uncertainty. Encoding failure/incomplete capture can
leave a prefix with unknown execution; this validator does not relabel it as
never executed or authorize resuming that engine.

Runtime owner recording, complete failure/admission dispositions, committed
compatibility projection, bound external I/O intents/completions, actual-engine
physical-order comparison/checkpoints, clock continuity and live/control/
supervisor migration remain required. Then rerun the unchanged registered failed
ingress workload. Native semantics/continuity, strict datasets, rsibot context,
calendar and hardware gates remain part of the full goal.

## Runtime migration gate: revocation during a callback

Separate source review identified a prospective format limitation. The existing
private owner can observe revocation or writer failure while its native
callback runs outside the owner lock. An honestly ordered history can therefore
observe a terminal fence before the already running callback completes. The
current validator rejects every Executed record after a terminal fence; it
cannot preserve that complete late operation in this order. This profile is
still unavailable to the daemon, so this is a migration gate rather than a
claim of a newly observed live failure.

Before enabling runtime recording, bind an explicit execution permit/start
observation to its accepted input before native mutation. This is the linearized
authorization/entry boundary, not proof that the first adapter mutation occurred;
a permit-only crash still reports ExecutionUnknown. Terminal revocation
must forbid new execution and releases while allowing complete evidence for
the one operation already in flight. Completion must never clear terminal
state, and a revoked operation must settle as withheld. Preserve the actual
observation order; moving the fence after completion changes its meaning.
ExecutionUnknown remains appropriate for crash/incomplete capture or an
unwritable journal, not deliberate omission of complete available evidence.

Required integration regressions include a callback blocked across explicit
revocation or status-observed writer failure, and expiry after callback return
before writer admission. A mid-callback deadline observation needs
an explicit watcher seam; the MFCJ3 private runtime refreshes it when status or
ingress APIs are called, without claiming an autonomous callback watchdog. Check exactly one execution, original
fence/completion order, no snapshot or output revival, terminal state retained
after completion, queued inputs settled without native execution, and crash
prefixes around start, fence, completion and withholding. Test later-source
ingress separately: its visibility/source fence is nonterminal and must preserve
valid newer recovery inputs.
Adding only an exception to the current Executed guard is insufficient: its
terminal assignment could otherwise clear an earlier revocation.
