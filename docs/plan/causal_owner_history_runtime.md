# Private durable owner runtime

The MFCJ3 profile implements actual ordered owner observations for the opt-in
Bybit disk/null library owner. It follows the [MFCJ2 structural validator](causal_owner_history.md)
and its revocation-during-callback review. Qualification is source-specific;
the goal remains active until the integration and failure gates below pass.
The normal daemon CLI and live Hyperliquid collection have not migrated to this
runtime. MFCJ2 remains inspectable and is rejected by the daemon factory.

## Binding and entry boundary

MFCJ3 has its own eight-byte header and version-3 causal envelopes. Older readers
reject that header before accessing a watermark. A canonical typed
HistoryRuntimePolicyDto is hash-bound in the schema-2 bootstrap's reserved
`owner-history-runtime-v1` reproduction input. Missing, duplicate, corrupt or
noncanonical policy bytes are rejected before factory construction. MFCJ1/2
cannot silently ignore the reserved role. Actor, writer, reader and cursor
resume retain the selected profile and reject mixed envelopes.

ExecutionPermitted names an accepted input, its next native callback ordinal
and generation. The original writer must acknowledge this record before the
owner may enter CaptureSession. This is authorization at an entry boundary,
not proof that the first native mutation occurred. A crash with only a permit
still has ExecutionUnknown disposition.

ExecutionCompleted names the exact prior permit and retains the full typed
native operation, including original input and actual drain effects. Native
and journal ordinals are independent. A terminal Revoked observation prevents
new execution and releases but permits completion of the one already permitted
callback. Its complete late evidence is committed and withheld; terminal state
never clears. Preflight noninvocation can instead settle the permit explicitly.
Incomplete/panicked capture or completion encoding overflow remains indeterminate;
queued inputs can be durably settled without inventing another native callback.
MFCJ2 Executed/Fence and MFCJ3 permit events cannot be mixed.

## Actual runtime ordering

One sequencer orders ingress, permits, completions, revocations, release intents
and observed internal handoffs. Encoding, callbacks and storage waits occur
outside the sequencer lock. It assigns accepted-input IDs only after preparation,
source and clock checks. A later Connected or safety input fences older output;
source fencing is nonterminal so valid newer connections can recover. Physical
execution source and latest ingress source are separate. A reconnect can make a
queued old frame nonexecuted without advancing the native ordinal.

No new operation state or drained output is exposed before completion and
ReleaseIntent acknowledgements. An actual internal handoff then queues
HandoffObserved. Its acknowledgement happens later, so a crash in between is
HandoffUnconfirmed. These observations do not acknowledge an external socket,
broker or application. Previous committed snapshots retain their own expiry.

An accepted original CommitTicket remains owned until its real result arrives,
including after revocation or a disappearing input waiter. Full admission
retries the same prepared transaction; native execution is never retried.
Writer/encoding failure preserves the honest committed prefix. Rejected ingress
returns its owned input; it does not claim durable retention of rejected bytes.
The coordinator must seal after the owner stops and accepted work settles.

## Bounds and limits

The bootstrap separately binds native pending count/bytes, capture/runner caps,
maximum canonical record size, observation count/preparation bytes, absolute
input age and finalization age. Admission reserves preparation capacity before
copying/encoding and rechecks source/deadline before assigning an input ID.
Reserved completion/control capacity prevents ingress from consuming those
slots. These are logical canonical/preparation bounds, not allocator/RSS limits;
the actor's encoded wire buffers and one native operation have additional caps.
Readers independently bound retained canonical input/record bytes.

Status and ingress calls refresh deadline/revocation visibility while a callback
runs. There is no autonomous mid-callback watchdog. Finalization age is an observable expiry threshold, not an abandonment
permission or finite-join guarantee. A healthy Full writer retains and retries
the identical prepared transaction, including predecessors of an available
completion. Expiry cannot discard complete available evidence, cancel an
accepted ticket or fabricate nonexecution. A stalled writer, filesystem or
native callback can therefore still delay shutdown indefinitely. The outer-worker panic catch
and outstanding-ticket reap path are statically reviewed; native-panic tests
exercise CaptureSession's inner catch, not an injected outer-worker panic.

## Remaining integration gates

This owner is a private disk/null foundation. Complete Hyperliquid bootstrap
factory support, complete failure/admission retention, deliberate committed
compatibility projection, bound external I/O intents/completions and actual-engine
physical-order replay/checkpoints remain required. Migrate daemon/live/control/
supervisor callers and shared consumers only with exact schema/identity/timing
proof. Then repeat the unchanged registered ingress workload with its original
caps, schedule and abort rules. Its existing failure remains failed.

Native semantic/continuity checks, strict dataset admission, hardware/calendar
qualification and the rsibot context reader remain open. cryptofeed-rs owns data
acquisition and replay; rsibot owns time-qualified features, model/ETF research
and admitted strategy decisions through its existing owners. hlscreen remains
a consumer. No economics, trading activation, deployment, merge or release is
authorized by this runtime milestone.
