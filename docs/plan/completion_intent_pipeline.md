# Bounded Completion and Intent admission

Objective: reduce serial durable publication barriers in the existing causal
owner, preserving exact replay and release authority. Joint13 local counters
show substantial owner-gate waiting before transport reads. This motivates a
bounded optimization; it does not prove which individual filesystem operation
caused acquisition backlog or promise a particular performance improvement.

Scope: cryptofeed-rs owner implementation and meaningful owner/replay tests.
Authority is repository-process-only public-data infrastructure. No orders,
economic runs, activation, deployment, credential changes or rsibot root
gitlinks. Existing journal and shared-feed schemas, record/byte/group/age caps,
deadlines, watchdogs and native execution permission remain unchanged. Rollback
is the separate reviewed implementation diff, retaining all evidence.

The owner may admit a completed operation and its eligible release Intent
before waiting for their separate actual receipts. There remain at most three
distinct owned journal records, counting the union of ordinals across prepared,
admitted and cached continuations. A pending ticket and a Completion settlement
cell referring to the same ordinal represent one record, not two. The same
applies to an Intent marker and its pending ticket. This does not bound the
number of bookkeeping structs to three: one Completion cell and one Intent
marker add a fixed amount of metadata. Their encoded transaction is moved into
admission, not copied; actor byte/count bounds remain separate and unchanged.
Eligibility must match the existing serial handler, use the operation's actual
generation and remain ordered against concurrent safety observations. Prepare
and encode before reserving an ordinal; fallback paths cannot leave a hole.

An explicit shared settlement cell retains the completed input's sender,
owned bytes, journal ordinal and validated own receipt. FIFO receipt processing
accounts native credits and sends Committed exactly once after this record and
its predecessors are acknowledged. A failed later Intent cannot rewrite that
known Completion to Unknown. Full retry, suffix failure and panic drains must
use the same settlement rule. Invalid predecessors still prohibit Committed.

A single preissued Intent marker follows Reserved, Admitted and Settled states.
It holds the exact immutable transaction and its own result. The ordinary
ordered observation consumes this marker without submitting or encoding twice.
Ready and snapshots require the Intent's own validated receipt plus fresh
failure/fence/deadline checks. Handoff and application wire barriers remain
unchanged. A revocation after reservation cannot reorder or omit that Intent;
it must prevent release. Closing and panic cleanup must settle accepted work
without losing a known Completion or reporting an artificial clean Stop.

Required regressions cover actual Connected generation changes and next Text,
every replayable source prefix, Full with an owned Completion predecessor and
immutable retry, separate-group suffix failure versus shared-group failure,
revocation and deadline changes around both receipts, panic after admission and
known Completion, invalid predecessors, ordinal/encoding failures and serial
fallback. Preserve the original paused32 and closing regressions unchanged.

The existing delayed-operation/Intent test assumed a physical interval with
Completion published and Intent absent. Co-publication intentionally removes
that interval. Its retained failing run has40 passing cases and one failure;
the original test remains available in Git66ed95e and retained baseline files.
The adapted test uses deterministic owner acknowledgement pauses: before the
Completion's own ACK is observed, durable count remains one; after that ACK and
before Intent's own ACK, it becomes two. Both stages require the old snapshot
and no ready output, even if both physical records already exist. Dropping the
input waiter still cannot cancel accepted work. Final write, callback and replay
assertions, capacities, deadlines and watchdogs remain. Independent static review
confirms only this original test function changes; actual successor execution
is required separately.

Qualification proceeds through focused safety tests and independent code review,
full stable and MSRV checks, release tests and fresh security checks, then the
same registered 120-second request with both consumers and strict native/data
audits. A clean final cursor or short projection tail alone cannot close
acquisition freshness. Hosted proof and consumer pins must identify the actual
published successor. Original480 qualification and the longer Razer run remain
separate required gates.

Current local result: exact source `e5cd4371` (570 inputs) passes all 17 checks,
including 1,699 stable workspace tests, MSRV 111/314/143/80, release 314/143/80
and the 41-case history suite. Independent final evidence review verifies
the source, raw logs, actual terminals and retained release images. See
[the qualification](../evidence/completion-intent-pipeline-20261021/README.md).

Joint 14 ran the unchanged 120-second request with both consumers. All four
children exited zero; the parent audit reconciles 9,163 records, 1,800 items
and 2,007 released native observations. Final file catch-up was 22.9 ms. The
unsynchronized perps BBO timestamp diagnostic ended at 0.765 seconds and peaked
at 2.624 seconds. Independent capture review, successor hosted checks and final
consumer pins remain separate gates; this is not a freshness SLA or a controlled
performance comparison.
