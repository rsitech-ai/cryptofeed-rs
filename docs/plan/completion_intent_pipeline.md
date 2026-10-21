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
owned descriptors, including prepared, admitted and cached continuations.
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

Qualification proceeds through focused safety tests and independent code review,
full stable and MSRV checks, release tests and fresh security checks, then the
same registered 120-second request with both consumers and strict native/data
audits. A clean final cursor or short projection tail alone cannot close
acquisition freshness. Hosted proof and consumer pins must identify the actual
published successor. Original480 qualification and the longer Razer run remain
separate required gates.
