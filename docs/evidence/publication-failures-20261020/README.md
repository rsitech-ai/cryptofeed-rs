# Publication failure and recovery repair

Feed watermark replacement can succeed before the subsequent directory barrier
fails. The old blanket claim that every failed transaction left an invisible
tail was false. The shared-feed writer now returns PublicationIndeterminate for
that boundary, retains its last acknowledged durable cursor and permanently
refuses further appends. Existing and new readers may observe the whole synced
transaction; they cannot observe half of a multi-record transaction. Pre-replace
failures retain the existing invisible-tail behavior and permanent poison.

Dataset mutation errors poison the writer instance. Reopen under the stable OS
lease before recovery. Reusing a matching immutable object now verifies/syncs its
opened file and parent; equality alone cannot bless an earlier failed namespace
barrier. Publication and recovered-intent reference validation independently do
this too, so caller-supplied ObjectRefs cannot bypass put(). Recovered intents and
reused immutable manifests receive corresponding barriers. Public/read-only
integrity paths retain no-write/no-sync/no-writer-lock behavior.

The OS file lease excludes other Store instances but did not exclude threads
sharing one Store. Complete public put/publish/recover operations now share an
in-process mutex; parent CAS stays inside that guard. Poison is checked after
lock acquisition. Validation-only stale CAS refusals leave the writer healthy.
Private helpers do not reacquire the operation mutex. Backup uses a destination
Store that has not escaped the synchronous backup operation.

Initialization now syncs each resolved directory ancestor from child to root,
including paths left by a failed earlier initialization. Syncing only the
immediate parent missed new grandparents. The helper is reused by feed, raw
pipeline and dataset initialization; failures remain errors. No platform skips,
ignored barriers, format changes or reinterpretation of original recordings.

Focused tests exercise post-watermark replacement failure with both reader
lifetimes, immutable-install/reuse failure and poison, direct-reference admission
in publish/recover, post-HEAD failure and recovered intent, eight concurrent
same-object writes/competing CAS and nested ancestor barriers/failure propagation.
The separate reviewer identified the direct-reference, concurrency and ancestor
gaps in the initial draft; the revised bounded portable slice has no remaining
finding in that review. Reviewer did not execute tests. Actual commands, source
hashes and result scopes are retained in validation.json.

Windows production directory sync still fails with OS5; it is not repaired or
qualified by this portable slice. The adjacent Windows probe demonstrates exact
write-access directory flush API compatibility on hosted local NTFS with an
administrative token. It does not prove unprivileged operation, power-loss
persistence, storage hardware guarantees or a working production backend.
Next integrate handle-verified filesystem support across every directory barrier
and run the Windows writer/reader/rotation/dataset-recovery paths. Strict dataset,
source-continuity, mixed causal-replay and sustained operational gates stay open.
