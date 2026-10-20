# Bounded multi-owner actual-engine replay

Continue the active integration goal with one physical MFCJ3 reader and a
SessionId-keyed map of privately persistent native owners. Session IDs remain
globally unique across venues. The existing single-owner reader retains its
explicit additional-owner rejection. No eviction, second bootstrap, fresh
machine cursor shortcut or live authorization is introduced.

Authority: repository implementation/tests/audit only. No economics, corpus
replacement, trading activation, deployment, merge or release. This slice does
not repair the registered failed ingress workload or provide committed consumer
projection, external I/O, live migration or native checkpoints.

Validate reader limits before opening; validate each physical bootstrap,
configuration, artifact and aggregate reservations before factory construction.
Keep one actual CaptureSession per admitted bootstrap. After structural ledger
admission, check aggregate retained resources before any native callback and
compare the complete native operation. Expose only matched observed handoffs
with full owner identity. A malformed/unsupported/unknown owner, resource cap,
codec/storage error or native mismatch poisons the entire instance. A known
owner's execution uncertainty or terminal status remains scoped while unrelated
owners can progress.

Separately bound owner count, admitted bootstrap payload bytes, canonical pending
input count/bytes, active operation plus retained previous-state canonical bytes,
source-declared pending count/bytes, and the sum of declared per-trace capture
reservations. Also reserve catalog/selected-instrument cardinalities and native
maintained-book/cached-delta level ceilings from the supported factory policies.
These are logical quantities, never measured allocator bytes or RSS. One bounded
record/native comparison has additional temporary copies; consumer-retained
returned handoffs are outside reader retention. Owners/reservations are never
silently evicted after terminal state.

Open at the physical header without prescanning bootstraps. Resume verifies one
exact initial committed global boundary, reconstructs every encountered owner in
physical order, and discards historical handoffs. A verified header cursor may
represent zero owners. Reject foreign/future/crossed boundaries before the
offending record can create a factory or execute a callback. This is prefix
reconstruction, not restart authority or a native checkpoint.

Required tests: actual interleaved Bybit and Hyperliquid spot/perpetual histories
with overlapping instrument IDs, independent reconnect state and delayed release;
every physical prefix/global resume and next append; idle and uncertain owners
while another progresses; native financial/drain mismatch; aggregate exact-cap
and cap+1 checks including canonical active/previous-state retention; corrupt,
unsupported and conflicting owners; all-instance poison; seal uncertainty;
disabled-feature entry before filesystem access. Run focused public regressions,
appropriate Rust 1.85/Clippy/workspace gates and independent source/evidence review.
Preserve original captures, frozen data and all earlier qualified artifacts.
Rollback removes this opt-in API and its tests while retaining existing readers.

Status: all16 frozen-source local checks pass, including unfiltered native29,
workspace1518/0failed/56existingignored across158targets, Rust1.85core554 and
public/bootstrap/owner gates, denied-warningClippy/docs and exact security tools.
The [qualification](../evidence/causal-multi-owner-replay-20261020/qualification.json)
binds532declared inputs, actual terminal0/markers and the retained test artifact.
Independent final evidence review passed without material findings. Draft
publication and new-source hosted qualification remain required. Published producer95/HLS735 hosted audits retain their original
source scopes and exclude this later candidate. Projection/live/operational
integration remains unfinished; the registered ingress failure was not rerun.
