# Verified physical source view

`MultiHistoryReplay::read_next_verified` exposes the source side of the
[projection contract](causal_native_observation_projection.md). It supports the
explicit MFCJ3/MFCJ4 actual replay profiles, including the
[native history layer](causal_native_observation_history.md). It does not write
a projection, deliver consumer data or migrate the real daemon's live loop.

## Verification and immutable contents

`VerifiedSourceStep` has no public constructor, mutable access or Deserialize
implementation. The privately persistent reader creates it only after the
existing profile, committed-prefix, bootstrap/artifact, structural, aggregate
resource and actual native comparison checks. A failed check returns no step
for that record and poisons subsequent reads/progress. Previously returned
verified prefixes remain distinct from a rejected later record.

Each step retains the exact physical before/after cursors, selected source
profile, original owner identity, and one original typed record:

| Record | Exposed source information |
| --- | --- |
| Bootstrap | Complete original BootstrapDto: artifact/build identity, catalog, subscriptions, topics, policies, runtime/capture bounds and public reproduction inputs |
| Owner | Complete original OwnerHistoryDto: route/session/generation, observation stamp, input connection/receipt clocks, controls, execution and release lineage; exact owner progress/uncertainty after the record |
| Seal | Original schema, recorded time and reason; all encountered owners' prefix progress and uncertainties in session order |

Bootstrap metadata appears only when its physical record is encountered. No
future catalog, projector artifact identity, local clock or later owner's data
is supplied at an earlier cursor. Native/canonical financial leaves retain their
typed exact representation; the source view does not parse raw market frames.
Completion DTOs and retained queues are evidence, not publication authority.

The optional handoff exists only at a committed HandoffObserved whose completion
already matched actual replay. `handoff.released_items()` restricts it to
eligible completed drains. Inputs, permits, completions, release intents, failed
actions, evictions and withheld outputs cannot independently authorize market
publication. Writes/HTTP remain evidence rather than inferred external delivery.

## Archive clocks

`ArchiveClockPolicy::OwnerStartedObservationV1` is an explicit policy identifier
for this view. The future target contract must bind its selected policy; this
does not rewrite historical bootstrap policies or create a target profile.

Before Started, the bootstrap clock is Unstarted. After Started, each owner step
retains the original signed Unix origin and unsigned monotonic observation.
Their sum is calculated in i128 and converted to i64 with a checked conversion.
The result is labeled approximate; out-of-range produces None and must remain
unavailable. Input receive, input monotonic and exchange clocks remain separate
original fields. No projector wall clock participates in reconstruction.
Neither the observation stamp nor its approximation proves the wall time of
durable acknowledgement or consumer availability. Strict dataset availability
stays unavailable unless independently bound by its own admitted evidence.

This approximation is per owner. Two interleaved owners may legitimately regress
in global wall time; the physical feed order remains authoritative. A future
projector must separately bind how its global feature clock handles that case.
Sorting records by approximate, input or exchange timestamps is not permitted.

Seal exposes StorageSeal with only its recorded storage time. It cannot advance
an owner's feature clock or settle unknown execution. Unresolved execution,
unreleased completions and unconfirmed handoffs remain explicit prefix states.

## Resume, compatibility and bounds

Resume reconstructs the physical prefix with the same actual factories, discards
prior handoffs, and preserves original origins before reading the next verified
step. It is neither a native checkpoint nor writable append takeover. Existing
`read_next` maps the same verified pass to its prior public handoff/cursor shape;
default profile selection and old-profile refusals remain unchanged.

The reader retains one additional optional origin per admitted owner. Existing
owner-count, bootstrap/catalog/capture reservations and pending/active limits
remain in force. Owner DTOs have already passed the negotiated and reader record
caps before the source-view decode. Seal stays under recording's global record
bound; its owner summaries are bounded by the admitted owner/input caps.
Returned typed DTOs, uncertainty summaries and handoffs are caller-owned rather
than reader retention. Temporary decoding/copying and caller retention remain
separate from logical counters; this does not establish RSS, throughput or
latency. The target projector still needs its own bounded prepared group/ticket.

## Remaining integration

Implement the separately negotiated target projection, atomic values/source
after-cursor groups and bounded retry of the same prepared group under healthy
Full. Unknown target settlement must stop publication. Migrate hlscreen and the
direct rsibot reader through their existing feature/strategy owners, coordinating
shared dependency pins and capabilities. Keep real daemon/control/external-I/O
repair, all-mids admission, strict datasets, continuity and native restart open.

The registered ingress workload remains failed and has not been rerun. Its
original schedule, caps, thresholds and abort rules must remain intact until the
actual integration repair. No economics, trading activation, deployment, merge,
release or operational promotion is granted. The full goal remains active.
