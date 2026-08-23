# EventPulse E2 Snapshot V2 contract amendment

Status: `SNAPSHOT_V2_CONTRACT_ONLY`

Blocker: `blocked:fixture-provenance`

Authority: research-only; capture, evidence, risk, execution, orders, and trading
authority are all false.

The canonical machine contract is
[`event-pulse-e2-snapshot-v2-contract.json`](event-pulse-e2-snapshot-v2-contract.json).
It binds the root wire V2 contract at `44f3e091`, the published consumer lock at
`258ed262`, the published transformer lock at `31868dda`, and the corresponding
cryptofeed defaults `f8e1ecc1` and `d069e59d`. It neither changes those files nor
implements Rust snapshot authorship.

Each dependency binding fixes the exact commit type, tree, parent list, and Git
committer timestamp. Root file dependencies additionally fix their path, regular
blob mode, byte length, and SHA-256. A later root commit containing identical
receipt bytes and the PR head sharing the transformer tree are not substitutes.

## E1-preserving family projection

The complete V4 prefix has at least fifteen non-System records: at least one
MARKET record for each of QUOTE, BOOK, TRADE, OPEN_INTEREST, LIQUIDATION, and
CONFIRMATION_PRICE; at least three CLOCK records; and at least six COVERAGE
records. Repeated family records, including a BOOK snapshot followed by multiple
deltas, are expected and the complete prefix remains bounded at 65,536 records.
The projected snapshot has exactly the latest six MARKET family cursors, three
CLOCK cursors, and six COVERAGE cursors after processing that complete prefix.
The current System policy is exact truthful-empty. Every market cursor is
projected from its preallocated `(contributor, family)` slot to a unique E1-valid
source ID. Contributor-level
aggregation is forbidden because it would collapse independent cursor domains.
Clock and Coverage retain their exact source IDs.

Native MARKET ranges remain exact. Derived MARKET ranges have identical start and
end, calculated with checked arithmetic as:

```text
raw_frame_seq * 2^32 + action_index * 2^16 + item_index
```

`raw_frame_seq` is restricted to `1..=2^31-1`, action to `0..=65534`, and item to
`0..=65535`, keeping the largest ordinary-action display value within E1's signed
64-bit maximum (the reserved action index is not an ordinary MARKET action).
A valid MechanicsInputV2 with a larger raw frame remains valid input but cannot be
represented by E1. Snapshot authorship returns the typed
`SNAPSHOT_V2_CURSOR_NOT_E1_REPRESENTABLE` result. It must not truncate, saturate,
lower to V1, author cache bytes, consume a revision, alter a predecessor, or seal
the prefix. Same-time repair remains possible.

Every projected E1 `source_payload_hash` is the exact authenticated
MechanicsInputV2 payload hash. Family provenance is never recomputed from an
envelope or V1 preimage. E1 ordering remains exactly
`(available_at, source_id, connection_epoch, sequence_start, sequence_end,
source_payload_hash)` and duplicate final ordering keys reject authorship.

Clock and Coverage projections use their own SourceKey ID and connection epoch,
their own native cursor start/end, their top-level `available_at`, and their exact
input payload hash. A sidecar never borrows its subject contributor or family-slot
epoch.

All nanosecond timestamps use Euclidean floor to microseconds before canonical
RFC3339 UTC serialization. MARKET `exchange_ts` supplies `source_event_time`;
MARKET `receive_ts` supplies `received_at`, `normalized_at`, and `available_at`.
Thus `1001ns` maps to `1970-01-01T00:00:00.000001Z`, while `-1ns` maps to
`1969-12-31T23:59:59.999999Z`; truncation toward zero is forbidden.

## Causal and transactional invariants

Contributors are the exact records used by emitted features and current mechanics
state at decision time. Market component maxima, the exact market causal anchor,
all-source availability maximum, fresh bound Clock evidence, and complete Coverage
follow constitution section 10 without future borrowing or partial aggregation.

`advance_to(T)` effects may persist. The authored cache, revision, predecessor hash,
and sealed-prefix watermark commit together only after E1/Q1 validation and hashing.
Any failure commits none of those four, consumes no revision, and seals no prefix.
A successful same-time retry must match a fresh processor over the same complete
prefix byte for byte and hash for hash.

## Ceiling

This amendment proves only a reviewed contract shape. Snapshot V2 Rust code and
Fixture V4 are not implemented or authored. It proves no fixture provenance,
source qualification, replay parity, capture, runtime, paper, canary, live, risk,
allocation, order, execution, or trading state. E2 remains `IN_PROGRESS /
blocked:fixture-provenance`; E3 remains `BLOCKED`.
