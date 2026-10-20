# Engine causal storage profile v1

This implements the recording negotiation/routing prerequisite in the
[integration contract](causal_journal_integration_contract.md). It does not yet
implement the engine payload codec, lossless bootstrap, commit-gated owner or
actual-engine replay. Strategy features and decisions remain rsibot-owned.

## Negotiation

The first eight bytes select the format before any record is read:

| Profile | Header bytes | Accepted records |
| --- | --- | --- |
| `Legacy` | `MFSF1\0\0\0` | Existing records and final seal; rejects causal records |
| `EngineCausalV1` | `MFCJ1\0\0\0` | Causal envelopes and final seal; rejects legacy records |

`FeedWriter::create`, `FeedReader::open` and `SealedFeedReader::new` retain
`Legacy`. The corresponding `create_profile`, `open_profile` and `new_profile`
APIs require an explicit expected profile and reject a mismatched header.
The writer and batch handle expose their immutable profile so a future engine
owner can reject a mismatched backend before factory creation or execution.
No automatic fallback or midstream format change is supported.

The existing random 40-character feed identity, 48-byte header, length/CRC
framing, sequence numbers, synced watermark, seal rules and indeterminate
publication semantics are reused. This is a different wire profile; adding the
public Rust enum variant also requires deliberate downstream source migration.

## Opaque envelope

Each causal `FeedRecord` contains `schema_version: 1`, `phase` (`bootstrap` or
`operation`), `route`, and a standard canonical `payload_base64` string. Unknown
envelope/route fields and unknown phase values fail deserialization. No engine
or financial payload is parsed by recording. Validation uses a fixed 3 KiB
scratch buffer; explicit payload decoding returns the original bounded bytes.

Route fields are `venue: u16`, nonzero `session: u64`, `ordinal: u64`,
`generation: u64`, and a 64-character lowercase hexadecimal `bootstrap_id`.
Recording checks the identifier's syntax. Engine must verify its digest over
the exact decoded bootstrap bytes, decode a supported typed payload and
cross-check payload identity/ordinal/generation against this envelope before
creating or executing a machine.

A bootstrap has ordinal and generation zero. It binds a logical session to
one venue and bootstrap ID for the entire feed. Duplicate bootstraps, including
rebinding the same session under another venue, fail. Operations require a
previous bootstrap, consecutive ordinals starting at one, and generation equal
to the prior value or its checked successor. Engine must additionally verify
which actual input permits a generation advance. Arithmetic never wraps.

The feed retains at most 4,096 historical routes without eviction. Encoded
payloads are nonempty and at most `32 MiB - 4096` bytes before decoding; complete
wire records remain limited to 32 MiB, and transactions to 64 MiB/65,536 records.
The constructor checks the decoded input bound before base64 allocation.
These are storage bounds, separate from engine capture/ingress budgets.

## Commit and resume behavior

All routing changes for an append group are staged before any write, alongside
the existing full encoding/framing validation. Only touched routes are copied.
A malformed later member changes neither file bytes, acknowledged cursor nor
route state. Route changes install only after the actual publication barriers
succeed. A directory-sync failure after watermark replacement issues no ACK
and poisons the writer; readers may still observe the complete published group.
The writer retains its previous acknowledged cursor and routes in that case.

Readers install route/cursor/seal changes only after the entire record validates.
Retrying a malformed, CRC-correct record cannot partially advance them.
Both legacy and causal readers also reject a known mismatch between final byte
offset and final sequence before returning the offending record: these two
coordinates must identify the same final record. This closes an inherited case
where the last record could escape before the following read reported the bad
watermark. Immutable checkpoint readers enforce the same condition.
`open_profile` rechecks the prefix to a supplied cursor and reconstructs routing.
That proves a storage cursor boundary. It does not reconstruct adapter, book,
queue or timer state. Engine replay must reexecute from the beginning while
suppressing delivery up to the requested cursor, or use a separately verified
complete engine checkpoint; no such checkpoint is implemented here.

Prepared actor transactions validate intrinsic envelope errors and reject mixed
profiles before admission. Admission rejects a transaction for another writer
profile without consuming an ordinal. Cross-record routing is validated by the
unique writer against its actual history. A ticket remains admission only; its
post-barrier receipt is the durability acknowledgment.

## Remaining integration

Implement the exact engine DTO/codec and lossless bootstrap, private persistent
owner retaining an executed operation across full admission, committed snapshots
and gated releases, bound external sink intents/completions, deliberate consumer
projection, actual-engine replay and live caller migration. Then rerun the
unchanged registered ingress workload. The previously measured ingress failure
remains failed and unqualified; storage tests do not repair or qualify it.
