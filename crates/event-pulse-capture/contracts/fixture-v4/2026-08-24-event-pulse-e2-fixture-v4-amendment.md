# EventPulse E2 Fixture V4 structural amendment

**Status:** contract only; fixture provenance blocked

**Output ceiling:** `STRUCTURAL_V4_CANDIDATE`

Fixture V4 is the append-only package format for the published
MechanicsInputV2 lane. It does not reinterpret Fixture V1 through V3 and it does
not prove that any captured byte is authentic. A structurally valid package
therefore cannot satisfy `--require-complete`, qualify a source, or authorize
capture, evidence authorship, runtime, risk, orders, paper, canary, or live use.

## Published inputs

The canonical contract independently binds the default-reachable root topology
and wire/admission contracts and the root consumer, transformer, and Snapshot V2
implementation receipts. A package repeats those bindings exactly. Coordinated
changes to a package manifest, its admission descriptor, and their hashes do not
change the independently frozen values.

The admission descriptor is exactly
`event-pulse-e2-prospective-admission/2.0`. It binds the BNB/USDT perpetual
topology with Binance USD-M PUBLIC and MARKET contributors and a Hyperliquid
confirmation contributor, three Clock sources, six Coverage sources, and the
truthful-empty processor System source. Source qualification remains
`UNVERIFIED` and every authority literal is false.

## Package

The manifest schema is `event-pulse-e2-prospective-fixture/4.0`. It has exactly
nine artifacts in this order: TRADE, QUOTE, BOOK, OPEN_INTEREST, LIQUIDATION,
CONFIRMATION, CLOCK, COVERAGE, SYSTEM. The first eight are nonempty. SYSTEM is
the unique canonical empty artifact: zero bytes and records, SHA-256 of empty
bytes, null bounds, and an empty identity array.

Every nonempty artifact is a regular package-relative `.jsonl` file. The package
contains only the canonical manifest, admission descriptor, and those nine
artifact files; symlinks, traversal, absolute paths, duplicate paths, and extra
files reject. Every line is strict unique-key sorted compact UTF-8 JSON followed
by one LF. Manifest byte/count/SHA/bounds must equal the bytes. The total is at
most 65,536 records and 16,777,216 artifact bytes.

MARKET lines are exact `event-pulse-mechanics-input/2.0` inputs. Their family,
contributor, BNBUSDT or Hyperliquid BNB/USDT catalog, cursor kind, typed retained
source provenance, source timestamp, envelope coordinate, and payload hash must
match the published wire contract. Binance PUBLIC records carry the exact PR43
PUBLIC catalog (venue 3, instrument 7, epoch 11/21, empty OI), while Binance
MARKET records carry the exact PR43 MARKET catalog (venue 3, instrument 7, epoch
12/22, OI `{7: CONTRACTS}`). They are not normalized into a package-global
catalog. The separately authored Hyperliquid confirmation record carries its
explicit accepted consumer catalog. TRADE and BOOK are native; QUOTE,
OPEN_INTEREST, LIQUIDATION, and CONFIRMATION_PRICE are derived. Native sequence
values are signed-i64 bounded. Derived frame values are unsigned-u64 bounded;
action and item are bounded by 65,534 and 65,535 and exactly equal the envelope
coordinates. Every payload hash is SHA-256 of canonical JSON with only the
top-level `payload_hash` removed.

CLOCK and COVERAGE lines remain exact non-market V1 wire inputs carried by V2.
They bind their own admitted SourceKey, source epoch/generation, native cursor,
top-level `available_at`, and exact payload hash. The package contains each of
the three admitted Clock sources and six admitted Coverage source/family pairs.
MARKET contains every one of the six admitted contributor/family pairs. Repeated
records are permitted, but records within an artifact must be strictly ordered
without conflating time and cursor state: availability is nondecreasing, and the
authenticated `(raw frame, action, item)` coordinate for every MARKET mode is
independently strictly increasing. A package rejects both identical and mutated
repeated coordinates even though runtime state ingest may ignore an identical
duplicate. Native cursor values separately drive family state. TRADE is the only
native non-BOOK family: each aggregate-trade cursor is
a singleton and every later record must be the checked signed-i64 successor of
the prior aggregate trade ID. Duplicates, regressions, gaps, and any record after
`i64::MAX` reject. BOOK retains its distinct state rule: a snapshot establishes
`lastUpdateId`, the first delta overlaps it inclusively, and later deltas require
exact `pu` continuity with strict final-ID progress.

Within CLOCK and COVERAGE artifacts, each admitted source has its own same-epoch
native cursor chain. A later record for that source must retain the source epoch
and generation and start at checked `prior end + 1`; regression, overlap, gap,
same-generation epoch drift, identical duplicate, and mutated duplicate reject.
Other source rows do not advance or reset that chain. Repeating a BOOK snapshot
identity after a snapshot is likewise a mutated duplicate and rejects; a
delta-to-snapshot reset retains the accepted BOOK recovery rule.

All record availability is within the immutable capture interval and not later
than decision time. MARKET availability is Euclidean-floor conversion of
`envelope.receive_ts` nanoseconds to canonical RFC3339 microseconds. CLOCK and
COVERAGE use their own top-level `available_at`. The manifest maximum equals the
maximum nonempty artifact availability.

The floor conversion is only the manifest boundary representation. MARKET state
ordering retains the original signed nanosecond `receive_ts`: it is
nondecreasing within each family artifact. Across the split role artifacts, the
validator reconstructs each contributor replay stream by exact
`(source_id, connection_id, session_id)` and requires authenticated raw
`(frame_seq, action_index, event_index)` coordinates to be unique and strictly
increasing with nondecreasing exact receive nanoseconds. Coordinates belonging
to distinct contributors remain independent. Exchange nanoseconds remain exact
causal evidence and must not exceed receive time; they are not a replay-order
cursor and may legitimately regress between venue messages.

Every emitted MARKET record sharing one contributor/session `frame_seq` must
carry the identical exact receive nanosecond from that raw MFR frame. For the
exact bound Binance routed-v4 PUBLIC and MARKET producers, visible
`action_index` values start at zero and are contiguous, every action authors
exactly one MechanicsInput with `event_index == 0`, and every frame is
homogeneous in family. QUOTE, TRADE, OPEN_INTEREST, and LIQUIDATION frames have
exactly one action `0`. BOOK is either one action `0` containing a snapshot or a
live delta, or multiple actions beginning with `BookSnapshot` at action `0` and
followed only by buffered `BookDelta` actions `1..N-1`. Multiple ordinary
same-family actions, a multi-delta frame without its snapshot, QUOTE+BOOK, or
TRADE+OPEN_INTEREST in one raw frame reject. In the accepted routed producer,
`MarkLive` is trailing no-output state and cannot justify a visible gap.
Authored frame coordinates may still skip raw frames that produced no MARKET
input. These routed shape constraints do not apply to the separately authored,
source-unqualified Hyperliquid confirmation producer; until that producer is
frozen, confirmation retains only strict raw coordinate and frame-time rules.

The routed TRADE payload `trade_id` is non-null and equals the canonical decimal
string of provenance `aggregate_trade_id`, which already equals both native
cursor and envelope source-sequence endpoints. The remaining routed audit binds
every duplicated field the accepted output actually carries: Quote, BOOK,
TRADE, OI, and LIQ exchange nanoseconds equal their selected provenance
transaction/source/order-trade milliseconds; BOOK update IDs equal the native
cursor and source sequence. Quote update ID exists only in provenance, BOOK
payload levels/checksum contain no update IDs, OI quantity has no duplicate
provenance value, and LIQ price/quantity/side plus outer event time have no
duplicated order identifier in MechanicsInputV2. The validator therefore does
not fabricate correlations for fields that the published wire does not carry.

The routed payload domain is equally closed. Quote bid/ask prices and quantities
are all present and positive fixed decimals. Routed aggregate Trade price and
quantity are positive, and its aggressor is exactly `Buy` or `Sell`, matching
the decoder path that discards nonpositive tape records and derives aggressor
from Binance maker-side `m`. A BOOK snapshot has `depth=1000`, null checksum,
and positive price/quantity levels. A BOOK delta has null checksum and positive
prices; a zero source quantity is normalized by the accepted adapter to
`Delete` with null quantity, while a nonzero positive source quantity is
`Upsert` with that quantity. The accepted adapter does not retain source order
IDs in Liquidation, and it does not impose an additional sign filter on OI or
Liquidation after exact fixed-decimal decoding, so this contract does not invent
those unavailable or unenforced correlations. Liquidation remains limited to
the published `Buy`/`Sell` mapping already required by the closed payload.

## Oracle separation

The byte-exact seven-record PR43 transformer oracle is retained as
`PR43_TRANSFORMER_REACHABILITY_ORACLE`: 7,736 LF-only bytes with SHA-256
`a65c1f39f7dc0150748d0f0facb0ea6cc09ca0dcedeaaff07284513c90040237`.
It proves only that the published transformer records reach this wire/catalog
validator unchanged. Its timestamps predate the final published bindings, so it
is explicitly not a prospective admission or fixture proof.

The structural candidate oracle is a separate Rust V2 composition. It reauthors
the same seven Binance semantics after the immutable capture floor, preserving
the PR43 source-specific catalogs while recomputing causal timestamps and payload
hashes, then adds one separately authored confirmation, three Clock, and six
Coverage records. It therefore has 17 records. The universal completeness
minimum remains 15 records: one per six MARKET families, three Clock sources,
and six Coverage sources. The extra two records are retained BOOK deltas, not a
new minimum and not evidence provenance.

## Completion boundary

Passing this validator establishes only structural candidacy. E2 remains
`IN_PROGRESS / blocked:fixture-provenance` until a real post-admission package is
authored by accepted producers, source-qualified, immutable and default-reachable,
strictly read back by the published Rust implementation, replayed into Snapshot
V2, independently reviewed, and source-locked. E3 remains `BLOCKED`.
