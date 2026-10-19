# Bybit liquidation semantics repair

Scope: offline native-channel semantics, propagation and consumer rejection of
unsupported execution-notional inputs. This is neither live delivery evidence
nor research admission. The active integration goal remains unfinished.

## Native contract and implementation

The reviewed [Bybit all-liquidation channel](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation)
defines `p` as bankruptcy price and `S` as position side (`Buy` means liquidated
long, `Sell` liquidated short). These fields do not establish execution price or
execution aggressor. Its `v` field describes size without a qualified WS unit
in this repair; REST units are not substituted. The adapter now emits bankruptcy
price basis, separate position side and unverified quantity units. Actual
aggressor is unknown. Unknown native side strings remain preserved and explicitly
unknown. Native `T`, `ts`, `s`, `S`, `p`, `v`, type and source channel survive.

Original-byte typed decoding follows bounded lexical/duplicate-key checks.
Positive exact financial strings, checked millisecond timestamps, snapshot kind,
topic/row/catalog/product scope and at most 4,096 rows are required before any
canonical batch is emitted. `T=0` remains zero, with no publication-time fallback.
A native snapshot batch does not imply historical delivery. Canonical source
sequence remains absent. Empty batches retain their topic and must still match
the derivative session catalog; foreign or malformed empty topics fail.

## Consumers and ownership

Optional typed liquidation provenance propagates through model, recording JSON,
protobuf, API view and UI. Bankruptcy prices, positions and unknown units are
labeled explicitly. The UI does not invent execution amounts or quote conversions.

The existing EventPulse V1/V2 execution-notional feature accepts explicitly
qualified execution-price/base-quantity records. An incompatible explicit record
becomes a causal marker in the same bounded five-second window. Any active marker
makes the optional feature null/UNAVAILABLE/OUT_OF_DOMAIN before rescaling,
empty-window zero or summation. Qualified records in the same window cannot
produce a misleading partial total. The exact five-second boundary is inclusive;
after expiry, ordinary freshness and coverage requirements still apply. Marker
records share normal capacity, admission, ownership, cursors and causal replay.
No new strategy, feature family or public failure code is introduced.

Cross-venue features, context models and strategy decisions belong in rsibot.
This feed repair preserves native evidence and prevents an existing generic
consumer from treating that evidence as execution notional. hlscreen and rsibot
consume the authoritative feed independently.

## Compatibility and limits

Adding optional `Liquidation.provenance` requires migrating Rust struct literals
and recompiling downstream source consumers. Bybit's decoded variant now carries
`LiquidationBatchDecoded { symbol, rows }`, retaining scope for empty batches.
Legacy absence stays omitted in model/recording JSON and protobuf; API view JSON
adds `provenance:null`. Existing absent-provenance EventPulse numeric behavior is
preserved for frozen contracts and is not newly qualified as native execution.
Other adapters remain legacy/unverified until individually audited.

The generated-protobuf challenge covers exact i128 extrema/scale 255, explicit
unknown versus absent position, legacy omission and native attributes. The
[retained finite mutation challenge](wire-oracle-challenge.py) rejects ten synchronized binary/JSON corruptions:
execution relabeling, invented base units, reversed position, invented execution
side, time fallback, lost coefficient/scale and changed optional presence. These
are synthetic oracle checks; the original fixture files remain unchanged. The
existing V1 mechanics JSON authoring path cannot encode arbitrary i128 JSON
numbers; its marker arithmetic-bypass test uses coefficient 1/scale 255 instead.
That legacy limit is not repaired or claimed covered by these mechanics tests.

Exact channel ACK readiness, settlement/expiry/contract units, source continuity,
general exact datasets and mixed causal replay remain open. No new live
liquidation capture, strict dataset admission, economic comparison, calendar
canary or multi-day operational qualification is claimed.
