# Binance USD-M routed v4 prerequisite ExecPlan

## Objective and boundary

Add the smallest append-only adapter prerequisite for the proposed E2 BNBUSDT topology: explicit public-read-only PUBLIC and MARKET USD-M sessions, exact route isolation, official source timestamps, and existing book continuity. This is repository-ready adapter behavior only. It does not create an admission, capture, fixture, preflight, package, source-qualification, runtime, or trading claim.

The owner repository is `cryptofeed-rs`; capital impact is research-only. Network, filesystem writer, credentials, private endpoints, orders, replay/capture execution, and evidence authorship are forbidden.

## Contract and options

The accepted historical factory and `BinanceUsdmSession::new` remain unchanged.

Two options were considered:

1. Change the default USD-M factory to split every session. Rejected because it would alter existing API and recording behavior before the prospective topology contract is complete.
2. Add an explicit routed constructor and role enum while sharing the existing decoder/book implementation. Chosen because it is additive, reviewable, and leaves the legacy path byte/behavior compatible.

The routed constructor accepts exactly one `BNBUSDT` instrument, explicit nonzero connection/session IDs, no caller subscription set, no candles, and the exact role endpoint. PUBLIC admits `bookTicker` + `depth@100ms` and REST depth; MARKET admits `aggTrade` + `forceOrder` and REST open interest. Routed replay start emits no System action.

Official USD-M source-time mapping:

- aggTrade: `T`
- bookTicker: `T`, while retaining decoded `E`, `T`, and `u`
- depth update: `T`, while retaining decoded `E` and `T`
- REST depth snapshot: `T`, while retaining decoded `E` and `T`
- open interest: `time`
- force order: inner `o.T`, while retaining decoded outer `E` and inner `o.T`

The REST depth schema currently and officially includes both `E` and `T`; routed snapshots require both. Legacy decode keeps them optional so historical fixtures remain accepted.

Routed construction is pair-only: one checked constructor returns PUBLIC and MARKET together and rejects aliased connection/session IDs or different instrument mappings. Successful routed snapshot activation emits no System action. MARKET OI polling permits at most one outstanding request per symbol, preventing timer-driven pending-map growth. These restrictions do not change the legacy constructor or factory.

The pair also requires a real `BNBUSDT` row in the supplied Binance USD-M `CatalogView`, with the exact configured instrument id, venue, and catalog version. Routed `E`/`T` provenance lives in an additive routed decode sidecar; the established public `UsdmDecoded` variant fields and legacy decode behavior remain unchanged. Native `a`, `u`, `U`, `pu`, and `lastUpdateId` values are admitted only through `i64::MAX`, matching the downstream canonical integer domain before any routed output or book-state mutation.

## Explicit hold

Preflight v4 remains `HOLD`: current EventPulse cursor derivation interprets `EventEnvelope.source_sequence` as NATIVE. The root topology requires non-contiguous bookTicker `u` to remain venue provenance while the QUOTE cursor is DERIVED. The routed decoder retains `u`, but the emitted EventEnvelope deliberately leaves `source_sequence` empty; no existing canonical EPIN field can retain `u` without incorrectly changing cursor mode. A separately accepted append-only provenance/cursor representation and frozen admission descriptor are prerequisites to preflight or fixture work.

## TDD and implementation steps

- [x] Add routed PUBLIC/MARKET tests first; capture unresolved enum/constructor RED.
- [x] Add the explicit constructor/role without changing the default factory.
- [x] Extend the decoder compatibly for optional bookTicker and snapshot `E`/`T`.
- [x] Enforce routed symbol, endpoint, session role, timestamp, and message-family boundaries.
- [x] Reuse and test inclusive snapshot bridge plus `pu` live continuity.
- [x] Preserve legacy adapter tests.
- [x] Run focused/full adapter, workspace/current, Rust 1.85, fmt, clippy, deny, and diff gates.
- [x] Prepare one clean adapter-only commit and record exact evidence.

## Tests and rollback

Focused tests cover exact subscribe/REST routes and ACK id, no replay-start or successful-snapshot System action, missing/out-of-range timestamps, differing `E`/`T`, retained aggregate-trade outer `E`, wrong/ignored family, unknown or retired HTTP correlation, source-compatible legacy decoded variants, exact catalog binding, native-id max/one-over boundaries, non-contiguous quote `u` with DERIVED envelope cursor, trade and book native cursors, paired identity uniqueness, bounded OI polling, official snapshot `E`/`T`, inclusive snapshot bridge, and next-`pu` continuity. Full adapter and workspace gates protect legacy behavior.

Rollback is one local commit. No migration or external state exists.
