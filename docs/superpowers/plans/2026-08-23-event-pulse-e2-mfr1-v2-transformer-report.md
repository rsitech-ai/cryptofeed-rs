# EventPulse E2 MFR1 MechanicsInputV2 Transformer Report

## Outcome

Implemented a pure offline, route-specific Binance USD-M MFR1-to-`MechanicsInputV2` transformer in the leaf `marketfeed-event-pulse-mfr1` crate. The existing V1 API and implementation remain unchanged.

The transformer consumes an owned routed Binance session and complete MFR1 segment bytes. Before invoking the session machine it validates the MFR1 v3 header and reserved word, exact framed length, 256 MiB/65,536-record bounds, capture and decision times, selected session, exact build/session metadata, route-specific admission/catalog/epoch identity, and every selected payload's role, symbol, native range, E/T provenance, and action-producing frame order.

PUBLIC and MARKET use distinct checked `ReplayCatalogV1` values because venue id 3 must resolve to exactly one admitted source per transform. MARKET output is authored directly through `MechanicsInputV2::market`; no V1 MARKET cursor or serialization path is used. V1 remains the exact path only for reserved ActionBuffer and MarketDispatch drop inputs.

## Deterministic BOOK correlation

Buffered BOOK correlation is deterministic without changing the adapter. Selected raw depth payloads are decoded before replay into a bounded ledger keyed by exact session plus native `U/u`; the retained provenance includes `pu`, E, and T. A snapshot response is keyed by its authoritative raw frame. When the adapter releases the snapshot and buffered bridge delta, the transformer normalizes their envelope coordinates to the response MFR group while consuming the snapshot and original delta provenance exactly once. Later live deltas consume their own native ledger entries. Duplicate, missing, mismatched, ambiguous, or unconsumed entries reject the complete transform without returning partial inputs or bytes.

## TDD evidence

- RED: `cargo test -p marketfeed-event-pulse-mfr1 --test transformer_v2` failed with `E0432` unresolved imports for the absent V2 API.
- GREEN: the focused suite passes 11/11. It covers PUBLIC quote full-`u64` provenance with derived raw cursor; snapshot/buffered/live BOOK; MARKET trade/OI/liquidation; fresh `SourceStateMachineV2` ingestion; actual dispatcher overflow and reserved drop authorship; wrong route/symbol/timestamps/native bounds; frame zero/regression; subscription ACK id binding; duplicate ledger identity; unknown HTTP request id; and 1/2/3-byte truncation.
- Existing V1 transformer suite remains 29/29 green.

## Verification

- `cargo test -p marketfeed-event-pulse-mfr1 --quiet` — GREEN (3 unit, 29 V1 integration, 11 V2 integration).
- `cargo test -p marketfeed-event-pulse -p marketfeed-adapter-binance -p marketfeed-recording -p marketfeed-replay -p marketfeed-dispatch --quiet` — GREEN; ignored network tests remained ignored.
- `cargo test --workspace --all-targets --all-features --quiet` — GREEN; ignored live-network tests remained ignored.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — GREEN.
- `cargo +1.85.0 test -p marketfeed-event-pulse-mfr1 -p marketfeed-event-pulse -p marketfeed-adapter-binance --quiet` — GREEN.
- `cargo +1.85.0 clippy -p marketfeed-event-pulse-mfr1 -p marketfeed-event-pulse -p marketfeed-adapter-binance --all-targets --all-features -- -D warnings` — GREEN.
- `cargo fmt --all -- --check` — GREEN.
- `cargo deny --offline --locked check` — GREEN: advisories, bans, licenses, and sources.
- `git diff --check` — GREEN.

## Dependency and authority audit

The only new dependency edge is `marketfeed-event-pulse-mfr1 -> marketfeed-adapter-binance`. Binance's normal dependency graph is pure adapter state, model, book, bytes, and Serde code; transport, engine, recording, and replay remain dev-only. `Cargo.lock` changes only by adding that existing workspace package name to the leaf crate's dependency list.

No adapter, recording, replay, engine, daemon, transport, filesystem, network, environment, credential, snapshot, capture, package, manifest, or trading implementation changed. Returned output explicitly reports `evidence_authoring_allowed=false` and `blocked:fixture-provenance`.

## Residual hold

This is a repo-ready offline transformation boundary only. It does not author a fixture, prove source qualification, produce Clock/Coverage/confirmation inputs, support SnapshotProcessor V2, or grant capture, evidence, runtime, paper, canary, live, order, risk, or execution authority. E2 remains `blocked:fixture-provenance`.
