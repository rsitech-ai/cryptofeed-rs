# EventPulse E2 MFR1 MechanicsInputV2 Transformer Report

## Outcome

Implemented a pure offline, route-specific Binance USD-M MFR1-to-`MechanicsInputV2` transformer in the leaf `marketfeed-event-pulse-mfr1` crate. The existing V1 API and implementation remain unchanged.

The transformer consumes an owned routed Binance session and complete MFR1 segment bytes. Before parsing the MFR or invoking replay, it requires an adapter-owned pristine routed-v4 identity matching the exact route, connection, session, instrument, and BNBUSDT symbol. It then validates the MFR1 v3 header and reserved word, exact framed length, 256 MiB/65,536-record bounds, capture and decision times, selected session, exact nonempty build/session metadata with no initial routed subscriptions, route-specific admission/catalog/epoch identity, and every selected payload's role, symbol, native range, E/T provenance, and action-producing frame order.

The causal admission/receive interval applies to the source field used by mechanics: Binance T for quote/book/trade, `time` for OI, and inner `o.T` for liquidation. Binance E remains bounded and retained exactly as provenance but is not misrepresented as the causal source time, so an E before capture start or after receive does not reject an otherwise truthful T.

PUBLIC and MARKET use distinct checked `ReplayCatalogV1` values because venue id 3 must resolve to exactly one admitted source per transform. MARKET output is authored directly through `MechanicsInputV2::market`; no V1 MARKET cursor or serialization path is used. V1 remains the exact path only for reserved ActionBuffer and MarketDispatch drop inputs.

## Deterministic BOOK correlation

Buffered BOOK correlation is deterministic without changing the adapter. Selected raw depth payloads are decoded before replay into a bounded ledger keyed by exact session plus native `U/u`; the retained provenance includes `pu`, E, and T. A snapshot response is keyed by its authoritative raw frame. When the adapter releases the snapshot and buffered bridge delta, the transformer normalizes their envelope coordinates to the response MFR group while consuming the snapshot and original delta provenance exactly once. Later live deltas consume their own native ledger entries. Duplicate, missing, mismatched, ambiguous, or unconsumed entries reject the complete transform without returning partial inputs or bytes.

## TDD evidence

- RED: `cargo test -p marketfeed-event-pulse-mfr1 --test transformer_v2` failed with `E0432` unresolved imports for the absent V2 API.
- Review RED: the exact 65,536 ordinary-action assertion returned `Ok(())` instead of `Err(Capacity)`, and `dispatch_capacity=16,384` constructed successfully under both policies instead of returning `InvalidExecutionMetadata`.
- Windows RED: `git check-attr text eol -- crates/event-pulse-mfr1/tests/fixtures/routed_v2_expected.jsonl` reported both attributes `unspecified`. The V2 writer already emitted the literal byte `b'\n'`; the fixture checkout, not the writer, was the platform-sensitive boundary.
- Archive repair RED: the first regression API did not exist (`E0425`), proving the repository-only attribute check had not been separated from unconditional fixture verification.
- GREEN: the review-hardened focused suite passes 20/20. In addition to the initial matrix, it rejects wrong-route, legacy, advanced, and wrong-config machines before replay; binds complete metadata; proves E/T causal separation; exercises exact 65,536/65,537 raw-record boundaries; proves DropNewest and FailEngine behavior; and proves no partial return after a late replay failure. The canonical oracle's 7 lines, 7,736 bytes, absence of CR bytes, and SHA-256 `a65c1f39f7dc0150748d0f0facb0ea6cc09ca0dcedeaaff07284513c90040237` are always checked. Only the LF Git-attribute assertion is skipped when `git rev-parse --show-toplevel` fails, with a temporary source-archive regression proving that path.
- Independent oracle: seven frozen canonical MARKET records (PUBLIC quote/snapshot/buffered delta/live delta and MARKET trade/liquidation/OI) match exact JSONL bytes and seven exact payload hashes. Each route strict-reads independently because the two source segments have independent availability sequences. Independently decoded expected values equal the transformer output, and fresh `SourceStateMachineV2` instances produce identical ingest outcomes and exact per-family cursor/state views.
- Aggregate-boundary regressions exercise exact and one-over predicates for 256 MiB input, 65,536 records, 65,536 authored inputs, 65,535 ordinary actions, and 16 MiB JSONL. The authoring ceiling is 65,535 actions (`action_index` 0 through 65,534), while a distinct 65,536-action observation buffer retains the one-over action without dropping it so the transformer can inspect then reject it deterministically. Both supported context policies reject `dispatch_capacity=16,384` because its derived authoring capacity would be 65,536. The JSONL writer itself accepts exactly 16 MiB then rejects the next byte.
- Existing V1 transformer suite remains 29/29 green.

## Verification

- `cargo test -p marketfeed-event-pulse-mfr1 --test transformer_v2` — GREEN (20 V2 integration, including the Windows LF proof, 65,536-record public boundary, and both-policy 65,536-action rejection).
- `cargo test -p marketfeed-adapter-binance --test usdm_routed_v4` — GREEN (11 routed-v4 integration).
- `cargo test -p marketfeed-event-pulse-mfr1 -p marketfeed-event-pulse -p marketfeed-adapter-binance --all-targets --all-features` — GREEN (4 MFR1 unit, 29 V1 integration, 20 V2 integration, and all EventPulse/Binance relevant suites; ignored network tests remained ignored).
- `cargo test -p marketfeed-event-pulse -p marketfeed-adapter-binance -p marketfeed-recording -p marketfeed-replay -p marketfeed-dispatch --quiet` — GREEN; ignored network tests remained ignored.
- `cargo test --workspace --all-targets --all-features --quiet` — GREEN; ignored live-network tests remained ignored.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — GREEN.
- `cargo +1.85.0 test -p marketfeed-event-pulse-mfr1 -p marketfeed-event-pulse -p marketfeed-adapter-binance --all-targets --all-features --quiet` — GREEN.
- `cargo +1.85.0 clippy -p marketfeed-event-pulse-mfr1 -p marketfeed-event-pulse -p marketfeed-adapter-binance --all-targets --all-features -- -D warnings` — GREEN.
- `cargo fmt --all -- --check` — GREEN.
- `cargo deny --offline --locked check` — GREEN: advisories, bans, licenses, and sources.
- `git diff --check` — GREEN.

## Dependency and authority audit

The production dependency edge remains `marketfeed-event-pulse-mfr1 -> marketfeed-adapter-binance`. Binance's normal dependency graph is pure adapter state, model, book, bytes, and Serde code; transport, engine, recording, and replay remain dev-only. The Windows regression adds only the already-locked workspace `sha2` crate as a dev dependency; `Cargo.lock` changes only by adding that package name to the leaf crate's dependency list.

The adapter change is additive and read-only: a routed-v4 machine exposes immutable identity only while it is in the exact factory-pristine state, and any input permanently retires that proof. Existing public decoded/event APIs and legacy/default behavior are preserved. No recording, replay, engine, daemon, transport, filesystem, network, environment, credential, snapshot, capture, package, manifest, or trading implementation changed. Returned output explicitly reports `evidence_authoring_allowed=false` and `blocked:fixture-provenance`.

## Residual hold

This is a repo-ready offline transformation boundary only. It does not author a fixture, prove source qualification, produce Clock/Coverage/confirmation inputs, support SnapshotProcessor V2, or grant capture, evidence, runtime, paper, canary, live, order, risk, or execution authority. E2 remains `blocked:fixture-provenance`.
