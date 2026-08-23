# Binance USD-M routed v4 prerequisite report

## Scope

Adapter-only prerequisite. No public prospective admission/preflight API is included because the root freeze does not yet pin that request shape. No authority or fixture claim is made.

## RED

`cargo test -p marketfeed-adapter-binance --test usdm_routed_v4` failed before implementation with unresolved `BinanceUsdmRouteV4` and missing `BinanceUsdmSession::try_new_routed_v4`.

The successor RED failed because `DepthUpdate` lacked separate `E`/`T`, `ForceOrder` lacked outer `E`/inner `o.T`, and the pair constructor did not exist. Runtime counterexamples additionally exposed routed snapshot System output, duplicate outstanding OI requests, ignored zero-value routed trades, permissive subscription ACK/HTTP correlation, saturating source-time conversion, and missing aggregate-trade outer `E` provenance.

The compatibility-review RED then failed to compile when the test constructed the historical `UsdmDecoded::AggTrade`, `DepthUpdate`, and `ForceOrder` shapes. Additional counterexamples showed an empty/mismatched catalog was accepted and native identifiers above `i64::MAX` could reach routed state/output.

## GREEN

- `cargo test -p marketfeed-adapter-binance --test usdm_routed_v4`: 9 passed after compatibility and native-bound corrections.
- `cargo test -p marketfeed-adapter-binance`: all non-ignored tests passed; network tests remained ignored by contract.
- `cargo test --workspace --all-targets --all-features`: passed across the workspace.
- `cargo clippy -p marketfeed-adapter-binance --all-targets --all-features -- -D warnings`: passed.
- `cargo +1.85.0 test -p marketfeed-adapter-binance --test usdm_routed_v4`: 9 passed.
- `cargo +1.85.0 clippy -p marketfeed-adapter-binance --all-targets --all-features -- -D warnings`: passed.
- `cargo +1.85.0 check --workspace --all-targets --all-features`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: advisories, bans, licenses, and sources passed.
- `git diff --check`: passed.

The routed HTTP failure tests also prove a malformed timestamp response produces no actions and does not consume the pending request; a corrected retry succeeds.

The successor correction binds routed BOOK and LIQUIDATION authoring to Binance transaction time (`T` / `o.T`) while retaining distinct decoded event and transaction timestamps, including aggregate-trade outer `E`. It also makes routed construction pair-only, suppresses successful snapshot System output, deduplicates outstanding OI polls, requires ACK id `1`, rejects ignored routed frames and unknown/retired HTTP ids, and checks every routed source millisecond before nanosecond authorship. Legacy/default authoring remains on its original timestamps and actions.

The compatibility correction restores the public `UsdmDecoded` variants exactly and moves routed provenance to `UsdmRoutedV4Decoded`/`UsdmRoutedV4SourceTimes`. Pair construction now proves the exact native catalog row, and all routed native sequence identifiers fail closed above `i64::MAX` before output/state mutation.

## Residual hold

`blocked:fixture-provenance`. Preflight v4 cannot truthfully retain bookTicker `u` provenance while authoring a DERIVED quote cursor with the current canonical EventEnvelope/EPIN representation. Source qualification and capture evidence are also unverified.
