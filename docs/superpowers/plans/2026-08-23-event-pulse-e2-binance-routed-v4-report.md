# Binance USD-M routed v4 prerequisite report

## Scope

Adapter-only prerequisite. No public prospective admission/preflight API is included because the root freeze does not yet pin that request shape. No authority or fixture claim is made.

## RED

`cargo test -p marketfeed-adapter-binance --test usdm_routed_v4` failed before implementation with unresolved `BinanceUsdmRouteV4` and missing `BinanceUsdmSession::try_new_routed_v4`.

## GREEN

- `cargo test -p marketfeed-adapter-binance --test usdm_routed_v4`: 4 passed.
- `cargo test -p marketfeed-adapter-binance`: all non-ignored tests passed; network tests remained ignored by contract.
- `cargo test --workspace --all-targets --all-features`: passed across the workspace.
- `cargo clippy -p marketfeed-adapter-binance --all-targets --all-features -- -D warnings`: passed.
- `cargo +1.85.0 test -p marketfeed-adapter-binance --test usdm_routed_v4`: 4 passed.
- `cargo +1.85.0 clippy -p marketfeed-adapter-binance --all-targets --all-features -- -D warnings`: passed.
- `cargo +1.85.0 check --workspace --all-targets --all-features`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny --offline --locked check`: advisories, bans, licenses, and sources passed.
- `git diff --check`: passed.

The routed HTTP failure tests also prove a malformed timestamp response produces no actions and does not consume the pending request; a corrected retry succeeds.

## Residual hold

`blocked:fixture-provenance`. Preflight v4 cannot truthfully retain bookTicker `u` provenance while authoring a DERIVED quote cursor with the current canonical EventEnvelope/EPIN representation. Source qualification and capture evidence are also unverified.
