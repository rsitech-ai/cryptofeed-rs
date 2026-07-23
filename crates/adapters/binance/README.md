# marketfeed-adapter-binance

Deterministic `SessionMachine` implementations for Binance Spot, Binance
USD-M futures, and Binance Coin-M (inverse). Pure protocol/state-machine logic —
no socket I/O, no wall-clock reads; the engine drives these via `SessionInput`
and consumes `SessionAction`.

## Sequence rules

Full sequence-rule documentation (Spot `U`/`u` buffer-then-drain, USD-M/Coin-M `pu`
continuity) lives as module docs in [`src/lib.rs`](src/lib.rs). Summary:

- **Spot**: buffer `depthUpdate` events while waiting on a REST snapshot;
  discard events the snapshot already covers; the first surviving buffered
  event must bridge `[U, u]` around `lastUpdateId`; then require
  `U == previous_applied_u + 1` while live. `u <= previous_applied_u` is a
  stale/duplicate event (dropped, no reconnect); `U` skipping ahead is a gap
  (invalidate + resync + reconnect).
- **USD-M / Coin-M**: same buffer/snapshot/drain shape, but live continuity is on
  `pu == previous_applied_u` instead of Spot's `U` rule. `u <=
  previous_applied_u` is still a stale/duplicate drop; a `pu` mismatch is a
  discontinuity (invalidate + resync + reconnect). Coin-M uses
  `/dapi/v1/depth` + `dstream` `@depth@100ms`.

## Checksum: N/A

Binance depth streams (Spot, USD-M, and Coin-M) carry **no checksum field at all** —
unlike, e.g., a per-message CRC32 `checksum` field used by some other venues.
Book integrity is guaranteed only by the `U`/`u`/`pu` sequence contiguity
rules described above. `BookSnapshot::checksum` and `BookDelta::checksum` are
always emitted as `None` from Binance session machines; there is nothing
to verify beyond sequence continuity, so no checksum verification code path
exists (or is needed) in this crate.

## Tests

- `tests/fixtures.rs`, `tests/usdm_fixtures.rs`, `tests/coinm_fixtures.rs` — decode + session fixtures,
  including Spot `@kline_*` candles, Spot `@ticker` → `Statistics24h`, Coin-M trades/mark/funding/OI/liq/L2, and record→replay determinism.
- `tests/l2_buffer.rs`, `tests/usdm_l2_buffer.rs`, `tests/coinm_l2_buffer.rs` — depth buffering, snapshot
  bridging, buffer-overflow invalidation, and the `SessionRunner` HTTP path.
- `tests/corpus_replay.rs` — replays committed `.mfr` byte corpora
  (`tests/corpus/*.mfr`) through `ReplayRunner` (trade/quote/mark) or the L2
  harness (WS frames + REST depth snapshot sidecar), asserting deterministic
  decoded events. Regenerate with `REGEN_CORPUS=1`, e.g.:

  ```bash
  REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_spot_trade_quote_corpus -- --ignored
  REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_usdm_mark_corpus -- --ignored
  REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_spot_l2_book_corpus -- --ignored
  REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_usdm_l2_book_corpus -- --ignored
  REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_coinm_l2_book_corpus -- --ignored
  ```

- `tests/decode_fuzz_smoke.rs` — lightweight no-panic smoke (Spot + Coin-M),
  CI-stable substitute for the real libFuzzer targets below.
- **C10 (`simd-json` feature):** Spot / USD-M / Coin-M `decode_text` can use
  optional `simd-json` via shared `json` helpers. Default remains `serde_json`.
  Parity (unit + `tests/decode_simd_parity.rs` on recorded fixtures):

  ```bash
  cargo test -p marketfeed-adapter-binance
  cargo test -p marketfeed-adapter-binance --features simd-json
  ```

  Enable only after parse latency profiles show need — see `src/json.rs` ponytail
  and [`docs/ops/latency_runtime.md`](../../../docs/ops/latency_runtime.md).

## Fuzzing

`decode_text` (Spot) and `decode_coinm_text` (Coin-M depth/kline) are exercised
by libFuzzer targets in the sibling [`fuzz/`](../../../fuzz) directory (a
standalone Cargo workspace — see [`fuzz/README.md`](../../../fuzz/README.md)).
Smoke tests above cover CI-run coverage in the meantime.


## Owner

Default review owner: `@s1korrrr` ([`CODEOWNERS`](../../../CODEOWNERS)).

## Limitations

- **Candles:** Binance Spot + USD-M + Coin-M `@kline_*` → `MarketEvent::Candle` (opt-in via
  `Channel::Candles` / `candle_intervals`). See ADR 0001.
- **Coin-M / inverse Binance (`VenueId(12)`):** trades + `@bookTicker` quote +
  mark/index/funding + OI REST timer (`/dapi/v1/openInterest`) + `@forceOrder`
  liquidations + dedicated `<pair>@indexPrice@1s` (peer OKX `index-tickers`; mark
  stream still carries embedded index) + opt-in L2 (`pu` on dapi depth) + opt-in
  `@kline_*` candles.
- **Live ping:** venue WS ping/pong is transport-owned; Spot also schedules an
  application silence watchdog (`HEARTBEAT_TIMER_ID`) — offline proof only until
  live canary.
- `live_ignored.rs` stays `#[ignore]` — CI remains offline.

## Maturity

| Venue | Label | Claim |
|---|---|---|
| `binance-spot` (VenueId 2) | **alpha+** / beta-ready offline | Offline fixtures + corpus + candles + this README/owner/limits + canary checklist |
| `binance-usdm` (VenueId 3) | **alpha** | Offline fixtures + L2 book corpus / OI timer; close-out docs still thin |
| `binance-coinm` (VenueId 12) | **alpha** | Fixtures for trades/quote/mark/funding/OI/liq/klines + L2 book corpus (`pu` / dapi) |

**Not beta for either venue.** Beta requires a scheduled live canary (§11.8).

- Matrix: [`docs/plan/maturity_matrix.md`](../../../docs/plan/maturity_matrix.md)
- Canary checklist: [`docs/ops/canary_checklist.md`](../../../docs/ops/canary_checklist.md)
