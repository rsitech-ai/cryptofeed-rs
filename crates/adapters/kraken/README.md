# marketfeed-adapter-kraken

Kraken Spot, WebSocket v2 (`wss://ws.kraken.com/v2`). Channels: `trade`, `ticker`
(BBO quotes), `book` (L2, depth 10, CRC32 checksum), opt-in `ohlc` candles
(`Channel::Candles` / `candle_intervals` → `MarketEvent::Candle`).

**Candles:** Kraken has no closed-bar flag — every trade may push a partial OHLC.
ponytail ceiling: consumers see in-progress bars; upgrade = buffer until
`interval_begin` advances.

## L2 book sync (`book` channel)

- Subscribe with `{"channel":"book","symbol":[...],"depth":10}`. The checksum is
  always computed over the **top 10** levels regardless of subscribed depth
  (Kraken guide), so depth 10 is hardcoded (`BOOK_DEPTH` in `session.rs`).
- First message per symbol is `"type":"snapshot"`; every message after is
  `"type":"update"`. Updates carry only the price levels that changed —
  `"qty":"0"` means delete. Levels that fall out of the top-10 window are
  **not** sent as deletes; the client must truncate its own book to depth.
- Both snapshot and update messages carry a `checksum` (CRC32).

### Checksum algorithm

For the top 10 asks (price low→high) then top 10 bids (price high→low): strip
`.` and leading zeros from each price and qty string, concatenate
`price + qty` per level, concatenate all levels, and run IEEE CRC32
(poly `0xEDB88320`, init/final XOR `0xFFFFFFFF`) over the resulting ASCII
bytes. See `checksum.rs`; a small local CRC32 table is implemented there —
**not** the CRC-32C in `marketfeed-recording` (different polynomial), and no
new crate dependency was added for it.

Trailing zeros in the price/qty text matter (stripping only removes *leading*
zeros), and Kraken does not always transmit a fixed number of decimals for a
given symbol. So the checksum sidecar in `session.rs` (`KrakenBook::wire`)
keeps the **literal wire string** for every top-10 level, keyed by exact price,
separately from the `Fixed`-typed `OrderBook` used for the model-facing
`BookSnapshot`/`BookDelta` events. Reformatting from `Fixed` is only a fallback
for a level that (should never but theoretically could) go unseen by the wire
cache.

### Gap handling

There is no book sequence number — Kraken relies entirely on the checksum for
sync verification. On any checksum mismatch (snapshot or update) or a
book-apply error (crossed book, non-exact scale, etc.), the session emits
`BookInvalidated` + `ResyncInstrument` and requests `Reconnect(SequenceGap)`;
a fresh connection re-subscribes and receives a new snapshot.

## Trades

`decode_trades` emits **every** row in a `trade` frame (`DecodedEvent::Trades`,
plural) — a single frame can batch multiple trades and all of them are
forwarded in one `EventBatch`.

## Tests

- `messages.rs` unit tests: trade/ticker/book decode, including the exact
  Kraken-docs golden book snapshot.
- `checksum.rs` unit tests: known CRC32 vector + the full golden checksum
  example from Kraken's book-checksum-v2 guide (`3310070434`).
- `tests/l2_sync.rs`: snapshot → checksum-verified delta → checksum-mismatch →
  `BookInvalidated` + `Reconnect(SequenceGap)`.
- `tests/fixtures.rs`: trade/quote decode, heartbeat no-op, record/replay
  determinism.

- `tests/corpus_replay.rs`: checked-in raw `.mfr` corpora (offline CI):
  - `tests/corpus/spot_trade_quote.mfr`
  - `tests/corpus/spot_l2_book.mfr` — golden snapshot + ≥2 CRC32-verified updates
  - `tests/corpus/futures_ticker_liq.mfr` — liq trade + ticker mark/index/funding/OI
  - `tests/corpus/futures_l2_book.mfr` — Futures `book_snapshot` + ≥2 `book` deltas

```bash
REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_spot_l2_book_corpus -- --ignored
REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_futures -- --ignored
```

## C10 (`simd-json` feature)

Optional `simd-json` for Spot WS v2 `decode_text` via `src/json.rs`. Default
remains `serde_json`. Parity (unit + `tests/decode_simd_parity.rs` on L2
fixtures):

```bash
cargo test -p marketfeed-adapter-kraken
cargo test -p marketfeed-adapter-kraken --features simd-json
```

Enable only after parse latency profiles show need — see
[`docs/ops/latency_runtime.md`](../../../docs/ops/latency_runtime.md).

## Futures candles (REST charts)

No public candle WS on Futures WS v1. Opt-in `Channel::Candles` / `candle_intervals`
polls `GET /api/charts/v1/trade/{symbol}/{resolution}?count=1` on `CANDLE_TIMER_ID`
(Bitstamp/Coinbase pattern). Exact `Fixed` OHLCV. Tick type = `trade`.

ponytail: poll re-emits latest bar each tick (no close-only filter).

## Maturity

**alpha** — offline L2 + trade/heartbeat proofs + trade/quote + L2 book corpora + REST
charts candles fixtures. **Not beta:** no scheduled live canary, no soak/RSS bound (§11.8).
