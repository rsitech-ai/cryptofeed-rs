# marketfeed-adapter-deribit

Deribit derivatives, JSON-RPC over WebSocket (`wss://www.deribit.com/ws/api/v2`).
Channels: `trades.{instrument}.100ms`, `ticker.{instrument}.100ms` (quote +
mark/index/funding/OI), dedicated `deribit_price_index.{index}` (peer OKX
`index-tickers`), `book.{instrument}.100ms`, opt-in
`chart.trades.{instrument}.{resolution}` candles
(`Channel::Candles` / `candle_intervals` → `MarketEvent::Candle`).

Public sessions cannot use `.raw` for trades or book (Deribit error 13778
`raw_subscriptions_not_available_for_unauthorized`); the adapter subscribes to
`.100ms` intervals only.

**Candles:** Deribit pushes many in-period updates with no close flag. ponytail
ceiling: partial bars; upgrade = emit only when `tick` advances.

## L2 book sync (`book.*` channel) — channel choice

Deribit exposes three book channel families:

- `book.{instrument}.{group}.{depth}.{interval}` — grouped, depth-limited,
  **periodic full snapshots only** (no `change_id`/`prev_change_id`).
- `book.{instrument}.raw` — full-depth incremental, but the `raw` interval is
  **authenticated-users only**; a `public/subscribe` request for it is rejected.
- `book.{instrument}.100ms` — full-depth incremental, public, and carries
  `change_id`/`prev_change_id` on every message.

The task requires `change_id`/`prev_change_id` gap detection, which only the
un-grouped incremental channel provides, and the `.raw` interval isn't usable
from a public connection — so this adapter subscribes to
**`book.{instrument}.100ms`** (see `session.rs`, `SessionInput::Connected`).

## Snapshot vs. change

- The first notification for an instrument has no `prev_change_id` (and, in
  practice, `"type":"snapshot"`); every one after it is a `"type":"change"`
  (or simply carries `prev_change_id`) — `messages.rs::decode_book` uses the
  `type` field when present and falls back to `prev_change_id.is_none()`
  otherwise.
- Levels are `[action, price, amount]` triples, `action` ∈ `new | change |
  delete`. The snapshot is the **complete order book, no depth limit**, so
  the local `OrderBook` is unbounded (`depth: None` in `session.rs`).

## Gap handling

Each `change` message's `prev_change_id` must equal the last applied
`change_id`. On a mismatch (or a book-apply error), the session emits
`SequenceGap` + `BookInvalidated` + `ResyncInstrument` and requests
`Reconnect(SequenceGap)`; the next connection re-subscribes and gets a fresh
snapshot.

Deribit's book channel carries no checksum — sync correctness rests entirely
on `change_id` continuity, unlike Kraken's CRC32-checksummed `book` channel.

## Tests

- `messages.rs` unit tests: trades/ticker/heartbeat plus book snapshot
  (no `prev_change_id`) and book change (`prev_change_id` present) decode.
- `tests/l2_sync.rs`: snapshot → change_id-matched delta → change_id gap →
  `BookInvalidated` + `Reconnect(SequenceGap)`.
- `tests/fixtures.rs`: trade/ticker decode, `heartbeat` `test_request` →
  `public/test` reply, record/replay determinism.

- `tests/corpus_replay.rs`: checked-in raw `.mfr` corpora (offline CI):
  - `tests/corpus/perp_trade_ticker.mfr`
  - `tests/corpus/perp_l2_book.mfr` — snapshot + ≥2 `change_id` deltas

```bash
REGEN_CORPUS=1 cargo test -p marketfeed-adapter-deribit --test corpus_replay regen_perp_l2_book_corpus -- --ignored
```

## C10 (`simd-json` feature)

Optional `simd-json` for JSON-RPC `decode_text` via `src/json.rs`. Default
remains `serde_json`. Parity (unit + `tests/decode_simd_parity.rs` on L2
fixtures):

```bash
cargo test -p marketfeed-adapter-deribit
cargo test -p marketfeed-adapter-deribit --features simd-json
```

Enable only after parse latency profiles show need — see
[`docs/ops/latency_runtime.md`](../../../docs/ops/latency_runtime.md).

## Maturity

**alpha** — offline L2 + trade/heartbeat proofs + trade/ticker + L2 book corpora. **Not beta:**
no scheduled live canary, no soak/RSS bound (§11.8).
