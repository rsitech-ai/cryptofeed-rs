# marketfeed-adapter-bybit

Bybit V5 public market-data adapter. `SessionMachine` only — no networking, no I/O; the
engine drives connect/reconnect/timers and this crate decides what to send/emit.

## Venues

| Category  | `VenueId` | Code            | WS endpoint                                    |
|-----------|----------:|-----------------|-------------------------------------------------|
| Linear    | 5         | `bybit-linear`  | `wss://stream.bybit.com/v5/public/linear`       |
| Spot      | 6         | `bybit-spot`    | `wss://stream.bybit.com/v5/public/spot`         |
| Inverse   | 11        | `bybit-inverse` | `wss://stream.bybit.com/v5/public/inverse`      |

See `docs/plan/venue_ids.md` (workspace root) for the canonical registry.

## Channels

| Topic | Segment | Emits |
|---|---|---|
| `publicTrade.{symbol}` | all | `MarketEvent::Trade` |
| `orderbook.1.{symbol}` | all | `MarketEvent::Quote` (best bid/ask) |
| `orderbook.{depth}.{symbol}` | all (opt-in `enable_l2`) | `MarketEvent::BookSnapshot` / `BookDelta` |
| `kline.{interval}.{symbol}` | linear (opt-in `candle_intervals`) | `MarketEvent::Candle` |

## L2 orderbook `u` rules (`orderbook.{depth}.{symbol}`)

From Bybit V5 public orderbook docs, enforced in `session.rs`:

1. On subscribe, WS pushes `type=snapshot` with update id `u` — apply atomically.
2. Each subsequent `type=delta` must have `u == previous_u + 1`.
3. `u <= previous_u` → stale/duplicate → **discard silently, no reconnect**.
4. `u > previous_u + 1` → sequence gap → invalidate book, resync, and reconnect.
5. `u == 1` after going live → treat as fresh snapshot (venue reset / tick-size change).
6. Qty `"0"` deletes the price level.

Cross-sequence `seq` is recorded when present; continuity is enforced on `u` (`u` must be
consecutive; `seq` is monotonic but not necessarily consecutive).

## Limitations

- **Live ping** uses `ScheduleTimer` → `SessionInput::Timer` → `{"op":"ping"}` (offline
  fixtures in `tests/fixtures.rs`). Engine timer fulfillment is on `main` (PR #10).
- `orderbook.1` (top-of-book quote) tracking is best-effort L1 only, not a full ladder.
- Explicit subscriptions are resolved through the catalog and preserve the requested native
  symbols; `BTCUSDT` / `BTCUSD` are fallback defaults only for an empty request.
- `live_ignored.rs` network smoke test is `#[ignore]`d by default — CI stays offline.

## Tests / corpora

- `tests/l2_sync.rs` — snapshot → contiguous `u` delta → gap reconnect; control resync via
  reconnect and a fresh WebSocket snapshot.
- `tests/corpus_replay.rs` — checked-in `.mfr` corpora (offline CI):
  - `tests/corpus/linear_trade_quote.mfr`
  - `tests/corpus/linear_l2_book.mfr` — snapshot + ≥2 `u` deltas, replay identity

```bash
REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_linear_l2_book_corpus -- --ignored
```

## C10 (`simd-json` feature)

Optional `simd-json` for shared `decode_text` (linear/spot/inverse — one V5 JSON
path) via `src/json.rs`. Default remains `serde_json`. Parity (unit +
`tests/decode_simd_parity.rs`):

```bash
cargo test -p marketfeed-adapter-bybit
cargo test -p marketfeed-adapter-bybit --features simd-json
```

Enable only after parse latency profiles show need — see
[`docs/ops/latency_runtime.md`](../../../docs/ops/latency_runtime.md).

## Maturity

**alpha** — offline fixtures + L2 `u` proofs + L2 book corpus + optional inverse VenueId 11.
**Not beta:** no scheduled live canary, no soak/RSS bound (§11.8 / audit WP-F).
