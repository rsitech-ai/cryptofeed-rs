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
| `kline.{interval}.{symbol}` | all (opt-in `candle_intervals`) | `MarketEvent::Candle` |

## L2 orderbook `u` rules (`orderbook.{depth}.{symbol}`)

**Open integrity finding:** the legacy `session.rs` currently enforces the
following policy, including a `u + 1` assumption that the reviewed official WS
contract does not establish. This is implementation behavior, not proof of
continuity. Removing that unsupported inference is a separate pending repair.
The official SDK also applies deltas without the `+1` test.

Current legacy policy:

1. On subscribe, WS pushes `type=snapshot` with update id `u` — apply atomically.
2. Each subsequent `type=delta` must have `u == previous_u + 1`.
3. `u <= previous_u` → stale/duplicate → **discard silently, no reconnect**.
4. `u > previous_u + 1` → sequence gap → invalidate book, resync, and reconnect.
5. `u == 1` after going live → treat as fresh snapshot (venue reset / tick-size change).
6. Qty `"0"` deletes the price level.

Neither native `u` nor cross-sequence `seq` establishes gap-free delivery under
the reviewed contract. Structural book synchronization and dataset continuity
must remain separate. The current L2 gap inference is not qualified evidence.

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

Financial DOM decoding uses the shared bounded lexical boundary, including
`simd-json` feature builds. The feature no longer selects a float financial DOM.
Parity tests share this boundary; explicit native expectations in
`tests/native_semantics.rs` supply independent expected values:

```bash
cargo test -p marketfeed-adapter-bybit
cargo test -p marketfeed-adapter-bybit --features simd-json
```

Enable only after parse latency profiles show need — see
[`docs/ops/latency_runtime.md`](../../../docs/ops/latency_runtime.md).

## Maturity

**alpha** — offline native semantics fixtures and book/replay corpora; no L2 continuity proof.
**Not beta:** no scheduled live canary, no soak/RSS bound (§11.8 / audit WP-F).

## Native trade and candle integrity

`publicTrade` validates a nonempty batch of at most1024 rows before emitting one
batch with stable row ordinals. Topic/row symbols and the selected session
catalog must agree. IDs, exact positive price/size, taker-side tokens, T, seq,
BT/RPI/L and frame ts are retained. Unknown side stays Unknown with its native
token; missing flags stay absent. Repeated seq across message chunks is valid
and neither implies duplicate executions nor proves continuity. The native
`snapshot` message type describes the real-time trade channel; it is not an
initial-history flag. No constituent counts are inferred.

Candles validate every row (capacity4096, a local bound), matching topic intervals,
positive consistent OHLC, nonnegative volume/turnover and checked native times
before emission. Confirmation is explicit `Some(false/true)`. Envelope exchange
time is frame system-generation ts; native start/end/last-match timestamp and
financial fields are separately retained in CandleProvenance. Observed receipt
is still the envelope receive time; none of these exchange clocks establishes
when data became usable in a strategy.

Volume stays native. Turnover stays native counter-volume; quote_volume is None.
The REST kline contract explicitly distinguishes inverse vs linear units; the
WS page names turnover without restating those units. A REST analogy is not WS
unit qualification. Liquidation bankruptcy-price/native-position provenance,
source-gap recovery and exact per-channel ACK readiness remain separate work.

The Rust alpha decoded API changes Candle to Candles(Vec<CandleDecoded>) and
adds TradeDecoded.provenance. Canonical Candle construction adds optional
CandleProvenance (None for unknown). Old serialized absent provenance remains
absent/unknown; this is not a promise of Rust constructor source compatibility.
Original MFR1/frozen corpora are unchanged.

Primary contracts reviewed2026-10-19:
[trades](https://bybit-exchange.github.io/docs/v5/websocket/public/trade),
[WS klines](https://bybit-exchange.github.io/docs/v5/websocket/public/kline),
[REST kline units](https://bybit-exchange.github.io/docs/v5/market/kline),
[orderbooks](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook),
[pinned official SDK](https://github.com/bybit-exchange/pybit/blob/c3309e14e577b1d5610c1f288a413df6990a6d25/pybit/_websocket_stream.py#L412-L450).
