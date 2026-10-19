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

## Native book rules (`orderbook.{depth}.{symbol}`)

The [official WS contract](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
does not establish consecutive `u` values. The
[reviewed official SDK](https://github.com/bybit-exchange/pybit/blob/c3309e14e577b1d5610c1f288a413df6990a6d25/pybit/_websocket_stream.py#L412-L450)
also applies deltas without a `+1` test. This adapter therefore uses the following
structural policy:

1. A native snapshot or `u == 1` atomically replaces the book.
2. Increasing delta `u` values are applied, including nonunit steps.
3. An exact repeat of the last successfully applied native message is ignored.
   Same/nonincreasing `u` with different contents or a regressing cross-sequence
   invalidates state and reconnects as a protocol anomaly.
4. Malformed/unknown text, unexpected binary frames and failed atomic book
   application invalidate active books/quotes. L2 deltas cannot resume until a
   fresh snapshot. These failures do not manufacture a confirmed `SequenceGap`.
5. Quantity zero deletes a delta level. Snapshot levels must be ordered, unique,
   positive and within the requested depth. Topic, symbol and depth are checked.
6. L1 is snapshot-only. Repeated idle snapshot IDs refresh BBO; a missing side
   invalidates the quote instead of retaining stale values.

Native `u`, `seq`, `ts`, optional `cts`, channel and original message type remain
in `BookProvenance` on quotes, snapshots and deltas. Canonical `source_sequence`
is absent because neither native ID establishes gap-free delivery. Book
structural synchronization does not qualify source continuity or a dataset.
Factory L2 subscriptions support depths 50/200/1000; unsupported/mixed depths and
explicit cadence requests fail instead of silently subscribing at another depth.

This alpha change adds optional provenance fields to the Rust `Quote`,
`BookSnapshot` and `BookDelta` structs and replaces the adapter's decoded book
variant with `BookDecoded`. Downstream Rust struct literals/patterns must migrate.
Older serialized records omit provenance; decoding defaults it to `None`.

## Limitations

- **Live ping** uses `ScheduleTimer` → `SessionInput::Timer` → `{"op":"ping"}` (offline
  fixtures in `tests/fixtures.rs`). Engine timer fulfillment is on `main` (PR #10).
- `orderbook.1` (top-of-book quote) tracking is best-effort L1 only, not a full ladder.
- Explicit subscriptions are resolved through the catalog and preserve the requested native
  symbols; `BTCUSDT` / `BTCUSD` are fallback defaults only for an empty request.
- `live_ignored.rs` network smoke test is `#[ignore]`d by default — CI stays offline.

## Tests / corpora

- `tests/l2_sync.rs` and `tests/book_semantics.rs` — nonunit updates, exact versus
  conflicting repeats, malformed/binary-frame recovery, resets, native provenance
  and action-ordered application to the real downstream book projection.
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
