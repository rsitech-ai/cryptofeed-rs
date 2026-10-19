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

## Catalog discovery

The [official instruments contract](https://bybit-exchange.github.io/docs/v5/market/instrument)
defines spot as unpaginated. Spot requests omit `limit`/`cursor`; derivatives
request up to 1000 rows and follow the exact opaque `nextPageCursor`, percent
encoded as a query value, until an explicit empty terminal cursor. Discovery
refuses incomplete pages, cycles, duplicate symbols, wrong categories, malformed
cursor types, duplicate JSON keys, nonpositive increments and responses after a
terminal page. A malformed later page cannot publish earlier definitions.

Bybit admission bounds are 16 pages, 8 MiB per page, 2048 cursor bytes, 1000 rows
per derivative page and 4096 spot rows. The daemon bounds primary/continuation
collection to 32 responses, 128 MiB and a 60-second request-phase deadline;
existing optional detail requests on other factories retain their own policy.
HTTP bytes, request IDs and full query provenance remain in the shared journal.
Other factories default to no continuation and retain their original ordering.
Callers of the factory outside the daemon must honor the continuation hook and
its documented resource/completion contract.

Derivative product kinds require category-correct native `contractType` values;
unknown or absent values never become perpetuals by default. Quantity increments
require derivative `qtyStep` or spot `basePrecision`; a field from another schema
cannot supply a missing required field or override the correct one.

Completion covers the returned pages for the requested endpoint/status scope.
It does not establish an atomic exchange-universe snapshot or all historical
statuses. Settlement/expiry/contract-unit qualification and source continuity
remain separate open work; strict dataset admission is unavailable.

## Channels

| Topic | Segment | Emits |
|---|---|---|
| `publicTrade.{symbol}` | all | `MarketEvent::Trade` |
| `orderbook.1.{symbol}` | all | `MarketEvent::Quote` (best bid/ask) |
| `orderbook.{depth}.{symbol}` | all (opt-in `enable_l2`) | `MarketEvent::BookSnapshot` / `BookDelta` |
| `kline.{interval}.{symbol}` | all (opt-in `candle_intervals`) | `MarketEvent::Candle` |
| `tickers.{symbol}` | all | Spot statistics or derivative mark/funding/open interest |
| `allLiquidation.{symbol}` | derivatives | `MarketEvent::Liquidation` with native bankruptcy/position provenance |

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

## Subscription admission and readiness

Each connection generation sends unique `req_id` values and retains the exact
requested topics per request. Requests contain at most ten topics for all three
categories. The connection is bounded to21,000 aggregate serialized JSON args
bytes, a conservative local bound relative to the native character limit.

An ACK must name a current outstanding request, contain boolean success=true,
nonempty native conn_id consistent across that connection, and ret_msg equal to
empty or subscribe. Exact duplicate ACKs are idempotent. Ping/pong cannot satisfy
an ACK. Missing/foreign IDs, negative/conflicting responses, unexpected controls
and malformed mixed control/data frames invalidate the active connection;
recovery requires a new Connected lifecycle input. The official SDK has a
fallback for servers without request IDs. This adapter refuses that ambiguous
admission rather than guessing which request succeeded; universal native ID echo
is not established by the protocol documentation.

MarkLive requires all request ACKs, a valid two-sided L1 quote for every selected
symbol, and every requested L2 book synchronized. Financial observations may be
emitted during connected startup; consumers must also inspect readiness/quality.
Cold, disconnected, stopped and failed sessions emit no later financial frames.
A20-second local receipt-timestamp startup deadline applies even when a frame
arrives before timer polling. At the exact deadline startup fails. A missing L1
side after live revokes readiness and starts a new20-second quote recovery
window; known ACKs remain bound to the same connection. Overflow fails closed.
This deadline is not proof of clock health, source freshness or continuity.

The decoded alpha API expands SubscribeAck with req_id/conn_id/ret_msg and adds
UnexpectedControl. Rust pattern users must migrate. Engine public callbacks
revoke readiness on any returned error, including errors after MarkLive during
publication, and revoke old admission before new connection/close/loss handling.
Partial actions from failed adapter callbacks are discarded.

Primary contracts: [connection/ACK/heartbeat and limits](https://bybit-exchange.github.io/docs/v5/ws/connect),
[pinned SDK fallback](https://github.com/bybit-exchange/pybit/blob/c3309e14e577b1d5610c1f288a413df6990a6d25/pybit/_websocket_stream.py#L473-L479).

## Tests / corpora

- `tests/readiness.rs` checks exact ACK correlation, all requested startup inputs,
  chunking, cumulative limits, timeout boundaries, reconnect isolation, cold
  frame suppression and engine readiness revocation on errors.
- `tests/l2_sync.rs` and `tests/book_semantics.rs` check native book rules and
  action-ordered application to the real downstream book projection.
- Original `linear_trade_quote.mfr`, `linear_l2_book.mfr` and
  `inverse_l2_book.mfr` remain byte-for-byte unchanged. Their uncorrelated ACKs
  are SHA-pinned and explicitly tested as quarantined under strict admission.
- Separately named `*_correlated_v1.mfr` fixtures cover the accepted path.
  Creation requires CREATE_CORRELATED_CORPUS=1 and create_new, which refuses
  overwrite. Do not regenerate original frozen bytes.
- MFR1 semantic replay comparison checks payloads, timestamps, venue/session and
  flags, with explicit runtime connection/ordinal exclusions. MFR1 omits physical
  connection identity, and ReplayRunner uses adapter ordinals rather than raw
  frame ordinals. These fixtures do not qualify exact runtime-envelope replay;
  authoritative shared-feed/mixed causal replay remains a separate requirement.

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
unit qualification. Liquidations now retain bankruptcy-price/native-position
provenance and unverified WS units; consumers must not treat them as executed
spot flow. Source-gap recovery and strict dataset qualification remain open.

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
