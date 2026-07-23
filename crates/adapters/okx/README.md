# marketfeed-adapter-okx

OKX Spot, SWAP (perpetual), and Futures (dated) market-data adapter. Pure
`SessionMachine`: it decodes bytes and emits actions/events — it never opens a
socket, spawns a task, sleeps, or touches disk. The engine owns all I/O.

## Venues

| `VenueId` | Code | Segment | Instruments endpoint |
|----------:|------|---------|-----------------------|
| 4 | `okx-spot` | Spot | `GET /api/v5/public/instruments?instType=SPOT` |
| 9 | `okx-swap` | Linear perpetuals | `GET /api/v5/public/instruments?instType=SWAP` |
| 10 | `okx-futures` | Linear dated futures | `GET /api/v5/public/instruments?instType=FUTURES` |

All three share one WS gateway (`wss://ws.okx.com:8443/ws/v5/public`) and one
`OkxSession` state machine (`OkxSessionConfig.venue` selects the envelope venue
id; `OkxSessionConfig.subscribe_mark_funding` gates the derivatives channels).
`VenueId(9)`/`VenueId(10)` are claimed in the workspace-wide
[`docs/plan/venue_ids.md`](../../../docs/plan/venue_ids.md) registry.

Only `ctType == "linear"` and `ctType == "inverse"` SWAP/FUTURES instruments are
mapped (`PerpetualLinear`/`FutureLinear` / `PerpetualInverse`/`FutureInverse`).
Linear and inverse share `okx-swap` / `okx-futures` VenueIds (same public WS).
(ponytail — see `instruments.rs` for the upgrade path).

## Channels

| Channel | Segment | Emits |
|---|---|---|
| `trades` | all | `MarketEvent::Trade` |
| `tickers` | all | `MarketEvent::Quote` + `MarketEvent::Statistics24h` |
| `books` | all (opt-in via `enable_l2`) | `MarketEvent::BookSnapshot` / `BookDelta` |
| `candle1m` / `candle5m` / `candle15m` / `candle1H` / `candle1D` | Spot + SWAP/Futures (opt-in via `candle_intervals`) | `MarketEvent::Candle` |
| `mark-price` | SWAP/Futures | `MarketEvent::MarkPrice` |
| `index-tickers` | SWAP/Futures | `MarketEvent::IndexPrice` |
| `funding-rate` | SWAP/Futures | `MarketEvent::Funding` |

`index-tickers` subscribes on the *underlying pair* instId (e.g. `BTC-USDT`),
not the SWAP/Futures symbol (e.g. `BTC-USDT-SWAP`), matching OKX's channel
semantics — derived from the symbol via `BASE-QUOTE[-SUFFIX]` splitting.

## L2 book continuity: `seqId` / `prevSeqId`, not checksum

OKX's `books` channel is kept live by a strict sequence chain, **not** the
legacy CRC32 `checksum` field:

1. On subscribe, OKX sends an `action: "snapshot"` frame carrying `seqId`.
   We apply it via `OrderBook::apply_snapshot` and record `seqId` as the book's
   sequence.
2. Every subsequent `action: "update"` frame carries `prevSeqId` (the `seqId`
   of the update it continues from) and a new `seqId`. We require
   `prevSeqId == <book's last applied seqId>` before applying bid/ask deltas.
3. Any mismatch (`prevSeqId` gap) — or a snapshot/delta apply error — invalidates
   the book, emits `SystemEvent::BookInvalidated` + `SequenceGap`, requests
   `ResyncInstrument`, and triggers `Reconnect(ReconnectReason::SequenceGap)` so
   OKX re-sends a fresh snapshot on the next connection.

**We parse `checksum` but do not use it for integrity.** OKX deprecated the CRC32
book checksum in June 2026: the field is still present on the wire but is
always `0`. Continuity is solely the `seqId`/`prevSeqId` chain above. A
**non-zero** checksum is treated as unexpected (legacy / corrupt) and fail-closes
via `ChecksumMismatch` + reconnect — we do **not** recompute IEEE CRC over
top-25 levels.

## Fixtures

`tests/fixtures.rs` covers both inline JSON strings and raw wire payloads
under `tests/fixtures/*.json`:

- `unknown_message.json` — unrecognized channel (`orders`) is reported via
  `SystemEvent::UnknownMessage`, not a decode error.
- `candle1m.json` — Spot `MarketEvent::Candle` (exact Fixed OHLCV).
- `candle1m_swap.json` — SWAP `MarketEvent::Candle` for `BTC-USDT-SWAP`.
- `mark_price.json`, `index_tickers.json`, `funding_rate.json` — derivatives
  channels for a SWAP session.
- `swap_trade.json` — a `trades` frame for `BTC-USDT-SWAP` (same decoder as
  Spot trades; SWAP/Futures reuse the Spot wire shape).
- `l2_snapshot.json` + `l2_update.json` / `l2_update2.json` — continuous
  snapshot→delta chain (`prevSeqId` follows `seqId`).
- `l2_update_gap.json` — update whose `prevSeqId` does not match (resync path).
- `tests/corpus/spot_l2_book.mfr` — checked-in snapshot+delta replay identity
  corpus (`tests/corpus_replay.rs`).

## C10 (`simd-json` feature)

Optional `simd-json` for shared `decode_text` (Spot/SWAP/Futures) via `src/json.rs`.
Default remains `serde_json`. Parity (unit + `tests/decode_simd_parity.rs`):

```bash
cargo test -p marketfeed-adapter-okx
cargo test -p marketfeed-adapter-okx --features simd-json
```

Enable only after parse latency profiles show need — see
[`docs/ops/latency_runtime.md`](../../../docs/ops/latency_runtime.md).

## Owner

Default review owner: `@s1korrrr` ([`CODEOWNERS`](../../../CODEOWNERS)).

## Limitations

- **Candles:** Spot + SWAP/Futures `candle*` channels → `MarketEvent::Candle` (opt-in via `Channel::Candles` / `candle_intervals`). See ADR 0001.
- Inverse OKX instruments (`ctType=inverse`) map to `PerpetualInverse` /
  `FutureInverse` on the same VenueIds as linear (no separate VenueId).
- **Checksum:** field parsed but not used for integrity post-2026 deprecation
  (non-zero -> fail-closed reconnect).
- `live_ignored.rs` stays `#[ignore]` — CI remains offline.

## Maturity

| Venue | Label | Claim |
|---|---|---|
| `okx-spot` (VenueId 4) | **alpha+** / beta-ready offline | Offline fixtures + corpus + this README/owner/limits + canary checklist |
| `okx-swap` / `okx-futures` (9 / 10) | **alpha** | Offline fixtures; close-out docs still thin |

**Not beta for any OKX segment.** Beta requires a scheduled live canary (§11.8).

- Matrix: [`docs/plan/maturity_matrix.md`](../../../docs/plan/maturity_matrix.md)
- Canary checklist: [`docs/ops/canary_checklist.md`](../../../docs/ops/canary_checklist.md)

## Develop

```bash
# offline (default): unit + fixture + record/replay tests
cargo test -p marketfeed-adapter-okx

# regenerate L2 book corpus after fixture edits
REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_spot_l2_book_corpus -- --ignored

# live network smoke, ignored by default — keep CI offline
cargo test -p marketfeed-adapter-okx --test live_ignored -- --ignored --nocapture
```
