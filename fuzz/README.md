# Fuzz targets (cargo-fuzz)

Not a workspace member. Requires nightly + `cargo-fuzz`.

```bash
cargo install cargo-fuzz
cargo +nightly fuzz run fixed_parse
cargo +nightly fuzz run venue_decode
cargo +nightly fuzz run candle_decode
cargo +nightly fuzz run coinm_decode
cargo +nightly fuzz run private_account
cargo +nightly fuzz run recording_reader
cargo +nightly fuzz run book_transition
```

| Target | Surface |
|---|---|
| `fixed_parse` | `Fixed::parse_decimal` |
| `venue_decode` | Spot-family + Coin-M `decode_*_text` |
| `candle_decode` | Same public decoders (kline/ohlc/candle ride them) |
| `coinm_decode` | Binance Coin-M depth / kline / aggTrade |
| `private_account` | Binance/OKX/Bybit private fixture SMs (no live keys) |
| `recording_reader` | MFR1 `RawSegmentReader` |
| `book_transition` | L2 `OrderBook` snapshot + delta transitions |

CI keeps green via lightweight no-panic smokes in `marketfeed-model`,
Binance/OKX adapter tests, `marketfeed-private`, `marketfeed-recording`, and
`marketfeed-book`. See `docs/plan/chaos_supply_chain.md`.

**Not in scope (C9 lite):** Windows / aarch64 CI matrix (Actions billing).
