# Hyperliquid public market data

Venue 21 is spot; venue 22 covers native and builder-deployed linear perpetuals.
`HyperliquidFactory::spot()` discovers `spotMeta` and `::perps()` discovers
`spotMeta` plus `allPerpMetas`. Spot token indices and collateral references are
resolved from metadata, never from an inferred ticker split. Spot wire names are
`@index`, with the native `PURR/USDC` exception; builder perps keep `dex:SYMBOL`.
Original HTTP responses are retained when shared capture is enabled.

Supports public trades, BBO, native L2 snapshots up to 20 levels, fixed-interval
candles, mark and 24h context; perps additionally expose funding, oracle/index
price and open interest. No private accounts or execution. L2 is a snapshot
source, without a claim of exchange delta sequence reconstruction. Liquidations
and unsupported candle intervals fail explicitly. Decimal strings and numeric
JSON tokens parse directly into exact i128+scale, with no float conversion.

Trade identity is `(coin, time, tid)` as required by the native protocol. All
rows of a frame validate before emission; an invalid late row rejects the whole
frame. Native hashes, order counts, candle end/count and extra source fields
remain in the original payload and shared native protocol types. Candle updates
are not labelled confirmed merely because their scheduled end has passed.

## Collector and shared consumer

Start from `crates/daemon/config.hyperliquid.example.toml`, using a new capture
directory each process lifetime and an explicit byte budget:

```sh
cargo run -p marketfeed-daemon --bin marketfeed -- run --config crates/daemon/config.hyperliquid.example.toml
# In the hlscreen checkout (same saved capture, no second exchange connection):
cargo run -p hls-cli --bin hls -- live --shared-feed ../cryptofeed-rs/.local/hyperliquid/capture-001 --duration-secs 60
```

MFSF1 stores metadata, original bytes, exact canonical batches, accepted-frame
markers, lifecycle and timers. The committed prefix is synced and checksummed;
readers tail it with capture-bound sequence/offset cursors. A clean daemon exit
seals the stream. A failed writer becomes unusable, and a new writer cannot
reuse an existing capture directory. Hlscreen rebuilds its feature windows from
committed accepted frames; only its feature/display projection uses floats.

Every requested native subscription must be acknowledged. Equivalent native L2
acknowledgement defaults are accepted; changed precision is rejected. Plans split
at 200 native subscriptions per session to respect engine queue capacity. Daemon
configuration checks the combined 1000 spot/perp subscription budget. Rolling
connection and outgoing-message limits are shared within this collector process;
other applications behind the same IP need their own coordination.

## Evidence and remaining gates

Contract tests cover exactness, atomic batches, identities, metadata, request
budgets, acknowledgements, heartbeat, snapshots and disconnect invalidation.
`python 3 scripts/audit_shared_feed.py CAPTURE_DIRECTORY` independently compares
original trade tokens with canonical values. The implementation ledger records
bounded live pilots and failed gates. This is development/alpha evidence:
reconnects do not prove missing trades were recovered. Strict dataset admission,
transactional Parquet publication, crash/restore and sustained canary gates must
pass separately before research or operational qualification.

Sources: [subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions),
[metadata](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals),
[limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits).
