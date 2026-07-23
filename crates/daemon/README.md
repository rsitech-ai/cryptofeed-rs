# marketfeed-daemon

Optional production binary `marketfeed`: validate / run / replay / inspect-recording,
loopback `/live` `/ready` `/metrics`, optional `[[sinks]]`, JSON/text tracing.

## Config

See [`config.example.toml`](./config.example.toml) and [`config.offline.toml`](./config.offline.toml).

```bash
cargo run -p marketfeed-daemon -- validate --config crates/daemon/config.example.toml
cargo run -p marketfeed-daemon -- run --config crates/daemon/config.offline.toml
```

### Authenticated Coinbase Exchange L2

For a `coinbase` venue using `segment = "spot"` or `"exchange"` with an `l2`
channel, provide `COINBASE_EXCHANGE_API_KEY`,
`COINBASE_EXCHANGE_API_SECRET`, and
`COINBASE_EXCHANGE_API_PASSPHRASE` in the process environment. Coinbase
Exchange credential fields in TOML are rejected. Trades/quotes without L2 do
not load credentials.

### Private user-data (C6c)

Optional enable flags only — **never** put API keys or secrets in TOML (validation
rejects secret-bearing fields / aliases).

| TOML section | Env (required) | Wire |
|---|---|---|
| `[private.binance_spot]` | unavailable | rejected until authenticated WebSocket API subscription support is implemented |
| `[private.okx_spot]` | library only | daemon rejects until account sink/readiness/reconnect supervision exists |
| `[private.bybit_spot]` | library only | daemon rejects until account sink/readiness/reconnect supervision exists |

The prior daemon path authenticated successfully but null-drained account
events and did not include private sessions in readiness or reconnect them
after a remote close. That behavior is now rejected at config validation and
again at the programmatic spawn boundary. No private credential is loaded by
the daemon until those runtime contracts are implemented.

The `marketfeed-private` library remains available to callers that provide an
explicit `AccountEventSink`:

```bash
cargo test -p marketfeed-private --features live --test live_ignored -- --ignored --nocapture
```

## Explicitly out of scope

- Order entry / execution
- Recording private payloads by default
- Putting credentials in TOML or committing `.env`
