# marketfeed-daemon

Optional production binary `marketfeed`: validate / run / replay / inspect-recording,
loopback `/live` `/ready` `/metrics`, optional `[[sinks]]`, JSON/text tracing.

The current `replay` command validates and scans a raw recording and reports
inbound-frame counts. Adapter-driven deterministic replay is provided by the
`marketfeed-replay` library and engine integration tests; the daemon command
does not yet execute the full adapter state machine.

Optional features:
- `ui-api` — loopback JSON view plane (`/v1/status`, `/v1/books`, `/v1/tape`)
- `ui` — `ui-api` + embedded SPA from `ui/dist` (see [`docs/ops/ui.md`](../../docs/ops/ui.md))

Ops dashboards: [`docs/ops/grafana/README.md`](../../docs/ops/grafana/README.md).

## Config

See [`config.example.toml`](./config.example.toml) and [`config.offline.toml`](./config.offline.toml).

```bash
cargo run -p marketfeed-daemon -- validate --config crates/daemon/config.example.toml
cargo run -p marketfeed-daemon -- run --config crates/daemon/config.offline.toml

# View API + SPA (offline synthetic)
cargo run -p marketfeed-daemon --features ui -- run --config crates/daemon/config.offline.toml
# then open http://127.0.0.1:19109/
```

### Sink isolation and readiness

Each `[[sinks]]` entry owns a bounded FIFO and a dedicated worker, so slow disk
or network I/O does not hold the process-wide venue fan-out lock. Give
operational sinks a stable, unique `id`. Set `required = true` when sink failure
must make `/ready` return `503`; otherwise failures remain isolated but visible
through the labeled `marketfeed_sink_*` metrics. Shutdown waits for queued and
in-flight sink work within the configured deadline plus the coordinator margin.
Configuration validation limits the daemon to 64 sinks and caps the aggregate
recording/mailbox/batch/system reservation at 1,048,576 eager queue slots.
Readiness also fails immediately during shutdown or recording disk pressure,
and a required L2 venue is not ready until every configured symbol has a
distinct valid book.

The daemon rejects standalone `type = "spill-wal"` configurations. The WAL
library remains available, but daemon use needs a real downstream sink plus an
explicit recovery/checkpoint consumer; accepting it as a terminal sink would
leave its in-memory prefix without a durable consumer.

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
