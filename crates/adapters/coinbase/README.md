# marketfeed-adapter-coinbase

| VenueId | Code | Protocol | Channels |
|--------:|------|----------|----------|
| **16** | `coinbase-spot` | Exchange Classic | T/Q/L2 + REST candles |
| **18** | `coinbase-adv` | Advanced Trade | T/Q/L2 + REST candles |
| **19** | `coinbase-intl` | INTX auth MD | T/Q/L2 (HMAC `CBINTLMD` subscribe) |

VenueId **19** credentials: env only (`COINBASE_INTL_API_KEY` / `_SECRET` / `_PASSPHRASE`). Alpha only; no order placement.

## Coinbase Exchange (`coinbase-spot`)

The Exchange `level2` channel requires authentication. Planning and catalog
discovery remain credential-free; an L2 live session loads these variables only
when the session is created:

- `COINBASE_EXCHANGE_API_KEY`
- `COINBASE_EXCHANGE_API_SECRET` — the base64-encoded Exchange API secret
- `COINBASE_EXCHANGE_API_PASSPHRASE`

The adapter signs `timestamp + "GET" + "/users/self/verify"` with HMAC-SHA256
and includes the base64 signature in every fresh subscribe frame. Credentials
must not be placed in daemon TOML. Non-L2 `matches` / `ticker` sessions remain
anonymous.

```toml
[[venues]]
id = "coinbase-spot"
adapter = "coinbase"
segment = "exchange"
symbols = ["BTC-USD"]
channels = ["trades", "quote", "l2"]
```

Authenticated L2 has offline signing, subscription, snapshot/delta, reconnect,
and replay proof. Credential-backed live proof remains optional and does not
promote the adapter beyond alpha.

## Coinbase International (`coinbase-intl`)

`wss://ws-md.international.coinbase.com` + `https://api.international.coinbase.com/api/v1`

- `SUBSCRIBE` with `MATCH`, `LEVEL1`, optional `LEVEL2` + HMAC auth fields
- `MATCH` → Trade; `LEVEL1` → Quote; `LEVEL2` → BookSnapshot/BookDelta
- `MarkLive` on first trade/quote (no L2) or after L2 snapshot
- REST `GET /instruments` public for catalog `--live`

```toml
[[venues]]
id = "coinbase-intl"
adapter = "coinbase"
segment = "intl"
symbols = ["BTC-PERP"]
channels = ["trades", "quote", "l2"]
```

## Tests

```bash
cargo test -p marketfeed-adapter-coinbase
cargo test -p marketfeed-adapter-coinbase --test live_ignored live_coinbase_spot_l2 -- --ignored --nocapture
```

**alpha** — offline fixtures + optional live smokes. Not beta.
