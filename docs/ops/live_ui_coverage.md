# Live UI market coverage

Configuration inventory for the broad multi-asset profile (top liquid majors
per venue). The 13/13 result below is historical run evidence; the 2026-07-29
embedded-UI audit used the deterministic offline synthetic profile and records
its evidence in [`live_ui_audit.md`](./live_ui_audit.md).

## Venues (13)

| Venue | Assets | Channels | Notes |
|---|---|---|---|
| binance-spot | BTC ETH SOL BNB | trades quote **l2** | XRP omitted (4dp vs stub scale=2) |
| binance-usdm | BTC ETH SOL XRP BNB | trades quote funding OI liq | |
| binance-coinm | BTC ETH SOL XRP BNB | trades mark funding | `*USD_PERP` |
| okx-spot | BTC ETH SOL XRP BNB | trades ticker | L2 dropped (stub-scale thrash) |
| okx-swap | BTC ETH SOL XRP BNB | trades ticker funding | L2 dropped |
| bybit-linear | BTC ETH SOL XRP BNB | trades quote | L2 dropped |
| bybit-spot | BTC ETH SOL XRP BNB | trades quote | |
| kraken-spot | BTC ETH SOL XRP BNB | trades quote | WS `BTC/USD` |
| deribit | BTC ETH | trades ticker mark | coin perps only |
| bitstamp | BTC ETH | trades quote | Always uses order_book for BBO; stub scale limits to BTC/ETH |
| gemini | BTC ETH SOL XRP BNB | trades quote | L2 dropped |
| coinbase-spot | BTC ETH SOL XRP BNB | trades quote | public T/Q |
| bitfinex | BTC ETH SOL XRP | trades quote | no `tBNBUSD`; L2 dropped |

**57 configured markets** across 5 logical assets (BTC/ETH/SOL/XRP/BNB).
Historical broad-profile run: **13/13 venues** reached live state with
reconnects=0 after warm-up. Re-run the live checks before making a current
exchange-readiness claim.

## Why L2 was narrowed

Daemon stub catalogs hardcode `price_scale=2` / `quantity_scale=8`. Subscribing
L2 on symbols whose wire scales differ (XRP 4dp, many alts) emits
`BookInvalidated` and reconnect-storms the **whole** venue session — taking
BTC/ETH down with them. Trades/quotes stay healthy without L2.

Depth books in the SPA: use **binance-spot** focus (BTC/ETH/SOL/BNB).

## Intentional omissions

- **coinbase-intl** — authenticated MD only
- **okx-futures** — dated contracts need rolling symbols
- **coinbase-adv / bitfinex-deriv / bybit-inverse / kraken-futures** — out of this live-ui set
- **Full exchange catalogs** — not subscribed (WS pressure); majors only
- **binance-spot XRP** — L2 scale mismatch with stub catalog

## SPA

Asset pills come from `GET /v1/instruments` via `listAssets()` — **hard-refresh**
(Cmd/Ctrl-Shift-R) after daemon restart.
