# Live market panel audit — 2026-07-29

## Smoke
- `scripts/live_ui_smoke.sh` → **RESULT: PASS** (see `smoke-latest.txt`)
- Browser probe → **BROWSER_OK** (`browser-audit.json`, `browser-audit.png`)
- View plane unit tests → 3/3 pass including `quote_flood_does_not_evict_trades`

## Bugs found & fixed

### Critical: quotes starved the trade tape
- **Wrong:** trades and quotes shared one ring + shared rate limit. Busy quote venues (binance-spot/usdm) returned almost only quotes; Market Trades often empty/stale.
- **Fix:** dual rings per instrument (`InstrumentTape` trades/quotes). `/v1/tape?kind=trade|quote|all`.
- SPA focus tape now polls `kind=trade`.

### Missing volume / trade counts
- Session vol existed; no trade count; no window-tied chips; no chart volume subplot; no multi-venue vol/#.
- **Fix:** CandleBuilder tracks trades; window stats by timeframe; HeaderBar Vol/Trades chips (click window↔session); legend per-venue vol/#; Multi vol/#; histogram volume subplot (toggleable).

### UX not interactive enough
- Legend not clickable; no settings persistence; depth/tape/poll fixed.
- **Fix:** legend click toggles series, Shift+click focuses venue; trade row click marks chart time; book depth 8/16/24; settings gear for depth/tape/poll; localStorage persistence.

### Display/time
- Times now labeled **UTC** (`…Z`).

## Verified correct
- Book bids descending / asks ascending / BBO ask≥bid (binance-spot, okx-spot, bybit-linear)
- Trade fields: price, quantity, aggressor, trade_id, timestamps
- Tape newest-first by `receive_ts_ns`
- SPA panel vol/# = sum of `/v1/tape?kind=trade` snapshot (parity check in smoke)
- Mid/spread/bps from book BBO
- Multi-venue % baseline = first sample per venue; cross-venue Δ from visible series

## Residual risks
- **binance-usdm:** live with quotes but **0 trades** in ring — adapter/channel ingest issue, not SPA (config has `trades`).
- **coinbase-spot / some venues:** no L2 book (404) — expected for quote/trade-only channels.
- **Multi vol/#:** sums **native qty units** (Deribit USD notionals dominate) — not USD-normalized.
- **Window/session vol:** SPA accumulates from first poll after focus/asset switch, not exchange session open.
- Rate limit still applies **per ring** (`ui_tape_max_per_sec`); extreme trade bursts can drop trades independently of quotes.
