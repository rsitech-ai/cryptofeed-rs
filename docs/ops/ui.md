# Market panel (view API + SPA)

Optional loopback JSON APIs and a dense trading-style SPA for live books/tape.
Requires building the daemon with feature flags (default binary stays lean).

## Features

| Cargo feature | What it enables |
|---|---|
| `ui-api` | `GET /v1/status`, `/v1/books`, `/v1/tape`, `/v1/instruments`, `/v1/stream`, `/v1/replay/*`, `POST /v1/alerts/test` |
| `ui` | `ui-api` + static SPA (`/`, `/assets/*`) |

## Config

```toml
[telemetry]
bind = "127.0.0.1:9108"
# Optional separate loopback bind for view API / SPA (recommended).
ui_bind = "127.0.0.1:9109"
ui_tape_capacity = 256
ui_tape_max_per_sec = 50
# Optional filesystem static dir (overrides embedded assets when set).
# ui_static_dir = "./ui/dist"
# Optional Grafana deep-link base for the SPA status bar.
# grafana_base_url = "http://127.0.0.1:3000"
# Optional webhook for POST /v1/alerts/test forwarding (http:// loopback or internal only).
# alert_webhook_url = "http://127.0.0.1:9200/alerts"
# Optional replay scrubber root (defaults to .local/live-ui/raw).
# replay_dir = "./.local/live-ui/raw"
```

`telemetry.ui_bind` (and `bind`) must be loopback — same security model as ADR-0009.

When `ui_bind` is unset but `ui-api`/`ui` is compiled in, `/v1/*` (and SPA) are
served on `telemetry.bind` alongside `/live` `/ready` `/metrics`.

## How to run (live market panel)

One-shot helper for macOS/zsh (builds `--features ui` if needed, creates a
local config under `.local/live-ui/` from the checked-in example):

```bash
./scripts/run_live_ui.sh
# open http://127.0.0.1:19109/?asset=BTC&mode=lines&dock=1
# Ctrl+C to stop
```

Useful flags:

```bash
./scripts/run_live_ui.sh --help
./scripts/run_live_ui.sh --rebuild          # always cargo build --features ui
./scripts/run_live_ui.sh --force            # kill stale listeners on 19108/19109
./scripts/run_live_ui.sh --background       # detach; pid/log under .local/live-ui/
```

Config preference: `.local/live-ui/config.live.ui.toml` if present; otherwise
copied from [`crates/daemon/config.live.ui.example.toml`](../../crates/daemon/config.live.ui.example.toml)
(bind `19108`, `ui_bind` `19109`, `ui_static_dir = "./ui/dist"`). No secrets —
public venues only. Edit the local copy freely (`.local/` is gitignored).

## Build & run

```bash
# Daemon with embedded UI (serves checked-in ui/dist; Node not required)
cargo run --locked -p marketfeed-daemon --features ui -- run --config crates/daemon/config.offline.toml
# open http://127.0.0.1:19109/

# Live multi-venue panel (same as run_live_ui.sh without the helper)
cargo build --locked -p marketfeed-daemon --features ui
./target/debug/marketfeed run --config crates/daemon/config.live.ui.example.toml
# or: .local/live-ui/config.live.ui.toml after the helper has created it

# API-only (no SPA embed)
cargo run --locked -p marketfeed-daemon --features ui-api -- run --config crates/daemon/config.offline.toml
```

Optional SPA rebuild from Svelte sources (Node 20+):

```bash
cd ui && npm ci && npm run build && cd ..
cargo run --locked -p marketfeed-daemon --features ui -- run --config crates/daemon/config.offline.toml
```

Dev SPA against a running API:

```bash
# terminal 1 — daemon with ui-api
cargo run --locked -p marketfeed-daemon --features ui-api -- run --config crates/daemon/config.offline.toml

# terminal 2 — Vite proxies /v1 to ui_bind / bind
cd ui && npm run dev
```

## Endpoints

### Poll (unchanged contract)

- `GET /v1/status` — process + per-venue health/metrics snapshot
  - Per venue: `feed_lag_ms`, `last_event_ts_ns`, `last_trade_ts_ns`, `last_quote_ts_ns`, `tape_trades`, `tape_quotes`, `tape_*_dropped`, plus existing counters (`valid_books`, `reconnects`, `book_invalidations`, …).
  - Top-level: optional `grafana_base_url`; `alert_webhook_configured` (bool — URL never echoed).
- `GET /v1/instruments` — configured venue/symbol map
- `GET /v1/books?venue=<id>&symbol=<sym>&depth=25` (or `instrument=<u32>`)
- `GET /v1/tape?venue=<id>&symbol=<sym>&limit=50&kind=trade|quote|all`
  - Trades and quotes use **separate rings** per instrument so quote floods cannot evict trades.
  - `kind=trade` / `kind=quote` filter; omit or `all` merges newest-first.
  - Trade entries include optional `notional` (`price * quantity` as exact decimal string).

### SSE stream

- `GET /v1/stream?asset=BTC` or `?venue=<id>&symbol=<sym>`
- `Content-Type: text/event-stream` over HTTP/1.1 chunked encoding; `: heartbeat` comments every 15s.
- Push rate capped at **10 Hz**; emits JSON only when the payload changes (status + optional focus book/tape).
- Event shape:

```json
{
  "ts_ns": 123,
  "status": { "...": "same as /v1/status" },
  "focus": {
    "venue": "synthetic-demo",
    "instrument": 1,
    "symbol": "BTC-USD",
    "book": { "...": "top-of-book snapshot" },
    "tape": [ "... TapeEntry ..." ]
  }
}
```

Poll endpoints remain available for clients that do not use SSE.

### Alerts (optional)

- `POST /v1/alerts/test` — body `{ "kind": "discrepancy"|"lag", "bps": number?, "message": string? }`
- Always returns `{ "ok": true, "forwarded": bool, "forward_error"?: string }`.
- When `telemetry.alert_webhook_url` is set, the daemon forwards the payload via HTTP POST (best-effort).

### Replay scrubber (read-only)

- `GET /v1/replay/files` — lists `.jsonl` / `.mfne` under `telemetry.replay_dir` (default `.local/live-ui/raw`).
- `GET /v1/replay?file=<name>&offset=0&limit=100` — tape-like trade/quote entries parsed from MFNE-JSON1 lines (honest 404 JSON if missing/unreadable).
- Does not require `[recording.raw]` to be enabled; reads existing files only.

## Out of scope (this panel)

- Funding / open-interest / liquidation UI panels — not implemented (no fake numbers).
- Telegram notifications — not implemented (in-app + optional webhook only).
- Private fills overlay — not implemented until a private path exists.
- True MBO / Bookmap footprint — VAP is trade-aggregated only; labels say so.

## SPA pro features (embedded UI)

- USD-normalized volume + trades/min per venue (not raw multi-venue qty).
- Venue discrepancy workspace (Δbps sparkline, configurable alert threshold).
- Watchlists / layouts + shareable URL state (`?asset=BTC&mode=lines&tf=1m&…`).
- SSE `/v1/stream` with poll fallback.
  - Daemon emits unnamed `data:` frames `{ ts_ns, status, focus?: { venue, symbol, book, tape } }`.
  - SPA parses that shape (and typed events); reconnects with `venue`+`symbol` for the selected focus.
  - Focus poll remains authoritative; SSE freshness skips redundant focus **book and tape** polls.
  - Combined SSE focus frames call `onFocus` once (no double `onBook`/`onTape` apply).
  - UI paint gates: book ~14 Hz, tape ~12 Hz, lines ~10 Hz, heatmap ~9 Hz; EMA-stable heatmap y-scale; offscreen blit.
  - `visibilitychange` forces a live refresh when the tab becomes visible again.
- Depth chart, time & sales filters, and synchronized multi-pane time ranges.
- Session presets (1m/5m/1h), keyboard shortcuts (`/` search, `1`–`5` TF), density toggle.
- **Series history retention** (`historySecs`, default **3600**): shared App-owned buffers
  keep ~1h of Lines / Candles / BPS / focus tape / OF depth columns across mode switches
  (Lines ↔ Candles ↔ Order Flow). Session presets and `ofView` only **clip the view**;
  they do not wipe the underlying ring. URL: `?historySecs=3600` (localStorage key
  `historySecs`). Older OF heat columns are downsampled (recent ~5 Hz, mid 1 Hz, older
  0.2 Hz) so 12+ venues × 1h stays memory-conscious — not full tick-density heat for
  the entire hour.
- Health strip + data-quality badges; Grafana link-out; replay scrubber (best-effort).
- **Order Flow chart mode** (`mode=orderflow`, tab next to Lines/Candles): desk-style
  **L2+tape reconstruction** (honest — **not MBO**):
  - Multi-layer canvas: liquidity heatmap (sampled `/v1/books`), buy/sell volume bubbles,
    mid/BBO lines, trade-aggregated **VAP** sidebar, **CVD** strip, volume bars.
  - Professional **DOM ladder** beside heat: bid size | price | ask size with size bars,
    cumulative USD, per-level imbalance %; keyed rows + paint gates (no flicker).
  - Controls (URL + localStorage): `ofTick`, `ofHeat`, `ofBubble`, `ofLayers`, plus session window.
  - Hover tooltips with resting size + print delta; **no L2** badge when book missing.
  - Paint: offscreen blit, EMA y-scale, ~9 Hz heat gate (see flicker paint-gate branch).
- **Flow & Pulse** bottom dock (single fixed panel — no Flow/Both/Pulse tabs):
  focus-instrument CVD / pressure / (tape VAP when not in Order Flow) + multi-venue
  pulse heat chips + large-print flags in one Binance/Bookmap-style bar.
  **Top Movers** (Binance Spot FAQ statuses) live in Focus Flow: Rise/Fall
  Small/Mid/High (5m & 2h), New 24hr High/Low, Pullback/Rally, Price±High Vol,
  Large Buy/Sell — formulas match
  [Understanding Top Movers Statuses](https://www.binance.com/en/support/faq/detail/18c97e8ab67a4e1b824edd590cae9f16).
  Honest coverage: SPA retains ~`historySecs` (≤2h), so 7d/30d extremes and the
  full 24×15m / 120h Large-order baselines are marked `partial` until history
  exists; we do not invent missing bars. Math: `ui/src/lib/topMovers.js`.
  **Dedup rules:** header owns last/vol/trades/cross-Δ; Order Book owns cumulative
  depth (hidden in Order Flow — DOM owns resting size); Order Flow chart owns VAP
  sidebar + CVD strip + DOM — dock then shows a compact CVD/Imb summary only
  (`showTapeProfile=false`). Pulse metrics are sort keys (score, trades/m, USD/m,
  median spread, avg imb) — no duplicate Cross Δ. Keyboard: `F`/`B`/`P` open the
  dock, `Esc` hides. URL: legacy `tab=flow|pulse|both|orderflow` maps to the open
  single view; `tab=hidden` or `dock=0` hides; `largeUsd=…`, `pulseAlert=72`,
  `dock=0|1`. Venue chips focus book/tape; pulse metrics sort chips; plots /
  sparklines / VAP support hover tooltips. Pulse alerts use the in-app toast and
  optional SPA webhook; daemon alert kinds remain discrepancy/lag only.

## Venue feed notes

- Binance USD-M / Coin-M subscribe `@trade` (fstream/dstream `@aggTrade` is silent).
- Bybit public subscribe args are chunked ≤10 (spot rejects larger batches).
- Live sessions REST-discover instrument scales so L2 can run without stub scale=2 thrash.

## Books correctness

Venue tasks do **not** share a process-wide [`EngineControl`](../../crates/engine/src/control.rs)
handle today (hot-reload is still a ponytail — see `crates/daemon/src/reload.rs`).
The view plane therefore rebuilds books from the same normalized
`BookSnapshot` / `BookDelta` events that feed sinks, with an HTTP surface that
mirrors `EngineControl::book_snapshot(venue, instrument, depth)`.

Offline synthetic (`config.offline.toml`) continuously injects book snaps,
trades, and quotes so `/v1/books` and `/v1/tape` stay populated without exchange
I/O.

Per-instrument trade/quote rings are each bounded (`ui_tape_capacity`, drop-oldest)
and rate-capped (`ui_tape_max_per_sec`, drop-newest). Poll books at ≤10–20 Hz from the SPA; prefer `/v1/stream` for live updates.

## Live smoke

With a live daemon on the view bind:

```bash
BASE=http://127.0.0.1:19109 ./scripts/live_ui_smoke.sh
```

SPA unit tests (CVD / VAP / imbalance / pulse / Top Movers math + 1h soak caps):

```bash
cd ui && npm test
```

### 24/7 SPA performance

The SPA keeps ~`historySecs` (default 3600) of series for mode switches, but
**display** paths are hard-capped so a multi-hour session stays responsive:

- Chart paints ≤ ~900 pts/venue (tiered downsample); LWC uses tip `update()` with throttled `setData`
- Market Trades DOM ≤ 160 rows; OF tape ≤ 4k; depth heat ≤ ~1800 columns
- Candles ingest only new trades (not the full ring); alerts capped
- Hidden tabs pause focus/multi polls and slow chart/OF paints

Probe live buffer sizes in DevTools: `globalThis.__mfHistoryDebug`.
Audit notes: `.local/live-ui/PERF_1H_AUDIT.md` (local).

See also `docs/ops/live_ui_audit.md` and `docs/ops/live_ui_coverage.md`.

## Prometheus / Grafana

Ops dashboards remain on `/metrics` — see [`grafana/README.md`](./grafana/README.md).
When `telemetry.grafana_base_url` is set, `/v1/status` exposes it for SPA deep links.
