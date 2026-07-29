# Market panel (view API + SPA)

Optional loopback JSON APIs and a dense trading-style SPA for live books/tape.
Requires building the daemon with feature flags (default binary stays lean).

## Features

| Cargo feature | What it enables |
|---|---|
| `ui-api` | `GET /v1/status`, `/v1/books`, `/v1/tape`, `/v1/instruments` |
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
```

`telemetry.ui_bind` (and `bind`) must be loopback — same security model as ADR-0009.

When `ui_bind` is unset but `ui-api`/`ui` is compiled in, `/v1/*` (and SPA) are
served on `telemetry.bind` alongside `/live` `/ready` `/metrics`.

## Build & run

```bash
# Daemon with embedded UI (serves checked-in ui/dist; Node not required)
cargo run -p marketfeed-daemon --features ui -- run --config crates/daemon/config.offline.toml
# open http://127.0.0.1:19109/

# API-only (no SPA embed)
cargo run -p marketfeed-daemon --features ui-api -- run --config crates/daemon/config.offline.toml
```

Optional SPA rebuild from Svelte sources (Node 20+):

```bash
cd ui && npm install && npm run build && cd ..
cargo run -p marketfeed-daemon --features ui -- run --config crates/daemon/config.offline.toml
```

Dev SPA against a running API:

```bash
# terminal 1 — daemon with ui-api
cargo run -p marketfeed-daemon --features ui-api -- run --config crates/daemon/config.offline.toml

# terminal 2 — Vite proxies /v1 to ui_bind / bind
cd ui && npm run dev
```

## Endpoints

- `GET /v1/status` — process + per-venue health/metrics snapshot
- `GET /v1/instruments` — configured venue/symbol map
- `GET /v1/books?venue=<id>&symbol=<sym>&depth=25` (or `instrument=<u32>`)
- `GET /v1/tape?venue=<id>&symbol=<sym>&limit=50&kind=trade|quote|all`
  - Trades and quotes use **separate rings** per instrument so quote floods cannot evict trades.
  - `kind=trade` / `kind=quote` filter; omit or `all` merges newest-first.

### Books correctness

Venue tasks do **not** share a process-wide [`EngineControl`](../../crates/engine/src/control.rs)
handle today (hot-reload is still a ponytail — see `crates/daemon/src/reload.rs`).
The view plane therefore rebuilds books from the same normalized
`BookSnapshot` / `BookDelta` events that feed sinks, with an HTTP surface that
mirrors `EngineControl::book_snapshot(venue, instrument, depth)`.

Offline synthetic (`config.offline.toml`) continuously injects book snaps,
trades, and quotes so `/v1/books` and `/v1/tape` stay populated without exchange
I/O.

Per-instrument trade/quote rings are each bounded (`ui_tape_capacity`, drop-oldest)
and rate-capped (`ui_tape_max_per_sec`, drop-newest). Poll books at ≤10–20 Hz from the SPA.

## Live smoke

With a live daemon on the view bind:

```bash
BASE=http://127.0.0.1:19109 ./scripts/live_ui_smoke.sh
```

See also `docs/ops/live_ui_audit.md` and `docs/ops/live_ui_coverage.md`.

## Prometheus / Grafana

Ops dashboards remain on `/metrics` — see [`grafana/README.md`](./grafana/README.md).
