# Live market panel audit — 2026-07-29

This record covers the loopback view API and embedded SPA on the offline
synthetic profile. It is runtime evidence for the audited commit, not proof of
credentialed private feeds, every exchange, a long soak, or a hosted release.

## Current evidence

- `npm test` — 54/54 pass.
- `npm run build` — production Vite bundle built successfully.
- `BASE=http://127.0.0.1:19109 ./scripts/live_ui_smoke.sh` — 13 shell and
  4 Python assertions pass against a running daemon.
- `/live` and `/ready` return 200; book, tape, status, instruments, SSE, replay,
  and static asset routes return their documented shapes.
- Two real daemon stop/start cycles shut down cleanly (all tasks joined and sink
  workers drained) and restarted without warnings or errors.
- In-app browser checks at 768×900 and 1440×900 show no horizontal overflow.
- Lines, Candles, and Order Flow transitions; Pulse/Order Flow/Escape shortcuts;
  search focus; and the replay file control work in the running embedded bundle.
- A live daemon stop leaves the SPA mounted and shows `SSE reconnecting`; restart
  restores `connected` / `SSE` without a reload or browser-console error.
- Synthetic tape timestamps and chart axes use current Unix wall time; feed lag
  remains bounded instead of presenting 1970-era samples or monotonic-clock lag.

## Bugs found and fixed

### Disposed chart callbacks

Visible-range and crosshair handlers were anonymous and survived removal of the
secondary chart. Mode transitions then called a disposed Lightweight Charts
object once per update. Synchronization now uses named logical-range handlers,
returns explicit disposers, and unwires both directions before either chart is
removed. The invalid cross-chart price/series coupling was removed.

### Event time and newest-first tape semantics

The synthetic source used a monotonic counter as `receive_ts`, producing
unbounded lag and 1970 chart data. Continuous and seed events now use Unix
nanoseconds while internal scheduling remains monotonic. OHLC and last-price
builders ingest newest-first API pages chronologically and ignore late older
pages when deriving the current close.

### Book lifecycle and request races

`BookInvalidated` now removes only the matching venue/instrument view. Temporary
book-404 suppression is scoped by venue and symbol and clears on a successful
response. Slow focus responses are rejected after a focus generation changes.

### Streaming, replay, and HTTP boundaries

- SSE reconnect callbacks clear sticky disconnected UI state.
- SSE payload suppression ignores the volatile server timestamp.
- HTTP request reads are bounded, handle fragmented bodies, enforce
  `Content-Length`, and have an overall deadline.
- Replay list/read work runs off the async executor, streams bounded records,
  accepts normalized/MFNE envelopes, and exposes a keyboard-focusable file input.

### Alerts

The UI maps discrepancy and lag alerts to the daemon's accepted contract.
Pulse alerts remain in-app or use the configured direct browser webhook; the UI
does not claim daemon delivery for an unsupported alert kind, and delivery
failures are visible.

## Known boundaries

- The offline profile proves UI/view behavior with a deterministic synthetic
  venue, not exchange-specific market correctness.
- Funding, open interest, liquidation panels, Telegram, private fills, and true
  MBO are intentionally not implemented; the UI labels those boundaries.
- Window/session statistics begin when the SPA starts observing the selected
  market, not at an exchange session boundary.
- Trade and quote rings are independently bounded and rate capped; drops are
  reported rather than silently expanding memory.
