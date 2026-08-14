# UI feature audit — 2026-08-13

## Decisions (keep these separate)

| Gate | Verdict | Why |
|---|---|---|
| **Public read-only UI** | **GO** | Supported SPA surfaces are labeled, wired, visually filled, and live-verified. Page JavaScript errors are 0. After the quotes-only book skip, late console 404s are 0. Charts share a time axis and fit available history instead of cropping to a right-edge sliver. |
| **Runtime / 24h unattended** | **HOLD** | Unchanged from [`release_canary_qualification_2026-08-13.md`](release_canary_qualification_2026-08-13.md): two-hour soak RSS peak 2468 MiB and Binance USD-M 28 reconnects. This UI audit did not restart a soak and does not waive that HOLD. |

UI GO means the **product surface** is complete enough to show to a public read-only beta user against a healthy daemon. It does **not** mean unattended production, 24h, or soak GO.

Out of scope (not added, not claimed): audio, trading, private feeds, order placement, Telegram, true MBO.

## Counts

| | n |
|---|---:|
| Features audited (engine → API → UI) | 42 |
| Shown and passing in the live SPA | 38 |
| Honest empty / partial (still production-quality) | 3 |
| Missing from SPA on purpose (ops-only) | 1 |
| Fixed in this audit loop | 14 |
| SPA unit tests | 170 pass / 0 fail |

Honest empty/partial: replay directory has no files; Top Movers 24h/7d coverage is `partial` until retained history exists; order-flow heatmap/DOM is only as rich as configured L2 (this live stack enables L2 on Binance Spot).

Ops-only: Prometheus `/metrics` is linked via Grafana, not inlined in the SPA.

## Live inspect

- SPA: `http://127.0.0.1:5174/?asset=BTC&mode=lines&dock=1&session=2h`
- View API: `127.0.0.1:19109` (`/v1/*`)
- Telemetry: `127.0.0.1:19108` (`/live`, `/ready`, `/metrics`)

This is a **short** live stack (L2 on Binance Spot; other venues trades/quotes), not the two-hour canary. `/live` and `/ready` returned 200 with 13/13 venues live during the audit.

## How it was verified

- Inventory: daemon `/v1/*` routes, SPA components/settings/URL state, `docs/ops/ui.md`, and `ui/src/lib/*.test.js`.
- Automated: `node --test src/lib/*.test.js` → **165 pass, 0 fail**.
- Browser: host-native IDE browser and the Browse plugin could not hold a tab (no Chrome.app / daemon timeout). Drive used Chrome for Testing + the already-installed Playwright core from the Browse plugin cache (same harness as the soak UI checkpoints). Evidence files are gitignored under `.local/evidence/ui-feature-audit-20260813/` and are cited **by screenshot name only**.
- Screenshots: `01`–`12` first drive; `13`–`14` post-crash-fix reverify; `15`–`22` full surface drive; `23` derivatives fallback + clean console.

## What was broken, then fixed

1. Analytics panes crashed (`fmtUsd`, `tapeTipSec`, `lastAppliedRangeKey` undefined) — Pulse/Imb/CVD/Vol went blank.
2. Lines X-crop (empty left, series pinned to the right) — live `fitContent` when retained history is shorter than the session.
3. Order Flow strip invented a flat 2h left void when the main Lightweight Chart was absent.
4. Header “Funding · OI · Liq” looked like coming-soon while `/v1/derivatives` existed.
5. Spot focus hid live USD-M open interest — fallback to a live perp for the same asset.
6. Multi-venue book polls 404’d on quotes-only venues (Chrome console errors). Skip when `valid_books === 0`.
7. Settings, Test alert, and keyboard cheatsheet were not discoverable (`?` + Test alert).
8. Grafana URL from `/v1/status` was not persisted.
9. Market Profile / bubble mode missing from shareable URL.
10. Order book had no explicit “no L2” empty state.
11. Replay empty copy implied files existed.
12. Market Profile values were unreadable exact decimals; display is compact, exact string stays on hover.
13. Favicon / apple-touch-icon 404s in Vite.
14. `Esc` ignored while an input was focused (dock would not hide).

## Feature matrix

Status: **pass** = engine + API + labeled UI + live evidence. **partial** = wired and honest, limited by config or retained history. **n/a** = excluded from public read-only beta.

| Feature | Engine / API | UI location | Status | Evidence |
|---|---|---|---|---|
| Health `/live` `/ready` | yes | Status bar lifecycle | pass | 200; `connected \| SSE \| lifecycle running \| venues 13/13 live` |
| Prometheus `/metrics` | yes | Grafana button (not inlined) | pass | Grafana chip present when status supplies a base URL |
| `GET /v1/status` | yes | Status bar, venue health, Grafana, webhook flag | pass | 13/13 live; per-venue lag chips |
| `GET /v1/instruments` | yes | Markets list, asset tabs | pass | BTC/ETH/SOL/XRP/BNB tabs |
| SSE `GET /v1/stream` | yes | Transport chip + live panes | pass | SSE connected; no page errors |
| Books `GET /v1/books` | yes | Order book + depth chart; OF heat | pass | Binance Spot L2 populated; quotes-only venues no longer 404-polled |
| Tape `GET /v1/tape` | yes | Market Trades | pass | Live prints; min-$ filter applied |
| Market Profile `GET /v1/analytics/profile` | yes | Strip under chart; VOL/TPO | pass | VAH/VAL/POC/Range/Volume/TPO/Rotation; `23-audit-derivatives.png` |
| Bubbles `GET /v1/analytics/bubbles` | yes | Order Flow layers + URL `ofBubbleMode` | pass | Controls + URL round-trip tests |
| Structural levels `GET /v1/analytics/levels` | yes | Order Flow layers | pass | Layers popover |
| Derivatives `GET /v1/derivatives` | yes | Derivatives strip; header chip | pass | `exchange-reported · binance-usdm BTCUSDT`, OI `109899.425` |
| Depth history `GET /v1/depth/history` | yes | Order Flow heatmap | pass | Heat canvas in `18-audit-orderflow.png` |
| DOM `GET /v1/dom` | yes | DOM ladder beside heat | pass | DOM column visible in OF mode |
| Replay `GET /v1/replay*` | yes | Replay scrubber | partial | Honest empty: no files in replay dir |
| Alerts `POST /v1/alerts/test` | yes | Settings → Test alert | pass | Toast “Test alert” |
| Lines overlay | client | Chart · Lines | pass | `15-audit-lines.png`; `logicalFrom=0`; visible span ≈ retained history |
| Candles | client | Chart · Candles | pass | `17-audit-candles.png` |
| Order Flow heat (not MBO) | L2+tape | Chart · Order Flow | partial | Works on Binance Spot; other venues quotes-only in this stack |
| % / Price modes | client | Chart toolbar | pass | `16-audit-price.png` |
| Volume subplot | client | Vol toggle | pass | Reduced right-scale margin so % lines use more plot height |
| Live pin / follow-live | client | Live button | pass | Fits session or shorter history |
| Pulse / Imb / CVD / Buy-Sell | client | Under-chart strip | pass | Labeled; synced `from/to` with main |
| Δbps discrepancy | client | BPS pane + alert threshold | pass | Header Cross-Δ + pane |
| Session 1m / 5m / 1h / 2h | client | Header presets | pass | `session=2h` active |
| History retention (default 7200s) | client | Settings · History sec | pass | `__mfHistoryDebug.historySecs=7200`; this stack had ~7–10 min of live data, shown in full |
| Chart time sync | client | Main + strip + BPS | pass | Identical wall `from/to` across panes |
| Splitters (book / chart / markets / dock / panes) | client | Drag handles | pass | Book column `250px` → `327px` (`21-audit-resized.png`) |
| Flow & Pulse dock | client | Bottom dock; `F` / `Esc` | pass | Open/hide; Top Prints + venue heat |
| Top Movers | client | Dock | partial | Formulas wired; 24h/7d marked partial until history exists |
| Markets search `/` | client | Markets | pass | Filter + live/spot/perp |
| Watchlists | client | Markets | pass | Save prompt + URL `watchlist` |
| Tape filters | client | Market Trades | pass | Min $, side, aggregate |
| Asset switch | client | BTC ETH SOL XRP BNB | pass | ETH `20-audit-eth.png` |
| Density | client | Header | pass | compact URL `density=compact` |
| Keyboard `1`–`5`, `?`, `F`, `Esc` | client | Chart / dock / settings | pass | `?` opens settings; `Esc` hides dock even from inputs |
| Settings (depth, tape, polls, alert, webhook, history) | client | Gear / `?` | pass | `19-audit-settings.png` |
| URL share (`asset`, `mode`, `session`, `profile`, `dock`, …) | client | Location bar | pass | Unit tests + live href |
| Header bid/ask/spread/H/L/vol/trades/events | client | Header | pass | `15-audit-lines.png` |
| Venue legend + live dots | client | Lines legend | pass | 13 venue chips |
| Quotes-only book empty state | client | Order book overlay | pass | “No L2 depth — quotes and tape only” |
| Narrow viewport | client | Layout | pass | `22-audit-narrow.png` |
| Recording / MFR1 ingest | engine | Replay only | n/a | Not a live-panel control |
| Audio / trading / private / orders | — | — | n/a | Excluded |

## Visual bar

| Check | Result |
|---|---|
| Charts fill the plot (no right-edge crop) | **pass** — `logicalFrom=0`; visible window equals retained history (~9 min on this stack, not a fake 2h empty left) |
| Shared time axis | **pass** — main / Pulse / Imb / CVD / Vol `from/to` match |
| Resize works and persists in layout CSS vars | **pass** |
| History shown honestly | **pass** — 2h session selected; X shows the data that actually exists |
| Empty / loading / error / replay-gated | **pass** — replay hint, derivatives fallback/unavailable copy, no-L2 overlay |
| Console | **pass** after book-skip — `pageErrors=0`, late 404s `[]` (`audit-console.json`) |
| Dense layout usable | **pass** — still tight; labels remain readable |

Percent-mode venue lines still sit in a narrow band when venues agree (here ~0.03%). That is the data, not a crop bug.

## Remaining gaps (not 24h blockers for the UI itself)

- **Runtime HOLD** still blocks unattended / 24h beta (RSS leak + USD-M reconnects).
- This live config enables **L2 on Binance Spot only**. Order-flow heat/DOM on other venues needs those books in config, not more SPA chrome.
- **Replay** is gated correctly but empty until recordings exist.
- **Top Movers** long-horizon statuses stay `partial` until `historySecs` fills.
- Docs previously said derivatives UI was unimplemented; `docs/ops/ui.md` now matches the strip.

## 2026-08-14 ship re-verify

Re-ran the public read-only stack after the remaining crash/security/layout fixes. Verdicts unchanged: **UI GO**, **runtime HOLD**.

Additional fixes before commit:

15. Restored `hiddenPollTimer` in `PriceChart.svelte` (strict-mode `ReferenceError` on tab hide / unmount).
16. Grafana / webhook URLs accept only `http:`/`https:` (`safeHttpUrl`) so `javascript:` cannot reach `window.open` or `fetch`.
17. Narrow layout no longer keeps a leftover 6px splitter column after splitters are hidden.
18. Docs 24/7 section default `historySecs` 3600 → 7200.

Live stack (Vite + release `marketfeed`):

- SPA: `http://127.0.0.1:5174/?asset=BTC&mode=lines&dock=1&session=2h`
- View API `19109`, telemetry `19108`
- Startup logs: INFO only until venues were live; `/live` `/ready` 200; 13/13 live
- `BASE=http://127.0.0.1:19109 ./scripts/live_ui_smoke.sh` → **PASS** (bash 13, python 58)
- Chrome for Testing: `pageErrors=[]`, `consoleErrors=[]`, HTTP 4xx `[]`, `logicalFrom=0`, `historySecs=7200`, SSE connected
- SPA tests: **170 pass / 0 fail**
- Embedded `ui/dist` rebuilt (favicon + panel sources)

One Binance USD-M `TransportError` reconnect appeared after startup during the live smoke. That is the existing runtime HOLD, not a UI regression.

## Files

Chart/layout work: `ui/src/lib/layout.js`, `layout.test.js`, `session.test.js`, plus the modified chart/history/sync/settings files.

Also: `App.svelte`, `ChartAnalyticsStrip.svelte`, `PriceChart.svelte`, `HeaderBar.svelte`, `MarketProfileStats.svelte`, `OrderBook.svelte`, `ReplayScrubber.svelte`, `DerivativesStats.svelte`, `chartSync.js`, `contracts.js`, `derivatives.js`, `urlState.js`, `settings.js`, `alerts.js`, `vite.config.js`, `index.html`, `ui/public/favicon.svg`, `ui/dist/*`, `docs/ops/ui.md`, and matching tests.
