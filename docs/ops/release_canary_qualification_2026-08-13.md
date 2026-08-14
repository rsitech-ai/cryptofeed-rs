# Release canary qualification — 2026-08-13

## Decision

**HOLD. Do not start the 24-hour read-only beta gate.** Commit
`a22c0d3e6dfa7338ddb757fb2a5d52fae644a3db` completed an uninterrupted two-hour
public-data canary with UI, but the official analyzer returned HOLD.

The two-hour run is runtime evidence, not a waiver. It does not promote
maturity, does not replace the 2026-08-11 one-hour GO, and does not authorize
unattended beta. Audio, trading, order placement, credentials, and private
exchange APIs remain outside the product scope.

## Exact release evidence

Evidence is gitignored under `.local/evidence/release-canary/runs/20260813T090257Z/`.
UI screenshots and CDP probes are under `.local/evidence/release-canary/ui-soak-20260813/`.

| Gate | 2026-08-11 one-hour `20260811T190518Z` | 2026-08-13 two-hour `20260813T090257Z` | Limit |
|---|---:|---:|---:|
| Verdict | GO | **HOLD** | no HOLD reasons |
| Window (UTC) | one hour | `2026-08-13T09:02:57Z`–`2026-08-13T11:03:03Z` (7200.0 s, 482 samples) | ~2 hours |
| Logical venues live | 13/13 minimum | **12/13 minimum** (end state 13/13) | 13/13 sampled |
| Qualified L2 books | retained | **Binance USD-M dropped below 5 books on 11 samples** | per-venue contract |
| Reconnect delta | 3 total; max 2/venue | **33 total; Binance USD-M 28** | at most 2 per venue |
| Queue occupancy | 0.20% peak | 2.9% peak | below 80% |
| API latency p95 | 1.330 ms | 1.457 ms | at most 500 ms |
| Process CPU p95 | 7.0% | 37.9% | at most 150% |
| Peak RSS | 172.06 MiB | **2468.7 MiB** | at most 1536 MiB |
| Linear RSS growth | 40.10 MiB/hour | **1156.4 MiB/hour** | at most 64 MiB/hour |
| Windowed p95 RSS growth | 36.73 MiB/hour | **1929.1 MiB/hour** | at most 64 MiB/hour |
| UI smoke | first attempt | first attempt, exit 0 | exit 0 |
| Shutdown | graceful exit 0 | graceful exit 0, no forced stop | no forced stop |

The exact release binary SHA-256 was
`1dd1d93d61e56d251be06e0c82472f50513ee14b18e19727759ea2c2908ef4fb`.
The config SHA-256 was
`b55e41fa49db8ec65a1574496d0e7abdee742553e1c9903ac816a52cd48b8a7d`
(`crates/daemon/config.live.ui.example.toml`). Git status at canary startup was
empty.

## How the two-hour run was launched

```bash
./scripts/release_canary.sh --self-check
MARKETFEED_LIVE_UI_CONFIG=crates/daemon/config.live.ui.example.toml \
  DURATION=2h ./scripts/release_canary.sh
```

The wrapper rebuilt `marketfeed-daemon --features ui` (release, locked) and
sampled isolated loopback binds `127.0.0.1:19208` (telemetry) and
`127.0.0.1:19209` (SPA / view API). The runner requires Python 3.11+
(`tomllib`); this host’s default `python3` is 3.9, so the canary was executed
with Anaconda Python 3.13 on `PATH`.

Live UI smoke (`./scripts/live_ui_smoke.sh`) ran once at qualification, first
attempt, exit 0 (bash 13 PASS, python audit 66 PASS). SPA unit tests ran against
the live tree at T+0: 148 passed, 0 failed (`node --test src/lib/*.test.js`;
`npm` was not on `PATH`).

## Hold reasons (official analyzer)

1. **RSS peak 2468.7 MiB** exceeded 1536 MiB. Trajectory after warmup was
   monotonic: ~159 MiB at 5 minutes, ~327 MiB at 30 minutes, ~543 MiB at 1 hour,
   ~1386 MiB at 90 minutes, **1542 MiB at 1h 32m** (first threshold breach),
   ~2450 MiB at 2 hours. Unlike the 2026-08-11 hour, this run showed no
   allocator reclaim that brought the envelope back under the leak bars.
2. **RSS growth** failed both leak measures: linear slope 1156.4 MiB/hour and
   windowed p95 envelope 1929.1 MiB/hour, each vs 64 MiB/hour. A leak HOLD
   requires both; both tripped.
3. **Binance USD-M reconnect allowance**: 28 reconnects vs max 2 per venue.
   Structured logs were cause-aware: `disconnect_reason=TransportError`,
   `Connection reset without closing handshake`, `backoff_ms` from 86 to 3196.
   Gemini stayed at 2 (at the cap). Binance Spot, Binance Coin-M, and Kraken
   Spot added 1 each.
4. **Binance USD-M liveness / books**: 8 samples with `live=false`, 11 samples
   with `valid_books < 5` (including 0-book gaps during reconnect). The session
   recovered: final sample was live with 5/5 books, 0 event drops, 0 book
   invalidations. Recovery does not satisfy the continuous-live / retain-books
   gates when more than two bad samples occur.

API p95 (1.457 ms), CPU p95 (37.9%), queue peak (2.9%), `/live` `/ready` `/`
`/v1/status` `/metrics` HTTP 200 on **all 482 samples**, and zero daemon ERROR
logs all stayed inside limits.

## External reconnect evidence

All 33 WARN lines were `session disconnected; reconnect scheduled`. No ERROR
lines. No `EventsDropped` increment on any configured venue (`events_dropped`
delta 0). No `book_invalidations` increment.

Binance USD-M (log `venue=3`) produced a reconnect storm through the first ~90
minutes, then quieter gaps (last USD-M disconnect 2026-08-13T10:31:38Z). Books
returned to 5/5 after each sampled outage; BBO remained ordered when the book
was present. This is recovered transport failure, but it is far outside the
per-venue allowance that the 2026-08-11 hour met (2 USD-M reconnects).

Gemini recovered from a transient offline sample (2 reconnects, at cap). Kraken
Spot showed high `feed_lag_ms` at times while remaining live.

## Memory

The two-hour window did **not** reproduce the 2026-08-11 reclaim story. RSS
kept climbing after the 300 s warmup with no late drop back toward the 1-hour
peak of 172 MiB. Depth-history and analytics buffers can explain a bounded
step-up; they do not explain a ~2.4 GiB peak and ~1.2–1.9 GiB/hour envelope.
Treat this as a leak / residency HOLD, not as macOS page-fault noise: both
growth measures failed, and peak RSS itself failed.

Tape ring drops (`tape_trades_dropped` / `tape_quotes_dropped`) are expected
under the configured `ui_tape_max_per_sec` cap and are not canary HOLD inputs.

## UI verification

Host-native browse MCP could not launch Google Chrome (not installed). The live
panel was driven with the already-cached Chrome for Testing binary over CDP.
That is a real browser against the canary SPA, not a mocked DOM.

| Checkpoint | When | Result |
|---|---|---|
| T+0 live browser | 2026-08-13T09:07Z–09:08Z | PASS feature matrix; console 0; SSE 200 `text/event-stream` |
| T+1h live browser | 2026-08-13T10:02Z–10:03Z (uptime ~3607 s) | PASS feature matrix; console 0; SSE active; 13/13 in the footer |
| T+2h live browser | intended ~T+6900 s | **Not captured** — the near-end browser pass started after the daemon had already stopped at 11:03:03Z |
| Continuous UI HTTP | all 482 canary samples | `ui_http=200` |

Feature checklist (T+0 and T+1h, same matrix):

| Feature | T+0 | T+1h |
|---|---|---|
| Market Profile VAH, VAL, POC, range, volume, TPO count, rotation factor | PASS (rotation 0 early session) | PASS (BTC rotation 4; volume/TPO grown) |
| Market Profile VOL / TPO modes | PASS (TPO click changed VAH/VAL/POC) | PASS |
| Order-flow heatmap, bubbles, layers | PASS | PASS |
| DOM columns and depth (including `depth=32`) | PASS | PASS |
| Structural levels API | PASS `/v1/analytics/levels` | PASS |
| Markets search / group / segment filters | PASS | PASS |
| Tape | PASS | PASS |
| Settings, density, session presets | PASS | PASS |
| Alerts (URL `alertBps` + smoke `POST /v1/alerts/test`) | PASS | PASS (smoke at start only) |
| Replay gating (API up, files empty, not in replay) | PASS | PASS |
| BTC / ETH switching | PASS (ETH profile populated) | PASS |
| Chart modes lines / candles / orderflow | PASS | PASS |
| Price modes percent / absolute | PASS | PASS |
| Responsive narrow layout (780 px) | PASS | PASS |
| Console warnings/errors | 0 / 0 | 0 / 0 |
| SSE connected | PASS | PASS |
| Audio / trading / private / order placement | excluded | excluded |

Network “failures” in CDP were almost entirely expected **404** on `/v1/books`
for non-L2 venues (same class as `live_ui_smoke.sh` WARN) plus `favicon.ico`.
No 5xx. L2 venues returned books; SSE probe and stream were 200.

## Shutdown

`forced_stop=false`, `daemon_exit_code=0`. Logs:

- `shutdown signal received`
- `coordinated shutdown initiated`
- 12 named `venue session stopped cleanly` (all configured venues except
  `binance-usdm`)
- `all daemon tasks joined cleanly`
- `all sink workers drained cleanly`
- `marketfeed daemon stopped cleanly`

Binance USD-M had no matching `venue session stopped cleanly` line. The process
still exited 0 without a kill. Record that as a shutdown observability gap on
the venue that had been reconnecting, not as a hang: wall-clock drain was
~210 ms from signal to `marketfeed daemon stopped cleanly`.

## Verification run on this tree

- `./scripts/release_canary.sh --self-check` — 17 python tests OK
- `DURATION=2h` release canary — 7200.0 s sampled, verdict HOLD
- `./scripts/live_ui_smoke.sh` (via the canary) — PASS, 1 attempt
- SPA `node --test src/lib/*.test.js` — 148 passed / 0 failed
- Live CDP UI matrix at T+0 and T+1h — PASS as tabulated
- Full workspace `cargo test` / clippy / deny were **not** re-run during the
  soak (would contend with RSS/CPU evidence). The 2026-08-11 merge already
  recorded those gates on the release-candidate line; this document is the
  two-hour runtime gate only.

## Boundary

Public read-only feeds and the loopback SPA only. No audio, trading, order
placement, private APIs, credentials, or external sinks. Laptop / operator
evidence; scheduled canary remains 0. This is not 24-hour, multi-day, or
unattended production proof.

## Readiness vs the one-hour GO

The truthful 2026-08-11 claim remains: **repo-ready, merged, one-hour
runtime-proven** for public read-only beta **on that hour**. This two-hour
extension **does not** raise readiness. It lowers confidence in stretching
that hour to a 24-hour gate until:

1. RSS stays under 1536 MiB with both growth measures ≤ 64 MiB/hour on a
   clean two-hour (or longer) rerun;
2. Binance USD-M reconnects stay within policy (or the source 1006 /
   connection-reset behavior is bounded and books never fail the retain
   rule for more than two samples);
3. A near-end live browser pass is captured while the daemon is still up.

Do not start the 24-hour public-data beta qualification on this HOLD.
