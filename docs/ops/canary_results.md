# Live canary results

**Status:** evidence log only — **does not** claim beta  
**Checklist:** [`canary_checklist.md`](./canary_checklist.md)  
**Laptop runner:** [`scripts/laptop_canary.sh`](../../scripts/laptop_canary.sh) (**not** scheduled `canary.yml`)  
**Maturity gate:** scheduled live canary green **≥7 consecutive** runs (§11.8) before any venue may be marked **beta**

This file archives operator / agent live canary runs. A single green run is necessary
but **not sufficient** for maturity promotion.

## Scoreboard (honest)

| Counter | Value | Notes |
|---|---|---|
| Laptop consecutive `live_ignored` archives (Binance Spot + OKX Spot) | **10/10** | runs 1–10; same-day / agent bursts, **not** calendar-spaced schedule |
| Public venues with laptop `live_ignored` (run 9) | **9** | Binance, OKX, Bybit, Kraken Spot, Kraken Futures, Deribit, Coinbase, Bitstamp, Gemini |
| Scheduled `canary.yml` live job greens | **0** | workflow still offline synthetic; **not** scheduled this PR (billing may block) |
| Checklist item 5 (intentional reconnect) | **PASS** (laptop) | `KillSwitchWebSocket` force `Closed` → reconnect → Live; Binance + OKX (runs 8–9) |
| Private live (`marketfeed-private`) | **SKIP** | no API keys / `.env` on operator host |
| Maturity action | **none** — remain `alpha+` | scheduled **= 0**; do **not** flip matrix to beta |

---

## 2026-07-22T15:14:57Z — run 10 (laptop via `scripts/laptop_canary.sh`, tip `233f676`) + alpha venues

| Field | Value |
|---|---|
| UTC window | `2026-07-22T15:14:57Z` → `2026-07-22T15:17:35Z` |
| Evidence | [`canary_evidence/runs/cycle_10/`](./canary_evidence/runs/cycle_10/) |
| Command | `INCLUDE_ALPHA=1 ./scripts/laptop_canary.sh` |
| Private live | **SKIP** (keys absent) |
| Scheduled canary | **0** (this script is **not** scheduled beta) |
| Maturity action | **none** — remain `alpha+` |

### Public `live_ignored` results

| Package | Test(s) | Result |
|---|---|---|
| `marketfeed-adapter-binance` | trade/quote + reconnect | **PASS** |
| `marketfeed-adapter-okx` | trade/quote + reconnect | **PASS** |
| `marketfeed-adapter-kraken` | futures trade/ticker (alpha) | **PASS** |
| `marketfeed-adapter-bitstamp` | spot trade/quote (alpha) | **PASS** |
| `marketfeed-adapter-gemini` | spot trade/quote (alpha) | **PASS** |
| `marketfeed-adapter-coinbase` | spot trade/quote (alpha) | **PASS** |

**Honesty:** laptop **10/10** ≠ scheduled §11.8 canary. No maturity promotion.

## 2026-07-22T12:19:21Z — run 9 (laptop, tip `c6db619`) + wave2 public live_ignored

| Field | Value |
|---|---|
| UTC window | `2026-07-22T12:19:21Z` → `2026-07-22T12:23:06Z` |
| Host | darwin laptop, network allowed |
| Binary / tip at run | `c6db619` (`origin/main` through #123) + this PR |
| Branch | `feat/andrzej_ops_evidence_w3_live` |
| Evidence | [`canary_evidence/runs/cycle_9/`](./canary_evidence/runs/cycle_9/) |
| Private live | **SKIP** (keys absent) |
| Scheduled canary | **0** (not wired / not claimed) |

### Public `live_ignored` results

| Package | Test(s) | Result | Duration | Log |
|---|---|---|---:|---|
| `marketfeed-adapter-binance` | `live_binance_spot_trade_or_quote` + `live_binance_spot_reconnect_probe` | **PASS** | 15.00s | [`cycle_9/binance_live_ignored.log`](./canary_evidence/runs/cycle_9/binance_live_ignored.log) |
| `marketfeed-adapter-okx` | `live_okx_spot_trade_or_quote` + `live_okx_spot_reconnect_probe` | **PASS** | 20.01s | [`cycle_9/okx_live_ignored.log`](./canary_evidence/runs/cycle_9/okx_live_ignored.log) |
| `marketfeed-adapter-bybit` | `live_bybit_linear_trade_or_quote` | **PASS** | 15.01s | [`cycle_9/bybit_live_ignored.log`](./canary_evidence/runs/cycle_9/bybit_live_ignored.log) |
| `marketfeed-adapter-kraken` | `live_kraken_spot_trade_or_quote` + KF futures (`trade_or_quote` at run tip; tip ships `trade_or_ticker`) | **PASS** | 20.01s | [`cycle_9/kraken_live_ignored.log`](./canary_evidence/runs/cycle_9/kraken_live_ignored.log) |
| `marketfeed-adapter-deribit` | `live_deribit_trade_or_ticker` | **PASS** | 20.00s | [`cycle_9/deribit_live_ignored.log`](./canary_evidence/runs/cycle_9/deribit_live_ignored.log) |
| `marketfeed-adapter-coinbase` | `live_coinbase_spot_trade_or_quote` | **PASS** | 20.01s | [`cycle_9/coinbase_live_ignored.log`](./canary_evidence/runs/cycle_9/coinbase_live_ignored.log) |
| `marketfeed-adapter-bitstamp` | `live_bitstamp_spot_trade_or_quote` | **PASS** | 20.01s | [`cycle_9/bitstamp_live_ignored.log`](./canary_evidence/runs/cycle_9/bitstamp_live_ignored.log) |
| `marketfeed-adapter-gemini` | `live_gemini_spot_trade_or_quote` | **PASS** | 20.01s | [`cycle_9/gemini_live_ignored.log`](./canary_evidence/runs/cycle_9/gemini_live_ignored.log) |

Commands:

```bash
cargo test -p marketfeed-adapter-binance --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-okx --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-bybit --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-kraken --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-deribit --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-coinbase --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-bitstamp --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-gemini --test live_ignored -- --ignored --nocapture
# private (skipped — no keys):
# cargo test -p marketfeed-private --features live --test live_ignored -- --ignored --nocapture
```

### Code shipped with this evidence

**New:** `live_ignored` smokes for Coinbase Spot, Bitstamp Spot, Gemini Spot, and Kraken Futures
(previously only Binance/OKX/Bybit/Kraken Spot/Deribit). All use `DropOldest` + small
`mirror_capacity` (parity with Binance/Deribit live smokes). Bybit live smoke aligned to
the same overflow policy.

**No subscribe/drain bugs found** on this laptop pass — all 9 public venues green first try.

### Checklist scorecard (run 9)

| # | Gate | Result | Evidence |
|--:|---|---|---|
| 1 | Secrets / allowlist | **n/a** (public WS only) | no credentials in repo; private **SKIP** |
| 2 | Session reaches `/live` + `/ready` 200 | **n/a this cycle** (adapter tests only) | prior daemon evidence unchanged |
| 3 | Primary channels | **PASS** | 9 public venues `live_ignored` |
| 4 | Metrics: frames, zero unexplained drops | **n/a this cycle** (adapter tests) | companion synthetic soak below |
| 5 | Intentional reconnect recovers Live | **PASS** (laptop) | Binance + OKX reconnect probes in run 9 |
| 6 | Archived for ≥7 consecutive **schedule** runs | **FAIL** (laptop **9/9**; scheduled **= 0**) | this file + `runs/cycle_9/` |

**Maturity action:** **none** — remain `alpha+`. Laptop **9/9** ≠ scheduled §11.8 canary.

Companion synthetic soak (15 min): [`soak_results.md`](./soak_results.md) + [`soak_evidence_w3/`](./soak_evidence_w3/).

---

## 2026-07-22T11:15:49Z — run 8 (laptop, tip `98aefa1`) + Deribit public trades fix

| Field | Value |
|---|---|
| UTC window | `2026-07-22T11:15:49Z` → `2026-07-22T11:17:31Z` |
| Host | darwin laptop, network allowed |
| Binary / tip at run | `98aefa1` (`origin/main` through #108) + this PR (Deribit public trades fix) |
| Branch | `feat/andrzej_canary_evidence_20260722` |
| Evidence | [`canary_evidence/runs/cycle_8/`](./canary_evidence/runs/cycle_8/) |
| Private live | **SKIP** (keys absent) |
| Scheduled canary | **0** (not wired / not claimed) |

### Public `live_ignored` results

| Package | Test(s) | Result | Duration | Log |
|---|---|---|---:|---|
| `marketfeed-adapter-binance` | `live_binance_spot_trade_or_quote` + `live_binance_spot_reconnect_probe` | **PASS** | 15.02s | [`cycle_8/binance_live_ignored.log`](./canary_evidence/runs/cycle_8/binance_live_ignored.log) |
| `marketfeed-adapter-okx` | `live_okx_spot_trade_or_quote` + `live_okx_spot_reconnect_probe` | **PASS** | 20.03s | [`cycle_8/okx_live_ignored.log`](./canary_evidence/runs/cycle_8/okx_live_ignored.log) |
| `marketfeed-adapter-bybit` | `live_bybit_linear_trade_or_quote` | **PASS** | 15.02s | [`cycle_8/bybit_live_ignored.log`](./canary_evidence/runs/cycle_8/bybit_live_ignored.log) |
| `marketfeed-adapter-kraken` | `live_kraken_spot_trade_or_quote` | **PASS** | 20.01s | [`cycle_8/kraken_live_ignored.log`](./canary_evidence/runs/cycle_8/kraken_live_ignored.log) |
| `marketfeed-adapter-deribit` | `live_deribit_trade_or_ticker` | **PASS** (after fix) | 20.01s | [`cycle_8/deribit_live_ignored.log`](./canary_evidence/runs/cycle_8/deribit_live_ignored.log) |

Commands:

```bash
cargo test -p marketfeed-adapter-binance --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-okx --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-bybit --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-kraken --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-deribit --test live_ignored -- --ignored --nocapture
# private (skipped — no keys):
# cargo test -p marketfeed-private --features live --test live_ignored -- --ignored --nocapture
```

### Code fix shipped with this evidence (Deribit public live)

**Bug:** public `live_deribit_trade_or_ticker` connected and marked Live but never received market data.
Subscribe used `trades.{instrument}.raw`, which Deribit rejects for unauthorized sessions
(error `13778` / `raw_subscriptions_not_available_for_unauthorized`). Because trades and
ticker were in one `public/subscribe`, the whole request failed — no ticker either.

**Fix:** subscribe to public `trades.{instrument}.100ms` (same policy as public `book.*.100ms`).
Offline guard in `fixtures.rs` asserts Connected emit does not advertise `.raw` trades.
Live smoke uses `DropOldest` + small `mirror_capacity` (parity with Binance/OKX smokes).

### Checklist scorecard (run 8)

| # | Gate | Result | Evidence |
|--:|---|---|---|
| 1 | Secrets / allowlist | **n/a** (public WS only) | no credentials in repo; private **SKIP** |
| 2 | Session reaches `/live` + `/ready` 200 | **n/a this cycle** (adapter tests only) | prior daemon evidence unchanged |
| 3 | Primary channels | **PASS** | Spot pair + Bybit/Kraken/Deribit `live_ignored` |
| 4 | Metrics: frames, zero unexplained drops | **n/a this cycle** (adapter tests) | prior short-daemon metrics unchanged |
| 5 | Intentional reconnect recovers Live | **PASS** (laptop) | Binance + OKX reconnect probes in run 8 |
| 6 | Archived for ≥7 consecutive **schedule** runs | **FAIL** (laptop **8/8**; scheduled **= 0**) | this file + `runs/cycle_8/` |

**Maturity action:** **none** — remain `alpha+`. Laptop **8/8** ≠ scheduled §11.8 canary.

---

## 2026-07-22 — reconnect probe (Binance Spot + OKX Spot, tip `e13ab87`)

| Field | Value |
|---|---|
| UTC | 2026-07-22 (laptop run this PR) |
| Host | darwin laptop, network allowed |
| Binary / tip at run | `e13ab87` (`origin/main` through #50) + this PR |
| Branch | `feat/andrzej_reconnect_probe` |
| Evidence | [`canary_evidence/reconnect_probe/`](./canary_evidence/reconnect_probe/) |

### How the probe works

Engine live loop already reconnects on transport `Closed` (`run_session_with_reconnect` → `note_reconnect` → backoff → `connect`). The probe wraps `TungsteniteWebSocket` in `KillSwitchWebSocket` (`marketfeed-transport`): after `live_signal` is true and events have been dispatched, it sets a one-shot kill flag so the next `read_frame` closes the socket and returns `TransportError::Closed`. Assertions:

1. `marketfeed` reconnect counter ≥ 1
2. `live_signal` returns true after recovery
3. `frames_received` increases past the pre-kill watermark
4. Session stops cleanly via `stop_signal` (`SessionLifecycle::Stopped`)
5. Mirror still has at least one trade/quote

Channels under test: trades + quote/ticker (no L2 books in this probe — recovery is session Live + market events, not book resync).

### Results

| Venue | Test | Result | Duration | Log |
|---|---|---|---:|---|
| Binance Spot | `live_binance_spot_reconnect_probe` | **PASS** | 2.48s | [`reconnect_probe/binance.log`](./canary_evidence/reconnect_probe/binance.log) |
| OKX Spot | `live_okx_spot_reconnect_probe` | **PASS** | 2.68s | [`reconnect_probe/okx.log`](./canary_evidence/reconnect_probe/okx.log) |

Commands:

```bash
cargo test -p marketfeed-adapter-binance --test live_ignored live_binance_spot_reconnect_probe -- --ignored --nocapture
cargo test -p marketfeed-adapter-okx --test live_ignored live_okx_spot_reconnect_probe -- --ignored --nocapture
# offline guard:
cargo test -p marketfeed-transport kill_switch
```

### Checklist scorecard (reconnect probe only)

| # | Gate | Result | Evidence |
|--:|---|---|---|
| 5 | Intentional reconnect recovers Live | **PASS** (laptop) | both venues above |
| 6 | Archived for ≥7 consecutive **schedule** runs | **FAIL** (scheduled **= 0**) | laptop 7/7 ≠ scheduled |

**Maturity action:** **none** — remain `alpha+`. Reconnect PASS does **not** satisfy scheduled ≥7.

---

## 2026-07-22T06:20:44Z — runs 2..7 (laptop consecutive, tip `f3a50f9`)

| Field | Value |
|---|---|
| UTC window | `2026-07-22T06:20:44Z` → `2026-07-22T06:28:24Z` |
| Host | darwin laptop, network allowed |
| Binary / tip at run | `f3a50f9` (`origin/main` through #48); docs PR tip may be later |
| Branch | `feat/andrzej_canary_soak_advance` |
| Config | [`canary_evidence/config.live_binance_okx.toml`](./canary_evidence/config.live_binance_okx.toml) |
| Evidence tree | [`canary_evidence/runs/`](./canary_evidence/runs/) |

### Per-cycle results

| Run | Start (UTC) | End (UTC) | Binance `live_ignored` | OKX `live_ignored` | Short daemon (~80s) | Overall |
|--:|---|---|---|---|---|---|
| 2 | 06:20:44Z | 06:21:22Z | **PASS** 15.00s | **PASS** 20.01s | **PASS** port 19282; drops 0 | **PASS** |
| 3 | 06:22:43Z | 06:23:19Z | **PASS** 15.02s | **PASS** 20.02s | skipped | **PASS** |
| 4 | 06:23:19Z | 06:23:54Z | **PASS** 15.00s | **PASS** 20.02s | **PASS** port 19284; drops 0 | **PASS** |
| 5 | 06:25:17Z | 06:25:52Z | **PASS** 15.02s | **PASS** 20.01s | skipped | **PASS** |
| 6 | 06:25:52Z | 06:26:27Z | **PASS** 15.00s | **PASS** 20.00s | **PASS** port 19286; drops 0 | **PASS** |
| 7 | 06:27:49Z | 06:28:24Z | **PASS** 15.01s | **PASS** 20.02s | skipped | **PASS** |

Commands (each cycle):

```bash
cargo test -p marketfeed-adapter-binance --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-okx --test live_ignored -- --ignored --nocapture
# optional short daemon on even cycles: marketfeed run --config canary_evidence/config.live_binance_okx.toml
```

Short-daemon pre-stop metrics (frames / dispatched / dropped / overflows / live_sessions):

| Run | frames | dispatched | dropped | overflows | live_sessions |
|--:|--:|--:|--:|--:|--:|
| 2 | 25273 | 25262 | 0 | 0 | 2 |
| 4 | 17020 | 17010 | 0 | 0 | 2 |
| 6 | 14643 | 14632 | 0 | 0 | 2 |

### Checklist scorecard (runs 2..7 batch)

| # | Gate | Result | Evidence |
|--:|---|---|---|
| 1 | Secrets / allowlist | **n/a** (public WS only) | config in `canary_evidence/` |
| 2 | Session reaches `/live` + `/ready` 200 | **PASS** (daemon runs 2/4/6) | `runs/cycle_{2,4,6}/daemon_health.log` |
| 3 | Primary channels | **PASS** | `live_ignored` PASS ×6 each venue |
| 4 | Metrics: frames, zero unexplained drops | **PASS** | short-daemon metrics + tests |
| 5 | Intentional reconnect recovers Live | **PASS** (later laptop probe) | see reconnect probe section above |
| 6 | Archived for ≥7 consecutive **schedule** runs | **FAIL** (laptop **7/7** consecutive; **not** scheduled) | this file + `runs/` |

**Maturity action:** **none** — remain `alpha+`. Laptop 7/7 ≠ scheduled §11.8 canary.

Companion live soak (~31 min): [`soak_results.md`](./soak_results.md) + [`soak_evidence/`](./soak_evidence/).

---

## 2026-07-22T05:54:48Z — run 1 Binance Spot + OKX Spot (manual, `origin/main` @ `d5a0c57`)

| Field | Value |
|---|---|
| UTC start | `2026-07-22T05:54:48Z` |
| UTC `/live`+`/ready` 200 | `2026-07-22T05:54:54Z` |
| Soak | `2026-07-22T05:55:02Z` → `2026-07-22T05:56:32Z` (~90s) |
| UTC stop (SIGTERM) | `2026-07-22T05:56:41Z` (clean join ~2s) |
| Host | local operator machine (darwin), network allowed |
| Commit base | `d5a0c57` (`origin/main`) + live-loop null-sink drain (this PR) |
| Branch / PR | `feat/andrzej_live_canary` |
| Config | [`canary_evidence/config.live_binance_okx.toml`](./canary_evidence/config.live_binance_okx.toml) |

### Checklist scorecard (this run)

| # | Gate | Result | Evidence |
|--:|---|---|---|
| 1 | Secrets / allowlist | **n/a** (public WS only; no credentials in repo) | config in `canary_evidence/` |
| 2 | Session reaches `/live` + `/ready` 200 | **PASS** | `canary_evidence/daemon_health.log` |
| 3 | Primary channels (trades + quote/ticker) | **PASS** | frames + dispatched events climbing through soak |
| 4 | Metrics: frames, zero unexplained drops | **PASS** | `events_dropped_total 0`, `queue_overflows_total 0` |
| 5 | Intentional reconnect recovers Live | **PASS** (later laptop probe) | reconnect probe section |
| 6 | Archived for ≥7 consecutive schedule runs | **FAIL** at time of run (**1**/7); later laptop batch reached **7/7** still not scheduled | this file |

**Maturity action:** **none** — remain `alpha+` / beta-ready offline. Do **not** mark beta.

### Live adapter tests (`#[ignore]`)

| Package | Test | Result | Duration | Notes |
|---|---|---|---:|---|
| `marketfeed-adapter-binance` | `live_binance_spot_trade_or_quote` | **PASS** | 15.03s | pre-fix: `Dispatch(FailEngine)` without null-sink drain / smoke `DropOldest` |
| `marketfeed-adapter-okx` | `live_okx_spot_trade_or_quote` | **PASS** | 20.02s | |

Commands:

```bash
cargo test -p marketfeed-adapter-binance --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-okx --test live_ignored -- --ignored --nocapture
```

Logs: `canary_evidence/binance_live_ignored.log`, `canary_evidence/okx_live_ignored.log`

Offline guard for the engine fix: `cargo test -p marketfeed-engine --test live_drain` → **PASS**.

### Daemon live path (`marketfeed run`)

| Probe | Result |
|---|---|
| `/live` | **200** for full soak |
| `/ready` | **200** from first poll through soak end (`live_sessions=2`) |
| `/metrics` | **200**; both venues live |
| Soak hold | **~90s** (9×10s health polls) |
| SIGTERM | both venues `stopped cleanly`; `marketfeed daemon stopped` |

Final metrics (pre-stop snapshot):

| Metric | Value |
|---|---:|
| `marketfeed_up` | 1 |
| `marketfeed_ready` | 1 |
| `marketfeed_live_sessions` | 2 |
| `marketfeed_frames_received_total` | 16713 |
| `marketfeed_events_dispatched_total` | 16700 |
| `marketfeed_events_dropped_total` | 0 |
| `marketfeed_queue_overflows_total` | 0 |

Logs: `canary_evidence/daemon_live.log`, `canary_evidence/daemon_health.log`, `canary_evidence/daemon_metrics_pre_stop.txt`

### Code fix shipped with this evidence (blocked live connect)

**Bug:** live session loop never drained `EventDispatcher` when no consumer was attached. Under `OverflowPolicy::FailEngine` (daemon default), high-rate venues filled the dispatch queue and aborted with `Dispatch(FailEngine)`.

**Fix:**

- `crates/engine/src/live.rs` — null-sink `drain_dispatch()` after each frame / timer tick (metrics already counted on push).
- Offline guard: `crates/engine/tests/live_drain.rs`.
- Live smoke tests use `DropOldest` + small `mirror_capacity` so diagnostic mirrors do not `FailEngine` while asserting trades/quotes.

### What is still required for **beta**

1. Wire scheduled `canary.yml` live job (or documented daily operator cadence) — laptop burst **7/7** does **not** satisfy this.
2. Keep archiving greens under the **scheduled** cadence (not only same-day repeats). Reconnect probe is **PASS** on laptop (checklist item 5).
3. Only then flip `maturity_matrix.md` / READMEs from `alpha+` → **beta**.
