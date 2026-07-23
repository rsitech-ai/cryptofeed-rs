# Soak results (laptop session)

**Status:** offline synthetic + **live** laptop soaks logged — **not** multi-day, **not** stable  
**Runbook:** [`soak_runbook.md`](./soak_runbook.md)  
**Laptop runner:** [`scripts/laptop_soak.sh`](../../scripts/laptop_soak.sh) (`DURATION=30m|1h|2h|…` or `SOAK_SECS`/`DURATION=7200`; **not** multi-day)  
**Maturity:** remains `alpha+` (canary laptop **10/10** but not scheduled; see [`canary_results.md`](./canary_results.md))

## Honesty bar

| Evidence here | Allowed claim |
|---|---|
| Offline synthetic 10–60+ min, bounded RSS | laptop memory smoke; no crash under synthetic load |
| Live 15 min across all currently usable public venue-segments | whole-public-matrix laptop smoke; not authenticated coverage, not a sink E2E, not stable |
| Live ~30+ min binance+okx, bounded RSS, 0 drops | laptop live soak smoke — **not** Spec §3.7 stable |
| Multi-day **live** soak + chaos inject | path to **stable** — **not claimed** |
| ≥7 consecutive **scheduled** canaries | **beta** — **not claimed** (laptop 10/10 only) |

---

## 2026-07-23 — all usable public venue-segments, 15 min

This run exercised the daemon's public WebSocket/REST ingress, normalization,
order-book validation, dispatch, and null-drain path concurrently. It did not
configure an external Kafka/NATS consumer, and it did not exercise authenticated
Coinbase International market data.

| Field | Value |
|---|---|
| UTC | `2026-07-23T16:51:12Z` ready → `2026-07-23T17:06:12Z` stop |
| Hold | **900s** / 15 min |
| Coverage | **18** public venue-segments + 1 synthetic control; **22** WebSocket sessions |
| Auth boundary | Coinbase International skipped: required market-data credentials were absent |
| Health sampling | `/live` 424/424 = **100%**; `/ready` 423/424 = **99.764%** |
| Readiness recovery | one Kraken Spot checksum mismatch; fail-closed invalidation and recovery in <2s sampling interval |
| Traffic | **1,187,432** frames; **1,260,589** normalized events; **338,695,481** bytes |
| Aggregate rate | **1,319.37 frames/s**; **1,400.65 events/s**; **0.359 MiB/s** |
| Data errors | 0 parse failures; 0 unknown messages; 0 sequence gaps |
| Pressure | 0 event drops; 0 queue overflows; 0 action overflows; maximum observed queue occupancy **11** |
| Integrity events | 1 checksum mismatch + 1 book invalidation (Kraken Spot); 4 rejected crossed Bitstamp replacement snapshots with the last valid book retained |
| Final books | **17/17** public L2 venue-segments valid; synthetic control valid |
| Process | exit 0; graceful shutdown **274ms**; 0 error logs |
| Resources | RSS min/mean/max **26.70/41.81/101.58 MiB**; CPU mean/max **5.92%/17.3%** |

The one non-ready sample was an observed integrity recovery, not a crash or
collector-induced abort. Kraken Spot rejected the mismatched update, invalidated
the book, reconnected, and returned ready before the next two-second sample.
A separate raw Kraken diagnostic replay validated 13,102 checksums without a
mismatch, so the intermittent discontinuity's exact origin remains unproven.

Latency percentiles below are histogram bucket upper bounds, not interpolated
quantiles. `frame mean` measures daemon frame-to-normalized-event processing,
not exchange-to-host network latency.

| Venue-segment | events/s | KiB/s | frame mean µs | frame p95 bucket µs | frame p99 bucket µs | max queue |
|---|---:|---:|---:|---:|---:|---:|
| Kraken Futures | 388.52 | 47.00 | 7.78 | 100 | 100 | 1 |
| Binance USD-M | 282.21 | 74.19 | 11.14 | 100 | 250 | 1 |
| Binance Spot | 157.02 | 30.82 | 11.82 | 100 | 100 | 1 |
| Bybit Linear | 71.56 | 17.38 | 26.67 | 100 | 250 | 1 |
| Bitfinex Derivatives | 58.66 | 1.42 | 2.73 | 100 | 100 | 4 |
| Binance COIN-M | 56.96 | 14.35 | 19.43 | 100 | 250 | 1 |
| Bybit Spot | 56.30 | 12.22 | 30.17 | 100 | 250 | 1 |
| Bybit Inverse | 44.56 | 11.04 | 33.96 | 100 | 250 | 1 |
| OKX Swap | 42.43 | 21.47 | 45.44 | 250 | 500 | 1 |
| Kraken Spot | 39.19 | 7.60 | 39.24 | 100 | 250 | 1 |
| Coinbase Advanced | 38.70 | 37.08 | 49.75 | 250 | 500 | 11 |
| Bitfinex Spot | 32.02 | 0.87 | 3.79 | 100 | 100 | 1 |
| Coinbase Exchange | 29.18 | 19.02 | 20.17 | 100 | 100 | 2 |
| OKX Spot | 26.06 | 10.16 | 46.95 | 250 | 500 | 1 |
| Deribit | 23.14 | 5.93 | 52.87 | 250 | 500 | 1 |
| OKX Futures | 19.66 | 4.08 | 32.75 | 100 | 250 | 1 |
| Bitstamp | 18.76 | 47.69 | 144.77 | 500 | 1,000 | 2 |
| Gemini | 15.72 | 5.19 | 20.65 | 100 | 250 | 1 |

This is short laptop evidence only. It does not promote maturity, replace
multi-day/chaos evidence, prove authenticated integrations, or prove delivery
through an external sink and consumer.

---

## W5-P1e close (honesty)

**Closed on #179** laptop synthetic **30m** (RSS plateau, **0** drops) — **not** multi-day / **not** stable. Optional operator preset: `DURATION=2h` / `DURATION=7200` (no 2h run required). #181 adds human `DURATION` presets + an extra **60m** laptop archive; neither unlocks Spec §3.7.

## 2026-07-22 — supplemental offline synthetic **60 min** (#181; tip `ecd9c42`) — not required for W5-P1e close

Laptop-only longer soak via `DURATION=1h ./scripts/laptop_soak.sh`. **Not** multi-day. **Not** stable. Does **not** unlock Spec §3.7.

| Field | Value |
|---|---|
| UTC start | `2026-07-22T15:41:11Z` |
| `/live`+`/ready` 200 | `2026-07-22T15:41:12Z` |
| Hold | **`DURATION=1h`** (`SOAK_SECS=3600`; RSS every 30s) |
| Last RSS sample | `2026-07-22T16:40:42Z` (`t≈3571s` of 3600s) |
| UTC stop note | `2026-07-22T16:41:12Z` (see wrap-up note in evidence) |
| Health | `/live=200` `/ready=200` on **all** 120 RSS samples |
| Host | darwin laptop, synthetic memory venue only |
| Bind | `127.0.0.1:19301` |
| Config | `crates/daemon/config.offline.toml` (bind rewritten) |
| Tip at run | `ecd9c42` (`origin/main` tip at start) |
| Evidence | [`soak_evidence/runs/synthetic_20260722T154111Z/`](./soak_evidence/runs/synthetic_20260722T154111Z/) |
| Command | `DURATION=1h MARKETFEED_BIND_PORT=19301 RSS_INTERVAL_SECS=30 READY_TIMEOUT_SECS=180 ./scripts/laptop_soak.sh` |

### RSS samples (`ps -o rss=` KiB, 30s)

| Metric | Value |
|---|---:|
| Samples | 120 |
| First (15:41:12Z) | 10224 KiB |
| Post-warmup plateau | ~8384–8480 KiB |
| Last (16:40:42Z) | 8448 KiB |
| Min | 8384 KiB |
| Max | 10512 KiB (early warmup) |
| Growth after warmup | **none** (flat / slightly down ~8.4 MiB) |
| Crashes | **0** |
| `events_dropped_total` (all samples) | **0** |
| `queue_overflows_total` | **0** |

### Exit criteria (this synthetic session)

- [x] `/live` + `/ready` 200 for full ~60 min hold (≥30 min required)
- [x] RSS bounded after warmup (no unbounded slope)
- [x] Evidence archived under `docs/ops/soak_evidence/runs/`
- [ ] Clean post-soak metrics snapshot — **partial** (in-place script edit mid-run interrupted the final SIGTERM/metrics path; daemon trap-cleaned; RSS CSV complete)
- [ ] Multi-day live soak — **OPS**
- [ ] Live disconnect / disk-full inject — **OPS**

### Script note (W5-P1e)

`scripts/laptop_soak.sh` accepts `DURATION=30m|1h|2h|4h|8h|…` (or bare seconds / `DURATION=7200`) in addition to `SOAK_SECS`. Soft-cap **8h** (use multi-day OPS soak beyond that). `SELF_CHECK=1` validates duration presets. Archives under `docs/ops/soak_evidence/runs/<mode>_<UTC>/` without clobbering historical root evidence.

---

## 2026-07-22T15:14:57Z — bounded synthetic soak via `scripts/laptop_soak.sh` (tip `233f676`)

| Field | Value |
|---|---|
| Mode | `synthetic` |
| Hold | **1800s** / 30m (`DURATION=1800`; W5-P1e closing evidence #179; optional `DURATION=2h`/`7200`) |
| UTC | `2026-07-22T15:14:57Z` → `2026-07-22T15:44:59Z` |
| Health | `/live=200` `/ready=200` on all 61 RSS samples |
| Host | darwin laptop, synthetic memory venue only |
| Bind | `127.0.0.1:19320` |
| Tip at run | `233f676` (`origin/main` through #158) |
| Evidence | [`soak_evidence/runs/synthetic_20260722T151457Z/`](./soak_evidence/runs/synthetic_20260722T151457Z/) |
| RSS | samples=61 min_kib=7328 max_kib=10272 (warmup); plateau ~8320 KiB |
| Drops / overflows | **0** |
| Maturity | **not** multi-day; **not** stable |

### Exit criteria (this synthetic session)

- [x] `/live` + `/ready` 200 for full 30 min hold
- [x] Graceful SIGTERM drain
- [x] RSS bounded after warmup (no unbounded slope)
- [ ] Multi-day live soak — **OPS**
- [ ] Live disconnect / disk-full inject — **OPS**


## 2026-07-22 — offline synthetic 15 min (RSS sampled, tip `c6db619`)

| Field | Value |
|---|---|
| UTC start | `2026-07-22T12:23:48Z` |
| Hold | `SOAK_SECS=900` (15 min) |
| UTC stop (SIGTERM) | `2026-07-22T12:38:49Z` (clean join) |
| Health | `/live=200` `/ready=200` continuous for full hold (31 RSS samples) |
| Host | darwin (laptop), synthetic memory venue only |
| Bind | `127.0.0.1:19128` |
| Config | `crates/daemon/config.offline.toml` (bind rewritten) |
| Tip at run | `c6db619` (`origin/main` through #123) + this PR |
| Evidence | [`soak_evidence_w3/`](./soak_evidence_w3/) (`rss.log`, `daemon.log`) |
| Command | `cargo build -p marketfeed-daemon` then `READY_TIMEOUT_SECS=60 RSS_INTERVAL_SECS=30 SOAK_SECS=900 MARKETFEED_BIND_PORT=19128 ./scripts/offline_daemon_e2e.sh` |

### RSS samples (`ps -o rss=` KiB, 30s)

| Metric | Value |
|---|---:|
| Samples | 31 |
| First (t=1s) | 9712 KiB |
| Post-warmup plateau | 7472–7552 KiB |
| Last (t=901s) | 7488 KiB |
| Min | 7472 KiB |
| Max | 9712 KiB (warmup only) |
| Growth after warmup | **none** (flat ~7.5 MiB) |
| Crashes | **0** |

### Exit criteria (synthetic session)

- [x] `/live` + `/ready` 200 for full 15 min hold
- [x] Graceful SIGTERM drain
- [x] RSS bounded after warmup (no unbounded slope)
- [ ] Multi-day live soak — **OPS**
- [ ] Live disconnect / disk-full inject — **OPS**

### Script note

Cold `cargo run` can miss the ready gate; this PR raises default `READY_TIMEOUT_SECS` to **60**
and pre-builds `marketfeed-daemon` before long soaks.

---

## 2026-07-22 — live Binance Spot + OKX Spot ~31 min (RSS sampled)

| Field | Value |
|---|---|
| UTC start | `2026-07-22T06:24:00Z` |
| `/live`+`/ready` 200 | `2026-07-22T06:24:01Z` |
| Hold | **~31 min** (`SOAK_SECS=1860`, samples every 30s) |
| UTC stop (SIGTERM) | `2026-07-22T06:55:05Z` (clean join; both venues stopped cleanly) |
| Health | `/live=200` `/ready=200` on **all** 62 RSS samples (`bad_health=0`) |
| Host | darwin laptop, live WS to Binance Spot + OKX Spot |
| Bind | `127.0.0.1:19278` |
| Config | [`soak_evidence/config.live.toml`](./soak_evidence/config.live.toml) (from canary live template) |
| Tip at run | binary built from `f3a50f9` (through #48) |
| Evidence | [`soak_evidence/`](./soak_evidence/) (`rss_samples.csv`, `daemon.log`, metrics) |

### RSS samples (`ps -o rss=` KiB, 30s)

| Metric | Value |
|---|---:|
| Samples | 62 |
| First (06:24:01Z) | 22544 KiB |
| Last (06:54:35Z) | 20272 KiB |
| Min | 20128 KiB |
| Max | 23504 KiB (early warmup) |
| Growth after warmup | **none** (slightly down / flat ~20–21 MiB) |
| Crashes | **0** |
| `events_dropped_total` (all samples) | **0** |
| `queue_overflows_total` | **0** |
| `live_sessions` | **2** throughout |

Representative samples:

| UTC | rss_kib | frames | dropped |
|---|---:|---:|---:|
| 06:24:01Z | 22544 | 9 | 0 |
| 06:30:01Z | 20528 | 70397 | 0 |
| 06:40:03Z | ~20400 | ~170k | 0 |
| 06:50:04Z | ~20240 | ~250k | 0 |
| 06:54:35Z | 20272 | 287088 | 0 |

### Pre-stop metrics

| Metric | Value |
|---|---:|
| `marketfeed_up` | 1 |
| `marketfeed_ready` | 1 |
| `marketfeed_live_sessions` | 2 |
| `marketfeed_frames_received_total` | 290470 |
| `marketfeed_events_dispatched_total` | 290281 |
| `marketfeed_events_dropped_total` | 0 |
| `marketfeed_queue_overflows_total` | 0 |

### Exit criteria (this live session)

- [x] `/live` + `/ready` 200 for full ~31 min hold
- [x] Graceful SIGTERM drain (both venues clean)
- [x] RSS bounded after warmup (no unbounded slope)
- [x] Zero unexplained drops / overflows under nominal live load
- [ ] Multi-day live soak — **OPS**
- [ ] Live disconnect / disk-full inject — **OPS**

---

## 2026-07-22 — offline synthetic 20 min (RSS sampled)

| Field | Value |
|---|---|
| UTC start | `2026-07-22T05:55:24Z` |
| Hold | `SOAK_SECS=1200` (20 min) |
| UTC stop (SIGTERM) | `2026-07-22T06:15:24Z` (clean join) |
| Health | `/live=200` `/ready=200` continuous for full hold |
| Host | darwin (laptop), synthetic memory venue only |
| Bind | `127.0.0.1:19118` |
| Config | `crates/daemon/config.offline.toml` (bind rewritten) |
| Tip at run | `origin/main` @ `fe60acc` (through #47) |
| Command | `SOAK_SECS=1200 MARKETFEED_BIND_PORT=19118 ./scripts/offline_daemon_e2e.sh` + external RSS sampler every 30s |

### RSS samples (`ps -o rss=` KiB)

| Metric | Value |
|---|---:|
| Samples | 39 |
| First (05:55:53Z) | 8960 KiB |
| Post-warmup plateau | 6992–7360 KiB |
| Last (06:14:56Z) | 6992 KiB |
| Min | 6992 KiB |
| Max | 8960 KiB (warmup only) |
| Growth after warmup | **none** (flat / slightly down) |
| Crashes | **0** |
| `events_dropped_total` (mid-soak) | **0** |

Representative samples:

| UTC | rss_kib |
|---|---:|
| 05:55:53Z | 8960 |
| 05:56:23Z | 8960 |
| 05:56:53Z | 7072 |
| 06:00:24Z | 7168 |
| 06:05:00Z | ~7040 |
| 06:10:00Z | ~6992 |
| 06:14:56Z | 6992 |

### Exit criteria (synthetic session)

- [x] `/live` + `/ready` 200 for full 20 min hold
- [x] Graceful SIGTERM drain
- [x] RSS bounded after warmup (no unbounded slope)
- [ ] Multi-day live soak — **OPS** (see live section above for laptop live smoke only)
- [ ] Live disconnect / disk-full inject — **OPS**

### Script note

`scripts/offline_daemon_e2e.sh` supports optional `RSS_INTERVAL_SECS` / `RSS_LOG` and fails if the daemon child exits mid-soak (avoids polling a stale listener after bind failure).

## Not a maturity promotion

Laptop synthetic soak (W5-P1e close = #179 **30m**; optional #181 **60m**) ≠ Spec §3.7 continuous soak for **stable**. Laptop live ~31 min ≠ multi-day soak. Canary remains **not beta** (laptop 10/10, scheduled **= 0**, reconnect **PASS** laptop). Do **not** mark beta / stable / 1.0 from this file alone.
