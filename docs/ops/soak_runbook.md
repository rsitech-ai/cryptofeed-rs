# Soak test runbook (spec §3.7, §27.9)

**Status:** offline synthetic executable; live multi-day soak remains ops  
**Owner:** ops / release captain  
**Automation:** `.github/workflows/soak.yml` (offline synthetic job + dispatch)  
**Laptop runner:** [`scripts/laptop_soak.sh`](../../scripts/laptop_soak.sh) — bounded RSS soak (synthetic/live); **not** multi-day / **not** stable  
**Laptop soak evidence:** [`soak_results.md`](./soak_results.md) (30m synthetic #179 W5-P1e + optional 60m #181 + 20m/15m + ~31m live — **not** stable)

## Goal

Prove the engine sustains continuous load **without unbounded memory growth**, and survives injected disconnects, malformed frames, snapshot failures, slow sinks, disk-full, and clock jumps per the production spec.

## Offline path (CI / laptop, no exchange I/O)

```bash
# Preferred one-shot bounded laptop soak (NOT multi-day / NOT stable):
./scripts/laptop_soak.sh                          # synthetic, default 30m
DURATION=1h ./scripts/laptop_soak.sh              # synthetic 60m
DURATION=2h ./scripts/laptop_soak.sh              # optional 2h operator run (alias DURATION=7200)
MODE=live DURATION=30m ./scripts/laptop_soak.sh   # live binance+okx
MODE=live SOAK_SECS=600 ./scripts/laptop_soak.sh  # live, raw seconds

# Ready check + graceful SIGTERM drain (~2s hold)
./scripts/offline_daemon_e2e.sh

# Mini-soak hold (synthetic stays /live+/ready 200)
SOAK_SECS=30 ./scripts/offline_daemon_e2e.sh

# Practical laptop soak with RSS samples (see soak_results.md)
RSS_LOG=/tmp/marketfeed_soak_rss.log RSS_INTERVAL_SECS=60 SOAK_SECS=1200 \
  MARKETFEED_BIND_PORT=19118 ./scripts/offline_daemon_e2e.sh

# Or cargo tests
cargo test -p marketfeed-daemon --test synthetic_ready --test marketfeed_run_e2e
```

Config: `crates/daemon/config.offline.toml` (synthetic memory venue only).

## Preconditions (live multi-day soak)

- [ ] At least one **beta+** adapter with live canary green for 7 days
- [x] Daemon starts venue sessions (not health-only shell)
- [x] Prometheus metrics for session/reconnect/queue/book exported (§23.2)
- [ ] Recording to disk with rotation enabled (for the live soak profile)
- [ ] Machine with enough disk for raw segments + headroom alert

## Procedure (live — manual until credentialed CI exists)

1. Deploy release candidate build (Linux x86_64 primary).
2. Configure one spot + one derivatives session on a primary venue.
3. Start soak for ≥ 24h (release gate) / ≥ 7d (stable claim).
4. Sample RSS / heap every 5 minutes; fail if monotonic growth without bound after warmup.
5. Inject: WS disconnect, REST 429, delayed snapshot, slow consumer (DropOldest), disk pressure.
6. Confirm every failure path emits metric + `SystemEvent` (including `EventsDropped`).
7. Archive recording segment set; replay offline and diff normalized market events.

## Exit criteria

- Offline: `/live`+`/ready` 200, metrics gates, clean SIGTERM drain
- Live: no OOM / unbounded RSS slope after warmup
- No silent drops (`EventsDropped` count matches policy expectations)
- Books recover to Live after chaos within reconnect policy
- Recording readable by current + previous minor reader

## Honest limits / allowed claims

| Evidence | Allowed claim |
|---|---|
| `scripts/laptop_soak.sh` + RSS CSV / metrics snapshots | **not** multi-day; **not** stable |
| Offline synthetic 20m RSS plateau | still **not** Spec §3.7 |
| Multi-day live soak + live chaos | path to **stable** (OPS-C) |

Multi-day live soak and scheduled credentialed canaries require human calendar time and venue credentials. This repo ships an **offline synthetic executable surface** plus a bounded laptop runner; green multi-day live soak is an ops sign-off, not a merge-button claim.
