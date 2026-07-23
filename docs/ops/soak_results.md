# Soak and latency evidence

**Status:** short laptop evidence; not multi-day, stable, or production-ready
**Runbook:** [`soak_runbook.md`](./soak_runbook.md)  
**Runner:** [`scripts/laptop_soak.sh`](../../scripts/laptop_soak.sh)
**Local evidence location:** ignored `.local/evidence/soak/`

## All usable public venue-segments — 15 minutes

On 2026-07-23, the daemon ran every usable public venue-segment concurrently,
plus a synthetic control. The test exercised public WebSocket and REST ingress,
normalization, L2 validation, bounded dispatch, metrics, readiness recovery, and
graceful shutdown. No external Kafka/NATS consumer or authenticated Coinbase
International session was configured.

| Field | Observed |
|---|---:|
| UTC window | `2026-07-23T16:51:12Z` to `2026-07-23T17:06:12Z` |
| Hold | 900 seconds |
| Coverage | 18 public venue-segments + 1 synthetic control |
| Concurrent WebSocket sessions | 22 |
| Frames received | 1,187,432 |
| Normalized events | 1,260,589 |
| Ingress bytes | 338,695,481 |
| Aggregate frames/s | 1,319.37 |
| Aggregate events/s | 1,400.65 |
| `/live` samples | 424/424 (100%) |
| `/ready` samples | 423/424 (99.764%) |
| Parse failures / unknown messages / sequence gaps | 0 / 0 / 0 |
| Event drops / queue overflows / action overflows | 0 / 0 / 0 |
| Maximum observed queue occupancy | 11 |
| Final public L2 books valid | 17/17 |
| RSS min / mean / max | 26.70 / 41.81 / 101.58 MiB |
| CPU mean / max | 5.92% / 17.3% |
| Shutdown | exit 0 in 274 ms |
| Error logs | 0 |

One Kraken Spot checksum mismatch caused a fail-closed book invalidation and a
single non-ready sample. The session reconnected and returned ready before the
next two-second sample. Four crossed Bitstamp replacement snapshots were
rejected while the last valid book was retained. These were observable
integrity protections, not silently accepted corruption.

Latency values are daemon frame-to-normalized-event processing time. They are
not exchange-to-host network latency. Percentiles are histogram bucket upper
bounds.

| Venue-segment | events/s | frame mean µs | p95 bucket µs | p99 bucket µs |
|---|---:|---:|---:|---:|
| Kraken Futures | 388.52 | 7.78 | 100 | 100 |
| Binance USD-M | 282.21 | 11.14 | 100 | 250 |
| Binance Spot | 157.02 | 11.82 | 100 | 100 |
| Bybit Linear | 71.56 | 26.67 | 100 | 250 |
| Bitfinex Derivatives | 58.66 | 2.73 | 100 | 100 |
| Binance COIN-M | 56.96 | 19.43 | 100 | 250 |
| Bybit Spot | 56.30 | 30.17 | 100 | 250 |
| Bybit Inverse | 44.56 | 33.96 | 100 | 250 |
| OKX Swap | 42.43 | 45.44 | 250 | 500 |
| Kraken Spot | 39.19 | 39.24 | 100 | 250 |
| Coinbase Advanced | 38.70 | 49.75 | 250 | 500 |
| Bitfinex Spot | 32.02 | 3.79 | 100 | 100 |
| Coinbase Exchange | 29.18 | 20.17 | 100 | 100 |
| OKX Spot | 26.06 | 46.95 | 250 | 500 |
| Deribit | 23.14 | 52.87 | 250 | 500 |
| OKX Futures | 19.66 | 32.75 | 100 | 250 |
| Bitstamp | 18.76 | 144.77 | 500 | 1,000 |
| Gemini | 15.72 | 20.65 | 100 | 250 |

## Additional historical evidence

- A 60-minute synthetic laptop run stayed live and ready for all 120 samples,
  reported zero drops/overflows, and held an approximately 8.4 MiB RSS plateau
  after warmup.
- A 31-minute Binance Spot + OKX Spot laptop run stayed live and ready for all
  62 samples and shut down cleanly.
- Intentional reconnect probes for Binance Spot and OKX Spot returned to Live
  and resumed frame ingress.

## Reproduce

```bash
./scripts/laptop_soak.sh
DURATION=1h ./scripts/laptop_soak.sh
MODE=live DURATION=15m MARKETFEED_LIVE_CONFIG=/path/to/public-venues.toml \
  ./scripts/laptop_soak.sh
```

Detailed logs remain local by design. Update this summary only after reviewing
the generated metrics, health samples, logs, configuration, source SHA, and exit
status.

## Honest limit

This evidence supports a short-run multi-venue runtime smoke. It does not prove
authenticated coverage, external consumer delivery, multi-day reliability,
calendar-spaced canaries, chaos recovery, or production readiness.
