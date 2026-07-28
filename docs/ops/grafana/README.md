# Prometheus + Grafana for marketfeed-daemon

Ops surface for the daemon’s loopback HTTP probes (`/live`, `/ready`, `/metrics`).
Metric names and labels are taken from the daemon’s Prometheus text exposition
(`crates/daemon/src/state.rs` + `crates/engine/src/metrics.rs`). **Do not invent
labels** — venue series use `id="<venue-config-id>"` only (no symbol labels).

## Scrape `/metrics`

Default bind (see `crates/daemon/config.example.toml`):

```text
http://127.0.0.1:9108/metrics
```

`telemetry.bind` must be loopback; remote scrape goes through an SSH tunnel or
a reverse proxy with TLS/auth — never bind `0.0.0.0` from the daemon.

Example Prometheus scrape fragment: [`prometheus-scrape.example.yml`](./prometheus-scrape.example.yml).

Quick check:

```bash
curl -sS http://127.0.0.1:9108/live
curl -sS http://127.0.0.1:9108/ready
curl -sS http://127.0.0.1:9108/metrics | head -n 40
```

## Import Grafana dashboard

1. Create a Prometheus datasource pointing at your Prometheus that scrapes the daemon.
2. Dashboards → Import → upload
   [`dashboards/marketfeed-overview.json`](./dashboards/marketfeed-overview.json)
   (or paste JSON).
3. Select the Prometheus datasource when prompted.

The dashboard covers process readiness, per-venue live/reconnects/drops/book
health, throughput rates, queue occupancy, recording, and latency histograms.

## Alert rules

Load [`alerts.yml`](./alerts.yml) into Prometheus (`rule_files`) or Grafana
Alerting. Rules cover:

| Alert | Intent | Runbook |
|---|---|---|
| `MarketfeedNotReady` | `marketfeed_ready == 0` while up | [restart](../../runbooks/restart.md) |
| `MarketfeedReconnectStorm` | reconnect rate | [restart](../../runbooks/restart.md) |
| `MarketfeedEventsDropped` | sink/dispatch drops | [slow_sink](../../runbooks/slow_sink.md) |
| `MarketfeedBookInvalidations` | book invalidation rate | [restart](../../runbooks/restart.md) |
| `MarketfeedDiskPressure` | recording free-space pressure | [disk_full](../../runbooks/disk_full.md) |

## Related

- ADR-0009 daemon health model
- Soak / canary: `docs/ops/soak_runbook.md`, `docs/ops/canary_checklist.md`
- Live market panel (optional feature): `ui/` + `docs/ops/ui.md`
