# Runbook: Restart marketfeed daemon

## Symptoms

- Process not responding on `/live` or `/ready`
- Deploy / config change requires bounce
- Venue sessions stuck after prolonged disconnect

## Safe restart

1. Confirm current health:
   ```bash
   curl -sS http://127.0.0.1:9108/live
   curl -sS http://127.0.0.1:9108/ready
   curl -sS http://127.0.0.1:9108/metrics | head
   ```
2. Validate config before bounce:
   ```bash
   marketfeed validate --config /path/to/config.toml
   ```
3. Send graceful stop (`SIGTERM` or Ctrl-C). The daemon:
   - marks `marketfeed_shutdown_draining 1`
   - sets per-venue stop signals (WS read loop polls ~250ms)
   - drains recording queues within `engine.shutdown_deadline_secs`
4. Wait for process exit (default deadline 20s). If it hangs, escalate to `SIGKILL` only after capturing `metrics` + logs.
5. Start again with the same config. Confirm `/ready` and `marketfeed_live_sessions`.

## Checks after restart

- `marketfeed_up 1` and `marketfeed_ready 1`
- Required venues show `marketfeed_venue_live{id="..."} 1`
- If recording enabled: `marketfeed_recording_healthy 1` and no sustained `marketfeed_disk_pressure 1`
