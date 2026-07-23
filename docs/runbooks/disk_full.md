# Runbook: Disk full / recording disk pressure

## Symptoms

- `marketfeed_disk_pressure 1` in `/metrics`
- Logs mention `DiskPressure` / recording drain failures
- Readiness fails when `require_recording_healthy = true`
- Host `df` shows free space below `recording.raw.min_free_space`

## Immediate actions

1. Confirm pressure and free space:
   ```bash
   curl -sS http://127.0.0.1:9108/metrics | grep -E 'disk_pressure|recording_'
   df -h <recording.raw.directory>
   ```
2. Stop producers of new segments if the volume is critically full (graceful `SIGTERM`).
3. Free space without deleting the newest open segment mid-write:
   - Archive/delete **closed** `seg-*-*.mfr1` files older than retention
   - Prefer move-to-cold-storage over `rm` when audit is required
4. Restart only after `df` shows free space above `min_free_space`.

## Config knobs

```toml
[recording.raw]
enabled = true
directory = "/var/lib/marketfeed/raw"
segment_size = "256MiB"
segment_duration = "15m"
queue_capacity = 8192
overflow = "fail_engine"   # or drop_oldest under explicit lossy policy
min_free_space = "20GiB"
```

## Verification

- `marketfeed_disk_pressure 0`
- New segments appear under the recording directory
- Optional: `marketfeed inspect-recording --input <segment>`
