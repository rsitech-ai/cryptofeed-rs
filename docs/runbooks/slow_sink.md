# Runbook: Slow sink / recording queue pressure

## Symptoms

- Rising `marketfeed_recording_queue_len`
- System events `QueuePressure` / `EventsDropped` in logs
- Process healthy on `/live` but consumers lag
- Shutdown takes the full `shutdown_deadline_secs` or times out

## Diagnose

1. Snapshot metrics:
   ```bash
   curl -sS http://127.0.0.1:9108/metrics | grep -E 'recording_|shutdown_|live_sessions'
   ```
2. Check disk latency on the recording volume (`iostat`, cloud volume burst credits).
3. Confirm rotation is happening (`marketfeed_recording_rotations_total` increasing) — stuck rotation often means FS errors.

## Mitigations (in order)

1. **Temporary:** lower subscription load (fewer symbols/channels) via config + restart.
2. **Capacity:** raise `recording.raw.queue_capacity` only if memory headroom exists.
   The daemon enforces a process-wide limit of 1,048,576 eagerly reserved queue
   slots across recording and all sink mailboxes/batch/system queues.
3. **Policy:** if lossy recording is acceptable, set `overflow = "drop_oldest"` (never claim lossless afterward).
4. **Storage:** move `directory` to a faster volume; shrink `segment_size` to reduce fsync batches.
5. **Shutdown:** increase `engine.shutdown_deadline_secs` so drains complete under backlog.

## After recovery

- Queue length should trend to ~0 while live
- No new `EventsDropped` for recording
- Replay/inspect a recent segment:
  ```bash
  marketfeed inspect-recording --input ./raw/seg-....mfr1
  marketfeed replay --input ./raw/seg-....mfr1
  ```
