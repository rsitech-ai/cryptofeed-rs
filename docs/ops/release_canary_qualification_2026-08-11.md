# Release canary qualification — 2026-08-11

## Decision

**GO to merge and the next 24-hour read-only beta gate.** Commit
`caccd3fe0ad31032b993b073b7907a5450a5c4e4` passed the clean 15-minute
qualification and an uninterrupted one-hour qualification on public data.

This is runtime proof for the read-only market-data and UI release candidate.
It is not 24-hour, multi-day, authenticated-feed, external-sink, or unattended
production proof. Audio, trading, order placement, credentials, and private
exchange APIs remain outside the product scope.

## Exact release evidence

Evidence is gitignored under `.local/evidence/release-canary/runs/`.

| Gate | 15-minute run `20260811T184959Z` | One-hour run `20260811T190518Z` | Limit |
|---|---:|---:|---:|
| Verdict | GO | GO | no HOLD reasons |
| Logical venues live | 13/13 minimum | 13/13 minimum | 13/13 sampled |
| Qualified L2 books | retained | retained | per-venue contract |
| Reconnect delta | 0 | 3 total: Binance USD-M 2, OKX Spot 1 | at most 2 per venue |
| Queue occupancy | 0.20% peak | 0.20% peak | below 80% |
| API latency p95 | 0.783 ms | 1.330 ms | at most 500 ms |
| Process CPU p95 | 6.1% | 7.0% | at most 150% |
| Peak RSS | 119.86 MiB | 172.06 MiB | at most 1536 MiB |
| Linear RSS growth | 0.00 MiB/hour | 40.10 MiB/hour | at most 64 MiB/hour |
| Windowed p95 RSS growth | 0.00 MiB/hour | 36.73 MiB/hour | at most 64 MiB/hour |
| UI smoke | first attempt | first attempt | exit 0 |
| Shutdown | graceful exit 0 | graceful exit 0 | no forced stop |

The exact release binary SHA-256 for both final runs was
`1333a7981b3a02be7284b12b6f205dacd4d1f0cf720efb35103e03feabc6cd07`.
The config SHA-256 was
`c0eda0498e52134f68374b325d9768bbc15da916d49505266d95604d9752ce2a`.
Both runs captured an empty Git status at startup.

## External reconnect evidence

The one-hour run recorded three recovered warnings, all with
`disconnect_reason=TransportError` and
`Connection reset without closing handshake`: two for Binance USD-M and one
for OKX Spot. They remained within the per-venue reconnect allowance, caused no
sampled liveness loss, invalidation, or event drop, and all required books
recovered.

An isolated Binance USD-M comparison reproduced the source behavior: the raw
35-stream WebSocket closed with code `1006` while the daemon recorded the same
connection-reset cause and recovered to 5/5 books with no invalidations or
drops. The engine now logs the reconnect cause and selected backoff so future
source incidents are attributable rather than represented by an unexplained
counter.

## Memory recovery

Native macOS `heap` and `vmmap` evidence separated retained live heap from RSS
page residency. The dominant bounded allocation was the depth history: 19 L2
instrument histories, each eventually holding 600 bid/ask samples. Those
buffers are now allocated and prefaulted on the first valid snapshot, reused on
eviction, and never exposed as synthetic samples.

The canary still reports the least-squares RSS slope, but it also compares the
p95 upper envelope across equal post-warmup windows. A leak HOLD requires both
growth measures to exceed the unchanged 64 MiB/hour threshold; monotonic growth
coverage still fails closed. The final hour showed repeated allocator reclaim,
including a drop from about 163 MiB to about 90 MiB near minute 50, and both
full-window growth measures passed.

## UI verification

The release UI was exercised in a real browser against live services. The
matrix covered chart modes, BTC/ETH switching, absolute/percentage price,
order-flow selectors and layers, DOM columns and depth, Market Profile VOL/TPO,
markets search/group/segment filters, settings, replay gating, desktop and
narrow layout, and clean shutdown.

All seven Market Profile values populated: VAH, VAL, POC, range, volume, TPO
count, and rotation factor. Replay remained correctly disabled without a
fixture. The browser recorded zero console warnings/errors, 248 API resources,
zero failed resources, and an active SSE connection. Audio, trading, and order
placement were not present or tested by design.

## Verification

The final integration gate includes:

- `cargo fmt --all -- --check`;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`;
- `cargo test --workspace --all-targets --all-features -q`;
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps`;
- `cargo deny check` — advisories, bans, licenses, and sources passed;
- `npm --prefix ui test` — 148 passed;
- `npm --prefix ui run build`;
- `./scripts/check-oss-readiness.sh` — no leaks found;
- `./scripts/release_canary.sh --self-check`;
- exact clean 15-minute and one-hour canaries;
- live browser scenario matrix with clean console/network evidence;
- `git diff --check` and complete review against `origin/main`.

All repository release-template checks passed on the final documented tree.
Any later failure returns the branch to HOLD.

## Next gate

After merge, run the separate 24-hour public-data beta qualification with the
same thresholds and cause-aware reconnect logs. That gate can prove bounded
beta stability; it still does not authorize trading or establish multi-day
unattended production maturity.
