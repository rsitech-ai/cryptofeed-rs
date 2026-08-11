# Release canary qualification — 2026-08-11

## Decision

**HOLD.** Commit `7c97336f5656df7a6bdc099218b6a46325dc5652` is repo-ready and has a clean 15-minute public-data runtime smoke, but it is not release-qualified. The latest exact canary exceeded the fail-closed RSS-growth limit, so the one-hour and 24-hour promotion gates were not started.

This qualification covers the read-only market-data and UI path only. Audio, authenticated trading, order placement, and private exchange APIs are outside the product scope and were neither implemented nor tested.

## Latest exact evidence

Evidence directory (gitignored): `.local/evidence/release-canary/runs/20260811T160728Z`

| Gate | Result | Observed | Limit |
|---|---:|---:|---:|
| Logical venues live | PASS | 13/13 minimum | 13/13 |
| Qualified L2 books | PASS | retained required valid books | per-venue contract |
| Reconnect delta | PASS with warning | 1 (`binance-usdm`) | 2 |
| Queue occupancy | PASS | 0.20% peak | 80% |
| API latency p95 | PASS | 0.915 ms | 500 ms |
| Process CPU p95 | PASS | 10.4% | 150% |
| Peak RSS | PASS | 161.39 MiB | 1536 MiB |
| RSS growth | **HOLD** | **109.50 MiB/hour** | 64 MiB/hour |
| Daemon log | PASS | 0 warnings, 0 errors, 0 malformed lines | clean |
| UI smoke | PASS | exit 0 on first attempt | exit 0 |
| Shutdown | PASS | graceful, exit 0, no forced stop | graceful |

The exact binary SHA-256 was `d1255aca8a218e8be82d72d7c937fd9ccd39a9edcbbfbdceafd4560ec650c326`. The config SHA-256 was `c0eda0498e52134f68374b325d9768bbc15da916d49505266d95604d9752ce2a`.

## Recovery chronology

The qualification work added a reproducible release canary, corrected venue lifecycle accounting, and then removed measured UI-path bottlenecks rather than weakening gates:

- bounded server-side book snapshots and the depth-history ring;
- isolated live profile and bubble projections from the global view lock;
- bounded finalized bubbles and adaptive candle history;
- cached adaptive thresholds per tier and market segment;
- removed rollover cloning and shared immutable candle history between volume and delta detectors;
- reduced the always-on UI calibration window from eight to four finalized candles, matching the adaptive detector's minimum-sample contract;
- retained graceful shutdown, exact Git/binary/config metadata, log review, API latency, CPU, queue, book, and reconnect checks.

Earlier exact or representative runs showed the progression:

- `20260811T112618Z`: one-hour HOLD, CPU p95 234.7%, RSS growth 127.2 MiB/hour.
- `20260811T141231Z`: one-hour HOLD, CPU p95 10.8%, RSS growth 76.26 MiB/hour, plus Binance USD-M book/reconnect failures.
- `20260811T154926Z`: 15-minute HOLD, CPU p95 13.3%, RSS growth 300.96 MiB/hour, three Binance USD-M reconnects.
- `20260811T160728Z`: 15-minute HOLD only on RSS slope; CPU, API, queues, venue liveness, books, reconnect allowance, logs, smoke, and shutdown passed.

Short-run RSS slope is sensitive to bounded history warm-up and allocator high-water behavior, but the gate is intentionally fail-closed. The latest result therefore cannot be promoted based on the expectation that growth may eventually plateau.

## Verification

The final code path was checked with:

- `cargo test -p marketfeed-analytics --test bubbles` — 9 passed;
- `cargo test -p marketfeed-daemon --features ui view::plane::tests::` — 14 passed after the final retention change;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — passed on the final code state;
- `cargo test --workspace --all-targets --all-features -q` — passed on the final code state;
- `npm --prefix ui test` — 148 passed;
- `npm --prefix ui run build` — passed;
- release-canary UI smoke — passed on the final release binary;
- `cargo fmt --all -- --check` and `git diff --check` — passed.

The host-local parser benchmark remained noisy and inconclusive, so its baseline was not rewritten or represented as a pass.

## Remaining work and promotion sequence

1. Instrument retained analytics allocations by projection and instrument, separating live profile state, adaptive history, depth history, and allocator-resident-but-free memory.
2. Replace retained full `CandleFlow` calibration history with compact, configuration-specific strength samples, or prove with allocator telemetry that the apparent slope is released memory rather than live heap.
3. Rerun the exact 15-minute gate until it passes without threshold changes or waivers.
4. Run one uninterrupted exact one-hour canary. Any book-integrity, reconnect, resource, API, log, or shutdown failure remains HOLD.
5. Only after the one-hour GO, run the separate 24-hour beta qualification. That would prove a bounded beta gate, not unattended production maturity.

Public venue maintenance and transient disconnects remain external dependencies. Deribit documents error `11051` as system maintenance in its official error reference; external incidents must be recorded, not hidden or converted into success.

No branch was pushed and no pull request or merge was created as part of this local recovery pass.
