# Release canary recovery reflection

## Task

- **ID / title:** Public-data UI release canary recovery
- **Date:** 2026-08-11
- **Scope:** Read-only multi-venue market data, analytics projections, UI runtime, and qualification tooling
- **Authority boundary:** No audio, trading, order placement, credentials, private feeds, or production deployment

## Success and Risk

- **Success criteria:** Clean repository checks, real-browser UI proof, exact 15-minute and one-hour GO, and evidence-backed merge readiness
- **Hypothesis 1:** The RSS slope was retained live heap in analytics history
- **Hypothesis 2:** Bounded depth buffers were being faulted lazily after the warmup window
- **Hypothesis 3:** Short-window RSS slope was distorted by macOS reclaim cycles despite a stable upper envelope
- **Rollback path:** Revert the isolated bounded-history and analyzer commits if semantics, retained counts, or monotonic-leak detection regress

## Candidate Directions

| Candidate | Expected benefit | Main risk | Evidence before choice | Decision |
|---|---|---|---|---|
| Compact every analytics representation again | Lower RSS | Semantic drift and unnecessary complexity | Native heap showed only about 1.86 MiB live-heap growth over 5.5 minutes | Rejected |
| Prefault bounded depth buffers and qualify both slope and p95 envelope | Stable warmup plus truthful leak detection | Could hide monotonic growth if implemented incorrectly | Exact 53.76 MiB depth allocation and repeated reclaim troughs; monotonic test retained | Selected |

## Evidence

- **First meaningful failure signal:** Exact 15-minute canary exceeded the 64 MiB/hour RSS-growth threshold while all functional gates passed
- **Commands or runtime checks:** macOS `heap`/`vmmap`, focused red-green Rust tests, analyzer unit/self-checks, full Rust/UI checks, browser interaction matrix, raw WebSocket comparison, exact 15-minute and one-hour canaries
- **What the evidence ruled in or out:** Ruled out unbounded depth retention, UI/API starvation, book corruption, and deterministic Binance adapter reconnects; confirmed allocator cycling and external connection resets

## Decision

- **Root cause or remaining unknown:** Sparse depth histories faulted their bounded buffers after warmup, and linear-only RSS analysis overfit allocator reclaim timing. Later reconnect bursts were external transport resets, proven against a raw stream.
- **Retained fix / direction:** Preallocate and reuse exact bounded depth buffers; retain linear growth plus p95-envelope qualification; log reconnect cause/backoff
- **Why alternatives were rejected:** Further broad compaction lacked retained-heap evidence, and weakening thresholds would have manufactured a pass
- **Residual risk:** The one-hour GO is not a 24-hour, multi-day, authenticated-feed, external-sink, or unattended-production proof
- **Rollback trigger:** Any semantic mismatch in depth samples, monotonic leak escaping HOLD, book invalidation/drop increase, or resource regression

## Reusable Lesson

- **Pattern to retain:** Separate live heap from resident pages, test the bounded invariant, and keep source failures attributable
- **Pattern to avoid:** Inferring a leak or a clean source from one short straight-line RSS fit or an aggregate reconnect counter
- **Where it applies next:** The 24-hour beta gate and future long-lived analytics projections
