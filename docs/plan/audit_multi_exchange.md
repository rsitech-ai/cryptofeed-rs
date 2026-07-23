# Audit: Multi-Exchange Market Data Engine vs Production Spec

**Auditor role:** validator / architecture compliance  
**Spec:** [`docs/spec/production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md) v1.0  
**Audit date:** 2026-07-21  
**Audit branch:** `feat/andrzej_spec_audit`  
**Audited tip:** `68ac51f` (`feat/andrzej_binance_derivatives` — richer than `origin/feat/andrzej_binance_spot`)  
**Scope:** architecture rules, adapter contract, Binance Spot/USD-M, daemon Phase 2, production-readiness honesty; **no feature implementation**

### Branch map (at audit time)

| Ref | Tip | Contents relative to audit |
|---|---|---|
| `origin/main` | `64369eb` | Spec + bootstrap only |
| `origin/feat/andrzej_binance_spot` | `530f244` | Domain → engine → Binance Spot L2 |
| Local `feat/andrzej_binance_derivatives` | `68ac51f` | Spot + USD-M + optional daemon shell (**audit base**) |
| `feat/andrzej_spec_audit` | this branch | Audit document only |

Workspace tests on audited tip: **pass** (`cargo test --workspace`).

---

## 1. Executive summary

The library-first core is real and largely aligned with ADRs 001–008 for a Phase 0 / early Phase 1 slice: `SessionMachine` purity holds, Fixed is canonical, L2 invalidation/resync exists for Binance Spot and USD-M, dispatch queues are bounded, raw record/replay path exists, `#![forbid(unsafe_code)]` is consistent.

**Not production-ready.** Phase 1 exit (Binance family at **beta**) and Phase 2 exit (operational daemon) are **not met**. The daemon is a health/metrics shell that does not start venue sessions. Engine observability (§23.2), silent-drop diagnostics, timer/heartbeat execution, sinks, protobuf, CI/release, and multi-family coverage are missing or incomplete.

### Blockers (must clear before claiming beta / production shell)

1. **Daemon does not drive the engine** — `marketfeed run` binds `/live` `/ready` `/metrics` but does not start adapters/sessions (explicit warn in `main.rs`).
2. **No silent-drop guarantee under Drop* policies** — `SessionRunner` ignores `PushOutcome::{DroppedNewest,DroppedOldest}`; no `SystemEvent::EventsDropped` / metric.
3. **§23.2 metrics absent from engine/adapters** — daemon exposes only process gauges; reconnect/gap/invalidation/parse/queue counters not exported.
4. **Engine does not execute `ScheduleTimer` / `CancelTimer`** — actions are parked in `other_actions`; live loop ignores Ping/Pong/Binary; heartbeat liveness incomplete.
5. **Unbounded runner mirrors** — `SessionRunner::{market_batches,system_events,other_actions}` grow without bound (violates “every queue/buffer has an explicit bound” for long live loops).
6. **Phase 1 maturity not beta** — no replay corpus packaging, no scheduled live canary, no soak, incomplete capability matrix/docs; Spot lacks candles; no inverse/coin-margined Binance segment.
7. **Zero CI / supply-chain / release gates** — no `.github`, no `cargo-deny`/`audit`, no LICENSE files, no SBOM/provenance.
8. **Fewer than three exchange families** — only synthetic + Binance; Phase 3 venues not present on this tip.

---

## 2. Architecture principles — pass / fail / gap

| # | Principle (spec §5) | Verdict | Evidence / gap |
|---|---|---|---|
| 1 | Engine owns I/O | **Pass** | Adapters emit `RequestHttp` / `SendText`; `engine::live` owns WS/HTTP. Networking in adapters limited to ignored live tests. |
| 2 | Adapters are deterministic SMs | **Pass** | `SessionMachine::on_input` → `ActionBuffer`; Spot/USD-M/synthetic match contract. |
| 3 | Hot state one owner | **Pass** (minor gap) | Per-session books in adapter; no global mutable maps. Catalog is immutable view. |
| 4 | All queues bounded | **Gap** | `dispatch::BoundedQueue` + book depth buffers bounded. **Fail-adjacent:** runner outcome Vecs unbounded; `ActionBuffer` unbounded; overflow policies `LatestPerKey`/`SpillToDisk`/`DisableSink` unimplemented. |
| 5 | Correctness ≻ availability | **Pass** | Gap → `BookInvalidated` + reconnect; buffer overflow invalidates; atomic snapshot apply in `OrderBook`. |
| 6 | No global ordering claim | **Pass** | Model/session scoped; no cross-venue sequencer. |
| 7 | Exact arithmetic canonical | **Pass** | `Fixed` parse path; adapters document no f64; only `to_f64_lossy` convenience. |
| 8 | Raw data recoverable | **Gap** | Raw segment writer/reader + stamp-before-normalize in runner. Default recorder is in-memory `Cursor`; no disk rotation/WAL in engine path. |
| 9 | Replay uses same adapters | **Pass** | `replay::ReplayRunner` feeds `SessionMachine`; engine integration test covers record→replay. |
| 10 | Public model independent of venue payloads | **Pass** | `MarketEvent` / envelopes in `model`. |
| 11 | Venue-specific without polluting events | **Gap** | No structured venue extensions / opaque metadata channel yet; unknown messages → system events only. |
| 12 | Unsafe isolated / forbidden | **Pass** | `#![forbid(unsafe_code)]` on project crates. |
| 13 | Operational failure observable | **Fail** | System events exist for many paths; **metrics + tracing fields (§23.1–23.2) not implemented** in libraries. Drop policies can lose events without diagnostics. |
| 14 | Dependencies replaceable | **Pass** (partial) | Transport traits exist; only tungstenite/reqwest backends. |
| 15 | Useful without daemon | **Pass** | Daemon optional; library crates stand alone. |

**ADR snapshot:** ADR-001…005 largely honored in code shape; ADR-006 partially violated by silent Drop* outcomes; ADR-010 (Protobuf) absent; ADR-011 baseline incomplete; ADR-013 license text files missing despite Cargo metadata.

---

## 3. Per-crate findings

Severity: **blocker** | **major** | **minor**

### `marketfeed-model`

| Sev | Finding |
|---|---|
| minor | `Fixed::to_f64_lossy` present (documented non-canonical) — acceptable if never used in adapters. |
| minor | Full event surface (Candle, etc.) exists ahead of adapters — good; unused paths untested. |

### `marketfeed-adapter-api`

| Sev | Finding |
|---|---|
| **Pass** | `VenueFactory` / `SessionMachine` / inputs / actions match §11.2–11.3. |
| major | No shared adapter test-kit crate (§11.7) — each venue reinvents fixtures. |
| minor | `SessionCommand` subscribe strings are opaque; typed channel subscribe not enforced at API. |

### `marketfeed-book`

| Sev | Finding |
|---|---|
| **Pass** | Validity lifecycle Valid/Synchronizing/Invalid; bounded delta buffer; gap → Invalid. |
| major | Shared `BookSynchronizer` assumes consecutive single-id sequences; Binance uses custom U/u (and USD-M `pu`) logic in adapters — dual models risk divergence. Prefer documenting “venue bridge owns range sync; shared helper is sequential-only” or extracting Binance bridge helpers. |
| minor | No checksum verification helper (venues that need it will reimplement). |

### `marketfeed-dispatch`

| Sev | Finding |
|---|---|
| **Pass** | Bounded queues; DropNewest/Oldest/FailEngine tested. |
| **blocker** | Drop outcomes do not auto-emit `EventsDropped` / metrics — callers must; engine does not. |
| major | `BlockWithDeadline` busy-waits (`yield_now`) — not production-safe under stall. |
| major | `LatestPerKey` / `SpillToDisk` / `DisableSink` enumerated but unsupported. |

### `marketfeed-transport`

| Sev | Finding |
|---|---|
| **Pass** | Rustls/webpki; no insecure fallback in workspace deps. |
| major | Live loop does not surface Ping/Pong to `SessionMachine` (empty arms in `live.rs`). |
| minor | `MemoryWebSocket` inbound `VecDeque` unbounded (test-only — document). |

### `marketfeed-recording` / `marketfeed-replay`

| Sev | Finding |
|---|---|
| **Pass** | Raw segment format + CRC; replay through same machine. |
| major | No normalized recording (§18.5); no disk rotation / multi-segment; no crash-recovery tests. |
| minor | Outbound recording shares inbound seq counter (ponytail noted). |

### `marketfeed-engine`

| Sev | Finding |
|---|---|
| **Pass** | Stamp → record → machine → dispatch order; reconnect/backoff loop. |
| **blocker** | Unbounded `market_batches` / `system_events` / `other_actions` accumulation. |
| **blocker** | Drop* `PushOutcome` ignored — silent loss risk under non-FailEngine policies. |
| **blocker** | Timers not scheduled/fired; heartbeat policy inert. |
| major | No metrics snapshot API for daemon; mono clock is wall-clock stand-in. |
| major | Supervisor is thin; no catalog refresh loop, dynamic subscribe control plane, or multi-session sink fan-out. |
| minor | Binary frames dropped on live path. |

### `marketfeed-adapter-synthetic`

| Sev | Finding |
|---|---|
| **Pass** | Phase 0 exit largely met for trades/book/disconnect/reconnect/record/replay. |
| minor | Not a full §11.6 directory template; adequate as mock venue. |

### `marketfeed-adapter-binance` (Spot)

| Sev | Finding |
|---|---|
| **Pass** | No I/O in adapter; Fixed parsing; trade/quote/L2 snapshot+delta; gap invalidates; bounded depth buffer; fixtures + L2 buffer tests; ignored live smoke. |
| major | Capability matrix incomplete vs Phase 1: no candles; dynamic subscribe/unsubscribe limited; planner ignores subscription set when catalog empty (hardcodes `btcusdt`). |
| major | Heartbeat timers not emitted; relies on venue silence / engine reconnect only. |
| major | Maturity = **experimental→early alpha**, not beta (§11.8): no packaged replay corpus, soak, runbook, canary schedule. |
| minor | Adapter README / ownership / limitations doc missing. |

### `marketfeed-adapter-binance` (USD-M)

| Sev | Finding |
|---|---|
| **Pass** | Trades (`aggTrade`), quote, mark+index+funding from markPrice, L2 with `pu`, OI REST, liquidations (`forceOrder`); fixtures cover happy paths + reconnect on gap. |
| major | No inverse/coin-M segment; no dated-futures-specific planning beyond linear catalog parse; candles absent. |
| major | OI is one-shot/on-connect REST — no timer-driven refresh (blocked by engine timer gap). |
| major | Same maturity gap as Spot (corpus/canary/soak/docs). |
| minor | Shared factory/session duplication Spot vs USD-M — fine for now; extract only if third Binance segment lands. |

### `marketfeed-daemon`

| Sev | Finding |
|---|---|
| **Pass** | Config validation; JSON/text tracing installed only in binary; `/live` `/ready` `/metrics`; readiness policy unit-tested. |
| **blocker** | Does not compose engine sessions from config (Phase 2 shell incomplete). |
| major | Metrics are daemon-local stubs, not §23.2. |
| major | No protobuf, normalized recorder, replay/inspect CLI, WAL sink, container, runbook. |
| minor | Hand-rolled CLI (no clap) — acceptable ponytail until Phase 2 CLI grows. |

### Repo / tooling (workspace root)

| Sev | Finding |
|---|---|
| **blocker** | No CI workflows; no `deny.toml` / advisory gate; no LICENSE/NOTICE/SECURITY/CONTRIBUTING. |
| major | No fuzz targets, Loom tests, chaos harness, soak harness. |
| minor | README accurately states “session loops not wired”. |

---

## 4. Binance Spot / USD-M vs Phase 1

Phase 1 deliverables (spec §33):

| Deliverable | Spot | USD-M | Notes |
|---|---|---|---|
| Spot instruments | Yes | n/a | `exchangeInfo` parse |
| USD-M perp/futures instruments | n/a | Partial | Linear USDT-M; inverse not present |
| Trades | Yes | Yes (`aggTrade`) | |
| Quote | Yes | Yes | |
| L2 snapshot/delta sync | Yes | Yes (`pu`) | Fixtures for gap→reconnect |
| Mark / index | n/a | Yes | From `markPriceUpdate` |
| Funding | n/a | Yes | |
| Open interest | n/a | Partial | REST; not periodic |
| Liquidations | n/a | Yes | `forceOrder` |
| Dynamic subscription planning | Partial | Partial | Combined-stream URL; limits in spec struct; little enforcement/tests |
| Live canary | Ignored test only | Same | Not scheduled |
| **Exit: beta maturity** | **Fail** | **Fail** | Corpus/soak/canary/docs missing |
| **Exit: deterministic replay corpus** | **Gap** | **Gap** | Machinery exists; no checked-in corpus |
| **Exit: no silent drop under stress** | **Fail** | **Fail** | Engine Drop* path |

Candles (v1 product boundary §2.1) — **not implemented** for either surface.

---

## 5. Daemon vs Phase 2 gates

| Phase 2 deliverable | Status |
|---|---|
| Standalone daemon | **Partial** — binary exists, does not run market data |
| Configuration validation | **Pass** |
| Metrics and structured logs | **Partial** — tracing OK; metrics stub |
| Health / readiness endpoints | **Pass** (process-level; readiness can pass without venues when `require_required_venues=false`) |
| Protobuf schema | **Fail** — absent |
| Normalized recorder | **Fail** — absent |
| CLI replay and inspection | **Fail** — only `validate` / `run` / `version` |
| Lossless file/WAL sink | **Fail** — absent |
| Container and release pipeline | **Fail** — absent |
| Exit: operational runbook | **Fail** |
| Exit: disk-full / sink-stall validated | **Fail** |
| Exit: reproducible RC | **Fail** |

---

## 6. Production-readiness gates not yet met (§3 / §36)

Honest checklist — **none** of the §3 success criteria are fully met:

1. ≥3 exchange families (spot+derivs) — **no** (Binance only + synthetic).
2. ≥2 adapters at `stable` — **no** (none at beta).
3. L2 deterministic sequence/gap/checksum/snapshot/replay tests — **partial** (gap/snapshot fixtures; no checksum venues; no corpus CI).
4. Every queue/buffer/cache/segment bounded — **no** (runner mirrors; some policies unimplemented).
5. No silent data drops — **no**.
6. Every reconnect/resync/invalidation/overflow/parse → metric + diagnostic — **no** (diagnostics partial; metrics missing).
7. Continuous soak without unbounded memory — **no**.
8. Chaos (disconnect, malformed, snapshot fail, slow sink, disk-full, clock jump) — **no**.
9. Dependency/license/vuln/API/provenance release artifacts — **no**.
10. Public API / config schema / recording / maturity matrix documented — **partial** (spec exists; maturity matrix / ops docs missing).

§36 engine 1.0 checklist is correspondingly unmet across API review, multi-family, pipelines, soak, chaos, SBOM, runbooks, ownership.

---

## 7. Required acceptance criteria — OKX / Bybit / Kraken / Deribit workers

Workers MUST treat the following as **done = mergeable experimental adapter**, and separately gate **beta** promotion. Do not claim Phase 3 exit until three families are beta+.

### 7.1 Hard architecture gates (merge blockers)

1. **No networking in adapter crate** (except `#[ignore]` live tests that use `marketfeed-transport` / engine).
2. Implement `VenueFactory` + `SessionMachine` only; all I/O via actions.
3. **No `f64` in parse→model path**; use `Fixed::parse_str` / shared decimal helper.
4. All pre-snapshot delta / message buffers **bounded** (count + bytes); overflow → invalidate + `BookInvalidated` + resync/reconnect action — never truncate middle.
5. Malformed frames → `ParseError` / `UnknownMessage` system events; **no panics**.
6. `#![forbid(unsafe_code)]`; no global mutable state; no logging subscriber install.
7. Unit/fixture tests offline; deterministic given fixtures.
8. Workspace member + README capability row; known limitations listed.

### 7.2 Channel / correctness gates (per claimed capability)

For each capability advertised in `VenueSpecification.capabilities`:

| Capability | Minimum acceptance |
|---|---|
| Instruments | REST discovery + deterministic native→`InstrumentId` mapping; scales from venue filters |
| Trades | Fixture with aggressor side verified against venue docs |
| Quote | BBO fixture; optional qty |
| L2 | Documented snapshot rule; first-delta rule test; duplicate test; gap → invalid + reconnect; buffer overflow → invalid |
| Mark / index / funding | Exact Fixed; ts → ns |
| OI / liquidations | Typed fixtures; REST or WS as venue requires |
| Candles | Only if claimed; interval mapping tested |
| Heartbeat | Adapter emits `ScheduleTimer` / ping actions **or** documents engine-owned venue timeout once engine timers land |
| Subscribe limits | Planner never exceeds `SubscriptionConstraints`; test |
| Ack / reject | Subscribe ack/error handled without panic |

### 7.3 Venue-specific must-cover (workers)

| Venue | Must prove early (distinct from Binance) |
|---|---|
| **OKX** | Unified/business channel login-free public; checksum books where applicable; instType spot/SWAP/FUTURES; arg-based subscribe batching |
| **Bybit** | v5 topic model; snapshot/delta seq; category spot/linear/inverse; ping/pong JSON heartbeats |
| **Kraken** | Spot vs futures (separate endpoints); token/checksum or sequence rules per book; symbol normalization (`XBT` vs `BTC`) |
| **Deribit** | heartbeat request/response protocol; incremental book + change_id; perpetual vs dated futures instrument keys |

### 7.4 Beta promotion (not required for first PR; required before “family done”)

Per §11.8 + §35:

- Packaged raw replay corpus in CI.
- Ignored→scheduled live canary for primary channels.
- Book corruption issues closed.
- Capability matrix + ops limitations documented.
- Soak ≥ declared duration with RSS bound.
- Named adapter owner.

### 7.5 Explicit non-goals for workers

- Do not implement daemon sinks, protobuf, or CI pipelines inside venue PRs.
- Do not add networking “just for convenience.”
- Do not widen core API without an RFC if SessionMachine actions are insufficient — prefer actions first.

---

## 8. Recommended fix order

1. **Engine correctness before more venues**  
   - Bound or remove runner mirror Vecs (consume via dispatcher only).  
   - Honor `PushOutcome` → emit `EventsDropped` + fail or metric.  
   - Implement timer schedule/fire + forward Ping/Pong/Binary to the machine.  
   - Prefer default `FailEngine` until drop diagnostics exist.

2. **Wire daemon → supervisor → sessions**  
   - Config venues start Spot/USD-M (and synthetic) loops.  
   - Ready = required venues live; scrape real reconnect/gap counters.

3. **Minimal §23.2 metric export** from engine (even a snapshot struct) so daemon `/metrics` is not fiction.

4. **Binance Phase 1 closeout**  
   - Checked-in replay corpus + CI job.  
   - Periodic OI timer once timers work.  
   - Candles only if still in v1 scope; else document deferral ADR.  
   - Maturity matrix row: experimental → beta after canary/soak.

5. **Phase 2 remainder** (after shell actually feeds data)  
   - Disk raw recorder + rotation; normalized recorder; replay/inspect CLI; WAL/file sink; disk-full tests; container; runbook.

6. **Tooling gates**  
   - LICENSE files, `cargo-deny`, PR CI (fmt/clippy/test/MSRV), SECURITY.md.

7. **Phase 3 venue workers** (OKX → Bybit → Kraken → Deribit) using §7 acceptance criteria — **only after** items 1–2 so workers do not paper over engine holes.

8. **Defer** latency profile, simd-json, second WS stack, protobuf cross-language until three families at beta (spec Phase 4/5 intent).

---

## 9. Audit method notes

- Compared tree at `68ac51f` to spec §§3–5, 11–18, 23, 27–28, 33–36.
- Grep/read for networking in adapters, `f64`, bounds, invalidation, daemon Phase 2 surface.
- Ran `cargo test --workspace` on audited tip (pass).
- Did not implement fixes beyond this document.

**Verdict:** Architecture direction is sound and Binance Spot/USD-M prove the adapter pattern, but **production / Phase 2 / multi-exchange claims are not yet earned**. Prioritize engine bounds + diagnostics + daemon session wiring, then finish Binance beta, then fan out venues under the acceptance criteria above.
