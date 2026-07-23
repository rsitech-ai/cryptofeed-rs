# Multi-exchange orchestration packet

**Status:** coordination (Phase 3 venue workers)  
**Spec SoT:** `docs/spec/production_rust_multi_exchange_market_data_spec.md`  
**Date:** 2026-07-21  
**Orchestration branch:** `feat/andrzej_exchange_orchestration` (plan only)

---

## 1. Worker base branch (mandatory)

| Fact | Value |
|---|---|
| **Base for all Phase 3 workers** | `origin/feat/andrzej_binance_spot` @ `530f244` |
| Stack on that tip | `main` ← domain ← engine ← **Binance Spot** |
| Local-only (do **not** base on) | `feat/andrzej_binance_derivatives` (USD-M + daemon; **not pushed**) |
| Force-push | **Forbidden** on `feat/andrzej_binance_derivatives` and on any worker branch another agent owns |

**Why not derivatives:** that branch adds daemon + Binance USD-M and is still local. Basing Phase 3 venues on it couples workers to unfinished/in-flight Binance work and thrashy `Cargo.toml` / daemon wiring.

Workers open PRs targeting the current merge tip of the stack once Binance Spot lands (or rebase onto `origin/feat/andrzej_binance_spot` until then).

---

## 2. Shared contract checklist (every venue PR)

Copy this into the PR body. All items are required for “experimental → ready for review”.

### Architecture

- [ ] Adapter is `SessionInput -> SessionMachine::on_input -> SessionAction` only
- [ ] **No networking** in the adapter crate (no sockets, `tokio::spawn`, sleep, disk, global logging, sink publish)
- [ ] Implements `VenueFactory` (`specification`, `instrument_requests`, `parse_instruments`, `plan`, `create_session`)
- [ ] Exact decimals — never `f64` as source of truth; trade side = aggressor/taker
- [ ] Timestamps normalized to `TimestampNs`
- [ ] Bounded delta buffers; overflow → invalidate / resync actions (no silent drop)
- [ ] Malformed remote input → structured `AdapterError` / system events, **never panic**

### Book sync (document in README or `book.rs` comments)

Per §16.3, each venue MUST document and fixture-test:

- Snapshot source + whether deltas may precede snapshot
- First valid delta rule, sequence range, duplicate / OOO / gap rules
- Checksum algorithm (if any) + canonicalization
- Resubscription / same-session snapshot requirements
- Sequence scope: per-instrument vs per-connection
- Buffering limits (count / bytes / time)

Reuse `marketfeed-book` for validated L2 maintenance — do **not** reimplement order-book storage.

### Tests & packaging

- [ ] Offline golden fixtures under `crates/adapters/<venue>/tests/fixtures/` (or `*_fixtures.rs`)
- [ ] Fixture coverage for: trades, quote/BBO (if claimed), L2 sync path (snapshot + deltas + gap)
- [ ] `#[ignore]` live canary in `tests/live_ignored.rs` (CI stays offline)
- [ ] Workspace member + `default-members` entry for the new crate
- [ ] Package name: `marketfeed-adapter-<venue>`
- [ ] `unsafe_code = "forbid"`
- [ ] Depend only on: `marketfeed-model`, `marketfeed-adapter-api`, `marketfeed-book`, plus serde/bytes as needed  
  Dev-deps MAY use engine/transport/replay for the ignored live test (mirror Binance Spot)

### Maturity target for this wave

**Experimental → Beta path.** Do not claim `stable`. Document known limitations in the adapter `README.md`.

---

## 3. What NOT to duplicate

| Reuse | Do not rebuild |
|---|---|
| `marketfeed-book` | Local BTree/hash book, sync state machine for storage |
| `marketfeed-engine` / supervisor | Session task loops, reconnect timers, I/O |
| `marketfeed-transport` | WebSocket / HTTP clients |
| `marketfeed-recording` / `marketfeed-replay` | Custom WAL / replay runners |
| `marketfeed-dispatch` | Event fan-out / backpressure queues |
| `marketfeed-model` / `marketfeed-adapter-api` | Parallel event enums or alternate SessionMachine traits |
| Binance Spot layout | Copy patterns; do **not** fork Binance message parsers into other venues |

Daemon session wiring is owned by the derivatives/daemon agent — Phase 3 workers ship **library adapters only** unless explicitly asked to register in daemon config.

---

## 4. Per-venue v1 scope

Spec Phase 3 order: OKX → Bybit → Kraken → Deribit (Coinbase later).  
v1 = enough for **beta candidacy** on primary public channels; not the full §2.1 matrix on day one.

### 4.1 OKX — `feat/andrzej_okx`

| | |
|---|---|
| **Crate** | `crates/adapters/okx` → `marketfeed-adapter-okx` |
| **Segments** | Spot + SWAP (perp) + Futures (dated) — one crate, multiple factories or segment flags OK |
| **v1 channels** | Instruments (REST), trades, books (L2 snapshot/delta + checksum if venue provides), bbo/ticker, mark/index, funding; OI + liquidations if cheap on same session model |
| **Defer** | Options, candles-only stretch, private streams |
| **Acceptance** | Fixture: instrument parse; trade aggressor; L2 sync to Live; sequence/checksum gap → `ResyncInstrument`/`Reconnect`; unknown msg no panic; `#[ignore]` live trade or book tick |
| **Differentiator** | OKX checksum + channel/arg subscription model — document explicitly |

### 4.2 Bybit — `feat/andrzej_bybit`

| | |
|---|---|
| **Crate** | `crates/adapters/bybit` → `marketfeed-adapter-bybit` |
| **Segments** | Spot + linear/inverse derivatives (v1: spot + linear perp minimum) |
| **v1 channels** | Instruments, trades, orderbook snapshot/delta, ticker/BBO, mark/index, funding |
| **Defer** | Full inverse matrix, options |
| **Acceptance** | Same checklist as OKX; fixture corpus for spot + one linear perp |
| **Differentiator** | Topic subscription + snapshot/delta sequence rules (document first-delta) |

### 4.3 Kraken — `feat/andrzej_kraken`

| | |
|---|---|
| **Crate** | `crates/adapters/kraken` → `marketfeed-adapter-kraken` |
| **Segments** | Spot + Futures (separate endpoints/factories as needed) |
| **v1 channels** | Spot: instruments, trades, book, ticker; Futures: trades + book + mark/funding where public |
| **Defer** | Full futures product matrix stretch goals |
| **Acceptance** | Spot L2 sync fixtures mandatory; futures at least trades + one book path or documented limitation |
| **Differentiator** | Spot vs Futures protocol split — keep factories separate if endpoints diverge |

### 4.4 Deribit — `feat/andrzej_deribit`

| | |
|---|---|
| **Crate** | `crates/adapters/deribit` → `marketfeed-adapter-deribit` |
| **Segments** | Perpetual + dated futures (options **out of v1**) |
| **v1 channels** | Instruments, trades, book, ticker, mark/index, funding |
| **Defer** | Options, combo instruments, private |
| **Acceptance** | Perp L2 sync + trade fixtures; futures instrument mapping deterministic |
| **Differentiator** | JSON-RPC style subscribe/heartbeat — drive via `SessionAction::{SendText,ScheduleTimer}` only |

### Reference layout (match Binance Spot spirit)

```text
crates/adapters/<venue>/
├── Cargo.toml
├── README.md          # sync rules + limitations
├── src/
│   ├── lib.rs
│   ├── specification.rs
│   ├── instruments.rs
│   ├── factory.rs
│   ├── session.rs
│   └── messages.rs    # (+ channels/book helpers as needed)
└── tests/
    ├── fixtures.rs    # offline SessionMachine drive
    ├── live_ignored.rs
    └── fixtures/      # raw JSON samples
```

---

## 5. Merge order (avoid `Cargo.toml` thrash)

Stack already in flight (open PRs): domain → engine → **binance_spot**.

### 5.1 Recommended land order (updated)

1. **Binance Spot** (`feat/andrzej_binance_spot`) — already open; land first.
2. **Shared blockers** (tiny PRs, before or between venues):
   - **VenueId collision cleanup** (in progress) — merge early so all adapters rebase once.
   - **`feat/andrzej_engine_timers`** (`ScheduleTimer` wiring) — only when it has commits *ahead of* its base; as of this board it still sits on the derivatives tip with **no unique commits**. Hold PR until timers land on an engine-based tip.
3. **OKX** (`feat/andrzej_okx`) — fullest Phase 3 L2 slice.
4. **Bybit** (`feat/andrzej_bybit`) — spot + linear L2.
5. **Kraken + Deribit** (`feat/andrzej_kraken_deribit`) — combined PR; experimental (no L2 yet). Prefer one PR, not two, while they share a branch.
6. **Binance USD-M + daemon** (`feat/andrzej_binance_derivatives`) — after Spot; rebase onto whatever venue already edited root `Cargo.toml`.
7. **Spec audit** (`feat/andrzej_spec_audit`) — docs-only; can merge anytime after Spot (low conflict risk).

### 5.2 Open PRs stacked on Spot?

**Yes — open venue PRs targeting the Spot tip (or `main` once Spot merges), not stacked on each other.**

| Branch | Open PR now? | Target |
|---|---|---|
| `feat/andrzej_okx` | **Yes** when ready for review | Spot tip / post-Spot `main` |
| `feat/andrzej_bybit` | **Yes** (parallel open, serial merge) | same |
| `feat/andrzej_kraken_deribit` | **Yes**, label experimental / no-L2 | same |
| `feat/andrzej_binance_derivatives` | **Wait** until Spot landed + VenueId fix known; then rebase | post-Spot (+ venues if already merged) |
| `feat/andrzej_engine_timers` | **No** until unique commits exist on an engine/Spot base | engine or post-Spot |
| `feat/andrzej_spec_audit` | Optional docs PR | any stack tip |
| orchestration | No push unless asked | — |

**Parallelism rule:** develop in parallel from Spot; **merge one adapter at a time**. Loser rebases and re-applies the single-line workspace member edit.

**Cargo.toml edit convention** (minimize conflicts):

```toml
# append only, never reorder existing members:
"crates/adapters/okx",
"crates/adapters/bybit",
"crates/adapters/kraken",
"crates/adapters/deribit",
```

Optional `workspace.dependencies` path alias — only if another crate needs it; prefer path deps inside the adapter’s own `Cargo.toml` for now.

---

## 6. Branch / PR hygiene

| Branch | Owner | Touches |
|---|---|---|
| `feat/andrzej_okx` | OKX worker | `crates/adapters/okx/**`, root `Cargo.toml` (+ lockfile) |
| `feat/andrzej_bybit` | Bybit worker | `crates/adapters/bybit/**`, root `Cargo.toml` (+ lockfile) |
| `feat/andrzej_kraken_deribit` | Kraken/Deribit worker | `crates/adapters/kraken/**`, `crates/adapters/deribit/**`, root `Cargo.toml` |
| `feat/andrzej_binance_derivatives` | Separate agent | Binance USD-M + daemon — **do not force-push** |
| `feat/andrzej_engine_timers` | Engine worker | engine `ScheduleTimer` — keep off derivatives tip when ready |
| `feat/andrzej_spec_audit` | Audit | `docs/plan/audit_multi_exchange.md` |
| `feat/andrzej_exchange_orchestration` | Orchestrator | This plan doc only |

PR title style: `feat: <Venue> adapter with <primary channels>`.

Do **not** change `adapter-api` / `model` / `book` / `engine` unless a missing capability blocks **all** venues — then open a tiny shared PR first and notify workers to rebase.

---

## 7. Git state snapshot (2026-07-21 evening)

| Ref | Tip | Remote? |
|---|---|---|
| `origin/main` | bootstrap + spec | yes |
| `origin/feat/andrzej_domain_foundations` | model, adapter-api, book, synthetic | yes (PR #1) |
| `origin/feat/andrzej_engine_runtime` | dispatch, transport, recording, replay, engine | yes (PR #2) |
| `origin/feat/andrzej_binance_spot` | Binance Spot L2 | yes (PR #3) **← venue base** |
| `feat/andrzej_okx` | `fd9d2ec` Spot trades/tickers/books L2 | local / worktree |
| `feat/andrzej_bybit` | `381cd27` linear/spot trades+L2 | local / worktree |
| `feat/andrzej_kraken_deribit` | `9e1359c` Kraken + Deribit (no L2) | local / worktree |
| `feat/andrzej_binance_derivatives` | `2617ffc` USD-M + daemon sessions | local / worktree |
| `feat/andrzej_engine_timers` | same tip as derivatives (no unique commits yet) | local / worktree |
| `feat/andrzej_spec_audit` | `1b75c10` audit doc | local |
| `feat/andrzej_venue_ids` | canonical VenueId map | local / worktree |

---

## 8. Delivery status board (2026-07-21)

| Branch | Worktree | Commit | Status |
|---|---|---|---|
| `feat/andrzej_okx` | `cryptofeed-okx-wt` | `fd9d2ec` | **Ready for PR** — Spot trades / tickers / books L2; based on Spot (+1) |
| `feat/andrzej_bybit` | `cryptofeed-bybit` | `381cd27` | **Ready for PR** — V5 linear/spot trades + L2 u-sync; based on Spot (+1) |
| `feat/andrzej_kraken_deribit` | `cryptofeed-kraken-deribit` | `9e1359c` | **PR as experimental** — Kraken trades/ticker; Deribit trades + deriv fields; **no L2** |
| `feat/andrzej_binance_derivatives` | `cryptofeed-derivatives-wt` | `2617ffc` | **Hold merge** — USD-M + daemon session wiring; rebase after Spot (+ VenueId) |
| `feat/andrzej_engine_timers` | `cryptofeed-engine-timers` | `2617ffc` (shared tip) | **In progress** — `ScheduleTimer` wiring; no unique commits yet |
| `feat/andrzej_spec_audit` | main worktree (was) | `1b75c10` | Audit doc vs production spec |
| `feat/andrzej_venue_ids` | `cryptofeed-venue-ids` | (doc) | **Canonical VenueId map** — see [`venue_ids.md`](./venue_ids.md) |
| `feat/andrzej_exchange_orchestration` | — | this doc | Coordination only |

**Wave read:** three venue families have adapter commits on the Spot base. OKX + Bybit carry L2; Kraken/Deribit need a follow-up for books. VenueId collisions resolved (canonical map 1–8). Remaining gaps: engine timers, daemon/derivatives rebase.

---

## 9. Canonical VenueId map

**SoT:** [`docs/plan/venue_ids.md`](./venue_ids.md) (`feat/andrzej_venue_ids`).

| Id | Venue |
|---:|-------|
| 1 | synthetic |
| 2 | binance-spot |
| 3 | binance-usdm |
| 4 | okx-spot |
| 5 | bybit-linear |
| 6 | bybit-spot |
| 7 | kraken-spot |
| 8 | deribit |

Claim the next free id in `venue_ids.md` before coding a new venue.

---

## 10. Orchestrator out of scope

- Implementing OKX / Bybit / Kraken / Deribit adapters
- Pushing or opening PRs for this plan unless asked
- Force-pushing or rewriting sibling worker / derivatives branches
