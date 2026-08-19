# marketfeed-private

**Alpha** — Phase 6 private account data (spec §26.4 / §33 Phase 6 / ADR-009).

Public market-data operation must never require credentials or this crate.

## What this milestone has (C6b + C6c + W5-P1d)

- Account event types: `Balance`, `BalanceDelta`, `OrderUpdate`, `Fill`, `Position`
- [`PrivateSessionMachine`] — mirror of public `SessionMachine`, reusing
  `SessionInput` / `SessionAction` (including `RequestHttp` / `SendText`) for wire ops
- **Binance Spot user-data migration scaffold:**
  - decodes current WebSocket API `subscriptionId` / nested `event` payloads
  - connection and live-runner entry points fail closed before transport I/O
  - authenticated `userDataStream.subscribe.signature` is not implemented yet
- **OKX Spot / Bybit Spot** private fixture state machines (`SendText` auth +
  account/orders/fills decode)
- **C6c live wire (library, feature `live`):**
  - Credentials from env only; `Debug` redacts secrets
  - Binance: blocked pending authenticated WebSocket API subscription support
  - OKX: `OKX_API_KEY` / `OKX_API_SECRET` / `OKX_API_PASSPHRASE`; HMAC login on
    private WS (`wss://ws.okx.com:8443/ws/v5/private`)
  - Bybit: `BYBIT_API_KEY` / `BYBIT_API_SECRET`; HMAC auth on private WS
    (`wss://stream.bybit.com/v5/private`)
  - Live runners drain account and operational system events through the
    caller-provided `AccountEventSink`; fixed-duration smoke wrappers use
    `NullAccountSink`
  - Remote close is an error so the caller must own reconnect supervision
  - Daemon enablement is rejected until a bounded durable account sink,
    readiness/liveness tracking, and reconnect supervision are implemented
  - `#[ignore]` tests in `live_ignored` — short / extended / reauth filters
- Laptop archive helper: [`scripts/laptop_private_canary.sh`](../../scripts/laptop_private_canary.sh)
  (skips venues whose keys are missing; no secrets in git)
- Offline tests under `tests/` (fixtures only)

## Explicitly out of scope

- Order entry or execution paths
- Recording private payloads by default
- Daemon private-session supervision and durable account-event delivery
- Committing `.env` / logging API keys (`.env` is gitignored; see `.env.example`)

## Run offline tests

```bash
cargo test -p marketfeed-private
cargo test -p marketfeed-private --features live
```

## Run live private smokes

Requires venue API keys in the environment (never commit them). Copy the template:

```bash
cp .env.example .env   # fill venue keys; never commit .env
set -a && source .env && set +a
```

| Filter | Needs | Notes |
|---|---|---|
| `live_*_login_and_ws` / `live_*_auth_and_ws` | venue keys | Short smoke (`PRIVATE_LIVE_SECS`, default 5) |
| `live_*_extended` | venue keys | Longer idle (`PRIVATE_LIVE_EXTENDED_SECS`, default 30); account events optional |
| `live_*_reauth_probe` | venue keys | MarkLive → close → fresh session → MarkLive (not engine kill-switch) |

Binance private streaming is intentionally fail-closed. Binance retired the
listen-key protocol that this repository previously used; the replacement
authenticated WebSocket API flow is not implemented yet.

```bash
# all ignored private live tests:
cargo test -p marketfeed-private --features live --test live_ignored -- --ignored --nocapture

# one supported venue:
cargo test -p marketfeed-private --features live --test live_ignored \
  live_okx_private_login_and_ws -- --ignored --nocapture

# laptop archive (SKIP venues with missing keys; exit 0 when all skipped):
./scripts/laptop_private_canary.sh
INCLUDE_EXTENDED=1 INCLUDE_REAUTH=1 ./scripts/laptop_private_canary.sh
DRY_RUN=1 ./scripts/laptop_private_canary.sh
```

Evidence: [`docs/ops/private_canary_results.md`](../../docs/ops/private_canary_results.md).
Missing keys are a clean skip, not a failure. **No order placement.**

