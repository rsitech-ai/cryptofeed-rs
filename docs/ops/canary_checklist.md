# Live canary checklist (Binance Spot + OKX Spot)

**Status:** checklist only — **does not** claim beta  
**Maturity after offline close-out:** `alpha+` / "beta-ready offline"  
**Owner:** `@s1korrrr` ([`CODEOWNERS`](../../CODEOWNERS))  
**Automation skeleton:** [`.github/workflows/canary.yml`](../../.github/workflows/canary.yml) (offline synthetic today)  
**Laptop runner:** [`scripts/laptop_canary.sh`](../../scripts/laptop_canary.sh) — archives `canary_evidence/runs/cycle_N/`; **not** scheduled beta  
**Private laptop runner (optional):** [`scripts/laptop_private_canary.sh`](../../scripts/laptop_private_canary.sh) — Binance/OKX/Bybit auth smokes; skips missing keys; **no orders**; **not** beta  
**Results archive:** [`canary_results.md`](./canary_results.md) (laptop consecutive **9/9**; scheduled **0**; reconnect **PASS** laptop — **not** a maturity promotion)

Live canary + multi-day soak remain **OPS**. Completing this checklist is what
promotes a venue from **alpha+** to **beta** (§11.8). Until then, READMEs and the
[maturity matrix](../plan/maturity_matrix.md) must say **not beta**.

## Scope (this close-out)

| VenueId | Code | Offline bar | Live bar (OPS) |
|--------:|------|-------------|----------------|
| 2 | `binance-spot` | fixtures + corpus + README owner/limits | scheduled canary + soak |
| 4 | `okx-spot` | fixtures + corpus + README owner/limits | scheduled canary + soak |

USD-M / OKX SWAP/Futures stay **alpha** until their own close-out rows land.

## Pre-flight (offline — already expected green)

- [ ] `cargo test -p marketfeed-adapter-binance`
- [ ] `cargo test -p marketfeed-adapter-okx`
- [ ] `cargo test -p marketfeed-engine --test chaos_harness`
- [ ] `./scripts/offline_daemon_e2e.sh` (synthetic)

## Scheduled live canary (OPS — required for beta)

Target cadence: daily via `canary.yml` once venue secrets + allowlist exist.
Until then the live job is a documented placeholder only.

For **each** of `binance-spot` and `okx-spot`:

1. [ ] Secrets / network allowlist reviewed (no credentials in repo)
2. [ ] `MARKETFEED_LIVE=1` session starts and reaches `/live` + `/ready` 200
3. [ ] Primary channels observed: trades + quote (+ L2 if enabled)
4. [ ] Metrics scrape shows frames + zero unexplained `EventsDropped` under nominal load
5. [x] Intentional reconnect recovers books to Live within reconnect policy (laptop probe **PASS**; still required on **scheduled** runs)
6. [ ] Result archived (log link / metric snapshot) for >=7 consecutive schedule runs (**scheduled = 0**)

Commands (examples; keep CI offline by default):

```bash
# Preferred one-shot laptop archive (NOT scheduled beta):
./scripts/laptop_canary.sh
INCLUDE_RECONNECT=0 ./scripts/laptop_canary.sh   # skip reconnect probes
INCLUDE_ALPHA=1 ./scripts/laptop_canary.sh       # also VenueIds 13–18 (still alpha)

cargo test -p marketfeed-adapter-binance --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-okx --test live_ignored -- --ignored --nocapture
# checklist item 5 (force transport Closed → reconnect → Live):
cargo test -p marketfeed-adapter-binance --test live_ignored live_binance_spot_reconnect_probe -- --ignored --nocapture
cargo test -p marketfeed-adapter-okx --test live_ignored live_okx_spot_reconnect_probe -- --ignored --nocapture
```

## Optional alpha laptop smokes (VenueIds 13–18) — **not** beta

Broader signal only. Same `#[ignore]` pattern as peers; does **not** promote maturity.

```bash
cargo test -p marketfeed-adapter-kraken --test live_ignored live_kraken_futures_trade_or_ticker -- --ignored --nocapture
cargo test -p marketfeed-adapter-bitstamp --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-gemini --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-coinbase --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-bitfinex --test live_ignored -- --ignored --nocapture
cargo test -p marketfeed-adapter-coinbase --test live_ignored live_coinbase_adv_candle -- --ignored --nocapture
```

## Soak gate (OPS — required for stable; recommended before calling beta "hardened")

Follow [`soak_runbook.md`](./soak_runbook.md). Offline synthetic != live soak.

## Honest exit language

| Evidence | Allowed claim |
|---|---|
| Offline tests + this checklist doc | **alpha+** / beta-ready offline |
| `scripts/laptop_canary.sh` + cycle_N archives | still **alpha+**; scheduled **= 0** |
| Laptop canary 7/7 + reconnect probe PASS | still **alpha+** (scheduled **= 0**) |
| Laptop canary 8/8 (incl. peer venues) + Deribit public trades fix | still **alpha+** (scheduled **= 0**) |
| Laptop canary 9/9 (9 public venues incl. Coinbase/Bitstamp/Gemini/KF) | still **alpha+** (scheduled **= 0**) |
| Alpha VenueIds 13–18 `live_ignored` PASS | still **alpha** (not beta) |
| REST candle corpora (14/15/16) | alpha offline confidence |
| Scheduled live canary green ≥7 | **beta** |
| Multi-day soak + RSS bound | path to **stable** |

Do **not** mark the maturity matrix **beta** from a merged PR alone.
