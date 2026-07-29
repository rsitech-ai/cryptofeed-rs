# Live canary summary

**Status:** historical laptop evidence only; no beta or production-readiness claim
**Checklist:** [`canary_checklist.md`](./canary_checklist.md)  
**Runner:** [`scripts/laptop_canary.sh`](../../scripts/laptop_canary.sh)
**Local evidence location:** ignored `.local/evidence/canary/`

## Current scoreboard

| Gate | Result | Meaning |
|---|---|---|
| Laptop public canary cycles | 10/10 passed on 2026-07-22 | Same-day operator bursts, not scheduled assurance |
| Public venue families observed | Binance, OKX, Bybit, Kraken, Deribit, Coinbase, Bitstamp, Gemini | At least one configured live smoke per family |
| Intentional reconnect probe | Passed for Binance Spot and OKX Spot | Forced transport closure returned to Live and resumed frames |
| Scheduled live canaries | 0 | The scheduled workflow remains synthetic/offline |
| Credentialed private canary | Skipped | Credentials were not present |
| Maturity action | None | Adapters remain alpha or alpha+ |

The latest cycle used:

```bash
INCLUDE_ALPHA=1 ./scripts/laptop_canary.sh
```

It exercised Binance and OKX trade/quote plus reconnect probes, and public
trade/quote smokes for Kraken Futures, Bitstamp, Gemini, and Coinbase. Earlier
cycles covered Bybit, Kraken Spot, and Deribit. All completed successfully on
the operator laptop.

## Evidence handling

The runner writes detailed logs and exit codes under ignored
`.local/evidence/canary/`. Raw operator logs are not committed because they can
contain workstation paths, transient venue payloads, and large repetitive
output. This checked-in summary records the durable result and the exact
reproduction entry point.

## Promotion boundary

These runs prove that the sampled public paths connected and emitted expected
events at that time. They do not prove continuous venue availability, all
instrument/channel combinations, authenticated feeds, external sink delivery,
or the calendar-spaced scheduled canaries required for beta. Multi-day live
soak remains a separate stable-adapter gate.
