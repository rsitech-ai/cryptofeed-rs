# Private laptop canary results

**Status:** operator archive only — **not** scheduled canary, **not** beta  
**Runner:** [`scripts/laptop_private_canary.sh`](../../scripts/laptop_private_canary.sh)  
**Evidence:** [`private_canary_evidence/runs/`](./private_canary_evidence/runs/)  
**Secrets:** env / `.env` only (see [`.env.example`](../../.env.example)); never commit keys  
**Orders:** **none** — authentication / idle-stream smokes only
**Binance:** **blocked** — retired listen-key flow removed; authenticated WebSocket API migration pending

| Scoreboard | Value |
|---|---|
| Scheduled private canary | **0** |
| Maturity action from this log | **none** (remain alpha) |

Missing API keys → venue **SKIP** (script exits 0 when all venues skip).

---
