# Private canary summary

**Status:** not executed in the public release gate; no credentials were present
**Runner:** [`scripts/laptop_private_canary.sh`](../../scripts/laptop_private_canary.sh)  
**Local evidence location:** ignored `.local/evidence/private-canary/`

The private runner supports authentication and idle user-data smokes for OKX
and Bybit Spot. It does not place orders. Missing credentials cause a clean
skip, not a pass.

Binance private user data remains blocked pending migration from the retired
listen-key flow to the authenticated WebSocket API.

| Gate | Result |
|---|---|
| Scheduled private canary | 0 |
| Credential-backed release evidence | Not available |
| Order placement | Not implemented |
| Maturity action | None; private paths remain alpha |

Credentials must be supplied through environment variables or a local ignored
`.env` file. Never commit keys or include them in logs. See
[`.env.example`](../../.env.example) and [`SECURITY.md`](../../SECURITY.md).
