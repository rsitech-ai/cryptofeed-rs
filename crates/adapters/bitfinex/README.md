# marketfeed-adapter-bitfinex

| VenueId | Code | Segment |
|--------:|------|---------|
| **17** | `bitfinex` | spot |
| **20** | `bitfinex-deriv` | derivatives |

Public WS v2 T/Q/L2/candles + Stats24h. Deriv adds REST `status/deriv` (mark/index/funding/OI) + WS `status`/`liq:global` liquidations. **alpha** only.

Peer-parity (alpha): `catalog --live` (`pub:list:pair:exchange` / `futures`), `session_config_from_catalog`, R6 status/catalog, L2 corpora (`spot_l2_book.mfr` / `deriv_l2_book.mfr`), laptop `INCLUDE_ALPHA` canary. **Not beta.**
