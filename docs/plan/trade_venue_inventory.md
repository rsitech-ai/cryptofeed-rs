# Trade venue and source inventory

Source audit: `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Official references accessed **2026-10-19**. Machine-readable companion: [trade_venue_inventory.json](trade_venue_inventory.json).

**This is specification evidence, not production coverage.** Nineteen real VenueIds (2–20) cover nine exchange families. Synthetic1 is a test harness and is excluded from real venue counts. Every product subfamily below is explicitly **not run by this source review**; fixtures are paths, not passing tests. Offline gates, bounded live observations, independent reconciliation, storage admission, consumer compatibility and operational promotion remain separate dimensions.

Actual pilot evidence at `.local/evidence/trade-audit/pilot-001` is integrated into the JSON companion and [validation report](../ops/trade_data_validation_report.md), with exact instruments, receipt bounds, counts and limitations. This source-review narrative retains its original review-only evidence scope. No product inherits a sibling product result. No source reviewer downloads or economic evaluations were performed.

## Current source drift and fidelity findings

- **INTX migration blocker:** the [official guide](https://docs.deribit.com/articles/intx-api-migration) dates consolidation into Deribit to 2026-10-01. Venue19 remains a historical identity; a new service is not a transparent source substitution. No account or production state migration is authorized. Existing Deribit discovery requests BTC/ETH only and misses USDC migrated perps.
- **OKX:** the implemented `trades` path aggregates executions; it drops count/source, supports no trades-all subscription, and only emits the first trade-array row. Same-time seqId reuse prevents unique-trade dedup by sequence alone.
- **Binance USD-M:** current [Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect) and [migration notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice) put aggTrade under `/market` and depth/bookTicker under `/public`. The obsolete Aggregate-Trade-Streams URL currently redirects to a generic landing page. Legacy comments about silent streams are insufficient current evidence.
- **Gemini:** official recent-trade retention text conflicts internally. Effective retention remains unresolved.
- **Metadata:** hardcoded defaults and inferred kinds are not exact contract specifications; derivative units require instrument/version-specific qualification.

## Evidence dimensions

| Dimension | Status in this inventory |
|---|---|
| Specified | Primary references reviewed with listed failures/contradictions |
| Implemented | Source inspected, including known defects; no universal completeness claim |
| Fixture | Test paths present; not executed by source reviewer |
| Offline tested | Not run by source reviewer; integrate root command report |
| Live observed | Not run by source reviewer; Venue19 migration blocked |
| Independently reconciled | Not run; pilot analyzer owns measured comparisons |
| Dataset/storage qualified | Not qualified |
| Target consumer compatible | Not demonstrated |
| Operational promotion | Not qualified; calendar canaries and multi-day soak unchanged |

## Coverage boundaries

| ID | Code | Explicit product subfamilies | Protocol |
|---:|---|---|---|
| 2 | `binance-spot` | spot_cash | Binance JSON WS; segment REST APIs |
| 3 | `binance-usdm` | linear_perpetual, linear_dated_future | Binance JSON WS; segment REST APIs |
| 4 | `okx-spot` | spot_cash | OKX V5 |
| 5 | `bybit-linear` | linear_perpetual, linear_dated_future | Bybit V5 |
| 6 | `bybit-spot` | spot_cash | Bybit V5 |
| 7 | `kraken-spot` | spot_cash | Kraken Spot WSv2 |
| 8 | `deribit` | inverse_perpetual_BTC_ETH, inverse_dated_future_BTC_ETH, linear_perpetual_parser_only, linear_dated_future_parser_only | JSON-RPC2 WS/APIv2 |
| 9 | `okx-swap` | linear_perpetual, inverse_perpetual | OKX V5 |
| 10 | `okx-futures` | linear_dated_future, inverse_dated_future | OKX V5 |
| 11 | `bybit-inverse` | inverse_perpetual, inverse_dated_future | Bybit V5 |
| 12 | `binance-coinm` | inverse_perpetual, inverse_dated_future | Binance JSON WS; segment REST APIs |
| 13 | `kraken-futures` | linear_perpetual_PF, inverse_perpetual_PI, inverse_dated_future_FI | Kraken Futures WSv1/RESTv3 |
| 14 | `bitstamp` | spot_cash | Bitstamp WSv2/RESTv2 |
| 15 | `gemini` | spot_cash | Current multiplexed JSON WS + RESTv1/v2 |
| 16 | `coinbase-spot` | spot_cash | Exchange Classic JSON WS/REST |
| 17 | `bitfinex` | spot_cash | Bitfinex WSv2/RESTv2 |
| 18 | `coinbase-adv` | spot_cash | Advanced Trade JSON WS/brokerage RESTv3 |
| 19 | `coinbase-intl` | legacy_linear_perpetual | Legacy INTX JSON WS/APIv1 |
| 20 | `bitfinex-deriv` | linear_perpetual, inverse_perpetual_inferred_unqualified | Bitfinex WSv2/RESTv2 |

Synthetic1: `crates/adapters/synthetic/src/{factory,session}.rs`; test-only, no exchange market evidence.

## Per-venue inventory

### 2 — binance-spot

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/binance/src/specification.rs`, `crates/adapters/binance/src/factory.rs`, `crates/adapters/binance/src/messages.rs`, `crates/adapters/binance/src/session.rs`, `crates/adapters/binance/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://stream.binance.com:9443/stream`, `https://api.binance.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `<symbol>@trade`; native individual trade; aggTrade separate unimplemented live alternative.

**Identity/ordering:** native t; m buyer-maker inverted to aggressor; trade identity scoped by venue/instrument/channel; gaps require reference. **Time:** T trade ms; E event ms; archives since2025 spot us, never guess by magnitude. **Quantity:** Native q spot base quantity; preserve quoteQty when acquired from history.

**Discovery:** exchangeInfo: symbol/assets/status/PRICE_FILTER/LOT_SIZE and notional; metadata snapshot required.

**Other implemented channel paths:** bookTicker; depth + REST snapshot; kline; ticker statistics. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** No evidence here of complete historical coverage or durable recording.

**Next actions:** Bounded raw-vs-archive reconciliation; calendar canaries remain required.

**Outside shipped/reviewed scope:** options, private account/trading, margin-account economics.

**Primary references:** [binance_spot](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams), [binance_archive](https://github.com/binance/binance-public-data).

### 3 — binance-usdm

**Products:** linear_perpetual, linear_dated_future. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/binance/src/usdm_specification.rs`, `crates/adapters/binance/src/usdm_factory.rs`, `crates/adapters/binance/src/usdm_messages.rs`, `crates/adapters/binance/src/usdm_session.rs`, `crates/adapters/binance/src/usdm_instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://fstream.binance.com/stream (legacy)`, `wss://fstream.binance.com/public/ws (routed-v4)`, `wss://fstream.binance.com/market/ws (routed-v4)`, `https://fapi.binance.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `legacy @trade; aggTrade decoder; routed-v4 market @aggTrade`; individual and aggregate variants normalize into same Trade model; must retain native channel and aggregation bounds.

**Identity/ordering:** t vs a distinct namespaces; aggTrade f/l discarded by legacy struct; m buyer-maker; no universal sequence promise. **Time:** T trade ms; routed-v4 source-time metadata differs from legacy; native E retained only where implemented. **Quantity:** Native q; do not label aggTrade constituent count or assume common quote/base contract conversion without catalog.

**Discovery:** exchangeInfo contractType classifies perpetual/dated; increments, contract metadata; filter exclusions must be recorded.

**Other implemented channel paths:** bookTicker; depth + REST snapshot pu; kline; ticker; mark/index/funding; REST open interest; forceOrder. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** Legacy silence comments are endpoint/time-specific, not current evidence; Aggregate first/last IDs and granularity not represented in canonical Trade.

**Next actions:** Verify exact configured endpoint; preserve f/l/a/nq/st when present; acquire each product subfamily separately.

**Outside shipped/reviewed scope:** options, private account/trading, margin-account economics.

**Primary references:** [binance_routes](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect), [binance_notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice), [binance_archive](https://github.com/binance/binance-public-data).

### 4 — okx-spot

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/okx/src/specification.rs`, `crates/adapters/okx/src/factory.rs`, `crates/adapters/okx/src/messages.rs`, `crates/adapters/okx/src/session.rs`, `crates/adapters/okx/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws.okx.com:8443/ws/v5/public`, `wss://ws.okx.com:8443/ws/v5/business`, `https://www.okx.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `trades on public; trades-all not accepted or subscribed`; trades aggregated by taker/price/source; cannot expand to executions.

**Identity/ordering:** tradeId last constituent ID; count/source dropped; seqId may repeat; session fallback conflates seqId/tradeId. **Time:** ts filled ms; canonical ns conversion; no source exchange-send time. **Quantity:** sz base on SPOT, contracts on derivatives; ctVal/ctValCcy/ctMult necessary for derived units.

**Discovery:** public/instruments?instType=SPOT; ctType linear/inverse share VenueId; OPTION/MARGIN excluded.

**Other implemented channel paths:** tickers Quote/Statistics24h; books; business candle*. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public trades/history; three-month history max100/page20req/2s, recent max500. EEA endpoint/eligibility must be verified separately; preserve EU spot1x cash/inventory context. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** decode_trades keeps first row only (messages.rs:370); count/source absent in TradeRow; trades-all unknown; source_sequence falls back from seqId to tradeId (session.rs:278); Neither recording semantics nor frozen consumer compatibility proven by channel availability.

**Next actions:** Specify native provenance sidecar; full-row batches and explicit granularity; bounded trades vs trades-all/reference reconciliation.

**Outside shipped/reviewed scope:** OPTION, MARGIN economics, EVENTS, spread/block/private channels.

**Primary references:** [okx](https://app.okx.com/docs-v5/en/).

### 5 — bybit-linear

**Products:** linear_perpetual, linear_dated_future. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/bybit/src/specification.rs`, `crates/adapters/bybit/src/factory.rs`, `crates/adapters/bybit/src/messages.rs`, `crates/adapters/bybit/src/session.rs`, `crates/adapters/bybit/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://stream.bybit.com/v5/public/linear`, `https://api.bybit.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `publicTrade.{symbol}`; individual rows in batch; <=1024rows and seq may span messages.

**Identity/ordering:** i opaque native ID; seq cross sequence not unique trade key; taker S. **Time:** T fill ms; outer ts generation ms dropped from trade rows. **Quantity:** Native v preserved; spot base; derivative denomination needs instrument-specific contract qualification before unit conversion.

**Discovery:** instruments-info category=linear limit1000; nextPageCursor completeness requires separate check; contractType split; options excluded.

**Other implemented channel paths:** orderbook; tickers; kline; mark/index/funding/OI ticker fields; allLiquidation. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** BT/RPI classification dropped by TradeRow; No arbitrary-gap recovery demonstrated; discovery pagination not qualified.

**Next actions:** Retain trade classifications and outer time; reconcile exact native IDs; verify cursor exhaustion.

**Outside shipped/reviewed scope:** option, private, RPI-specific depth and other unimplemented channel variants.

**Primary references:** [bybit](https://bybit-exchange.github.io/docs/v5/websocket/public/trade), [bybit_history](https://bybit-exchange.github.io/docs/v5/market/recent-trade).

### 6 — bybit-spot

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/bybit/src/specification.rs`, `crates/adapters/bybit/src/factory.rs`, `crates/adapters/bybit/src/messages.rs`, `crates/adapters/bybit/src/session.rs`, `crates/adapters/bybit/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://stream.bybit.com/v5/public/spot`, `https://api.bybit.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `publicTrade.{symbol}`; individual rows in batch; <=1024rows and seq may span messages.

**Identity/ordering:** i opaque native ID; seq cross sequence not unique trade key; taker S. **Time:** T fill ms; outer ts generation ms dropped from trade rows. **Quantity:** Native v preserved; spot base; derivative denomination needs instrument-specific contract qualification before unit conversion.

**Discovery:** instruments-info category=spot limit1000; nextPageCursor completeness requires separate check; contractType split; options excluded.

**Other implemented channel paths:** orderbook; tickers; kline. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** BT/RPI classification dropped by TradeRow; No arbitrary-gap recovery demonstrated; discovery pagination not qualified.

**Next actions:** Retain trade classifications and outer time; reconcile exact native IDs; verify cursor exhaustion.

**Outside shipped/reviewed scope:** option, private, RPI-specific depth and other unimplemented channel variants.

**Primary references:** [bybit](https://bybit-exchange.github.io/docs/v5/websocket/public/trade), [bybit_history](https://bybit-exchange.github.io/docs/v5/market/recent-trade).

### 7 — kraken-spot

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/kraken/src/specification.rs`, `crates/adapters/kraken/src/factory.rs`, `crates/adapters/kraken/src/messages.rs`, `crates/adapters/kraken/src/session.rs`, `crates/adapters/kraken/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws.kraken.com/v2`, `https://api.kraken.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `trade`; distinct trade rows batched; batching not one taker.

**Identity/ordering:** trade_id sequence unique per book; taker side; do not infer continuous +1 from examples. **Time:** RFC3339 timestamp into ns; absent timestamp handling must be qualified. **Quantity:** Native qty; preserve raw decimal token; spot base denomination requires catalog association.

**Discovery:** AssetPairs; wsname/altname/asset aliases; increments and status; no tokenized_asset claim.

**Other implemented channel paths:** ticker; book CRC32; ohlc. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** Snapshot recent50 is context, not complete history; No historical retention guarantee reviewed.

**Next actions:** Snapshot/live dedup and REST last-cursor overlap reconciliation.

**Outside shipped/reviewed scope:** tokenized/xStocks, margin economics, WSv1, private.

**Primary references:** [kraken_spot](https://docs.kraken.com/api/docs/websocket-v2/trade/), [kraken_history](https://docs.kraken.com/api/docs/rest-api/get-recent-trades/).

### 8 — deribit

**Products:** inverse_perpetual_BTC_ETH, inverse_dated_future_BTC_ETH, linear_perpetual_parser_only, linear_dated_future_parser_only. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/deribit/src/specification.rs`, `crates/adapters/deribit/src/factory.rs`, `crates/adapters/deribit/src/messages.rs`, `crates/adapters/deribit/src/session.rs`, `crates/adapters/deribit/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://www.deribit.com/ws/api/v2`, `https://www.deribit.com/api/v2`. Configured means source code, not current availability.

**Trade channel/granularity:** `trades.{instrument}.100ms`; array of native trades at100ms delivery; raw auth separate.

**Identity/ordering:** trade_id unique per currency; trade_seq per instrument; taker direction; snapshots/recovery not inferred. **Time:** timestamp ms; optional Starbase causal ns and match ID not retained. **Quantity:** amount differs from contracts; inverse USD and linear base coin per migration guide; never universally treat amount as contracts.

**Discovery:** Factory requests only BTC/ETH kind=future; parser can classify is_inverse=false but missing defaults true; USDC/other currencies not discovered; options filtered.

**Other implemented channel paths:** ticker Q/mark/index/funding/OI/statistics; book100ms; chart.trades; deribit_price_index; trade liquidation. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public100ms; raw authenticated. REST count<=1000 with explicit seq/time filters/sort. Public historical retention unverified; private historical guide cannot establish public retention. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** USDC migrated products omitted; Optional contracts/Starbase/block/combo provenance dropped; Catalog settlement derived from quote; explicit settlement_currency not parsed; Public-stream host evolution needs exact endpoint check.

**Next actions:** Expand bounded discovery design by currency/version; preserve raw numeric tokens and native amount/contract fields; separately qualify each subfamily.

**Outside shipped/reviewed scope:** options, spot, combos, USDC live discovery until implemented, Starbase SBE/private raw.

**Primary references:** [deribit](https://docs.deribit.com/subscriptions/trades/tradesinstrument_nameinterval), [deribit_history](https://docs.deribit.com/api-reference/market-data/public-get_last_trades_by_instrument), [deribit_collection](https://docs.deribit.com/articles/market-data-collection-best-practices), [intx_migration](https://docs.deribit.com/articles/intx-api-migration).

### 9 — okx-swap

**Products:** linear_perpetual, inverse_perpetual. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/okx/src/specification.rs`, `crates/adapters/okx/src/factory.rs`, `crates/adapters/okx/src/messages.rs`, `crates/adapters/okx/src/session.rs`, `crates/adapters/okx/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws.okx.com:8443/ws/v5/public`, `wss://ws.okx.com:8443/ws/v5/business`, `https://www.okx.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `trades on public; trades-all not accepted or subscribed`; trades aggregated by taker/price/source; cannot expand to executions.

**Identity/ordering:** tradeId last constituent ID; count/source dropped; seqId may repeat; session fallback conflates seqId/tradeId. **Time:** ts filled ms; canonical ns conversion; no source exchange-send time. **Quantity:** sz base on SPOT, contracts on derivatives; ctVal/ctValCcy/ctMult necessary for derived units.

**Discovery:** public/instruments?instType=SWAP; ctType linear/inverse share VenueId; OPTION/MARGIN excluded.

**Other implemented channel paths:** tickers Quote/Statistics24h; books; business candle*; mark-price; index-tickers; funding-rate (perpetual applicable only); open-interest; liquidation-orders. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public trades/history; three-month history max100/page20req/2s, recent max500. EEA endpoint/eligibility must be verified separately; preserve EU spot1x cash/inventory context. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** decode_trades keeps first row only (messages.rs:370); count/source absent in TradeRow; trades-all unknown; source_sequence falls back from seqId to tradeId (session.rs:278); Neither recording semantics nor frozen consumer compatibility proven by channel availability.

**Next actions:** Specify native provenance sidecar; full-row batches and explicit granularity; bounded trades vs trades-all/reference reconciliation.

**Outside shipped/reviewed scope:** OPTION, MARGIN economics, EVENTS, spread/block/private channels.

**Primary references:** [okx](https://app.okx.com/docs-v5/en/).

### 10 — okx-futures

**Products:** linear_dated_future, inverse_dated_future. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/okx/src/specification.rs`, `crates/adapters/okx/src/factory.rs`, `crates/adapters/okx/src/messages.rs`, `crates/adapters/okx/src/session.rs`, `crates/adapters/okx/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws.okx.com:8443/ws/v5/public`, `wss://ws.okx.com:8443/ws/v5/business`, `https://www.okx.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `trades on public; trades-all not accepted or subscribed`; trades aggregated by taker/price/source; cannot expand to executions.

**Identity/ordering:** tradeId last constituent ID; count/source dropped; seqId may repeat; session fallback conflates seqId/tradeId. **Time:** ts filled ms; canonical ns conversion; no source exchange-send time. **Quantity:** sz base on SPOT, contracts on derivatives; ctVal/ctValCcy/ctMult necessary for derived units.

**Discovery:** public/instruments?instType=FUTURES; ctType linear/inverse share VenueId; OPTION/MARGIN excluded.

**Other implemented channel paths:** tickers Quote/Statistics24h; books; business candle*; mark-price; index-tickers; funding-rate (perpetual applicable only); open-interest; liquidation-orders. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public trades/history; three-month history max100/page20req/2s, recent max500. EEA endpoint/eligibility must be verified separately; preserve EU spot1x cash/inventory context. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** decode_trades keeps first row only (messages.rs:370); count/source absent in TradeRow; trades-all unknown; source_sequence falls back from seqId to tradeId (session.rs:278); Neither recording semantics nor frozen consumer compatibility proven by channel availability.

**Next actions:** Specify native provenance sidecar; full-row batches and explicit granularity; bounded trades vs trades-all/reference reconciliation.

**Outside shipped/reviewed scope:** OPTION, MARGIN economics, EVENTS, spread/block/private channels.

**Primary references:** [okx](https://app.okx.com/docs-v5/en/).

### 11 — bybit-inverse

**Products:** inverse_perpetual, inverse_dated_future. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/bybit/src/specification.rs`, `crates/adapters/bybit/src/factory.rs`, `crates/adapters/bybit/src/messages.rs`, `crates/adapters/bybit/src/session.rs`, `crates/adapters/bybit/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://stream.bybit.com/v5/public/inverse`, `https://api.bybit.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `publicTrade.{symbol}`; individual rows in batch; <=1024rows and seq may span messages.

**Identity/ordering:** i opaque native ID; seq cross sequence not unique trade key; taker S. **Time:** T fill ms; outer ts generation ms dropped from trade rows. **Quantity:** Native v preserved; spot base; derivative denomination needs instrument-specific contract qualification before unit conversion.

**Discovery:** instruments-info category=inverse limit1000; nextPageCursor completeness requires separate check; contractType split; options excluded.

**Other implemented channel paths:** orderbook; tickers; kline; mark/index/funding/OI ticker fields; allLiquidation. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** BT/RPI classification dropped by TradeRow; No arbitrary-gap recovery demonstrated; discovery pagination not qualified.

**Next actions:** Retain trade classifications and outer time; reconcile exact native IDs; verify cursor exhaustion.

**Outside shipped/reviewed scope:** option, private, RPI-specific depth and other unimplemented channel variants.

**Primary references:** [bybit](https://bybit-exchange.github.io/docs/v5/websocket/public/trade), [bybit_history](https://bybit-exchange.github.io/docs/v5/market/recent-trade).

### 12 — binance-coinm

**Products:** inverse_perpetual, inverse_dated_future. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/binance/src/coinm_specification.rs`, `crates/adapters/binance/src/coinm_factory.rs`, `crates/adapters/binance/src/coinm_messages.rs`, `crates/adapters/binance/src/coinm_session.rs`, `crates/adapters/binance/src/coinm_instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://dstream.binance.com/stream`, `https://dapi.binance.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `legacy @trade; aggTrade decoder`; individual and aggregate variants normalize into same Trade model; must retain native channel and aggregation bounds.

**Identity/ordering:** t vs a distinct namespaces; aggTrade f/l discarded by legacy struct; m buyer-maker; no universal sequence promise. **Time:** T trade ms; routed-v4 source-time metadata differs from legacy; native E retained only where implemented. **Quantity:** Native q contract quantity; contractSize needed; archive baseQty differs from USD-M quoteQty.

**Discovery:** exchangeInfo contractType classifies perpetual/dated; increments, contract metadata; filter exclusions must be recorded.

**Other implemented channel paths:** bookTicker; depth + REST snapshot pu; kline; ticker; mark/index/funding; REST open interest; forceOrder. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** Legacy silence comments are endpoint/time-specific, not current evidence; Aggregate first/last IDs and granularity not represented in canonical Trade.

**Next actions:** Verify exact configured endpoint; preserve f/l/a/nq/st when present; acquire each product subfamily separately.

**Outside shipped/reviewed scope:** options, private account/trading, margin-account economics.

**Primary references:** [binance_coinm](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect), [binance_archive](https://github.com/binance/binance-public-data).

### 13 — kraken-futures

**Products:** linear_perpetual_PF, inverse_perpetual_PI, inverse_dated_future_FI. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/kraken/src/futures_specification.rs`, `crates/adapters/kraken/src/futures_factory.rs`, `crates/adapters/kraken/src/futures_messages.rs`, `crates/adapters/kraken/src/futures_session.rs`, `crates/adapters/kraken/src/futures_instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://futures.kraken.com/ws/v1`, `https://futures.kraken.com/derivatives/api/v3`, `https://futures.kraken.com/api/charts/v1`. Configured means source code, not current availability.

**Trade channel/granularity:** `trade`; native matched trade rows and snapshot.

**Identity/ordering:** uid opaque; seq subscription message sequence; taker side; type fill/liquidation/termination/block. **Time:** time trade ms. **Quantity:** Native qty product units; metadata contract_size; unit conversion not validated.

**Discovery:** instruments parser PF/PI/FI; quantity_scale0 and prefix fallback; FI expiry_ns absent.

**Other implemented channel paths:** ticker Q/mark/index/funding/OI/statistics; book; REST charts candles; trade liquidation. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** Dated FI metadata lacks expiry in parsed definition; Type/classification fidelity beyond Liquidation not demonstrated; Native product unit and fractional PF precision need independent validation.

**Next actions:** Audit qty increment, contract unit and expiry per instrument; recover overlapping snapshot/live IDs.

**Outside shipped/reviewed scope:** linear dated futures not supported by parser, options, private.

**Primary references:** [kraken_futures](https://docs.kraken.com/api/docs/futures-api/websocket/trade/).

### 14 — bitstamp

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/bitstamp/src/specification.rs`, `crates/adapters/bitstamp/src/factory.rs`, `crates/adapters/bitstamp/src/messages.rs`, `crates/adapters/bitstamp/src/session.rs`, `crates/adapters/bitstamp/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws.bitstamp.net`, `https://www.bitstamp.net/api/v2`. Configured means source code, not current availability.

**Trade channel/granularity:** `live_trades_{pair}`; native trade callback; present protocol claims source-inspected only.

**Identity/ordering:** id/id_str; type0/1 decoded as buy/sell but current official WS parsed page empty. **Time:** microtimestamp us preferred by parser; preserve second timestamp when present. **Quantity:** amount_str/price_str preferred; native spot quantity; no derivative unit claim.

**Discovery:** trading-pairs-info base/counter decimals only; parsed Spot; min_notional etc absent.

**Other implemented channel paths:** continuous full order_book live L2/BBO; diff_order_book decode/replay only; REST OHLC; REST ticker statistics. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public endpoints; transactions minute/hour/day windows. No full-history retention verified. Commercial usage terms require acquisition-specific review. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** Fresh official WS side semantic confirmation blocked by empty document; Spot catalog precision fields do not fully specify tick/minimum trading rules.

**Next actions:** Obtain official render/reference field confirmation; bounded transactions overlap; retain precise timestamp/strings.

**Outside shipped/reviewed scope:** derivatives, L3/live_orders, private.

**Primary references:** [bitstamp](https://www.bitstamp.net/api/), [bitstamp_ws](https://www.bitstamp.net/websocket/v2/).

### 15 — gemini

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/gemini/src/specification.rs`, `crates/adapters/gemini/src/factory.rs`, `crates/adapters/gemini/src/messages.rs`, `crates/adapters/gemini/src/session.rs`, `crates/adapters/gemini/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws.gemini.com/?snapshot=-1`, `https://api.gemini.com/v1`, `https://api.gemini.com/v2`. Configured means source code, not current availability.

**Trade channel/granularity:** `{symbol}@trade`; current multiplexed native trade event.

**Identity/ordering:** t exact numeric ID; m buyer-maker inverted; no proven global-contiguous IDs. **Time:** E event ns; no distinct execution time field in reviewed WS schema. **Quantity:** p/q decimal strings; native spot base; raw lexical retention distinct from Fixed model.

**Discovery:** symbols classified Spot Active with default scales; optional capped symbol details default0; incomplete metadata cannot qualify execution precision.

**Other implemented channel paths:** @bookTicker; @depth@100ms snapshot=-1; REST candles; REST ticker statistics. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** REST retention docs conflict:7 calendar days versus timestamp90-day hard limit; Default scales/status insufficient exact instrument metadata; Broken/corrected trade handling not demonstrated.

**Next actions:** Resolve retention by bounded query and source correspondence; request capped representative metadata; compare exact WS/REST IDs.

**Outside shipped/reviewed scope:** perpetuals, prediction markets, stocks, private, legacy market-data-v1/v2 WS.

**Primary references:** [gemini](https://developer.gemini.com/websocket/streams), [gemini_history](https://developer.gemini.com/trading/rest-api/market-data/list-trades).

### 16 — coinbase-spot

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/coinbase/src/specification.rs`, `crates/adapters/coinbase/src/factory.rs`, `crates/adapters/coinbase/src/messages.rs`, `crates/adapters/coinbase/src/session.rs`, `crates/adapters/coinbase/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws-feed.exchange.coinbase.com`, `https://api.exchange.coinbase.com`. Configured means source code, not current availability.

**Trade channel/granularity:** `matches`; native match; initial last_match context.

**Identity/ordering:** native trade_id; sequence channel/product context; maker side inverted by decoder; REST needed to recover documented drops. **Time:** RFC3339 time into ns. **Quantity:** native size spot base; precision metadata required.

**Discovery:** /products Spot; increments/status; native asset identifiers.

**Other implemented channel paths:** ticker Quote/statistics; authenticated level2; status instruments; REST candles. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public trades/ticker; repository Exchange L2 authentication is separate. Retention/rate/access not independently qualified. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** Documented possible silent match loss requires heartbeat/REST recovery; L2 auth availability independent of trade quality.

**Next actions:** Audit heartbeat last_trade_id watchdog and overlap REST recovery; mark last_match context.

**Outside shipped/reviewed scope:** Advanced Trade protocol, INTX/global derivatives, private.

**Primary references:** [coinbase_classic](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels).

### 17 — bitfinex

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/bitfinex/src/specification.rs`, `crates/adapters/bitfinex/src/factory.rs`, `crates/adapters/bitfinex/src/messages.rs`, `crates/adapters/bitfinex/src/session.rs`, `crates/adapters/bitfinex/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://api-pub.bitfinex.com/ws/2`, `https://api-pub.bitfinex.com/v2`. Configured means source code, not current availability.

**Trade channel/granularity:** `trades`; snapshot recent rows plus te; tu deliberately discarded.

**Identity/ordering:** native ID; signed amount taker side; per-connection chanId mapping; te/tu same-trade policy. **Time:** MTS execution ms. **Quantity:** signed amount absolute quantity plus aggressor; derivative unit/contract interpretation unqualified.

**Discovery:** conf pair-list only; hardcoded price scale2 qty8; derivative kind inferred from quote suffix; contract_size/settlement absent.

**Other implemented channel paths:** ticker; book; candles. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public hist max10000, inclusive start/end in ms, MTS sort,15req/min. Same-millisecond page boundaries need ID coverage proof; universal retention unknown. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** tu correction policy absent (all skipped); Snapshots emitted without explicit historical-context provenance in Trade; Catalog precision and derivative units not authoritative.

**Next actions:** Specify correction/dedup and snapshot flags; obtain exact contract/increment metadata; REST hist overlap at equal MTS.

**Outside shipped/reviewed scope:** funding-market trades f*, dated futures not cataloged, options, private.

**Primary references:** [bitfinex](https://docs.bitfinex.com/reference/ws-public-trades), [bitfinex_history](https://docs.bitfinex.com/reference/rest-public-trades).

### 18 — coinbase-adv

**Products:** spot_cash. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/coinbase/src/advanced/specification.rs`, `crates/adapters/coinbase/src/advanced/factory.rs`, `crates/adapters/coinbase/src/advanced/messages.rs`, `crates/adapters/coinbase/src/advanced/session.rs`, `crates/adapters/coinbase/src/advanced/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://advanced-trade-ws.coinbase.com`, `https://api.coinbase.com/api/v3/brokerage/market`. Configured means source code, not current availability.

**Trade channel/granularity:** `market_trades`; rows collected over250ms, not one economic execution.

**Identity/ordering:** trade_id strings; maker side inverted; sequence_num envelope distinct from native identity; USD/USDC aliases need explicit instrument lineage. **Time:** trade time RFC3339; outer timestamp source distinct. **Quantity:** native size spot base; derivative denomination out of scope.

**Discovery:** /market/products SPOT filter; non-SPOT skipped; fallback increments require qualification.

**Other implemented channel paths:** ticker; level2 l2_data; status; heartbeats; REST candles preferred; WS5m only when received. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Anonymous public market data; regional/network access and usage rights not independently established. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** Full discovery pagination and alias behavior unqualified; Current docs cover US/global derivatives beyond code boundary.

**Next actions:** Verify heartbeats plus exact product/channel lifecycle, aliases and pagination; batch cardinality fixture.

**Outside shipped/reviewed scope:** US derivatives, global derivatives, INTX, private.

**Primary references:** [coinbase_adv](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/market-trades), [coinbase_endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints).

### 19 — coinbase-intl

**Products:** legacy_linear_perpetual. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/coinbase/src/intl/specification.rs`, `crates/adapters/coinbase/src/intl/factory.rs`, `crates/adapters/coinbase/src/intl/messages.rs`, `crates/adapters/coinbase/src/intl/session.rs`, `crates/adapters/coinbase/src/intl/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://ws-md.international.coinbase.com`, `https://api.international.coinbase.com/api/v1`. Configured means source code, not current availability.

**Trade channel/granularity:** `MATCH (legacy env-auth)`; native MATCH no initial snapshot; current service migration blocked.

**Identity/ordering:** match_id opaque; sequence; aggressor OPENING_FILL is mapped Buy by current decoder although native field is not a BUY/SELL value; retain original and qualify policy. **Time:** RFC3339 gateway time; no independently proven execution vs gateway equivalence. **Quantity:** trade_qty native; base_asset_multiplier absent in parsed metadata; unit conversion unqualified.

**Discovery:** /instruments filters PERP; hardcoded fallback increments; contract multiplier/settlement absent.

**Other implemented channel paths:** LEVEL1; LEVEL2 top20; engine status; REST catalog. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Legacy WS requires env-only HMAC credentials. Official migration guide states new Deribit endpoint/keys and legacy history about12months; no credentials or live auth attempted. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** MIGRATION_BLOCKED: official consolidation into Deribit2026-10-01; Legacy docs still accessible do not prove live operation; No automatic replacement VenueId or historical source substitution allowed; `intl/messages.rs:136` maps OPENING_FILL to Buy, collapsing native auction/opening semantic with unproven correctness.

**Next actions:** Preserve historical Venue19 identity; document dated source transition; separately authorize/implement market-data migration only after scope review.

**Outside shipped/reviewed scope:** INTX spot, funding/risk/candles/index available in legacy docs but not implemented, trading/private state migration.

**Primary references:** [coinbase_intl](https://docs.cdp.coinbase.com/international-exchange/websocket-feed/channels), [intx_migration](https://docs.deribit.com/articles/intx-api-migration).

### 20 — bitfinex-deriv

**Products:** linear_perpetual, inverse_perpetual_inferred_unqualified. Every listed subfamily has independent not-run status; parser-only/inferred scopes remain unqualified.

**Source/runtime:** `crates/adapters/bitfinex/src/specification.rs`, `crates/adapters/bitfinex/src/factory.rs`, `crates/adapters/bitfinex/src/messages.rs`, `crates/adapters/bitfinex/src/session.rs`, `crates/adapters/bitfinex/src/instruments.rs` at `1c668981ee2d5b456dfa6d085fbd558b1998937d`. Shared daemon config/run/catalog discovery and engine runner/live paths are listed in JSON.

**Configured endpoints:** `wss://api-pub.bitfinex.com/ws/2`, `https://api-pub.bitfinex.com/v2`. Configured means source code, not current availability.

**Trade channel/granularity:** `trades`; snapshot recent rows plus te; tu deliberately discarded.

**Identity/ordering:** native ID; signed amount taker side; per-connection chanId mapping; te/tu same-trade policy. **Time:** MTS execution ms. **Quantity:** signed amount absolute quantity plus aggressor; derivative unit/contract interpretation unqualified.

**Discovery:** conf pair-list only; hardcoded price scale2 qty8; derivative kind inferred from quote suffix; contract_size/settlement absent.

**Other implemented channel paths:** ticker; book; candles; REST status/deriv mark/index/funding/OI; WS status liq:global. Channel presence does not establish field-by-field fidelity or independent candle agreement.

**History/access:** Public hist max10000, inclusive start/end in ms, MTS sort,15req/min. Same-millisecond page boundaries need ID coverage proof; universal retention unknown. Only explicitly cited history bounds apply; outside them, retention and recovery are unverified.

**Defects/blockers:** tu correction policy absent (all skipped); Snapshots emitted without explicit historical-context provenance in Trade; Catalog precision and derivative units not authoritative.

**Next actions:** Specify correction/dedup and snapshot flags; obtain exact contract/increment metadata; REST hist overlap at equal MTS.

**Outside shipped/reviewed scope:** funding-market trades f*, dated futures not cataloged, options, private.

**Primary references:** [bitfinex](https://docs.bitfinex.com/reference/ws-public-trades), [bitfinex_history](https://docs.bitfinex.com/reference/rest-public-trades).

## Official sources and reviewed claims

- [okx](https://app.okx.com/docs-v5/en/) — V5 trades aggregates per taker/price/source; count and last constituent tradeId; repeated seqId allowed; trades-all on business emits individual trades; spot size base, derivatives contracts; public history three months, max100 rows, 20req/2s. Regional eligibility/access not tested. Accessed 2026-10-19.
- [binance_spot](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams) — Spot raw trade differs from taker-aggregated aggTrade; IDs and first/last constituent IDs differ; m is buyer-maker. Accessed 2026-10-19.
- [binance_routes](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect) — Current USD-M public/market/private routing; aggregate trades use market; unrouted connections only receive public-class streams; 24-hour connection lifetime; no live route inference. Accessed 2026-10-19.
- [binance_notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice) — Maps bookTicker/depth to public and aggTrade/mark/kline/ticker/liquidations to market. Legacy migration deadline stated 2026-04-23. Verify actual runtime instead of assuming legacy comments remain true. Accessed 2026-10-19.
- [binance_archive](https://github.com/binance/binance-public-data) — Official spot/USD-M/Coin-M daily/monthly archive schemas, per-file checksums and possible later replacements. Spot archive times since 2025-01-01 are microseconds; futures schemas distinguish quoteQty and baseQty. Accessed 2026-10-19.
- [binance_coinm](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect) — Coin-M dstream connection reference; shared-looking protocol is not evidence of USD-M routing parity or current contract migration completion. Accessed 2026-10-19.
- [kraken_spot](https://docs.kraken.com/api/docs/websocket-v2/trade/) — v2 trade rows may batch unrelated takers; taker side; trade_id unique sequence per book; RFC3339 time; snapshot latest50. Accessed 2026-10-19.
- [kraken_history](https://docs.kraken.com/api/docs/rest-api/get-recent-trades/) — Public Trades since/count<=1000; example cursor last; no universal retention established. Current API includes tokenized_asset parameter outside reviewed crypto catalog. Accessed 2026-10-19.
- [kraken_futures](https://docs.kraken.com/api/docs/futures-api/websocket/trade/) — Futures WS uid, taker side, subscription seq, millisecond trade time, fill/liquidation/termination/block classification. Accessed 2026-10-19.
- [bybit](https://bybit-exchange.github.io/docs/v5/websocket/public/trade) — V5 publicTrade taker side, fill milliseconds, opaque ID, block/RPI flags; up to1024 rows, multiple messages can share seq; option subscription differs. Accessed 2026-10-19.
- [bybit_history](https://bybit-exchange.github.io/docs/v5/market/recent-trade) — Recent REST spot max60, other categories1000, links official archive website; not proof arbitrary missing intervals are recoverable. Accessed 2026-10-19.
- [deribit](https://docs.deribit.com/subscriptions/trades/tradesinstrument_nameinterval) — Instrument trade channel has interval raw/100ms/agg2 and arrays of trades; raw access requires authorization; notification batching does not justify inventing constituent executions. Accessed 2026-10-19.
- [deribit_history](https://docs.deribit.com/api-reference/market-data/public-get_last_trades_by_instrument) — REST per-currency trade_id, per-instrument trade_seq, taker direction, ms timestamp, amount and optional contracts; seq/time filters, explicit sorting, count max1000. Optional Starbase causal ns, liquidation/block/combo fields; no public universal retention proven. Accessed 2026-10-19.
- [deribit_collection](https://docs.deribit.com/articles/market-data-collection-best-practices) — Raw auth, batching, per-instrument ordering, instrument lifecycle feed and public trade backfill methods. Private historical-retention statements must not be applied to public data. Accessed 2026-10-19.
- [intx_migration](https://docs.deribit.com/articles/intx-api-migration) — States INTX consolidated into Deribit 2026-10-01, new endpoints/keys/runtime USDC discovery, linear amount base versus inverse USD; legacy historical APIs about12 months. No account, strategy or state migration authorized here. Accessed 2026-10-19.
- [gemini](https://developer.gemini.com/websocket/streams) — Current multiplexed @trade: E ns, t numeric ID, p/q decimal strings, m buyer-maker; @bookTicker and depth snapshot parameter. Accessed 2026-10-19.
- [gemini_history](https://developer.gemini.com/trading/rest-api/market-data/list-trades) — Max500; since_tid; newest-first; taker side; broken-trade option. Contradiction: headline seven calendar days versus timestamp description90-day hard limit; effective retention unresolved. Accessed 2026-10-19.
- [bitstamp](https://www.bitstamp.net/api/) — Transactions time=minute/hour/day. Current page includes derivatives surfaces beyond implemented spot. Commercial usage terms and endpoint limits require acquisition-specific review; no universal history established. Accessed 2026-10-19.
- [bitstamp_ws](https://www.bitstamp.net/websocket/v2/) — Accessible response but parsed content empty; cannot independently confirm current WS field/side claims from this fetch. Accessed 2026-10-19.
- [bitfinex](https://docs.bitfinex.com/reference/ws-public-trades) — WSv2 signed AMOUNT is taker buy/sell, MTS ms, ID; snapshot plus te/tu updates. Funding trades are a different tuple/schema. Accessed 2026-10-19.
- [bitfinex_history](https://docs.bitfinex.com/reference/rest-public-trades) — Public hist: max10000, inclusive ms start/end, MTS sort, 15req/min; retention not guaranteed by reviewed page. Accessed 2026-10-19.
- [coinbase_classic](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) — Exchange matches may drop; heartbeat last trade ID plus REST recovery documented; initial last_match is historical context. Ticker batching not a full tape. Accessed 2026-10-19.
- [coinbase_adv](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/market-trades) — Advanced market_trades batches250ms; batch is transport delivery, preserve rows. Accessed 2026-10-19.
- [coinbase_endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints) — Current spot/US derivatives and separate global derivatives endpoints; heartbeats needed for idle subscriptions; documented USD/USDC aliases. Catalog implementation remains spot only. Accessed 2026-10-19.
- [coinbase_intl](https://docs.cdp.coinbase.com/international-exchange/websocket-feed/channels) — Legacy INTX MATCH no snapshot, aggressor_side includes OPENING_FILL; additional FUNDING/RISK/CANDLES exist in docs. Published legacy docs do not prove post-migration service viability. Accessed 2026-10-19.

## Documentation failures and disagreements

- [https://www.okx.com/docs-v5/en/](https://www.okx.com/docs-v5/en/): web fetch exceeded4MiB; official app.okx.com/docs-v5/en/ fetched instead; no regional equivalence asserted.
- [https://docs.deribit.com/subscriptions/market-data/tradesinstrument_nameinterval](https://docs.deribit.com/subscriptions/market-data/tradesinstrument_nameinterval): obsolete path inaccessible; resolved official /subscriptions/trades/tradesinstrument_nameinterval.
- [https://docs.deribit.com/_llms/production.txt](https://docs.deribit.com/_llms/production.txt): index inaccessible; navigate rendered official pages.
- [https://docs.gemini.com/websocket/overview](https://docs.gemini.com/websocket/overview): old path inaccessible; current developer.gemini.com/websocket/streams fetched.
- [https://www.bitstamp.net/websocket/v2/](https://www.bitstamp.net/websocket/v2/): empty parsed document; current WS side claims not freshly source-verified.
- [https://developer.gemini.com/trading/rest-api/market-data/list-trades](https://developer.gemini.com/trading/rest-api/market-data/list-trades): 7-day headline vs90-day timestamp limit; unresolved.
- [https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams): redirect currently resolves generic documentation landing; route contract verified using Connect and Important-WebSocket-Change-Notice instead.
- [https://developers.binance.info/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream](https://developers.binance.info/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream): search returned combined stream material under misleading mini-ticker URL; not used as authoritative aggregate semantics in this inventory.
- [https://docs.deribit.com/articles/intx-api-migration](https://docs.deribit.com/articles/intx-api-migration): new migration guide conflicts with apparent currency of still-published legacy INTX docs; current service remains migration-blocked.

No universal venue, archive, product, history-retention or production-readiness claim follows from this inventory. Source failures and conflicts remain visible rather than being filled by plausible assumptions.

## Protocol/channel dispositions

The JSON `trade_protocol_scopes` explicitly expands the 31 product subfamilies
into 37 channel/protocol scopes: legacy USD-M individual trades and specialized
BNBUSDT routed-v4 aggregate trades are separate, and all five relevant OKX
subfamilies include a non-passing `trades-all` alternative. The latter is
unsupported by the current implementation; library routed-v4 has offline
evidence only. Snapshot/live/te/tu origin distinctions remain documented within
the source channel and measured pilot origin counts, rather than claiming each
message kind is an independent channel.
