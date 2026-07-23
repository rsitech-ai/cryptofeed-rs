//! Bitfinex public WS v2 SessionMachine (spot **17** + derivatives **20**).
//!
//! Channel-ID protocol: `subscribed` binds `chanId` → channel+symbol; data frames
//! are `[chanId, …]`. Fits SessionMachine cleanly (deterministic HashMap).
//! Candles: WS `candles` with key `trade:{tf}:{symbol}`.
//! Ticker LAST/VOLUME/HIGH/LOW → `Statistics24h` (W6-P0a).
//! Derivatives: REST `GET /v2/status/deriv` on `STATUS_TIMER_ID` → mark/index/funding/OI.
//! Liquidations: WS `status` key `liq:global` → `MarketEvent::Liquidation` (subscribed symbols).

#![forbid(unsafe_code)]

mod factory;
mod instruments;
mod messages;
mod session;
mod specification;

pub use factory::{
    BitfinexDerivFactory, BitfinexFactory, candle_intervals_from, session_config_from_catalog,
};
pub use instruments::{parse_futures_pair_list, parse_pair_list};
pub use messages::{
    ChanBinding, ChanKind, Decoded, DerivStatusRow, LiquidationRow, candle_interval_ns,
    candle_time_frame, decode_candles_rest, decode_status_deriv, decode_text, parse_candle_key,
};
pub use session::{BitfinexSession, BitfinexSessionConfig};
pub use specification::{
    BITFINEX_DERIV_SPEC, BITFINEX_DERIV_VENUE_ID, BITFINEX_SPEC, BITFINEX_VENUE_ID,
    PING_INTERVAL_MS, PING_TIMER_ID, REST_BASE, STATUS_POLL_INTERVAL_MS, STATUS_TIMER_ID, WS_URL,
    ws_url,
};
