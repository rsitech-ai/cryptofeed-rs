//! Coinbase Advanced Trade public market-data protocol.
//!
//! Separate from Exchange Classic (`coinbase-spot` VenueId 16): different WS host,
//! REST base, and message envelope (`channel`/`events` vs `type`).

mod factory;
mod instruments;
mod messages;
mod session;
mod specification;

pub use factory::{CoinbaseAdvFactory, session_config_from_catalog};
pub use instruments::parse_products;
pub use messages::{
    BookLevelChange, BookSideWire, DecodedEvent, TradeRow, WS_CANDLE_INTERVAL_NS,
    candle_granularity, candle_granularity_secs, candle_interval_ns, candles_url,
    decode_candles_rest, decode_text, ns_to_ts, trade_id_source,
};
pub use session::{CoinbaseAdvSession, CoinbaseAdvSessionConfig};
pub use specification::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, COINBASE_ADV_SPEC, COINBASE_ADV_VENUE_ID, REST_BASE,
    ws_url,
};
