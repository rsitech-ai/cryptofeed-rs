//! Coinbase adapters — Exchange Classic + Advanced Trade + INTX auth MD.
//!
//! | Code | VenueId | Protocol |
//! |------|--------:|----------|
//! | `coinbase-spot` | **16** | Exchange Classic WS + REST |
//! | `coinbase-adv` | **18** | Advanced Trade WS + public REST |
//! | `coinbase-intl` | **19** | INTX auth MD WS (env credentials) |

#![forbid(unsafe_code)]

pub mod advanced;
pub mod intl;

mod exchange_credentials;
mod factory;
mod instruments;
mod messages;
mod session;
mod specification;

pub use advanced::{
    CANDLE_POLL_INTERVAL_MS as ADV_CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID as ADV_CANDLE_TIMER_ID,
    COINBASE_ADV_SPEC, COINBASE_ADV_VENUE_ID, CoinbaseAdvFactory, CoinbaseAdvSession,
    CoinbaseAdvSessionConfig, REST_BASE as ADV_REST_BASE, candles_url as adv_candles_url,
    decode_candles_rest as decode_adv_candles_rest, decode_text as decode_adv_text,
    session_config_from_catalog as adv_session_config_from_catalog,
    trade_id_source as adv_trade_id_source, ws_url as adv_ws_url,
};
pub use exchange_credentials::{
    CoinbaseExchangeCredentials, CoinbaseExchangeCredentialsError, CoinbaseExchangeSubscribeAuth,
};
pub use factory::{CoinbaseSpotFactory, candle_intervals_from};
pub use instruments::parse_products;
pub use intl::{
    COINBASE_INTL_SPEC, COINBASE_INTL_VENUE_ID, CoinbaseIntlCredentials, CoinbaseIntlFactory,
    CoinbaseIntlSession, CoinbaseIntlSessionConfig, REST_BASE as INTL_REST_BASE,
    decode_text as decode_intl_text,
    session_config_from_catalog as intl_session_config_from_catalog, ws_url as intl_ws_url,
};
pub use messages::{
    BookLevelChange, BookSideWire, DecodedEvent, Heartbeat, SubscriptionChannel,
    candle_granularity_secs, candle_interval_ns, decode_candles_rest, decode_text, trade_id_source,
};
pub use session::{CoinbaseHeartbeatState, CoinbaseSessionConfig, CoinbaseSpotSession};
pub use specification::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, COINBASE_SPOT_SPEC, COINBASE_SPOT_VENUE_ID,
    HEARTBEAT_INTERVAL_MS, HEARTBEAT_TIMEOUT_MS, HEARTBEAT_TIMER_ID, REST_BASE, ws_url,
};
