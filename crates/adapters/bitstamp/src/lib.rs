//! Bitstamp Spot — public WS + REST OHLC + REST Stats24h SessionMachine.
//! Candles via `GET /ohlc/{pair}/` on `CANDLE_TIMER_ID` (no public candle WS).
//! Stats24h via `GET /ticker/{pair}/` on `STATS_TIMER_ID` (no free WS 24h fields).

#![forbid(unsafe_code)]

mod factory;
mod instruments;
mod messages;
mod session;
mod specification;

pub use factory::{BitstampFactory, candle_intervals_from, session_config_from_catalog};
pub use instruments::parse_trading_pairs;
pub use messages::{
    Decoded, candle_interval_ns, candle_step_secs, decode_ohlc_rest, decode_text,
    decode_ticker_rest,
};
pub use session::{BitstampSession, BitstampSessionConfig};
pub use specification::{
    BITSTAMP_SPEC, BITSTAMP_VENUE_ID, CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, PING_INTERVAL_MS,
    PING_TIMER_ID, REST_BASE, STATS_POLL_INTERVAL_MS, STATS_TIMER_ID, WS_URL, ws_url,
};
