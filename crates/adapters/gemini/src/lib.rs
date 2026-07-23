//! Gemini Spot — current public WebSocket streams + REST candles/Stats24h.
//! Candles via `GET /v2/candles/{symbol}/{tf}` on `CANDLE_TIMER_ID` (no public candle WS).
//! Stats24h via `/v2/ticker` (OHLC) + `/v1/pubticker` (volume) on `STATS_TIMER_ID`.

#![forbid(unsafe_code)]

mod factory;
mod instruments;
mod messages;
mod session;
mod specification;

pub use factory::{GeminiFactory, candle_intervals_from, session_config_from_catalog};
pub use instruments::{
    DEFAULT_PRICE_SCALE, DEFAULT_QTY_SCALE, LIVE_DETAILS_MAX_ENV, apply_symbol_details,
    live_details_max_from_env, parse_symbols,
};
pub use messages::{
    Decoded, candle_interval_ns, candle_time_frame, decode_candles_rest, decode_pubticker_rest,
    decode_text, decode_ticker_rest,
};
pub use session::{GeminiSession, GeminiSessionConfig};
pub use specification::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, CANDLES_REST_BASE, GEMINI_SPEC, GEMINI_VENUE_ID,
    REST_BASE, STATS_POLL_INTERVAL_MS, STATS_TIMER_ID, TICKER_REST_BASE, WS_URL, ws_url,
};
