//! Kraken adapters — Spot (WS v2) and Futures (WS v1 derivatives).
//!
//! # Spot
//!
//! Channels (WS v2): `trade`, `ticker` (BBO quotes), `book` (L2, depth 10, CRC32),
//! opt-in `ohlc` → `MarketEvent::Candle`.
//!
//! # Futures (`VenueId(13)` / `kraken-futures`)
//!
//! Separate protocol surface (`wss://futures.kraken.com/ws/v1`): public `trade`,
//! `ticker` (BBO), and `book` (`book_snapshot` + incremental `book` deltas).
//! Candles via REST charts poll on `CANDLE_TIMER_ID` (no public candle WS).
//! SessionMachine only — no networking inside the adapter.
//!
//! # Maturity
//!
//! Spot + Futures are **alpha** (offline fixtures). Not beta until soak + scheduled
//! live canary land (§11.8).

#![forbid(unsafe_code)]

mod checksum;
mod factory;
mod futures_factory;
mod futures_instruments;
mod futures_messages;
mod futures_session;
mod futures_specification;
mod instruments;
mod json;
mod messages;
mod session;
mod specification;

pub use factory::KrakenSpotFactory;
pub use futures_factory::{
    KrakenFuturesFactory, candle_intervals_from as futures_candle_intervals_from,
    session_config_from_catalog as futures_session_config_from_catalog,
};
pub use futures_instruments::parse_futures_instruments;
pub use futures_messages::{
    FuturesDecoded, candle_interval_ns as futures_candle_interval_ns,
    candle_resolution as futures_candle_resolution, decode_charts_rest, decode_futures_text,
};
pub use futures_session::{KrakenFuturesSession, KrakenFuturesSessionConfig};
pub use futures_specification::{
    CANDLE_POLL_INTERVAL_MS as FUTURES_CANDLE_POLL_INTERVAL_MS,
    CANDLE_TIMER_ID as FUTURES_CANDLE_TIMER_ID, FUTURES_CHARTS_REST_BASE, FUTURES_REST_BASE,
    FUTURES_WS_URL, KRAKEN_FUTURES_SPEC, KRAKEN_FUTURES_VENUE_ID,
    PING_INTERVAL_MS as FUTURES_PING_INTERVAL_MS, PING_TIMER_ID as FUTURES_PING_TIMER_ID,
    futures_ws_url,
};
pub use instruments::parse_asset_pairs;
#[cfg(feature = "simd-json")]
pub use messages::decode_text_simd;
pub use messages::{
    DecodedEvent, candle_interval_ns, decode_text, decode_text_serde, ohlc_interval_minutes,
};
pub use session::{KrakenSessionConfig, KrakenSpotSession};
pub use specification::{KRAKEN_SPOT_SPEC, KRAKEN_SPOT_VENUE_ID, ws_url};
