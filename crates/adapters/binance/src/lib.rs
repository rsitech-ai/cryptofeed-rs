//! Binance family adapters — protocol state machines only; engine owns I/O.
//!
//! # Spot L2 sequence rules (`@depth` + REST snapshot)
//!
//! From Binance Spot depth sync docs:
//! 1. Buffer `@depth` events while requesting REST `/api/v3/depth`.
//! 2. Apply snapshot atomically; set local update id to `lastUpdateId`.
//! 3. Drop buffered events with `u <= lastUpdateId`.
//! 4. First retained event must bridge: `U <= lastUpdateId` and `u >= lastUpdateId`.
//! 5. Live: discard when `u <= last_u` (stale/duplicate); gap when `U > last_u + 1`
//!    → `SequenceGap` + invalidate + reconnect.
//! 6. Qty `"0"` deletes the price level.
//!
//! Checksum: **N/A** — Binance Spot/USD-M/Coin-M depth payloads have no book checksum field;
//! events emit `checksum: None`. Continuity is solely via `U`/`u` (Spot) or `pu` (USD-M / Coin-M).
//!
//! # USD-M L2 sequence rules (`@depth` + REST `/fapi/v1/depth`)
//!
//! Same buffer/snapshot/drain shape as Spot, with one documented difference: every
//! `depthUpdate` event also carries `pu` (the `u` of the *previous* event on the same
//! stream). Rules:
//! 1. Buffer while requesting the REST snapshot, same as Spot.
//! 2. Drop buffered events with `u <= lastUpdateId` after the snapshot lands.
//! 3. The first retained buffered event must bridge: its `pu` equals the snapshot's
//!    `lastUpdateId`.
//! 4. Live continuity is `pu == previous_applied_u` (**not** Spot's `U == last_u + 1`).
//!    `u <= previous_applied_u` is still a stale/duplicate drop (no gap, no reconnect);
//!    `pu != previous_applied_u` is a discontinuity → `SequenceGap` + invalidate + resync
//!    + reconnect.
//! 5. Buffering is bounded (`max_buffered_events` / `max_buffered_bytes` per symbol); an
//!    oversized/excess event while still buffering invalidates the book and reconnects
//!    rather than growing memory unbounded — see `tests/usdm_l2_buffer.rs`.
//!
//! # Coin-M L2 sequence rules (`@depth` + REST `/dapi/v1/depth`)
//!
//! Same `pu` continuity rules as USD-M, on `dstream` / `dapi` (`VenueId(12)`). Trades +
//! quote + mark/index/funding + forceOrder always; dedicated `<pair>@indexPrice@1s`
//! (peer OKX `index-tickers`); OI via REST poll timer; L2 is opt-in via `enable_l2`.
//! See `tests/coinm_fixtures.rs` and `tests/coinm_l2_buffer.rs`.
//!
//! # Spot candles
//!
//! Native `@kline_{interval}` → `MarketEvent::Candle` (exact Fixed OHLCV).
//!
//! # Heartbeat
//!
//! Venue WS ping/pong is transport-owned. Spot also schedules an application silence
//! watchdog via `ScheduleTimer` (`HEARTBEAT_TIMER_ID`) using `heartbeat_policy.timeout_ms`.
//! Engine must fire timers (landed on main); offline proof in `tests/fixtures.rs`.
//!
//! # Maturity
//!
//! See maturity notes on [`specification::BINANCE_SPOT_SPEC`] / USD-M / Coin-M specs. This
//! crate does **not** claim beta until soak + scheduled live canary land (§11.8).

#![forbid(unsafe_code)]

mod coinm_factory;
mod coinm_instruments;
mod coinm_messages;
mod coinm_session;
mod coinm_specification;
mod factory;
mod instruments;
mod json;
mod messages;
mod session;
mod specification;
mod usdm_factory;
mod usdm_instruments;
mod usdm_messages;
mod usdm_session;
mod usdm_specification;

pub use coinm_factory::{BinanceCoinmFactory, coinm_session_config_from_catalog};
pub use coinm_instruments::parse_coinm_exchange_info;
#[cfg(feature = "simd-json")]
pub use coinm_messages::decode_text_simd as decode_coinm_text_simd;
pub use coinm_messages::{
    CoinmDecoded, decode_text as decode_coinm_text, decode_text_serde as decode_coinm_text_serde,
};
pub use coinm_session::{BinanceCoinmSession, BinanceCoinmSessionConfig};
pub use coinm_specification::{
    BINANCE_COINM_SPEC, BINANCE_COINM_VENUE_ID, OI_POLL_INTERVAL_MS as COINM_OI_POLL_INTERVAL_MS,
    OI_TIMER_ID as COINM_OI_TIMER_ID,
};

pub use factory::{BinanceSpotFactory, candle_intervals_from, session_config_from_catalog};
pub use instruments::parse_exchange_info;
#[cfg(feature = "simd-json")]
pub use messages::decode_text_simd;
pub use messages::{
    DecodedEvent, candle_interval_ns, decode_text, decode_text_serde, kline_stream_interval,
};
pub use session::{BinanceSessionConfig, BinanceSpotSession};
pub use specification::{
    BINANCE_SPOT_SPEC, BINANCE_SPOT_VENUE_ID, HEARTBEAT_TIMEOUT_MS, HEARTBEAT_TIMER_ID,
};

pub use usdm_factory::{BinanceUsdmFactory, usdm_session_config_from_catalog};
pub use usdm_instruments::parse_usdm_exchange_info;
#[cfg(feature = "simd-json")]
pub use usdm_messages::decode_text_simd as decode_usdm_text_simd;
pub use usdm_messages::{
    UsdmDecoded, UsdmRoutedV4Decoded, UsdmRoutedV4SourceTimes,
    decode_routed_v4_text as decode_usdm_routed_v4_text, decode_text as decode_usdm_text,
    decode_text_serde as decode_usdm_text_serde,
};
pub use usdm_session::{BinanceUsdmRouteV4, BinanceUsdmSession, BinanceUsdmSessionConfig};
pub use usdm_specification::{
    BINANCE_USDM_SPEC, BINANCE_USDM_VENUE_ID, OI_POLL_INTERVAL_MS, OI_TIMER_ID,
};
