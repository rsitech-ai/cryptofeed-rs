//! Deribit adapter — protocol state machine only; engine owns I/O.
//!
//! Channels: `trades.{instrument}.100ms`, `ticker.{instrument}.100ms`
//! (quote + mark/index/funding/OI when present), dedicated
//! `deribit_price_index.{index}` (peer OKX `index-tickers`),
//! `book.{instrument}.100ms` (L2 via `change_id`/`prev_change_id`), opt-in
//! `chart.trades.{instrument}.{resolution}` → `MarketEvent::Candle`.
//! Public sessions must not use `.raw` (Deribit error 13778).
//!
//! # L2 book sync (`book.*.100ms`)
//!
//! Public incremental channel (not authenticated `.raw`; not grouped snapshot-only).
//! 1. First notification has no `prev_change_id` (snapshot).
//! 2. Each change must have `prev_change_id ==` last applied `change_id`.
//! 3. Gap or apply error → `BookInvalidated` + `Reconnect(SequenceGap)`.
//! 4. Levels are `[action, price, amount]` with `action ∈ {new, change, delete}`.
//!
//! No book checksum — continuity is solely `change_id`. Details: adapter `README.md`,
//! `tests/l2_sync.rs`.
//!
//! # Heartbeat
//!
//! `method=heartbeat` with `type=test_request` → reply `public/test` (offline fixture).
//!
//! # Maturity
//!
//! See maturity notes on [`specification::DERIBIT_SPEC`]. Not beta until soak +
//! scheduled live canary land (§11.8).

#![forbid(unsafe_code)]

mod factory;
mod instruments;
mod json;
mod messages;
mod session;
mod specification;

pub use factory::DeribitFactory;
pub use instruments::parse_instruments;
#[cfg(feature = "simd-json")]
pub use messages::decode_text_simd;
pub use messages::{
    DecodedEvent, candle_interval_ns, chart_resolution, decode_text, decode_text_serde,
};
pub use session::{DeribitSession, DeribitSessionConfig};
pub use specification::{DERIBIT_SPEC, DERIBIT_VENUE_ID, ws_url};
