//! Bybit V5 adapter — protocol state machine only; engine owns I/O.
//!
//! # L2 sequence rules (`orderbook.{depth}.{symbol}`)
//!
//! From Bybit V5 public orderbook docs:
//! 1. On subscribe, WS pushes `type=snapshot` with update id `u` — apply atomically.
//! 2. Each subsequent `type=delta` must have `u == previous_u + 1`.
//! 3. `u <= previous_u` → discard (stale/duplicate).
//! 4. `u > previous_u + 1` → sequence gap → invalidate book and reconnect.
//! 5. `u == 1` after going live → treat as fresh snapshot (venue reset / tick-size change).
//! 6. Qty `"0"` deletes the price level.
//!
//! Cross-sequence `seq` is recorded when present; continuity is enforced on `u`
//! (`u` consecutive; `seq` monotonic but not necessarily consecutive).
//!
//! # Live ping via engine timers
//!
//! [`BybitSession`] emits `SessionAction::ScheduleTimer` on connect and reacts to
//! `SessionInput::Timer` by sending `{"op":"ping"}` and rescheduling. Offline coverage:
//! `tests/fixtures.rs::trade_and_quote_fixtures`, `tests/fixtures.rs::ping_timer_sends_ping`.
//! The engine fulfills these timer actions without embedding network I/O in the adapter.
//!
//! # Maturity
//!
//! See maturity notes on [`specification::BYBIT_LINEAR_SPEC`] / spot / inverse.
//! This crate does **not** claim beta until soak + scheduled live canary land (§11.8).

#![forbid(unsafe_code)]

mod factory;
mod instruments;
mod json;
mod messages;
mod session;
mod specification;

pub use factory::BybitFactory;
pub use instruments::parse_instruments_info;
#[cfg(feature = "simd-json")]
pub use messages::decode_text_simd;
pub use messages::{
    DecodedEvent, candle_interval_ns, decode_text, decode_text_serde, kline_topic_interval,
};
pub use session::{BybitSession, BybitSessionConfig};
pub use specification::{
    BYBIT_INVERSE_SPEC, BYBIT_INVERSE_VENUE_ID, BYBIT_LINEAR_SPEC, BYBIT_LINEAR_VENUE_ID,
    BYBIT_SPOT_SPEC, BYBIT_SPOT_VENUE_ID, BybitCategory,
};
