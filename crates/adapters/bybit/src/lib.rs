//! Bybit V5 adapter — protocol state machine only; engine owns I/O.
//!
//! # Native book rules (`orderbook.{depth}.{symbol}`)
//!
//! A snapshot or `u == 1` replaces the book atomically. Increasing delta update
//! IDs need not be consecutive. Only an exact repeat of the last applied native
//! message is ignored; conflicting/nonincreasing updates or a regressing `seq`
//! invalidate state and reconnect without claiming a confirmed source gap.
//! Malformed/unknown text and unexpected binary frames also invalidate active
//! book/quote state; L2 deltas require a new snapshot after invalidation.
//! Quantity zero deletes a delta level. L1 is snapshot-only and missing sides
//! invalidate its quote. Native `u`, `seq`, `ts`, optional `cts`, channel and
//! message type are preserved as provenance; canonical source sequence is absent.
//!
//! Book structural validity is separate from unknown source continuity. Neither
//! native ID establishes gap-free source delivery under the reviewed contract.
//!
//! # Native liquidation rules (`allLiquidation.{symbol}`)
//!
//! Native `p` is bankruptcy price and `S` is position side. Execution aggressor
//! and WS quantity units remain unknown/unverified. Updated/publication times
//! and exact native attributes are retained separately. Complete batches are
//! checked before emission; even empty batches require derivative catalog scope.
//! The decoded batch retains its symbol independently of its rows.
//!
//! # Scoped startup admission
//!
//! Each generation correlates every subscribe ACK by req_id and native conn_id.
//! MarkLive additionally requires all selected two-sided L1 quotes and requested
//! L2 snapshots before the local20-second receipt deadline. Ping is not an ACK.
//! Missing IDs are refused conservatively; native universal echo is not assumed.
//! Failed/stopped/cold sessions emit no financial frames until Connected.
//! Source continuity and exact runtime-envelope replay remain separate gates.
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

/// Actual decoder feature in this compiled adapter, independent of a caller's
/// Cargo flags. Used by lossless causal bootstrap reproduction bindings.
/// Enabled features in this compiled package, for exact bootstrap bindings.
pub fn compiled_features() -> Vec<String> {
    [
        ("default", cfg!(feature = "default")),
        ("simd-json", cfg!(feature = "simd-json")),
    ]
    .into_iter()
    .filter(|(_, active)| *active)
    .map(|(name, _)| name.to_owned())
    .collect()
}

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
    BookDecoded, CandleDecoded, DecodedEvent, LiquidationBatchDecoded, LiquidationDecoded,
    OrderbookKind, TradeDecoded, candle_interval_ns, decode_text, decode_text_serde,
    kline_topic_interval,
};
pub use session::{BybitSession, BybitSessionConfig};
pub use specification::{
    BYBIT_INVERSE_SPEC, BYBIT_INVERSE_VENUE_ID, BYBIT_LINEAR_SPEC, BYBIT_LINEAR_VENUE_ID,
    BYBIT_SPOT_SPEC, BYBIT_SPOT_VENUE_ID, BybitCategory,
};
