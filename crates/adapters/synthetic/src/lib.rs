//! Synthetic venue: proves adapter = deterministic SessionMachine without I/O.

#![forbid(unsafe_code)]

mod factory;
mod session;
mod specification;

pub use factory::SyntheticFactory;
pub use session::SyntheticSession;
pub use specification::{SYNTHETIC_SPEC, SYNTHETIC_VENUE_ID};

/// Wire protocol: one UTF-8 command per text frame (no JSON dependency).
///
/// ```text
/// SUB <native_symbol>
/// UNSUB <native_symbol>
/// TRADE <seq> <price> <qty> BUY|SELL [trade_id]
/// QUOTE <bid> <ask> [<bid_qty> <ask_qty>]
/// CANDLE <open> <high> <low> <close> <volume> <interval_ns> <start_ts>
/// BOOK_SNAP <seq> BID <p>:<q>[,...] ASK <p>:<q>[,...]
/// BOOK_DELTA <seq> BID|ASK UPSERT|DELETE <price> [<qty>]
/// STATS24H <open> <high> <low> <close> <volume> <quote_volume>
/// DISCONNECT
/// ```
///
/// Spot test venue: no mark/index/funding/OI/liq (N/A).
pub mod proto {
    pub const SUB_PREFIX: &str = "SUB ";
    pub const UNSUB_PREFIX: &str = "UNSUB ";
    pub const TRADE_PREFIX: &str = "TRADE ";
    pub const QUOTE_PREFIX: &str = "QUOTE ";
    pub const CANDLE_PREFIX: &str = "CANDLE ";
    pub const BOOK_SNAP_PREFIX: &str = "BOOK_SNAP ";
    pub const BOOK_DELTA_PREFIX: &str = "BOOK_DELTA ";
    pub const STATS24H_PREFIX: &str = "STATS24H ";
    pub const DISCONNECT: &str = "DISCONNECT";
}
