//! OKX Spot/SWAP/Futures adapter — protocol state machine only; engine owns I/O.
//!
//! # Books sequence / checksum rules
//!
//! Continuity is enforced on `prevSeqId` / `seqId` (OKX V5 public books):
//! 1. First books push after subscribe is `action=snapshot` with `seqId`.
//! 2. Each `action=update` must have `prevSeqId ==` last applied `seqId`.
//! 3. Gap → `SequenceGap` + `BookInvalidated` + reconnect.
//! 4. Qty `"0"` deletes the price level.
//!
//! **Checksum:** OKX deprecated the books `checksum` field (demo 2026-06-02,
//! production 2026-06-23). Post-deprecation the field remains but is always `0` and
//! must not be used for integrity. This adapter accepts `checksum: 0` (or absent) and
//! relies on `prevSeqId` continuity. A non-zero checksum emits `ChecksumMismatch` +
//! reconnect (legacy / unexpected), without IEEE CRC over top-25 levels.
//!
//! ponytail: full CRC32 verify dropped after OKX deprecation; ceiling = ignore
//! non-zero forever; upgrade = restore CRC helper if OKX reintroduces checksums.
//!
//! # Heartbeat
//!
//! Server text `ping` → client `pong`. Client also `ScheduleTimer` (`PING_TIMER_ID`)
//! every `PING_INTERVAL_MS` to send application `ping` (requires engine timer fire).
//!
//! # Maturity
//!
//! See maturity notes on [`specification::OKX_SPOT_SPEC`] / SWAP / Futures specs.
//! Not beta until soak + scheduled live canary land.

#![forbid(unsafe_code)]

mod factory;
mod instruments;
mod json;
mod messages;
mod session;
mod specification;

pub use factory::{OkxFuturesFactory, OkxSpotFactory, OkxSwapFactory};
pub use instruments::{OkxInstType, parse_instruments_response};
#[cfg(feature = "simd-json")]
pub use messages::decode_text_simd;
pub use messages::{
    DecodedEvent, candle_channel, candle_interval_ns, decode_text, decode_text_serde,
};
pub use session::{OkxSession, OkxSessionConfig};
pub use specification::{
    BUSINESS_WS_URL, OKX_FUTURES_SPEC, OKX_FUTURES_VENUE_ID, OKX_SPOT_SPEC, OKX_SPOT_VENUE_ID,
    OKX_SWAP_SPEC, OKX_SWAP_VENUE_ID, PING_INTERVAL_MS, PING_TIMER_ID, PUBLIC_WS_URL,
};
