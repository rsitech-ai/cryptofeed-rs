//! Coinbase Exchange spot venue specification.
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (matches / ticker + REST candles live; authenticated
//! Exchange level2 code path with credential-gated live proof).
//! Do **not** promote to beta/stable without scheduled live canary (§11.8).
//!
//! Offline proofs in this crate:
//! - Typed fixtures for `match` trades, `ticker` BBO, and historical
//!   `snapshot`/`l2update` L2 replay
//! - L2 snapshot → delta apply → qty=0 delete (`tests/l2_sync.rs`)
//! - REST candles via `ScheduleTimer` (`CANDLE_TIMER_ID`) + `/products/{id}/candles`
//!
//! Still required for **beta**:
//! - Credential-backed scheduled Exchange level2 canary
//! - Scheduled live canary
//! - Soak ≥ declared duration
//! - Named owner + ops limitations beyond adapter README
//!
//! Checksum / sequence continuity: N/A on Exchange `level2` — Coinbase sends a
//! full `snapshot` then unordered `l2update` deltas (`size=0` deletes).

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

pub const COINBASE_SPOT_VENUE_ID: VenueId = VenueId(16);

/// Periodic candles REST poll timer (engine fires `SessionInput::Timer`).
pub const CANDLE_TIMER_ID: u64 = 1;
/// Heartbeat silence watchdog timer.
pub const HEARTBEAT_TIMER_ID: u64 = 2;
/// Coinbase Exchange publishes heartbeat messages once per second.
pub const HEARTBEAT_INTERVAL_MS: i64 = 1_000;
/// Allow fifteen missed heartbeat intervals before reconnecting.
///
/// The public feed promises one heartbeat per second, but a five-second local
/// watchdog caused false reconnect storms during bounded multi-venue load.
/// Fifteen seconds remains fail-closed while tolerating short scheduler/network
/// stalls observed by the release canary.
pub const HEARTBEAT_TIMEOUT_MS: i64 = 15_000;
/// Default candle poll cadence (60s), Binance OI pattern.
pub const CANDLE_POLL_INTERVAL_MS: i64 = 60_000;

pub const REST_BASE: &str = "https://api.exchange.coinbase.com";

pub static COINBASE_SPOT_SPEC: VenueSpecification = VenueSpecification {
    id: COINBASE_SPOT_VENUE_ID,
    code: "coinbase-spot",
    environments: &[
        Environment::Production,
        Environment::Sandbox,
        Environment::Test,
    ],
    segments: &[MarketSegment::Spot],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::Candles,
        Capability::Statistics24h,
    ],
    endpoints: &[
        EndpointSpec {
            name: "ws",
            url: "wss://ws-feed.exchange.coinbase.com",
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "rest",
            url: "https://api.exchange.coinbase.com",
            segment: MarketSegment::Spot,
        },
    ],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 100,
        max_symbols_per_subscribe: 50,
        max_url_bytes: 4096,
    },
    heartbeat_policy: HeartbeatPolicy {
        interval_ms: HEARTBEAT_INTERVAL_MS as u64,
        timeout_ms: HEARTBEAT_TIMEOUT_MS as u64,
    },
    reconnect_policy: ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 30_000,
        reset_after_live_ms: 60_000,
    },
    max_frame_bytes: 8 * 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

pub fn ws_url() -> String {
    "wss://ws-feed.exchange.coinbase.com".into()
}
