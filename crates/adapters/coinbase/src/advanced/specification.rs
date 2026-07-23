//! Coinbase Advanced Trade public venue specification (VenueId 18).
//!
//! Distinct protocol from Exchange Classic (`coinbase-spot` VenueId 16):
//! - WS: `wss://advanced-trade-ws.coinbase.com`
//!   (`market_trades` / `ticker` / `level2` / `status` / `heartbeats`; no JWT)
//! - REST public MD: `https://api.coinbase.com/api/v3/brokerage/market/...`
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (public T/Q/L2 + REST candles + status offline). Do **not**
//! promote to beta/stable without scheduled live canary (§11.8).
//!
//! Private/authenticated Advanced Trade endpoints are out of scope.
//! Exchange Classic (`coinbase-spot` VenueId 16) remains a separate protocol.

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

/// Claimed in `docs/plan/venue_ids.md` (Advanced Trade public MD).
pub const COINBASE_ADV_VENUE_ID: VenueId = VenueId(18);

/// Periodic candles REST poll timer (engine fires `SessionInput::Timer`).
pub const CANDLE_TIMER_ID: u64 = 1;
/// Default candle poll cadence (60s), Binance OI / Exchange Classic pattern.
pub const CANDLE_POLL_INTERVAL_MS: i64 = 60_000;

/// Public Advanced Trade REST base (`/market/...` needs no auth).
pub const REST_BASE: &str = "https://api.coinbase.com/api/v3/brokerage/market";

pub static COINBASE_ADV_SPEC: VenueSpecification = VenueSpecification {
    id: COINBASE_ADV_VENUE_ID,
    code: "coinbase-adv",
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
        Capability::InstrumentStatus,
        Capability::Statistics24h,
    ],
    endpoints: &[
        EndpointSpec {
            name: "ws",
            url: "wss://advanced-trade-ws.coinbase.com",
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "rest",
            url: "https://api.coinbase.com/api/v3/brokerage/market",
            segment: MarketSegment::Spot,
        },
    ],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 100,
        max_symbols_per_subscribe: 50,
        max_url_bytes: 4096,
    },
    heartbeat_policy: HeartbeatPolicy {
        interval_ms: 30_000,
        timeout_ms: 60_000,
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
    "wss://advanced-trade-ws.coinbase.com".into()
}
