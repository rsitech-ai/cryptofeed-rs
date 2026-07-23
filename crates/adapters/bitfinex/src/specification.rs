//! Bitfinex Spot (**17**) + Derivatives (**20**) venue specifications.
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (trades/ticker/book + WS candles + Stats24h;
//! der. also REST `status/deriv` mark/index/funding/OI + WS `liq:global` liquidations).
//! Do **not** promote to beta/stable without scheduled live canary.
//!
//! Candles: public WS `candles` channel (`key=trade:{tf}:{symbol}`).
//! Ticker LAST/VOLUME/HIGH/LOW → `Statistics24h`.
//! Derivatives mark/funding: public REST `GET /v2/status/deriv` on `STATUS_TIMER_ID`.
//! Liquidations: public WS `status` key `liq:global` (filter to subscribed symbols).

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

/// Claimed in `docs/plan/venue_ids.md` (spot).
pub const BITFINEX_VENUE_ID: VenueId = VenueId(17);
/// Claimed in `docs/plan/venue_ids.md` (derivatives; do **not** overload **17**).
pub const BITFINEX_DERIV_VENUE_ID: VenueId = VenueId(20);

pub const WS_URL: &str = "wss://api-pub.bitfinex.com/ws/2";
/// Public REST base (instrument list + `status/deriv`).
pub const REST_BASE: &str = "https://api-pub.bitfinex.com/v2";

pub const PING_TIMER_ID: u64 = 1;
pub const PING_INTERVAL_MS: i64 = 15_000;

/// Derivatives `status/deriv` REST poll timer (mark / index / funding / OI).
pub const STATUS_TIMER_ID: u64 = 2;
pub const STATUS_POLL_INTERVAL_MS: i64 = 60_000;

const SPOT_CAPS: &[Capability] = &[
    Capability::Trades,
    Capability::Quote,
    Capability::L2Book,
    Capability::Candles,
    Capability::Statistics24h,
];

const DERIV_CAPS: &[Capability] = &[
    Capability::Trades,
    Capability::Quote,
    Capability::L2Book,
    Capability::Candles,
    Capability::Statistics24h,
    Capability::MarkPrice,
    Capability::IndexPrice,
    Capability::Funding,
    Capability::OpenInterest,
    Capability::Liquidations,
];

const SUBSCRIPTION_CONSTRAINTS: SubscriptionConstraints = SubscriptionConstraints {
    max_streams_per_connection: 30,
    max_symbols_per_subscribe: 10,
    max_url_bytes: 2048,
};

const HEARTBEAT: HeartbeatPolicy = HeartbeatPolicy {
    interval_ms: 15_000,
    timeout_ms: 45_000,
};

const RECONNECT: ReconnectPolicy = ReconnectPolicy {
    min_delay_ms: 200,
    max_delay_ms: 30_000,
    reset_after_live_ms: 60_000,
};

pub static BITFINEX_SPEC: VenueSpecification = VenueSpecification {
    id: BITFINEX_VENUE_ID,
    code: "bitfinex",
    environments: &[Environment::Production, Environment::Test],
    segments: &[MarketSegment::Spot],
    capabilities: SPOT_CAPS,
    endpoints: &[EndpointSpec {
        name: "ws",
        url: WS_URL,
        segment: MarketSegment::Spot,
    }],
    subscription_constraints: SUBSCRIPTION_CONSTRAINTS,
    heartbeat_policy: HEARTBEAT,
    reconnect_policy: RECONNECT,
    max_frame_bytes: 4 * 1024 * 1024,
    max_decompressed_bytes: 4 * 1024 * 1024,
};

pub static BITFINEX_DERIV_SPEC: VenueSpecification = VenueSpecification {
    id: BITFINEX_DERIV_VENUE_ID,
    code: "bitfinex-deriv",
    environments: &[Environment::Production, Environment::Test],
    segments: &[MarketSegment::Linear, MarketSegment::Inverse],
    capabilities: DERIV_CAPS,
    endpoints: &[EndpointSpec {
        name: "ws",
        url: WS_URL,
        segment: MarketSegment::Linear,
    }],
    subscription_constraints: SUBSCRIPTION_CONSTRAINTS,
    heartbeat_policy: HEARTBEAT,
    reconnect_policy: RECONNECT,
    max_frame_bytes: 4 * 1024 * 1024,
    max_decompressed_bytes: 4 * 1024 * 1024,
};

pub fn ws_url() -> String {
    WS_URL.into()
}
