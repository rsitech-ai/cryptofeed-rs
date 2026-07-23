//! Coinbase International venue specification (VenueId 19).

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

pub const COINBASE_INTL_VENUE_ID: VenueId = VenueId(19);
pub const WS_URL: &str = "wss://ws-md.international.coinbase.com";
pub const REST_BASE: &str = "https://api.international.coinbase.com/api/v1";

pub static COINBASE_INTL_SPEC: VenueSpecification = VenueSpecification {
    id: COINBASE_INTL_VENUE_ID,
    code: "coinbase-intl",
    environments: &[
        Environment::Production,
        Environment::Sandbox,
        Environment::Test,
    ],
    segments: &[MarketSegment::Linear],
    capabilities: &[Capability::Trades, Capability::Quote, Capability::L2Book],
    endpoints: &[
        EndpointSpec {
            name: "ws",
            url: WS_URL,
            segment: MarketSegment::Linear,
        },
        EndpointSpec {
            name: "rest",
            url: REST_BASE,
            segment: MarketSegment::Linear,
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
    WS_URL.into()
}
