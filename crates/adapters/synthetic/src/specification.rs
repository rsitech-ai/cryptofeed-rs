//! Static venue specification for the synthetic exchange.

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

pub const SYNTHETIC_VENUE_ID: VenueId = VenueId(1);

pub static SYNTHETIC_SPEC: VenueSpecification = VenueSpecification {
    id: SYNTHETIC_VENUE_ID,
    code: "synthetic",
    environments: &[Environment::Test],
    segments: &[MarketSegment::Spot],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::Candles,
    ],
    endpoints: &[EndpointSpec {
        name: "ws",
        url: "synthetic://local",
        segment: MarketSegment::Spot,
    }],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 64,
        max_symbols_per_subscribe: 64,
        max_url_bytes: 2048,
    },
    heartbeat_policy: HeartbeatPolicy {
        interval_ms: 30_000,
        timeout_ms: 90_000,
    },
    reconnect_policy: ReconnectPolicy {
        min_delay_ms: 50,
        max_delay_ms: 1_000,
        reset_after_live_ms: 5_000,
    },
    max_frame_bytes: 64 * 1024,
    max_decompressed_bytes: 64 * 1024,
};
