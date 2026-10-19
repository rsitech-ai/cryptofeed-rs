use marketfeed_adapter_api::*;
use marketfeed_model::VenueId;
pub const HYPERLIQUID_SPOT_VENUE_ID: VenueId = VenueId(21);
pub const HYPERLIQUID_PERPS_VENUE_ID: VenueId = VenueId(22);
pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const fn spec(id: VenueId, code: &'static str) -> VenueSpecification {
    VenueSpecification {
        id,
        code,
        environments: &[Environment::Production],
        segments: &[],
        capabilities: &[
            Capability::Trades,
            Capability::Quote,
            Capability::L2Book,
            Capability::Candles,
            Capability::MarkPrice,
            Capability::IndexPrice,
            Capability::Funding,
            Capability::OpenInterest,
            Capability::Statistics24h,
        ],
        endpoints: &[],
        subscription_constraints: SubscriptionConstraints {
            max_streams_per_connection: 1000,
            max_symbols_per_subscribe: 1,
            max_url_bytes: 2048,
        },
        heartbeat_policy: HeartbeatPolicy {
            interval_ms: 20_000,
            timeout_ms: 60_000,
        },
        reconnect_policy: ReconnectPolicy {
            min_delay_ms: 1000,
            max_delay_ms: 30_000,
            reset_after_live_ms: 60_000,
        },
        max_frame_bytes: 4 * 1024 * 1024,
        max_decompressed_bytes: 4 * 1024 * 1024,
    }
}
pub static HYPERLIQUID_SPOT_SPEC: VenueSpecification = VenueSpecification {
    segments: &[MarketSegment::Spot],
    endpoints: &[EndpointSpec {
        name: "ws",
        url: WS_URL,
        segment: MarketSegment::Spot,
    }],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::Candles,
        Capability::MarkPrice,
        Capability::Statistics24h,
    ],
    ..spec(HYPERLIQUID_SPOT_VENUE_ID, "hyperliquid-spot")
};
pub static HYPERLIQUID_PERPS_SPEC: VenueSpecification = VenueSpecification {
    segments: &[MarketSegment::Linear],
    endpoints: &[EndpointSpec {
        name: "ws",
        url: WS_URL,
        segment: MarketSegment::Linear,
    }],
    ..spec(HYPERLIQUID_PERPS_VENUE_ID, "hyperliquid-perps")
};
