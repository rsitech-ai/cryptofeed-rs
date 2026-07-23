//! OKX Spot / SWAP / Futures venue specifications.
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level (Spot): **alpha+** / beta-ready offline (not §11.8 beta).
//! SWAP/Futures remain **alpha** until their own close-out rows land.
//!
//! Spot close-out docs: README owner/limits + canary checklist + ADR 0001 candles.
//!
//! Still required for **beta** (§11.8) — do not claim until done:
//! - Scheduled live canary (today: `#[ignore]` only) — see `docs/ops/canary_checklist.md`
//! - Soak >= declared duration with RSS bound
//!
//! Checksum: N/A for integrity after OKX 2026-06 deprecation (field always `0`).

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

pub const OKX_SPOT_VENUE_ID: VenueId = VenueId(4);
/// Claimed in `docs/plan/venue_ids.md` alongside `okx-futures` (10).
pub const OKX_SWAP_VENUE_ID: VenueId = VenueId(9);
pub const OKX_FUTURES_VENUE_ID: VenueId = VenueId(10);

pub const PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
pub const BUSINESS_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/business";
pub const REST_BASE: &str = "https://www.okx.com";

/// OKX requires a text `pong` within ~30s of server `ping`; we also emit client pings.
pub const PING_TIMER_ID: u64 = 1;
pub const PING_INTERVAL_MS: i64 = 20_000;

pub static OKX_SPOT_SPEC: VenueSpecification = VenueSpecification {
    id: OKX_SPOT_VENUE_ID,
    code: "okx-spot",
    environments: &[Environment::Production],
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
            name: "ws-public",
            url: PUBLIC_WS_URL,
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "ws-business",
            url: BUSINESS_WS_URL,
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "rest",
            url: REST_BASE,
            segment: MarketSegment::Spot,
        },
    ],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 240,
        max_symbols_per_subscribe: 100,
        max_url_bytes: 2048,
    },
    heartbeat_policy: HeartbeatPolicy {
        interval_ms: 20_000,
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

/// Derivatives capabilities shared by SWAP and FUTURES. Public market channels use
/// `/public`; candlesticks use the distinct `/business` WebSocket endpoint.
/// Linear + inverse (`ctType`) share VenueIds; kinds differ in the catalog.
const DERIVATIVE_CAPS: &[Capability] = &[
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

const DERIVATIVE_SEGMENTS: &[MarketSegment] = &[MarketSegment::Linear, MarketSegment::Inverse];

const DERIVATIVE_ENDPOINTS: &[EndpointSpec] = &[
    EndpointSpec {
        name: "ws-public",
        url: PUBLIC_WS_URL,
        segment: MarketSegment::Linear,
    },
    EndpointSpec {
        name: "ws-business",
        url: BUSINESS_WS_URL,
        segment: MarketSegment::Linear,
    },
    EndpointSpec {
        name: "rest",
        url: REST_BASE,
        segment: MarketSegment::Linear,
    },
    EndpointSpec {
        name: "ws-public-inverse",
        url: PUBLIC_WS_URL,
        segment: MarketSegment::Inverse,
    },
    EndpointSpec {
        name: "ws-business-inverse",
        url: BUSINESS_WS_URL,
        segment: MarketSegment::Inverse,
    },
    EndpointSpec {
        name: "rest-inverse",
        url: REST_BASE,
        segment: MarketSegment::Inverse,
    },
];

const DERIVATIVE_SUBSCRIPTION_CONSTRAINTS: SubscriptionConstraints = SubscriptionConstraints {
    max_streams_per_connection: 240,
    max_symbols_per_subscribe: 100,
    max_url_bytes: 2048,
};

const DERIVATIVE_HEARTBEAT: HeartbeatPolicy = HeartbeatPolicy {
    interval_ms: 20_000,
    timeout_ms: 60_000,
};

const DERIVATIVE_RECONNECT: ReconnectPolicy = ReconnectPolicy {
    min_delay_ms: 200,
    max_delay_ms: 30_000,
    reset_after_live_ms: 60_000,
};

pub static OKX_SWAP_SPEC: VenueSpecification = VenueSpecification {
    id: OKX_SWAP_VENUE_ID,
    code: "okx-swap",
    environments: &[Environment::Production],
    segments: DERIVATIVE_SEGMENTS,
    capabilities: DERIVATIVE_CAPS,
    endpoints: DERIVATIVE_ENDPOINTS,
    subscription_constraints: DERIVATIVE_SUBSCRIPTION_CONSTRAINTS,
    heartbeat_policy: DERIVATIVE_HEARTBEAT,
    reconnect_policy: DERIVATIVE_RECONNECT,
    max_frame_bytes: 8 * 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

pub static OKX_FUTURES_SPEC: VenueSpecification = VenueSpecification {
    id: OKX_FUTURES_VENUE_ID,
    code: "okx-futures",
    environments: &[Environment::Production],
    segments: DERIVATIVE_SEGMENTS,
    capabilities: DERIVATIVE_CAPS,
    endpoints: DERIVATIVE_ENDPOINTS,
    subscription_constraints: DERIVATIVE_SUBSCRIPTION_CONSTRAINTS,
    heartbeat_policy: DERIVATIVE_HEARTBEAT,
    reconnect_policy: DERIVATIVE_RECONNECT,
    max_frame_bytes: 8 * 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specifications_advertise_only_the_routed_production_environment() {
        assert_eq!(OKX_SPOT_SPEC.environments, &[Environment::Production]);
        assert_eq!(OKX_SWAP_SPEC.environments, &[Environment::Production]);
        assert_eq!(OKX_FUTURES_SPEC.environments, &[Environment::Production]);
    }

    #[test]
    fn derivative_endpoint_metadata_covers_inverse_and_linear_segments() {
        for segment in [MarketSegment::Linear, MarketSegment::Inverse] {
            assert!(
                DERIVATIVE_ENDPOINTS
                    .iter()
                    .any(|endpoint| endpoint.name.starts_with("ws-public")
                        && endpoint.segment == segment)
            );
            assert!(
                DERIVATIVE_ENDPOINTS
                    .iter()
                    .any(|endpoint| endpoint.name.starts_with("ws-business")
                        && endpoint.segment == segment)
            );
            assert!(
                DERIVATIVE_ENDPOINTS.iter().any(
                    |endpoint| endpoint.name.starts_with("rest") && endpoint.segment == segment
                )
            );
        }
    }
}
