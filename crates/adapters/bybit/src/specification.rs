//! Bybit V5 venue specification: one `VenueSpecification` per category
//! (linear/spot/inverse are distinct venues per `docs/plan/venue_ids.md`).
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (experimental + primary public channels + offline fixtures).
//!
//! Offline proofs present in this crate:
//! - Typed fixtures for linear/spot/inverse trades + quotes; L2 snapshot/delta/gap/stale-`u`
//! - Checked-in raw replay corpus under `tests/corpus/` + `tests/corpus_replay.rs`
//! - Application ping via `ScheduleTimer` + `SessionInput::Timer` → `{"op":"ping"}`
//! - L2 `u` continuity documented in crate root `lib.rs` / `README.md`
//!
//! Still required for **beta** (§11.8 / audit WP-F) — do not claim until done:
//! - Scheduled live canary (today: `#[ignore]` only)
//! - Soak ≥ declared duration with RSS bound
//! - Named owner + ops limitations runbook beyond adapter README
//!
//! Checksum: N/A for Bybit public orderbook (continuity via consecutive `u`).

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

pub const BYBIT_LINEAR_VENUE_ID: VenueId = VenueId(5);
pub const BYBIT_SPOT_VENUE_ID: VenueId = VenueId(6);
/// Claimed in `docs/plan/venue_ids.md` on this branch.
pub const BYBIT_INVERSE_VENUE_ID: VenueId = VenueId(11);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BybitCategory {
    Linear,
    Spot,
    Inverse,
}

impl BybitCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::Spot => "spot",
            Self::Inverse => "inverse",
        }
    }

    pub fn venue_id(self) -> VenueId {
        match self {
            Self::Linear => BYBIT_LINEAR_VENUE_ID,
            Self::Spot => BYBIT_SPOT_VENUE_ID,
            Self::Inverse => BYBIT_INVERSE_VENUE_ID,
        }
    }
}

const SUBSCRIPTION_CONSTRAINTS: SubscriptionConstraints = SubscriptionConstraints {
    max_streams_per_connection: 200,
    max_symbols_per_subscribe: 10,
    max_url_bytes: 4096,
};

// Bybit requires application-level ping ~every 20s (all categories).
const HEARTBEAT_POLICY: HeartbeatPolicy = HeartbeatPolicy {
    interval_ms: 20_000,
    timeout_ms: 40_000,
};

const RECONNECT_POLICY: ReconnectPolicy = ReconnectPolicy {
    min_delay_ms: 200,
    max_delay_ms: 30_000,
    reset_after_live_ms: 60_000,
};

/// Linear (USDT/USDC perpetual & futures) — v1 primary.
pub static BYBIT_LINEAR_SPEC: VenueSpecification = VenueSpecification {
    id: BYBIT_LINEAR_VENUE_ID,
    code: "bybit-linear",
    environments: &[Environment::Production, Environment::Test],
    segments: &[MarketSegment::Linear],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::Candles,
        Capability::MarkPrice,
        Capability::IndexPrice,
        Capability::Funding,
        Capability::OpenInterest,
        Capability::Liquidations,
        Capability::Statistics24h,
    ],
    endpoints: &[
        EndpointSpec {
            name: "ws-linear",
            url: "wss://stream.bybit.com/v5/public/linear",
            segment: MarketSegment::Linear,
        },
        EndpointSpec {
            name: "rest",
            url: "https://api.bybit.com",
            segment: MarketSegment::Linear,
        },
    ],
    subscription_constraints: SUBSCRIPTION_CONSTRAINTS,
    heartbeat_policy: HEARTBEAT_POLICY,
    reconnect_policy: RECONNECT_POLICY,
    max_frame_bytes: 8 * 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

/// Spot.
pub static BYBIT_SPOT_SPEC: VenueSpecification = VenueSpecification {
    id: BYBIT_SPOT_VENUE_ID,
    code: "bybit-spot",
    environments: &[Environment::Production, Environment::Test],
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
            name: "ws-spot",
            url: "wss://stream.bybit.com/v5/public/spot",
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "rest",
            url: "https://api.bybit.com",
            segment: MarketSegment::Spot,
        },
    ],
    subscription_constraints: SUBSCRIPTION_CONSTRAINTS,
    heartbeat_policy: HEARTBEAT_POLICY,
    reconnect_policy: RECONNECT_POLICY,
    max_frame_bytes: 8 * 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

/// Inverse (coin-margined) perpetual & futures.
pub static BYBIT_INVERSE_SPEC: VenueSpecification = VenueSpecification {
    id: BYBIT_INVERSE_VENUE_ID,
    code: "bybit-inverse",
    environments: &[Environment::Production, Environment::Test],
    segments: &[MarketSegment::Inverse],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::Candles,
        Capability::MarkPrice,
        Capability::IndexPrice,
        Capability::Funding,
        Capability::OpenInterest,
        Capability::Liquidations,
        Capability::Statistics24h,
    ],
    endpoints: &[
        EndpointSpec {
            name: "ws-inverse",
            url: "wss://stream.bybit.com/v5/public/inverse",
            segment: MarketSegment::Inverse,
        },
        EndpointSpec {
            name: "rest",
            url: "https://api.bybit.com",
            segment: MarketSegment::Inverse,
        },
    ],
    subscription_constraints: SUBSCRIPTION_CONSTRAINTS,
    heartbeat_policy: HEARTBEAT_POLICY,
    reconnect_policy: RECONNECT_POLICY,
    max_frame_bytes: 8 * 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

pub fn public_ws_url(category: &str) -> &'static str {
    match category {
        "spot" => "wss://stream.bybit.com/v5/public/spot",
        "inverse" => "wss://stream.bybit.com/v5/public/inverse",
        _ => "wss://stream.bybit.com/v5/public/linear",
    }
}
