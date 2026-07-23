//! Deribit venue specification (perpetual / futures market data).
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (trades/ticker derivatives fields + `book.*.100ms` L2
//! offline fixtures on tip; no soak/canary). Do **not** promote to beta/stable
//! without scheduled live canary.
//!
//! Offline proofs present in this crate:
//! - Typed fixtures for trades, ticker (quote/mark/index/funding/OI), `chart.trades` candles,
//!   heartbeat `test_request`
//! - L2 `book.*.100ms` snapshot → `change_id` continuity → gap reconnect (`tests/l2_sync.rs`)
//! - Checked-in raw replay corpus under `tests/corpus/` + `tests/corpus_replay.rs`
//!
//! Still required for **beta** (§11.8 / audit WP-H) — do not claim until done:
//! - Scheduled live canary (today: `#[ignore]` only)
//! - Soak ≥ declared duration with RSS bound
//! - Named owner + ops limitations runbook beyond adapter README
//!
//! Checksum: N/A — Deribit book sync is `change_id`/`prev_change_id` only.

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

pub const DERIBIT_VENUE_ID: VenueId = VenueId(8);

pub static DERIBIT_SPEC: VenueSpecification = VenueSpecification {
    id: DERIBIT_VENUE_ID,
    code: "deribit",
    environments: &[
        Environment::Production,
        Environment::Sandbox,
        Environment::Test,
    ],
    segments: &[MarketSegment::Inverse, MarketSegment::Linear],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::MarkPrice,
        Capability::IndexPrice,
        Capability::Funding,
        Capability::OpenInterest,
        Capability::Candles,
        // Via trades `liquidation` field ("T"/"M"); no dedicated public liq channel.
        Capability::Liquidations,
        Capability::Statistics24h,
    ],
    endpoints: &[
        EndpointSpec {
            name: "ws",
            url: "wss://www.deribit.com/ws/api/v2",
            segment: MarketSegment::Inverse,
        },
        EndpointSpec {
            name: "rest",
            url: "https://www.deribit.com/api/v2",
            segment: MarketSegment::Inverse,
        },
    ],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 200,
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
    "wss://www.deribit.com/ws/api/v2".into()
}
