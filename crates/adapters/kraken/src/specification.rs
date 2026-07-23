//! Kraken Spot venue specification (WebSocket v2).
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (trades/ticker/book L2 offline fixtures on tip; no
//! soak/canary). Do **not** promote to beta/stable without scheduled live canary.
//!
//! Offline proofs present in this crate:
//! - Typed fixtures for trades, ticker quotes, `ohlc` candles, heartbeat no-op, record/replay
//! - L2 book snapshot → CRC32-verified delta → checksum-mismatch reconnect (`tests/l2_sync.rs`)
//! - IEEE CRC32 golden vector from Kraken book-checksum-v2 guide (`checksum.rs`)
//! - Checked-in raw replay corpus under `tests/corpus/` + `tests/corpus_replay.rs`
//!
//! Still required for **beta** (§11.8 / audit WP-G) — do not claim until done:
//! - Scheduled live canary (today: `#[ignore]` only)
//! - Soak ≥ declared duration with RSS bound
//! - Futures segment decision + named owner/ops runbook beyond adapter README

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

pub const KRAKEN_SPOT_VENUE_ID: VenueId = VenueId(7);

pub static KRAKEN_SPOT_SPEC: VenueSpecification = VenueSpecification {
    id: KRAKEN_SPOT_VENUE_ID,
    code: "kraken-spot",
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
            name: "ws-v2",
            url: "wss://ws.kraken.com/v2",
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "rest",
            url: "https://api.kraken.com",
            segment: MarketSegment::Spot,
        },
    ],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 256,
        max_symbols_per_subscribe: 50,
        max_url_bytes: 2048,
    },
    heartbeat_policy: HeartbeatPolicy {
        interval_ms: 0,
        timeout_ms: 20_000,
    },
    reconnect_policy: ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 30_000,
        reset_after_live_ms: 60_000,
    },
    max_frame_bytes: 4 * 1024 * 1024,
    max_decompressed_bytes: 4 * 1024 * 1024,
};

pub fn ws_url() -> String {
    "wss://ws.kraken.com/v2".into()
}
