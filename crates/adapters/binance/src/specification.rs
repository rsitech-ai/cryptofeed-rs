//! Binance Spot venue specification.
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha+** / beta-ready offline (not §11.8 beta).
//!
//! Offline close-out docs: README owner/limits + canary checklist.
//! Native klines: `Capability::Candles` via `@kline_*` (see ADR 0001 superseded).
//!
//! Still required for **beta** (§11.8) — do not claim until done:
//! - Scheduled live canary (today: `#[ignore]` only) — see `docs/ops/canary_checklist.md`
//! - Soak >= declared duration with RSS bound
//!
//! Checksum: N/A for Binance Spot (no venue checksum field).

use marketfeed_adapter_api::{
    CandleInterval, Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment,
    ReconnectPolicy, SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

use crate::messages::kline_stream_interval;

pub const BINANCE_SPOT_VENUE_ID: VenueId = VenueId(2);

/// Application silence-watchdog timer (not venue WS ping).
pub const HEARTBEAT_TIMER_ID: u64 = 1;
/// Matches [`BINANCE_SPOT_SPEC`].heartbeat_policy.timeout_ms.
pub const HEARTBEAT_TIMEOUT_MS: i64 = 600_000;

pub static BINANCE_SPOT_SPEC: VenueSpecification = VenueSpecification {
    id: BINANCE_SPOT_VENUE_ID,
    code: "binance-spot",
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
            name: "ws-stream",
            url: "wss://stream.binance.com:9443",
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "rest",
            url: "https://api.binance.com",
            segment: MarketSegment::Spot,
        },
    ],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 1024,
        max_symbols_per_subscribe: 200,
        max_url_bytes: 4096,
    },
    heartbeat_policy: HeartbeatPolicy {
        interval_ms: 180_000,
        timeout_ms: 600_000,
    },
    reconnect_policy: ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 30_000,
        reset_after_live_ms: 60_000,
    },
    max_frame_bytes: 8 * 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

/// Build combined-stream URL for trades + bookTicker + ticker (+ optional depth / klines).
pub fn combined_stream_url(
    symbols_lower: &[String],
    include_depth: bool,
    candle_intervals: &[CandleInterval],
) -> String {
    let mut streams = Vec::new();
    for s in symbols_lower {
        streams.push(format!("{s}@trade"));
        streams.push(format!("{s}@bookTicker"));
        streams.push(format!("{s}@ticker"));
        if include_depth {
            streams.push(format!("{s}@depth@100ms"));
        }
        for interval in candle_intervals {
            streams.push(format!("{s}@kline_{}", kline_stream_interval(*interval)));
        }
    }
    format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        streams.join("/")
    )
}
