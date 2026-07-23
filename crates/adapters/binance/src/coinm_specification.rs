//! Binance Coin-M (inverse) futures venue specification.
//!
//! Trades / quote / mark/index/funding / forceOrder / OI REST + optional L2 (`pu`)
//! + optional klines. Peer-parity with USD-M public channels on `dstream` / `dapi`.

use marketfeed_adapter_api::{
    CandleInterval, Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment,
    ReconnectPolicy, SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

use crate::coinm_messages::coinm_index_pair;
use crate::messages::kline_stream_interval;

/// Claimed in `docs/plan/venue_ids.md`.
pub const BINANCE_COINM_VENUE_ID: VenueId = VenueId(12);

/// Periodic open-interest REST poll timer (engine fires `SessionInput::Timer`).
pub const OI_TIMER_ID: u64 = 2;
/// Default OI poll cadence (60s).
pub const OI_POLL_INTERVAL_MS: i64 = 60_000;

pub static BINANCE_COINM_SPEC: VenueSpecification = VenueSpecification {
    id: BINANCE_COINM_VENUE_ID,
    code: "binance-coinm",
    environments: &[Environment::Production, Environment::Test],
    segments: &[MarketSegment::Inverse],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::MarkPrice,
        Capability::IndexPrice,
        Capability::Funding,
        Capability::OpenInterest,
        Capability::Liquidations,
        Capability::Candles,
        Capability::Statistics24h,
    ],
    endpoints: &[
        EndpointSpec {
            name: "ws-dstream",
            url: "wss://dstream.binance.com",
            segment: MarketSegment::Inverse,
        },
        EndpointSpec {
            name: "rest-dapi",
            url: "https://dapi.binance.com",
            segment: MarketSegment::Inverse,
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

/// Combined-stream URL for Coin-M trades/quote/mark/forceOrder (+ optional depth / klines).
pub fn combined_stream_url(
    symbols_lower: &[String],
    include_depth: bool,
    candle_intervals: &[CandleInterval],
) -> String {
    let mut streams = Vec::new();
    for s in symbols_lower {
        streams.push(format!("{s}@aggTrade"));
        streams.push(format!("{s}@bookTicker"));
        streams.push(format!("{s}@ticker"));
        streams.push(format!("{s}@markPrice@1s"));
        let pair = coinm_index_pair(s).to_ascii_lowercase();
        streams.push(format!("{pair}@indexPrice@1s"));
        streams.push(format!("{s}@forceOrder"));
        if include_depth {
            streams.push(format!("{s}@depth@100ms"));
        }
        for interval in candle_intervals {
            streams.push(format!("{s}@kline_{}", kline_stream_interval(*interval)));
        }
    }
    format!(
        "wss://dstream.binance.com/stream?streams={}",
        streams.join("/")
    )
}
