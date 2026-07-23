//! Kraken Futures (derivatives) venue specification — separate protocol from Spot WS v2.
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (public trade/ticker/book + ticker mark/index/funding/OI
//! + liq via trade `type=liquidation` + REST charts candles fixtures + `.mfr` corpora on tip).
//!
//! Do **not** promote to beta/stable without scheduled live canary.
//!
//! No native public candles on Futures WS v1 — candles via REST
//! `GET /api/charts/v1/trade/{symbol}/{resolution}` on `CANDLE_TIMER_ID`.

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

/// Claimed in `docs/plan/venue_ids.md`.
pub const KRAKEN_FUTURES_VENUE_ID: VenueId = VenueId(13);

pub const FUTURES_WS_URL: &str = "wss://futures.kraken.com/ws/v1";
pub const FUTURES_REST_BASE: &str = "https://futures.kraken.com";
/// Public charts REST base (OHLCV candles).
pub const FUTURES_CHARTS_REST_BASE: &str = "https://futures.kraken.com/api/charts/v1";

/// Client application ping interval (venue requires activity within ~60s).
pub const PING_TIMER_ID: u64 = 1;
pub const PING_INTERVAL_MS: i64 = 30_000;

/// Periodic charts REST poll timer (engine fires `SessionInput::Timer`).
pub const CANDLE_TIMER_ID: u64 = 2;
/// Default candle poll cadence (60s), Bitstamp/Coinbase REST pattern.
pub const CANDLE_POLL_INTERVAL_MS: i64 = 60_000;

pub static KRAKEN_FUTURES_SPEC: VenueSpecification = VenueSpecification {
    id: KRAKEN_FUTURES_VENUE_ID,
    code: "kraken-futures",
    environments: &[Environment::Production, Environment::Test],
    // Flexible perps (PF_*) + inverse (PI_*) + dated (FI_*) share one gateway.
    segments: &[MarketSegment::Linear, MarketSegment::Inverse],
    capabilities: &[
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::MarkPrice,
        Capability::IndexPrice,
        Capability::Funding,
        Capability::OpenInterest,
        // Via trade `type=liquidation`; no dedicated public liq channel.
        Capability::Liquidations,
        // REST charts poll (no public candle WS).
        Capability::Candles,
        Capability::Statistics24h,
    ],
    endpoints: &[
        EndpointSpec {
            name: "ws-v1",
            url: FUTURES_WS_URL,
            segment: MarketSegment::Linear,
        },
        EndpointSpec {
            name: "rest",
            url: FUTURES_REST_BASE,
            segment: MarketSegment::Linear,
        },
    ],
    subscription_constraints: SubscriptionConstraints {
        max_streams_per_connection: 256,
        max_symbols_per_subscribe: 50,
        max_url_bytes: 2048,
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
    max_frame_bytes: 4 * 1024 * 1024,
    max_decompressed_bytes: 4 * 1024 * 1024,
};

pub fn futures_ws_url() -> String {
    FUTURES_WS_URL.into()
}
