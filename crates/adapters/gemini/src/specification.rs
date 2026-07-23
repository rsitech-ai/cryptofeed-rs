//! Gemini Spot venue specification (current public WebSocket API).
//!
//! # Maturity notes (honest — not beta)
//!
//! Current level: **alpha** (public depth/trades/book-ticker + REST candles + REST ticker Stats24h).
//! Do **not** promote to beta/stable without scheduled live canary.
//! Candles: no public WS; polled via `GET /v2/candles/{symbol}/{tf}` on
//! `CANDLE_TIMER_ID`.
//! Stats24h: no public WS fields; polled via `GET /v2/ticker/{symbol}` (OHLC) and
//! `GET /v1/pubticker/{symbol}` (volume) on `STATS_TIMER_ID`.

use marketfeed_adapter_api::{
    Capability, EndpointSpec, Environment, HeartbeatPolicy, MarketSegment, ReconnectPolicy,
    SubscriptionConstraints, VenueSpecification,
};
use marketfeed_model::VenueId;

/// Claimed in `docs/plan/venue_ids.md`.
pub const GEMINI_VENUE_ID: VenueId = VenueId(15);

/// `snapshot=-1` makes the first differential depth frame a full snapshot.
pub const WS_URL: &str = "wss://ws.gemini.com/?snapshot=-1";
pub const REST_BASE: &str = "https://api.gemini.com/v1";
pub const CANDLES_REST_BASE: &str = "https://api.gemini.com/v2/candles";
pub const TICKER_REST_BASE: &str = "https://api.gemini.com/v2/ticker";

/// Periodic candles REST poll timer (engine fires `SessionInput::Timer`).
pub const CANDLE_TIMER_ID: u64 = 1;
/// Default candle poll cadence (60s), Binance OI pattern.
pub const CANDLE_POLL_INTERVAL_MS: i64 = 60_000;

/// Periodic 24h ticker REST poll timer (engine fires `SessionInput::Timer`).
pub const STATS_TIMER_ID: u64 = 2;
/// Default Stats24h poll cadence (60s), Binance OI pattern.
pub const STATS_POLL_INTERVAL_MS: i64 = 60_000;

pub static GEMINI_SPEC: VenueSpecification = VenueSpecification {
    id: GEMINI_VENUE_ID,
    code: "gemini",
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
            url: WS_URL,
            segment: MarketSegment::Spot,
        },
        EndpointSpec {
            name: "rest",
            url: REST_BASE,
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
        timeout_ms: 30_000,
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
    WS_URL.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertises_only_the_environment_implemented_by_its_endpoints() {
        assert_eq!(GEMINI_SPEC.environments, &[Environment::Production]);
        assert!(
            GEMINI_SPEC
                .endpoints
                .iter()
                .all(|endpoint| !endpoint.url.contains("sandbox"))
        );
    }
}
