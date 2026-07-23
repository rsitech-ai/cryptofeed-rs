//! Venue capability and endpoint metadata.

use marketfeed_model::VenueId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Environment {
    Production,
    Sandbox,
    Test,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketSegment {
    Spot,
    Linear,
    Inverse,
    Options,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    Trades,
    Quote,
    L2Book,
    L3Book,
    Candles,
    MarkPrice,
    IndexPrice,
    Funding,
    OpenInterest,
    Liquidations,
    Statistics24h,
    InstrumentStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointSpec {
    pub name: &'static str,
    pub url: &'static str,
    pub segment: MarketSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionConstraints {
    pub max_streams_per_connection: u32,
    pub max_symbols_per_subscribe: u32,
    pub max_url_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatPolicy {
    pub interval_ms: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
    pub reset_after_live_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueSpecification {
    pub id: VenueId,
    pub code: &'static str,
    pub environments: &'static [Environment],
    pub segments: &'static [MarketSegment],
    pub capabilities: &'static [Capability],
    pub endpoints: &'static [EndpointSpec],
    pub subscription_constraints: SubscriptionConstraints,
    pub heartbeat_policy: HeartbeatPolicy,
    pub reconnect_policy: ReconnectPolicy,
    pub max_frame_bytes: usize,
    pub max_decompressed_bytes: usize,
}
