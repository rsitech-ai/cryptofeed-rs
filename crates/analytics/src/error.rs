use marketfeed_model::FixedError;
use thiserror::Error;

/// Explicit failures returned by deterministic analytics operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AnalyticsError {
    #[error("invalid configuration for {field}: {detail}")]
    InvalidConfig { field: &'static str, detail: String },
    #[error("invalid fixed-point value for {field}: {source}")]
    Fixed {
        field: &'static str,
        #[source]
        source: FixedError,
    },
    #[error("{field} must be positive")]
    NonPositive { field: &'static str },
    #[error("price is not aligned to the configured tick size")]
    MisalignedPrice,
    #[error("arithmetic overflow while {operation}")]
    ArithmeticOverflow { operation: &'static str },
    #[error(
        "trade timestamp {timestamp_ns} is older than finalized boundary {finalized_before_ns}"
    )]
    LateTrade {
        timestamp_ns: i64,
        finalized_before_ns: i64,
    },
    #[error("event is missing an instrument id")]
    MissingInstrument,
    #[error("event payload is not a trade")]
    NonTradeEvent,
    #[error("instrument mismatch: expected {expected}, got {actual}")]
    InstrumentMismatch { expected: u32, actual: u32 },
    #[error("{resource} capacity exceeded (limit {limit})")]
    CapacityExceeded {
        resource: &'static str,
        limit: usize,
    },
    #[error("unsupported analytics snapshot schema {actual}; expected {expected}")]
    UnsupportedSnapshotVersion { expected: u16, actual: u16 },
    #[error("analytics snapshot configuration does not match")]
    SnapshotConfigMismatch,
    #[error("corrupt analytics snapshot: {detail}")]
    CorruptSnapshot { detail: String },
}

pub(crate) fn invalid_config(field: &'static str, detail: impl Into<String>) -> AnalyticsError {
    AnalyticsError::InvalidConfig {
        field,
        detail: detail.into(),
    }
}

pub(crate) fn overflow(operation: &'static str) -> AnalyticsError {
    AnalyticsError::ArithmeticOverflow { operation }
}
