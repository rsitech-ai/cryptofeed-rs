//! Wall-clock and frame timing.

use serde::{Deserialize, Serialize};

/// Nanoseconds since Unix epoch (wall clock). Never conflate with monotonic time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampNs(pub i64);

/// Receipt metadata for one transport frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameStamp {
    pub receive_ts: TimestampNs,
    /// Monotonic nanoseconds for latency / clock-jump detection (engine-owned).
    pub mono_ns: u64,
}
