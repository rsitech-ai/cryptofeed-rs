//! Explicit overflow / backpressure policies for bounded queues.

/// What happens when a bounded queue or buffer is full.
///
/// `SpillToDisk` needs `marketfeed_sinks::SpillWalSink` (bounded WAL + fail-closed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OverflowPolicy {
    BlockWithDeadline,
    DropNewest,
    DropOldest,
    LatestPerKey,
    SpillToDisk,
    DisableSink,
    FailEngine,
}
