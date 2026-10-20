//! Immutable, actual-replay-verified physical source steps. These are inputs
//! for a future projector, not target commits or external delivery receipts.
use super::*;

/// Versioned archive approximation. Neither exchange nor input receive clocks
/// are replaced. A projector must bind this policy in its own output contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveClockPolicy {
    OwnerStartedObservationV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifiedSourceClock {
    /// Bootstrap precedes the recorded owner's Started origin.
    Unstarted,
    Owner {
        started_unix_ns: i64,
        observed_ns: u64,
        /// Checked origin + observation, an approximation rather than a fresh
        /// wall clock. None means outside i64; consumers must report unavailable.
        approximate_unix_ns: Option<i64>,
    },
    /// Storage closure does not advance an owner's feature clock or settle
    /// execution. Preserve this field as recorded evidence only.
    StorageSeal { recorded_now_ns: i64 },
}
impl VerifiedSourceClock {
    pub(super) fn owner(started_unix_ns: i64, observed_ns: u64) -> Self {
        Self::Owner {
            started_unix_ns,
            observed_ns,
            approximate_unix_ns: i64::try_from(
                i128::from(started_unix_ns) + i128::from(observed_ns),
            )
            .ok(),
        }
    }
}

/// Original typed source content, never reconstructed from accepted raw frames.
/// Cloning this content does not manufacture a verified source-step capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedSourceRecord {
    Bootstrap(Box<BootstrapDto>),
    Owner {
        observation: Box<OwnerHistoryDto>,
        after: OwnerReplayProgress,
    },
    Seal {
        schema_version: u16,
        now_ns: i64,
        reason: String,
        /// Physical-prefix state, including unresolved execution. Session order.
        owners: Vec<OwnerReplayProgress>,
    },
}

/// Constructible only by the privately persistent actual replay reader, after
/// structural, resource, artifact and native checks. No Deserialize/constructor
/// accepts caller-supplied claims. Returned DTOs/handoffs are caller-owned and
/// outside reader retention; bounded decoding/copying is not an RSS guarantee.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedSourceStep {
    pub(super) profile: FeedProfile,
    pub(super) before: FeedCursor,
    pub(super) after: FeedCursor,
    pub(super) owner: Option<OperationIdentity>,
    pub(super) record: VerifiedSourceRecord,
    pub(super) clock: VerifiedSourceClock,
    pub(super) handoff: Option<RecordedOwnerHandoff>,
}
impl VerifiedSourceStep {
    pub fn profile(&self) -> FeedProfile {
        self.profile
    }
    pub fn before_cursor(&self) -> &FeedCursor {
        &self.before
    }
    pub fn after_cursor(&self) -> &FeedCursor {
        &self.after
    }
    pub fn owner(&self) -> Option<&OperationIdentity> {
        self.owner.as_ref()
    }
    pub fn record(&self) -> &VerifiedSourceRecord {
        &self.record
    }
    pub fn clock_policy(&self) -> ArchiveClockPolicy {
        ArchiveClockPolicy::OwnerStartedObservationV1
    }
    pub fn clock(&self) -> VerifiedSourceClock {
        self.clock
    }
    /// Present only at a committed native-matched HandoffObserved. Full operation
    /// evidence is not release authority; use handoff.released_items().
    pub fn handoff(&self) -> Option<&RecordedOwnerHandoff> {
        self.handoff.as_ref()
    }
    pub(super) fn into_replay_step(self) -> MultiReplayStep {
        MultiReplayStep {
            cursor: self.after,
            owner: self.owner,
            handoff: self.handoff,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_clock_is_checked_approximation_with_unsigned_observation_extrema() {
        for (origin, observed, expected) in [
            (i64::MIN, u64::MAX, Some(i64::MAX)),
            (i64::MAX, 0, Some(i64::MAX)),
            (i64::MAX, 1, None),
            (-7, 6, Some(-1)),
            (0, u64::MAX, None),
        ] {
            assert_eq!(
                VerifiedSourceClock::owner(origin, observed),
                VerifiedSourceClock::Owner {
                    started_unix_ns: origin,
                    observed_ns: observed,
                    approximate_unix_ns: expected,
                }
            );
        }
    }
}
