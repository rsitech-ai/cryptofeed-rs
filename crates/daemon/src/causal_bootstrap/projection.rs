//! One prepared group or accepted ticket, outside source owner execution. Fresh
//! target only: no append takeover, external delivery or consumer activation.
use super::replay::*;
use super::*;
use marketfeed_engine::causal::projection::*;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ProjectionError {
    #[error(transparent)]
    Replay(#[from] ReplayError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error(transparent)]
    Commit(#[from] CommitFailure),
    #[error("projection admission failed: {0:?}")]
    Admission(AdmissionFailure),
    #[error("invalid projection: {0}")]
    Invalid(&'static str),
    #[error("projection stopped after error; reconstruct into a new target feed")]
    Poisoned,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionCheckpoint {
    pub source: FeedCursor,
    pub target: FeedCursor,
    pub sealed: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPoll {
    Prepared,
    Writing,
    Full,
    Committed,
    CaughtUp,
    Sealed,
}
struct Prepared {
    after: FeedCursor,
    sealed: bool,
    transaction: PreparedFeedTransaction,
}
struct Accepted {
    after: FeedCursor,
    sealed: bool,
    ticket: CommitTicket,
}
pub struct ProjectionPump {
    source: MultiHistoryReplay,
    target: FeedBatchActor,
    policy: ProjectionPolicy,
    checkpoint: ProjectionCheckpoint,
    prepared: Option<Prepared>,
    accepted: Option<Accepted>,
    prepared_groups: u64,
    poisoned: bool,
}
fn owner_progress(p: &OwnerReplayProgress) -> ProjectionOwnerProgress {
    ProjectionOwnerProgress {
        identity: p.identity.clone(),
        journal_ordinal: p.journal_ordinal,
        native_ordinal: p.native_ordinal,
        terminal: p.terminal,
        uncertainties: p.uncertainties.clone(),
    }
}
fn group(
    step: &VerifiedSourceStep,
    policy: &ProjectionPolicy,
) -> Result<ProjectionGroup, ProjectionError> {
    if step.profile() != policy.source_feed_profile()?
        || step.clock_policy() != ArchiveClockPolicy::OwnerStartedObservationV1
    {
        return Err(ProjectionError::Invalid("source profile/clock policy"));
    }
    let clock = match step.clock() {
        VerifiedSourceClock::Unstarted => ProjectionClock::Unstarted,
        VerifiedSourceClock::Owner {
            started_unix_ns,
            observed_ns,
            approximate_unix_ns,
        } => ProjectionClock::Owner {
            started_unix_ns,
            observed_ns,
            approximate_unix_ns,
        },
        VerifiedSourceClock::StorageSeal { recorded_now_ns } => {
            ProjectionClock::StorageSeal { recorded_now_ns }
        }
    };
    let source = match step.record() {
        VerifiedSourceRecord::Bootstrap(dto) => ProjectionSource::Bootstrap(dto.clone()),
        VerifiedSourceRecord::Owner { observation, after } => ProjectionSource::Owner {
            route: observation.route.clone(),
            observed_ns: observation.observed_ns,
            event: ProjectionOwnerEvent::from_event(&observation.event)?,
            after: Box::new(owner_progress(after)),
        },
        VerifiedSourceRecord::Seal {
            schema_version,
            now_ns,
            reason,
            owners,
        } => ProjectionSource::Seal {
            schema_version: *schema_version,
            now_ns: *now_ns,
            reason: reason.clone(),
            owners: owners.iter().map(owner_progress).collect(),
        },
    };
    let mut items = Vec::new();
    let mut item_positions = Vec::new();
    let mut outbound_evidence = Vec::new();
    if let Some(h) = step.handoff() {
        for (index, item) in h.released_items().enumerate() {
            if index >= policy.limits.max_items {
                return Err(ProjectionError::Codec(CodecError::TooLarge));
            }
            let source_index = index as u32;
            match item {
                ItemDto::Market(_) | ItemDto::ObservedMarket(_) | ItemDto::System(_) => {
                    items.push(item.clone());
                    item_positions.push(source_index);
                }
                ItemDto::Write(write) => outbound_evidence.push(ProjectionEvidence {
                    source_index,
                    value: ProjectionOutbound::Write(write.clone()),
                }),
                ItemDto::Http(http) => outbound_evidence.push(ProjectionEvidence {
                    source_index,
                    value: ProjectionOutbound::Http(http.clone()),
                }),
            }
        }
    }
    Ok(ProjectionGroup {
        schema_version: 1,
        policy: policy.clone(),
        policy_id: policy.id()?,
        source_before: step.before_cursor().clone(),
        source_after: step.after_cursor().clone(),
        owner: step.owner().cloned(),
        clock,
        source,
        release: step.handoff().map(|h| ProjectionRelease {
            input_id: h.input_id,
            intent_ordinal: h.intent_ordinal,
            observed_ns: h.observed_ns,
            outputs_eligible: h.outputs_eligible,
            operation_route: h.operation.route.clone(),
        }),
        items,
        item_positions,
        outbound_evidence,
    })
}
impl ProjectionPump {
    /// All compatibility/bound checks precede new target creation. The private
    /// actor has one submitter; consumers may independently tail its disk feed.
    pub fn create(
        source: MultiHistoryReplay,
        policy: ProjectionPolicy,
        path: impl AsRef<Path>,
        max_bytes: u64,
        batch: BatchConfig,
    ) -> Result<Self, ProjectionError> {
        policy.validate()?;
        batch.validate()?;
        let progress = source.progress()?;
        if source.profile() != policy.source_feed_profile()?
            || progress.cursor.sequence != 0
            || progress.cursor.offset != 48
            || !progress.owners.is_empty()
            || progress.sealed
        {
            return Err(ProjectionError::Invalid(
                "fresh projection requires source header and matching profile",
            ));
        }
        // Standard base64 never escapes. 4096 conservatively covers the two
        // fixed-width cursor objects, envelope, WireRecord and length/CRC bytes.
        let worst = policy
            .limits
            .max_group_bytes
            .div_ceil(3)
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(ProjectionError::Invalid("target sizing overflow"))?;
        if batch.max_group_bytes < worst || batch.max_pending_bytes < batch.max_group_bytes {
            return Err(ProjectionError::Invalid(
                "target actor cannot retain one maximum projection group",
            ));
        }
        let writer =
            FeedWriter::create_profile(path, max_bytes, FeedProfile::ObservationProjectionV1)?;
        let target_cursor = writer.cursor().clone();
        let target = FeedBatchActor::spawn(writer, batch)?;
        Ok(Self {
            source,
            target,
            policy,
            checkpoint: ProjectionCheckpoint {
                source: progress.cursor,
                target: target_cursor,
                sealed: false,
            },
            prepared: None,
            accepted: None,
            prepared_groups: 0,
            poisoned: false,
        })
    }
    /// Last ACK only. After failure, a later indeterminate complete target group
    /// may exist; this does not assert that the target ends at this checkpoint.
    pub fn last_acknowledged(&self) -> &ProjectionCheckpoint {
        &self.checkpoint
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn prepared_groups(&self) -> u64 {
        self.prepared_groups
    }
    pub fn poll(&mut self) -> Result<ProjectionPoll, ProjectionError> {
        if self.poisoned {
            return Err(ProjectionError::Poisoned);
        }
        let handle = self.target.handle();
        let result = self.poll_with(|transaction| handle.try_submit(transaction));
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn poll_with(
        &mut self,
        admit: impl FnOnce(PreparedFeedTransaction) -> Result<CommitTicket, RejectedTransaction>,
    ) -> Result<ProjectionPoll, ProjectionError> {
        if self.checkpoint.sealed {
            return Ok(ProjectionPoll::Sealed);
        }
        if let Some(a) = self.accepted.as_mut() {
            let Some(result) = a.ticket.try_result() else {
                return Ok(ProjectionPoll::Writing);
            };
            let receipt = result?;
            if Some(receipt.first_sequence) != self.checkpoint.target.sequence.checked_add(1)
                || receipt.first_sequence != receipt.last_sequence
                || receipt.published.sequence != receipt.last_sequence
                || receipt.published.feed_id != self.checkpoint.target.feed_id
                || receipt.published.offset <= self.checkpoint.target.offset
                || Some(a.after.sequence) != self.checkpoint.source.sequence.checked_add(1)
                || a.after.feed_id != self.checkpoint.source.feed_id
            {
                return Err(ProjectionError::Invalid(
                    "target publication receipt/source boundary",
                ));
            }
            self.checkpoint = ProjectionCheckpoint {
                source: a.after.clone(),
                target: receipt.published,
                sealed: a.sealed,
            };
            self.accepted = None;
            return Ok(ProjectionPoll::Committed);
        }
        if let Some(p) = self.prepared.take() {
            return match admit(p.transaction) {
                Ok(ticket) => {
                    self.accepted = Some(Accepted {
                        after: p.after,
                        sealed: p.sealed,
                        ticket,
                    });
                    Ok(ProjectionPoll::Writing)
                }
                Err(RejectedTransaction {
                    reason: AdmissionFailure::Full,
                    transaction,
                }) => {
                    self.prepared = Some(Prepared {
                        after: p.after,
                        sealed: p.sealed,
                        transaction,
                    });
                    Ok(ProjectionPoll::Full)
                }
                Err(rejected) => Err(ProjectionError::Admission(rejected.reason)),
            };
        }
        let Some(step) = self.source.read_next_verified()? else {
            return Ok(ProjectionPoll::CaughtUp);
        };
        if step.before_cursor() != &self.checkpoint.source {
            return Err(ProjectionError::Invalid(
                "source cursor advanced beyond acknowledged target",
            ));
        }
        let envelope = group(&step, &self.policy)?.encode()?;
        let sealed = envelope.source_sealed;
        let transaction = PreparedFeedTransaction::new(vec![FeedRecord::Projection(envelope)])?;
        self.prepared = Some(Prepared {
            after: step.after_cursor().clone(),
            sealed,
            transaction,
        });
        self.prepared_groups = self
            .prepared_groups
            .checked_add(1)
            .ok_or(ProjectionError::Invalid("group ordinal overflow"))?;
        Ok(ProjectionPoll::Prepared)
    }
    /// Accepted work is drained even if no poll observed its ACK. Recover any
    /// such complete group using the target reader, not an invented source ACK.
    pub fn shutdown(self) -> Result<BatchReport, BatchFailure> {
        self.target.shutdown()
    }
}

/// Independent native comparison through the target's initial observed durable
/// bound. Growth after reader open cannot extend this one-shot comparison.
/// A standalone target reader checks structure, not authenticity of maliciously
/// rewritten values. This comparison uses the original executable-bound source.
/// Source may be ahead of a live target; caught-up does not imply source closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionComparison {
    pub groups: u64,
    pub source_after: Option<FeedCursor>,
    pub target_after: FeedCursor,
    pub sealed: bool,
}
pub fn compare_with_source(
    mut source: MultiHistoryReplay,
    mut target: ProjectionReader,
) -> Result<ProjectionComparison, ProjectionError> {
    if source.progress()?.cursor.sequence != 0 || target.progress()?.target_cursor.sequence != 0 {
        return Err(ProjectionError::Invalid(
            "whole-prefix comparison requires both headers",
        ));
    }
    let mut groups = 0u64;
    let bound = target.committed_bound()?.clone();
    while let Some(actual) = target.read_next_through(&bound)? {
        let step = source
            .read_next_verified()?
            .ok_or(ProjectionError::Invalid(
                "projection exceeds committed source prefix",
            ))?;
        if group(&step, &actual.policy)? != actual {
            return Err(ProjectionError::Invalid(
                "projection differs from actual verified source",
            ));
        }
        groups = groups.checked_add(1).ok_or(ProjectionError::Invalid(
            "projection comparison count overflow",
        ))?;
    }
    let progress = target.progress()?;
    if progress.target_cursor != bound {
        return Err(ProjectionError::Invalid(
            "projection comparison ended before bound",
        ));
    }
    Ok(ProjectionComparison {
        groups,
        source_after: progress.source_cursor,
        target_after: progress.target_cursor,
        sealed: progress.sealed,
    })
}

#[cfg(all(test, feature = "causal-journal"))]
#[path = "projection_tests.rs"]
mod tests;
