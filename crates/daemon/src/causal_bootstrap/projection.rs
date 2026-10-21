//! One prepared bounded transaction or accepted ticket, outside source owner execution. Fresh
//! target only: no append takeover, external delivery or consumer activation.
use super::replay::*;
use super::*;
use marketfeed_engine::causal::projection::*;
use std::{
    path::Path,
    time::{Duration, Instant},
};

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
/// Last actually observed ACK interval, exclusive before/inclusive after.
/// It is progress evidence, not authority to append or a claim that a later
/// indeterminate publication cannot exist on disk.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ProjectionPublicationReceipt {
    pub groups: u64,
    pub source_before: FeedCursor,
    pub source_after: FeedCursor,
    pub target_before: FeedCursor,
    pub target_after: FeedCursor,
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
    groups: usize,
    after: FeedCursor,
    sealed: bool,
    ticket: CommitTicket,
}
pub struct ProjectionPump {
    source: MultiHistoryReplay,
    target: FeedBatchActor,
    policy: ProjectionPolicy,
    checkpoint: ProjectionCheckpoint,
    last_publication: Option<ProjectionPublicationReceipt>,
    prepared: Option<Prepared>,
    accepted: Option<Accepted>,
    prepared_groups: u64,
    max_groups_per_publication: usize,
    max_publication_bytes: usize,
    worst_record_bytes: usize,
    max_preparation_age: Duration,
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
        Self::create_batched(source, policy, path, max_bytes, batch, 1)
    }
    /// Opt-in physical grouping of complete, independently resumable MFOP
    /// records. No lookahead is held: another native replay step is read only
    /// when the remaining actor byte budget can fit a policy-maximum record.
    /// Preparation checks the existing grouping age between bounded replay
    /// calls; scheduling, native execution and I/O are not interruptible here.
    pub fn create_batched(
        source: MultiHistoryReplay,
        policy: ProjectionPolicy,
        path: impl AsRef<Path>,
        max_bytes: u64,
        batch: BatchConfig,
        max_groups_per_publication: usize,
    ) -> Result<Self, ProjectionError> {
        policy.validate()?;
        batch.validate()?;
        if !(1..=8).contains(&max_groups_per_publication)
            || max_groups_per_publication > batch.max_group_operations
        {
            return Err(ProjectionError::Invalid("physical projection batch count"));
        }
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
            last_publication: None,
            prepared: None,
            accepted: None,
            prepared_groups: 0,
            max_groups_per_publication,
            max_publication_bytes: batch.max_group_bytes,
            worst_record_bytes: worst,
            max_preparation_age: batch.max_age,
            poisoned: false,
        })
    }
    /// Last ACK only. After failure, a later indeterminate complete target group
    /// may exist; this does not assert that the target ends at this checkpoint.
    pub fn last_acknowledged(&self) -> &ProjectionCheckpoint {
        &self.checkpoint
    }
    pub fn last_publication(&self) -> Option<&ProjectionPublicationReceipt> {
        self.last_publication.as_ref()
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn prepared_groups(&self) -> u64 {
        self.prepared_groups
    }
    pub fn max_groups_per_publication(&self) -> usize {
        self.max_groups_per_publication
    }
    pub fn poll(&mut self) -> Result<ProjectionPoll, ProjectionError> {
        if self.poisoned {
            return Err(ProjectionError::Poisoned);
        }
        let handle = self.target.handle();
        self.poll_with(|transaction| handle.try_submit(transaction))
    }
    fn poll_with(
        &mut self,
        admit: impl FnOnce(PreparedFeedTransaction) -> Result<CommitTicket, RejectedTransaction>,
    ) -> Result<ProjectionPoll, ProjectionError> {
        let began = Instant::now();
        self.poll_with_preparation_age(admit, || began.elapsed())
    }
    fn poll_with_preparation_age(
        &mut self,
        admit: impl FnOnce(PreparedFeedTransaction) -> Result<CommitTicket, RejectedTransaction>,
        preparation_age: impl FnMut() -> Duration,
    ) -> Result<ProjectionPoll, ProjectionError> {
        if self.poisoned {
            return Err(ProjectionError::Poisoned);
        }
        let result = self.poll_inner(admit, preparation_age);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn poll_inner(
        &mut self,
        admit: impl FnOnce(PreparedFeedTransaction) -> Result<CommitTicket, RejectedTransaction>,
        mut preparation_age: impl FnMut() -> Duration,
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
                || a.groups == 0
                || a.groups > self.max_groups_per_publication
                || Some(receipt.last_sequence)
                    != self.checkpoint.target.sequence.checked_add(a.groups as u64)
                || receipt.published.sequence != receipt.last_sequence
                || receipt.published.feed_id != self.checkpoint.target.feed_id
                || receipt.published.offset <= self.checkpoint.target.offset
                || Some(a.after.sequence)
                    != self.checkpoint.source.sequence.checked_add(a.groups as u64)
                || a.after.offset <= self.checkpoint.source.offset
                || a.after.feed_id != self.checkpoint.source.feed_id
            {
                return Err(ProjectionError::Invalid(
                    "target publication receipt/source boundary",
                ));
            }
            self.last_publication = Some(ProjectionPublicationReceipt {
                groups: a.groups as u64,
                source_before: self.checkpoint.source.clone(),
                source_after: a.after.clone(),
                target_before: self.checkpoint.target.clone(),
                target_after: receipt.published.clone(),
            });
            self.checkpoint = ProjectionCheckpoint {
                source: a.after.clone(),
                target: receipt.published,
                sealed: a.sealed,
            };
            self.accepted = None;
            return Ok(ProjectionPoll::Committed);
        }
        if let Some(p) = self.prepared.take() {
            let groups = p.transaction.record_count();
            return match admit(p.transaction) {
                Ok(ticket) => {
                    self.accepted = Some(Accepted {
                        groups,
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
        let mut after = self.checkpoint.source.clone();
        let mut records = Vec::with_capacity(self.max_groups_per_publication);
        let mut charged = 0usize;
        let mut sealed = false;
        while records.len() < self.max_groups_per_publication {
            // Never consume a step that would have to be held for a later
            // transaction. A maximum record must fit before native replay.
            if !records.is_empty()
                && (self.max_publication_bytes.saturating_sub(charged) < self.worst_record_bytes
                    || preparation_age() >= self.max_preparation_age)
            {
                break;
            }
            let Some(step) = self.source.read_next_verified()? else {
                break;
            };
            if step.before_cursor() != &after {
                return Err(ProjectionError::Invalid(
                    "source cursor advanced beyond prepared transaction",
                ));
            }
            let envelope = group(&step, &self.policy)?.encode()?;
            sealed = envelope.source_sealed;
            charged = charged
                .checked_add(envelope.payload_base64.len())
                .and_then(|n| n.checked_add(4096))
                .filter(|n| *n <= self.max_publication_bytes)
                .ok_or(ProjectionError::Invalid(
                    "projection transaction byte budget",
                ))?;
            after = step.after_cursor().clone();
            records.push(FeedRecord::Projection(envelope));
            if sealed {
                break;
            }
        }
        if records.is_empty() {
            return Ok(ProjectionPoll::CaughtUp);
        }
        let transaction = PreparedFeedTransaction::new(records)?;
        if transaction.encoded_bytes_bound() > self.max_publication_bytes {
            return Err(ProjectionError::Invalid(
                "projection transaction framing budget",
            ));
        }
        self.prepared_groups = self
            .prepared_groups
            .checked_add(transaction.record_count() as u64)
            .ok_or(ProjectionError::Invalid("group ordinal overflow"))?;
        self.prepared = Some(Prepared {
            after,
            sealed,
            transaction,
        });
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
