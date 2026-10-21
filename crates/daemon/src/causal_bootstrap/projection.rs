//! Bounded prepared and accepted transactions, outside source owner execution. Fresh
//! target only: no append takeover, external delivery or consumer activation.
use super::replay::*;
use super::*;
use marketfeed_engine::causal::projection::*;
use std::{
    collections::VecDeque,
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
    bytes: usize,
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
    accepted: VecDeque<Accepted>,
    last_acknowledged_admission: u64,
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
    /// records. Prepared and accepted work share the same bounded record/byte
    /// window; another native replay step requires room for a maximum record.
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
            accepted: VecDeque::new(),
            last_acknowledged_admission: 0,
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
        self.poll_with_receipts(admit, preparation_age, CommitTicket::try_result)
    }
    fn poll_with_receipts(
        &mut self,
        admit: impl FnOnce(PreparedFeedTransaction) -> Result<CommitTicket, RejectedTransaction>,
        preparation_age: impl FnMut() -> Duration,
        receive: impl FnMut(&mut CommitTicket) -> Option<Result<CommitReceipt, CommitFailure>>,
    ) -> Result<ProjectionPoll, ProjectionError> {
        if self.poisoned {
            return Err(ProjectionError::Poisoned);
        }
        let result = self.poll_inner(admit, preparation_age, receive);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn accepted_budget(&self) -> Result<(usize, usize), ProjectionError> {
        let mut groups = 0usize;
        let mut bytes = 0usize;
        let mut sealed = false;
        for a in &self.accepted {
            if sealed || a.groups == 0 || a.bytes == 0 {
                return Err(ProjectionError::Invalid("accepted projection window"));
            }
            groups = groups
                .checked_add(a.groups)
                .filter(|n| *n <= self.max_groups_per_publication)
                .ok_or(ProjectionError::Invalid("projection record window"))?;
            bytes = bytes
                .checked_add(a.bytes)
                .filter(|n| *n <= self.max_publication_bytes)
                .ok_or(ProjectionError::Invalid("projection encoded byte window"))?;
            sealed = a.sealed;
        }
        Ok((groups, bytes))
    }
    fn validate_window(&self) -> Result<(), ProjectionError> {
        let (groups, bytes) = self.accepted_budget()?;
        if let Some(p) = &self.prepared {
            if self.accepted.back().is_some_and(|a| a.sealed)
                || p.transaction.record_count() == 0
                || groups
                    .checked_add(p.transaction.record_count())
                    .is_none_or(|n| n > self.max_groups_per_publication)
                || bytes
                    .checked_add(p.transaction.encoded_bytes_bound())
                    .is_none_or(|n| n > self.max_publication_bytes)
            {
                return Err(ProjectionError::Invalid("prepared projection window"));
            }
        }
        Ok(())
    }
    /// A ticket names one transaction, but its published cursor covers the
    /// entire physical actor group. Keep every reservation until all receipts
    /// for that known group prove the same complete target/source interval.
    fn observe_publication(
        &mut self,
        receive: &mut impl FnMut(&mut CommitTicket) -> Option<Result<CommitReceipt, CommitFailure>>,
    ) -> Result<bool, ProjectionError> {
        let Some(front) = self.accepted.front_mut() else {
            return Ok(false);
        };
        let Some(result) = receive(&mut front.ticket) else {
            return Ok(false);
        };
        let first_receipt = result?;
        let published = &first_receipt.published;
        if published.feed_id != self.checkpoint.target.feed_id
            || published.sequence <= self.checkpoint.target.sequence
            || published.offset <= self.checkpoint.target.offset
        {
            return Err(ProjectionError::Invalid("target publication cursor"));
        }
        let mut target_sequence = self.checkpoint.target.sequence;
        let mut source_after = self.checkpoint.source.clone();
        let mut admission = self.last_acknowledged_admission;
        let mut groups = 0usize;
        let mut sealed = false;
        let mut covered = None;
        for (index, a) in self.accepted.iter().enumerate() {
            target_sequence = target_sequence
                .checked_add(a.groups as u64)
                .ok_or(ProjectionError::Invalid("target sequence overflow"))?;
            if sealed
                || admission.checked_add(1) != Some(a.ticket.admission())
                || source_after.sequence.checked_add(a.groups as u64) != Some(a.after.sequence)
                || source_after.feed_id != a.after.feed_id
                || source_after.offset >= a.after.offset
            {
                return Err(ProjectionError::Invalid("accepted source/admission chain"));
            }
            source_after = a.after.clone();
            admission = a.ticket.admission();
            groups += a.groups; // validate_window already bounds the whole FIFO.
            sealed = a.sealed;
            if target_sequence >= published.sequence {
                if target_sequence != published.sequence {
                    return Err(ProjectionError::Invalid("publication splits transaction"));
                }
                covered = Some(index + 1);
                break;
            }
        }
        let covered = covered.ok_or(ProjectionError::Invalid("unknown publication boundary"))?;
        let mut first_sequence = self
            .checkpoint
            .target
            .sequence
            .checked_add(1)
            .ok_or(ProjectionError::Invalid("target sequence overflow"))?;
        for index in 0..covered {
            let a = &mut self.accepted[index];
            let receipt = if index == 0 {
                first_receipt.clone()
            } else {
                let Some(result) = receive(&mut a.ticket) else {
                    // CommitTicket caches already observed receipts. Actor
                    // status/refunded credits cannot release this reservation.
                    return Ok(false);
                };
                result?
            };
            let last_sequence = first_sequence
                .checked_add(a.groups as u64 - 1)
                .ok_or(ProjectionError::Invalid("target sequence overflow"))?;
            if receipt.admission != a.ticket.admission()
                || receipt.first_sequence != first_sequence
                || receipt.last_sequence != last_sequence
                || receipt.published != first_receipt.published
            {
                return Err(ProjectionError::Invalid("physical publication receipts"));
            }
            if index + 1 != covered {
                first_sequence = last_sequence
                    .checked_add(1)
                    .ok_or(ProjectionError::Invalid("target sequence overflow"))?;
            }
        }
        self.last_publication = Some(ProjectionPublicationReceipt {
            groups: groups as u64,
            source_before: self.checkpoint.source.clone(),
            source_after: source_after.clone(),
            target_before: self.checkpoint.target.clone(),
            target_after: first_receipt.published.clone(),
        });
        self.checkpoint = ProjectionCheckpoint {
            source: source_after,
            target: first_receipt.published,
            sealed,
        };
        self.last_acknowledged_admission = admission;
        for _ in 0..covered {
            self.accepted.pop_front();
        }
        Ok(true)
    }
    fn poll_inner(
        &mut self,
        admit: impl FnOnce(PreparedFeedTransaction) -> Result<CommitTicket, RejectedTransaction>,
        mut preparation_age: impl FnMut() -> Duration,
        mut receive: impl FnMut(&mut CommitTicket) -> Option<Result<CommitReceipt, CommitFailure>>,
    ) -> Result<ProjectionPoll, ProjectionError> {
        self.validate_window()?;
        if self.checkpoint.sealed {
            if !self.accepted.is_empty() || self.prepared.is_some() {
                return Err(ProjectionError::Invalid("work after acknowledged seal"));
            }
            return Ok(ProjectionPoll::Sealed);
        }
        if self.observe_publication(&mut receive)? {
            return Ok(ProjectionPoll::Committed);
        }
        if let Some(p) = self.prepared.take() {
            let groups = p.transaction.record_count();
            let bytes = p.transaction.encoded_bytes_bound();
            return match admit(p.transaction) {
                Ok(ticket) => {
                    self.accepted.push_back(Accepted {
                        groups,
                        bytes,
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
        let (pending_groups, pending_bytes) = self.accepted_budget()?;
        if pending_groups == self.max_groups_per_publication
            || self.accepted.back().is_some_and(|a| a.sealed)
            || self.max_publication_bytes - pending_bytes < self.worst_record_bytes
        {
            return Ok(ProjectionPoll::Writing);
        }
        let mut after = self
            .accepted
            .back()
            .map_or_else(|| self.checkpoint.source.clone(), |a| a.after.clone());
        let mut records = Vec::with_capacity(self.max_groups_per_publication - pending_groups);
        let mut charged = 0usize;
        let mut sealed = false;
        while records.len() < self.max_groups_per_publication - pending_groups {
            // Reserve a policy-maximum record before EVERY replay read, even
            // the first in a new candidate beside an accepted transaction.
            if self.max_publication_bytes - pending_bytes - charged < self.worst_record_bytes
                || (!records.is_empty() && preparation_age() >= self.max_preparation_age)
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
                .filter(|n| *n <= self.max_publication_bytes - pending_bytes)
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
            return Ok(if self.accepted.is_empty() {
                ProjectionPoll::CaughtUp
            } else {
                ProjectionPoll::Writing
            });
        }
        let transaction = PreparedFeedTransaction::new(records)?;
        if transaction.encoded_bytes_bound() > self.max_publication_bytes - pending_bytes {
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
