//! Physical-order multi-owner replay. All native state remains private; one
//! owner's unknown execution never authorizes reconstruction of its missing turn.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

macro_rules! resources {
    ($( $(#[$meta:meta])* $field:ident ),+ $(,)?) => {
        /// Logical reservations/retention, not measured native bytes or RSS.
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub struct ReplayResources { $( $(#[$meta])* pub $field: usize, )+ }
        impl ReplayResources {
            fn updated(self, remove: Self, add: Self) -> Result<Self, ReplayError> {
                Ok(Self { $( $field: self.$field.checked_sub(remove.$field)
                    .and_then(|n| n.checked_add(add.$field))
                    .ok_or(ReplayError::Invalid("aggregate replay accounting overflow"))?, )+ })
            }
            fn within(&self, maximum: &Self) -> bool {
                $( self.$field <= maximum.$field )&&+
            }
        }
    }
}
resources! {
    owners,
    /// Sum of admitted bootstrap canonical payload lengths; never evicted.
    bootstrap_payload_bytes,
    pending_inputs,
    pending_input_bytes,
    /// Active OperationDto plus separately retained previous_after StateDto.
    active_operation_and_state_bytes,
    source_owned_pending_inputs,
    /// Declared source-owned bounds, distinct from canonical encoded bytes.
    source_owned_pending_bytes,
    /// Sum of per-trace capture max_bytes declarations, not native allocations.
    capture_trace_reservation_bytes,
    catalog_entries,
    selected_instruments,
    /// Reserved successful native maintained L2 levels at configured depth.
    maintained_book_levels,
    /// Bybit max decoded delta rows plus cached top-of-book quote pairs.
    cached_book_levels,
}
impl From<OwnerHistoryRetention> for ReplayResources {
    fn from(r: OwnerHistoryRetention) -> Self {
        Self {
            pending_inputs: r.pending_inputs,
            pending_input_bytes: r.pending_input_bytes,
            active_operation_and_state_bytes: r.active_operation_and_state_bytes,
            source_owned_pending_inputs: r.source_owned_pending_inputs,
            source_owned_pending_bytes: r.source_owned_pending_bytes,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiReplayLimits {
    pub per_owner: OwnerHistoryLimits,
    /// Zero is permitted for resources the caller intentionally refuses.
    /// Owners/bootstrap/capture/catalog/selected bounds must be positive.
    pub maximum: ReplayResources,
}
impl MultiReplayLimits {
    fn validate(&self) -> Result<(), ReplayError> {
        self.per_owner.validate()?;
        let m = self.maximum;
        let byte_ceiling = 512 * 1024 * 1024;
        if m.owners == 0
            || m.owners > 4096
            || m.bootstrap_payload_bytes == 0
            || m.bootstrap_payload_bytes > byte_ceiling
            || m.capture_trace_reservation_bytes == 0
            || m.capture_trace_reservation_bytes > byte_ceiling
            || m.catalog_entries == 0
            || m.catalog_entries > 65_536
            || m.selected_instruments == 0
            || m.selected_instruments > 65_536
            || m.pending_inputs > 65_536
            || m.source_owned_pending_inputs > 65_536
            || m.pending_input_bytes > byte_ceiling
            || m.active_operation_and_state_bytes > byte_ceiling
            || m.source_owned_pending_bytes > byte_ceiling
            || m.maintained_book_levels > 65_536 * 8194
            || m.cached_book_levels > 65_536 * 8194
        {
            return Err(ReplayError::Invalid("aggregate replay reader bounds"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerReplayProgress {
    pub identity: OperationIdentity,
    pub journal_ordinal: u64,
    pub native_ordinal: u64,
    pub terminal: bool,
    pub retention: OwnerHistoryRetention,
    pub uncertainties: Vec<OwnerPrefixUncertainty>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiReplayProgress {
    pub cursor: FeedCursor,
    pub sealed: bool,
    pub resources: ReplayResources,
    /// SessionId order, independent of physical journal order.
    pub owners: Vec<OwnerReplayProgress>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiReplayStep {
    pub cursor: FeedCursor,
    /// Full identity for this physical owner record; None only for Seal.
    pub owner: Option<OperationIdentity>,
    /// Only a committed, native-matched observed handoff. Returned values are
    /// owned by the caller and no longer counted as reader retention.
    pub handoff: Option<RecordedOwnerHandoff>,
}

/// One disk cursor and a bounded map of persistent actual factories. No
/// transport, external delivery, native checkpoint or writable feed exists here.
pub struct MultiHistoryReplay {
    reader: FeedReader,
    owners: BTreeMap<u64, OwnerReplayState>,
    limits: MultiReplayLimits,
    resources: ReplayResources,
    sealed: bool,
    poisoned: bool,
}
impl MultiHistoryReplay {
    /// Starts at the physical header. Encountering a bootstrap on read_next
    /// creates its factory once; no later records are prescanned for metadata.
    pub fn open(path: impl AsRef<Path>, limits: MultiReplayLimits) -> Result<Self, ReplayError> {
        Self::open_profile(path, limits, FeedProfile::EngineCausalV3)
    }
    pub fn open_profile(
        path: impl AsRef<Path>,
        limits: MultiReplayLimits,
        profile: FeedProfile,
    ) -> Result<Self, ReplayError> {
        if !cfg!(feature = "causal-journal") {
            return Err(ReplayError::Invalid("causal-journal feature disabled"));
        }
        limits.validate()?;
        if !matches!(
            profile,
            FeedProfile::EngineCausalV3 | FeedProfile::EngineCausalV4
        ) {
            return Err(ReplayError::Invalid(
                "unsupported actual owner replay profile",
            ));
        }
        let reader = FeedReader::open_profile(path, None, profile)?;
        Ok(Self {
            reader,
            owners: BTreeMap::new(),
            limits,
            resources: ReplayResources::default(),
            sealed: false,
            poisoned: false,
        })
    }
    /// Reconstruct all encountered owners to one exact global cursor and
    /// discard historical handoffs. An exact header cursor has zero owners.
    pub fn resume(
        path: impl AsRef<Path>,
        target: FeedCursor,
        limits: MultiReplayLimits,
    ) -> Result<Self, ReplayError> {
        Self::resume_profile(path, target, limits, FeedProfile::EngineCausalV3)
    }
    pub fn resume_profile(
        path: impl AsRef<Path>,
        target: FeedCursor,
        limits: MultiReplayLimits,
        profile: FeedProfile,
    ) -> Result<Self, ReplayError> {
        let mut replay = Self::open_profile(path, limits, profile)?;
        let initial = replay.reader.cursor();
        let bound = replay.reader.observed_commit();
        if target.feed_id != bound.feed_id
            || target.sequence > bound.sequence
            || target.offset > bound.offset
            || target.offset < initial.offset
            || (target.sequence == 0 && &target != initial)
        {
            return Err(ReplayError::Invalid(
                "resume exceeds initial committed bound or changes feed",
            ));
        }
        while replay.reader.cursor().sequence < target.sequence {
            if replay.advance(Some(&target))?.is_none() {
                return Err(ReplayError::Invalid("resume exceeds committed prefix"));
            }
        }
        if replay.reader.cursor() != &target {
            return Err(ReplayError::Invalid(
                "resume is not an exact physical boundary",
            ));
        }
        Ok(replay)
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn progress(&self) -> Result<MultiReplayProgress, ReplayError> {
        if self.poisoned {
            return Err(ReplayError::Poisoned);
        }
        let owners = self
            .owners
            .values()
            .map(|o| {
                Ok(OwnerReplayProgress {
                    identity: o.history.identity().clone(),
                    journal_ordinal: o.history.ordinal(),
                    native_ordinal: o.history.native_ordinal(),
                    terminal: o.history.is_terminal(),
                    retention: o.history.retention()?,
                    uncertainties: o.history.uncertainties(),
                })
            })
            .collect::<Result<_, ReplayError>>()?;
        Ok(MultiReplayProgress {
            cursor: self.reader.cursor().clone(),
            sealed: self.sealed,
            resources: self.resources,
            owners,
        })
    }
    pub fn read_next(&mut self) -> Result<Option<MultiReplayStep>, ReplayError> {
        if self.poisoned {
            return Err(ReplayError::Poisoned);
        }
        let result = self.advance(None);
        if result.is_err() {
            self.poisoned = true;
            // A rejected structural update may temporarily exceed an aggregate
            // limit by one bounded record. Do not retain that over-budget state
            // for the lifetime of a poisoned reader. No usable owner is evicted.
            self.owners.clear();
            self.resources = ReplayResources::default();
        }
        result
    }
    fn advance(
        &mut self,
        ceiling: Option<&FeedCursor>,
    ) -> Result<Option<MultiReplayStep>, ReplayError> {
        let Some(record) = self.reader.read_next()? else {
            return Ok(None);
        };
        if ceiling.is_some_and(|target| {
            self.reader.cursor().sequence > target.sequence
                || self.reader.cursor().offset > target.offset
                || (self.reader.cursor().sequence == target.sequence
                    && self.reader.cursor() != target)
        }) {
            return Err(ReplayError::Invalid("record crosses exact resume boundary"));
        }
        let (owner, handoff) =
            match record {
                FeedRecord::Causal(envelope) if envelope.phase == CausalPhase::Bootstrap => {
                    if self.owners.contains_key(&envelope.route.session) {
                        return Err(ReplayError::Invalid("duplicate global session bootstrap"));
                    }
                    let (history, dto) =
                        verified_bootstrap(&envelope, self.limits.per_owner.clone())?;
                    let reserve = bootstrap_reservation(&dto, envelope.payload()?.len())?;
                    let candidate = self
                        .resources
                        .updated(ReplayResources::default(), reserve)?;
                    if !candidate.within(&self.limits.maximum) {
                        return Err(ReplayError::Invalid("aggregate replay bootstrap budget"));
                    }
                    // All aggregate checks precede the one pure native construction.
                    let capture = capture_from_supported(&dto)?;
                    let identity = history.identity().clone();
                    self.owners
                        .insert(identity.session, OwnerReplayState { history, capture });
                    self.resources = candidate;
                    (Some(identity), None)
                }
                FeedRecord::Causal(envelope) => {
                    let owner = self.owners.get_mut(&envelope.route.session).ok_or(
                        ReplayError::Invalid("record has no verified owner bootstrap"),
                    )?;
                    let previous = ReplayResources::from(owner.history.retention()?);
                    let handoff = owner.history.apply(&envelope)?;
                    let next = ReplayResources::from(owner.history.retention()?);
                    let candidate = self.resources.updated(previous, next)?;
                    if !candidate.within(&self.limits.maximum) {
                        return Err(ReplayError::Invalid(
                            "aggregate replay retained-state budget",
                        ));
                    }
                    owner.verify_native(&envelope)?;
                    self.resources = candidate;
                    (Some(owner.history.identity().clone()), handoff)
                }
                FeedRecord::Seal { .. } => {
                    self.sealed = true;
                    (None, None)
                }
                _ => return Err(ReplayError::Invalid("non-owner record in owner journal")),
            };
        Ok(Some(MultiReplayStep {
            cursor: self.reader.cursor().clone(),
            owner,
            handoff,
        }))
    }
}

fn bootstrap_reservation(
    dto: &BootstrapDto,
    payload_bytes: usize,
) -> Result<ReplayResources, ReplayError> {
    let selected: BTreeSet<_> = dto
        .session
        .subscriptions
        .iter()
        .map(|s| s.instrument)
        .collect();
    let mut books = BTreeMap::<u32, u32>::new();
    for s in &dto.session.subscriptions {
        if let ChannelDto::L2Book { depth, .. } = s.channel {
            let default = match dto.factory {
                FactoryDto::BybitPublic { .. } => 50,
                _ => 20,
            };
            let depth = depth.unwrap_or(default);
            books
                .entry(s.instrument)
                .and_modify(|old| *old = (*old).max(depth))
                .or_insert(depth);
        }
    }
    let maintained_book_levels = books
        .values()
        .try_fold(0usize, |total, depth| {
            total.checked_add((*depth as usize).checked_mul(2)?)
        })
        .ok_or(ReplayError::Invalid("native book reservation overflow"))?;
    // Current Bybit paths never buffer BookSynchronizer deltas; they retain
    // one decoded message (<=4096 rows/side) and a separate best quote pair.
    let cached_book_levels = match dto.factory {
        FactoryDto::BybitPublic { enable_l2, .. } => selected
            .len()
            .checked_mul(if enable_l2 { 8194 } else { 2 })
            .ok_or(ReplayError::Invalid("native cache reservation overflow"))?,
        FactoryDto::HyperliquidPublic { .. } => 0,
    };
    Ok(ReplayResources {
        owners: 1,
        bootstrap_payload_bytes: payload_bytes,
        capture_trace_reservation_bytes: usize::try_from(dto.capture.max_bytes)
            .map_err(|_| ReplayError::Invalid("capture reservation overflow"))?,
        catalog_entries: dto.catalog.instruments.len(),
        selected_instruments: selected.len(),
        maintained_book_levels,
        cached_book_levels,
        ..ReplayResources::default()
    })
}
