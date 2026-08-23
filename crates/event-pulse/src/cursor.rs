//! Bounded, preallocated source epoch and cursor state for EventPulse mechanics.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::wire::{
    ClockSourceKeyV1, ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1,
    CoverageSourceKeyV1, CursorModeV1, CursorV1, DropCategoryV1, FamilyV1, FaultScopeRefV1,
    MechanicsConfigV1, MechanicsInputRefV1, MechanicsInputV1, Rfc3339Time, SystemChainPreimage,
    SystemFaultRefV1, SystemSourceKeyV1,
};
use crate::wire_v2::{MarketCursorV2, MechanicsInputRefV2, MechanicsInputV2};

const WARMUP_NS: i64 = 60_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Cold,
    Warming,
    Live,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalidity {
    Recoverable,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    AcceptedWarming,
    AcceptedLive,
    Invalidated,
    IgnoredDuplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CursorError {
    #[error("stable source identity is not configured")]
    UnconfiguredIdentity,
    #[error("subject contributor is not initialized")]
    SubjectNotInitialized,
    #[error("subject contributor epoch does not match current state")]
    SubjectEpochMismatch,
    #[error("source epoch regressed or changed without a greater generation")]
    EpochMismatch,
    #[error("source epoch was already used")]
    EpochReused,
    #[error("bounded epoch history is exhausted")]
    EpochHistoryExhausted,
    #[error("native cursor overlaps the accepted range")]
    NativeOverlap,
    #[error("native cursor has a gap")]
    NativeGap,
    #[error("native cursor regressed")]
    NativeRegression,
    #[error("derived cursor regressed")]
    DerivedRegression,
    #[error("cursor coordinate was reused with different payload")]
    MutatedDuplicate,
    #[error("availability time decreased")]
    AvailabilityRegression,
    #[error("cursor mode does not match configured source")]
    CursorMode,
    #[error("system causal predecessor does not match current chain head")]
    SystemPredecessor,
    #[error("system fault scope does not match current target state")]
    FaultScopeMismatch,
    #[error("system fault expands to no initialized configured contributors")]
    EmptyFaultExpansion,
    #[error("checked timestamp arithmetic overflowed")]
    TimeOverflow,
    #[error("source slot is terminally invalid and requires a new processor")]
    TerminalInvalid,
}

impl CursorError {
    pub fn invalidates_state(&self) -> bool {
        matches!(
            self,
            Self::NativeOverlap
                | Self::NativeGap
                | Self::NativeRegression
                | Self::DerivedRegression
                | Self::MutatedDuplicate
                | Self::AvailabilityRegression
                | Self::EpochMismatch
                | Self::EpochReused
                | Self::EpochHistoryExhausted
                | Self::CursorMode
                | Self::TimeOverflow
                | Self::TerminalInvalid
        )
    }

    fn invalidates_slot(&self) -> bool {
        self.invalidates_state()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorView {
    pub epoch: String,
    pub epoch_generation: u8,
    pub cursor: CursorV1,
    pub available_at_ns: i64,
    pub payload_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCursorViewV2 {
    pub epoch: String,
    pub epoch_generation: u8,
    pub cursor: MarketCursorV2,
    pub available_at_ns: i64,
    pub payload_hash: String,
}

#[derive(Debug, Clone)]
struct MarketFamilySlotV2 {
    state: SlotState,
    invalidity: Option<Invalidity>,
    epoch: Option<String>,
    generation: Option<u8>,
    history: BTreeSet<String>,
    first_available_ns: Option<i64>,
    cursor: Option<MarketCursorV2>,
    available_at_ns: Option<i64>,
    retained_available_at_ns: Option<i64>,
    payload_hash: Option<String>,
    book_continuity: BookContinuityV2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookContinuityV2 {
    NotBook,
    AwaitingSnapshot,
    Snapshot(u64),
    Delta(u64),
}

impl MarketFamilySlotV2 {
    fn new(family: FamilyV1) -> Self {
        Self {
            state: SlotState::Cold,
            invalidity: None,
            epoch: None,
            generation: None,
            history: BTreeSet::new(),
            first_available_ns: None,
            cursor: None,
            available_at_ns: None,
            retained_available_at_ns: None,
            payload_hash: None,
            book_continuity: if family == FamilyV1::Book {
                BookContinuityV2::AwaitingSnapshot
            } else {
                BookContinuityV2::NotBook
            },
        }
    }

    fn reset_book_continuity(&mut self) {
        if self.book_continuity != BookContinuityV2::NotBook {
            self.book_continuity = BookContinuityV2::AwaitingSnapshot;
        }
    }

    fn clear_current(&mut self) {
        if self.invalidity == Some(Invalidity::Terminal) {
            self.state = SlotState::Invalid;
            return;
        }
        self.state = SlotState::Cold;
        self.invalidity = None;
        self.epoch = None;
        self.generation = None;
        self.first_available_ns = None;
        self.cursor = None;
        self.available_at_ns = None;
        self.payload_hash = None;
        self.reset_book_continuity();
    }

    fn preflight_epoch_transition(&mut self, epoch: &str) -> Result<(), CursorError> {
        if self.history.contains(epoch) {
            self.invalidate_terminal();
            return Err(CursorError::EpochReused);
        }
        if self.history.len() >= 256 {
            self.invalidate_terminal();
            return Err(CursorError::EpochHistoryExhausted);
        }
        Ok(())
    }

    fn begin_epoch(&mut self, epoch: &str, generation: u8, at_ns: i64) -> Result<(), CursorError> {
        if self.invalidity == Some(Invalidity::Terminal) {
            return Err(CursorError::TerminalInvalid);
        }
        self.preflight_epoch_transition(epoch)?;
        self.preflight_time(at_ns)?;
        self.history.insert(epoch.to_owned());
        self.epoch = Some(epoch.to_owned());
        self.generation = Some(generation);
        self.first_available_ns = Some(at_ns);
        self.cursor = None;
        self.available_at_ns = None;
        self.retained_available_at_ns = Some(at_ns);
        self.payload_hash = None;
        self.reset_book_continuity();
        self.state = SlotState::Warming;
        self.invalidity = None;
        Ok(())
    }

    fn prepare_epoch(
        &mut self,
        epoch: &str,
        generation: u8,
        at_ns: i64,
        allow_book_resnapshot: bool,
    ) -> Result<(), CursorError> {
        if self.invalidity == Some(Invalidity::Terminal) {
            return Err(CursorError::TerminalInvalid);
        }
        if self.epoch.as_deref() != Some(epoch) {
            self.preflight_epoch_transition(epoch)?;
        }
        match (self.generation, self.epoch.as_deref()) {
            (None, None) => self.begin_epoch(epoch, generation, at_ns),
            (Some(current), Some(current_epoch))
                if generation == current && epoch == current_epoch =>
            {
                if self.state == SlotState::Invalid {
                    if allow_book_resnapshot && self.invalidity == Some(Invalidity::Recoverable) {
                        self.preflight_time(at_ns)?;
                        self.state = SlotState::Warming;
                        self.invalidity = None;
                        self.first_available_ns = Some(at_ns);
                        self.cursor = None;
                        self.available_at_ns = None;
                        self.payload_hash = None;
                        self.reset_book_continuity();
                        Ok(())
                    } else {
                        Err(CursorError::EpochMismatch)
                    }
                } else {
                    if self.state == SlotState::Cold {
                        self.first_available_ns = Some(at_ns);
                        self.state = SlotState::Warming;
                    }
                    Ok(())
                }
            }
            (Some(current), _) if generation > current => {
                self.begin_epoch(epoch, generation, at_ns)
            }
            _ => {
                self.invalidate_recoverable();
                Err(CursorError::EpochMismatch)
            }
        }
    }

    fn accept_cursor(
        &mut self,
        cursor: &MarketCursorV2,
        at_ns: i64,
        payload_hash: &str,
    ) -> Result<IngestOutcome, CursorError> {
        self.preflight_time(at_ns)?;
        if self.available_at_ns.is_some_and(|last| at_ns < last) {
            self.invalidate_recoverable();
            return Err(CursorError::AvailabilityRegression);
        }
        if let Some(previous) = self.cursor.clone() {
            if &previous == cursor {
                if self.payload_hash.as_deref() == Some(payload_hash) {
                    return Ok(IngestOutcome::IgnoredDuplicate);
                }
                self.invalidate_recoverable();
                return Err(CursorError::MutatedDuplicate);
            }
            match (&previous, cursor) {
                (
                    MarketCursorV2::Native {
                        first_sequence: previous_start,
                        last_sequence: previous_end,
                    },
                    MarketCursorV2::Native {
                        first_sequence: start,
                        ..
                    },
                ) => {
                    if start <= previous_end {
                        self.invalidate_recoverable();
                        return Err(if start < previous_start {
                            CursorError::NativeRegression
                        } else {
                            CursorError::NativeOverlap
                        });
                    }
                    if previous_end.checked_add(1) != Some(*start) {
                        self.invalidate_recoverable();
                        return Err(CursorError::NativeGap);
                    }
                }
                (MarketCursorV2::Derived { .. }, MarketCursorV2::Derived { .. }) => {
                    if cursor < &previous {
                        self.invalidate_recoverable();
                        return Err(CursorError::DerivedRegression);
                    }
                }
                _ => {
                    self.invalidate_recoverable();
                    return Err(CursorError::CursorMode);
                }
            }
        }
        let first = self.first_available_ns.ok_or_else(|| {
            self.invalidate_recoverable();
            CursorError::TimeOverflow
        })?;
        let elapsed = at_ns.checked_sub(first).ok_or_else(|| {
            self.invalidate_recoverable();
            CursorError::TimeOverflow
        })?;
        self.cursor = Some(cursor.clone());
        self.available_at_ns = Some(at_ns);
        self.retained_available_at_ns = Some(at_ns);
        self.payload_hash = Some(payload_hash.to_owned());
        if elapsed >= WARMUP_NS {
            self.state = SlotState::Live;
            Ok(IngestOutcome::AcceptedLive)
        } else {
            self.state = SlotState::Warming;
            Ok(IngestOutcome::AcceptedWarming)
        }
    }

    fn accept_book_snapshot(
        &mut self,
        cursor: &MarketCursorV2,
        last_update_id: u64,
        at_ns: i64,
        payload_hash: &str,
    ) -> Result<IngestOutcome, CursorError> {
        self.preflight_time(at_ns)?;
        if matches!(self.book_continuity, BookContinuityV2::Snapshot(_)) {
            if let Some(outcome) = self.preflight_duplicate(cursor, payload_hash)? {
                return Ok(outcome);
            }
        }
        self.book_continuity = BookContinuityV2::Snapshot(last_update_id);
        self.commit_cursor(cursor, at_ns, payload_hash)
    }

    fn accept_book_delta(
        &mut self,
        cursor: &MarketCursorV2,
        first_update_id: u64,
        final_update_id: u64,
        previous_final_update_id: u64,
        at_ns: i64,
        payload_hash: &str,
    ) -> Result<IngestOutcome, CursorError> {
        self.preflight_time(at_ns)?;
        let result = match self.book_continuity {
            BookContinuityV2::Snapshot(last_update_id) => {
                if first_update_id <= last_update_id && last_update_id <= final_update_id {
                    Ok(())
                } else {
                    Err(CursorError::NativeGap)
                }
            }
            BookContinuityV2::Delta(prior_final_update_id) => {
                if let Some(outcome) = self.preflight_duplicate(cursor, payload_hash)? {
                    return Ok(outcome);
                }
                if previous_final_update_id != prior_final_update_id {
                    Err(CursorError::NativeGap)
                } else if final_update_id <= prior_final_update_id {
                    Err(CursorError::NativeRegression)
                } else {
                    Ok(())
                }
            }
            BookContinuityV2::NotBook | BookContinuityV2::AwaitingSnapshot => {
                Err(CursorError::NativeGap)
            }
        };
        if let Err(error) = result {
            self.invalidate_book_recoverable();
            return Err(error);
        }
        self.book_continuity = BookContinuityV2::Delta(final_update_id);
        self.commit_cursor(cursor, at_ns, payload_hash)
    }

    fn preflight_duplicate(
        &mut self,
        cursor: &MarketCursorV2,
        payload_hash: &str,
    ) -> Result<Option<IngestOutcome>, CursorError> {
        if self.cursor.as_ref() != Some(cursor) {
            return Ok(None);
        }
        if self.payload_hash.as_deref() == Some(payload_hash) {
            Ok(Some(IngestOutcome::IgnoredDuplicate))
        } else {
            self.invalidate_book_recoverable();
            Err(CursorError::MutatedDuplicate)
        }
    }

    fn commit_cursor(
        &mut self,
        cursor: &MarketCursorV2,
        at_ns: i64,
        payload_hash: &str,
    ) -> Result<IngestOutcome, CursorError> {
        let first = self.first_available_ns.ok_or_else(|| {
            self.invalidate_recoverable();
            CursorError::TimeOverflow
        })?;
        let elapsed = at_ns.checked_sub(first).ok_or_else(|| {
            self.invalidate_recoverable();
            CursorError::TimeOverflow
        })?;
        self.cursor = Some(cursor.clone());
        self.available_at_ns = Some(at_ns);
        self.retained_available_at_ns = Some(at_ns);
        self.payload_hash = Some(payload_hash.to_owned());
        if elapsed >= WARMUP_NS {
            self.state = SlotState::Live;
            Ok(IngestOutcome::AcceptedLive)
        } else {
            self.state = SlotState::Warming;
            Ok(IngestOutcome::AcceptedWarming)
        }
    }

    fn view(&self) -> Option<MarketCursorViewV2> {
        Some(MarketCursorViewV2 {
            epoch: self.epoch.clone()?,
            epoch_generation: self.generation?,
            cursor: self.cursor.clone()?,
            available_at_ns: self.available_at_ns?,
            payload_hash: self.payload_hash.clone()?,
        })
    }

    fn retire_cursor(&mut self) {
        if self.invalidity == Some(Invalidity::Terminal) {
            self.state = SlotState::Invalid;
            return;
        }
        self.state = SlotState::Cold;
        self.invalidity = None;
        self.first_available_ns = None;
        self.cursor = None;
        self.available_at_ns = None;
        self.payload_hash = None;
        self.reset_book_continuity();
    }

    fn preflight_time(&mut self, at_ns: i64) -> Result<(), CursorError> {
        if self
            .retained_available_at_ns
            .is_some_and(|retained| at_ns < retained)
        {
            self.invalidate_recoverable();
            Err(CursorError::AvailabilityRegression)
        } else {
            Ok(())
        }
    }

    fn invalidate_recoverable(&mut self) {
        if self.invalidity != Some(Invalidity::Terminal) {
            self.state = SlotState::Invalid;
            self.invalidity = Some(Invalidity::Recoverable);
        }
    }

    fn invalidate_terminal(&mut self) {
        self.state = SlotState::Invalid;
        self.invalidity = Some(Invalidity::Terminal);
    }

    fn invalidate_book_recoverable(&mut self) {
        self.retire_cursor();
        self.invalidate_recoverable();
    }
}

#[derive(Debug, Clone)]
struct Slot {
    state: SlotState,
    invalidity: Option<Invalidity>,
    epoch: Option<String>,
    generation: Option<u8>,
    history: BTreeSet<String>,
    first_available_ns: Option<i64>,
    cursor: Option<CursorV1>,
    available_at_ns: Option<i64>,
    retained_available_at_ns: Option<i64>,
    payload_hash: Option<String>,
    cursor_mode: Option<CursorModeV1>,
    observation_present: bool,
    observation_subject_epoch: Option<(String, u8)>,
    book_eligible: bool,
    book_snapshot_permitted: bool,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            state: SlotState::Cold,
            invalidity: None,
            epoch: None,
            generation: None,
            history: BTreeSet::new(),
            first_available_ns: None,
            cursor: None,
            available_at_ns: None,
            retained_available_at_ns: None,
            payload_hash: None,
            cursor_mode: None,
            observation_present: false,
            observation_subject_epoch: None,
            book_eligible: false,
            book_snapshot_permitted: false,
        }
    }
}

impl Slot {
    fn clear_current(&mut self) {
        if self.invalidity == Some(Invalidity::Terminal) {
            self.state = SlotState::Invalid;
            self.clear_observation();
            return;
        }
        self.state = SlotState::Cold;
        self.invalidity = None;
        self.epoch = None;
        self.generation = None;
        self.first_available_ns = None;
        self.cursor = None;
        self.available_at_ns = None;
        self.payload_hash = None;
        self.cursor_mode = None;
        self.observation_present = false;
        self.observation_subject_epoch = None;
        self.book_eligible = false;
        self.book_snapshot_permitted = false;
    }

    fn begin_epoch(&mut self, epoch: &str, generation: u8, at_ns: i64) -> Result<(), CursorError> {
        if self.invalidity == Some(Invalidity::Terminal) {
            return Err(CursorError::TerminalInvalid);
        }
        self.preflight_epoch_transition(epoch)?;
        self.preflight_time(at_ns)?;
        self.history.insert(epoch.to_owned());
        self.epoch = Some(epoch.to_owned());
        self.generation = Some(generation);
        self.first_available_ns = Some(at_ns);
        self.cursor = None;
        self.available_at_ns = None;
        self.retained_available_at_ns = Some(at_ns);
        self.payload_hash = None;
        self.cursor_mode = None;
        self.observation_present = false;
        self.observation_subject_epoch = None;
        self.book_eligible = false;
        self.book_snapshot_permitted = false;
        self.state = SlotState::Warming;
        self.invalidity = None;
        Ok(())
    }

    fn preflight_epoch_transition(&mut self, epoch: &str) -> Result<(), CursorError> {
        if self.history.contains(epoch) {
            self.invalidate_terminal();
            return Err(CursorError::EpochReused);
        }
        if self.history.len() >= 256 {
            self.invalidate_terminal();
            return Err(CursorError::EpochHistoryExhausted);
        }
        Ok(())
    }

    fn prepare_epoch(
        &mut self,
        epoch: &str,
        generation: u8,
        at_ns: i64,
    ) -> Result<bool, CursorError> {
        if self.invalidity == Some(Invalidity::Terminal) {
            return Err(CursorError::TerminalInvalid);
        }
        if self.epoch.as_deref() != Some(epoch) {
            self.preflight_epoch_transition(epoch)?;
        }
        match (self.generation, self.epoch.as_deref()) {
            (None, None) => self.begin_epoch(epoch, generation, at_ns).map(|()| true),
            (Some(current), Some(current_epoch))
                if generation == current && epoch == current_epoch =>
            {
                if self.state == SlotState::Invalid {
                    Err(CursorError::EpochMismatch)
                } else {
                    if self.state == SlotState::Cold {
                        self.first_available_ns = Some(at_ns);
                        self.state = SlotState::Warming;
                    }
                    Ok(false)
                }
            }
            (Some(current), _) if generation > current => {
                self.begin_epoch(epoch, generation, at_ns).map(|()| true)
            }
            _ => {
                self.invalidate_recoverable();
                Err(CursorError::EpochMismatch)
            }
        }
    }

    fn accept_cursor(
        &mut self,
        cursor: &CursorV1,
        at_ns: i64,
        payload_hash: &str,
        mode: CursorModeV1,
    ) -> Result<IngestOutcome, CursorError> {
        self.preflight_time(at_ns)?;
        if self.cursor_mode.is_some_and(|current| current != mode) {
            self.invalidate_recoverable();
            return Err(CursorError::CursorMode);
        }
        if self.available_at_ns.is_some_and(|last| at_ns < last) {
            self.invalidate_recoverable();
            return Err(CursorError::AvailabilityRegression);
        }
        if let Some(previous) = &self.cursor {
            if previous == cursor {
                if self.payload_hash.as_deref() == Some(payload_hash) {
                    return Ok(IngestOutcome::IgnoredDuplicate);
                }
                self.invalidate_recoverable();
                return Err(CursorError::MutatedDuplicate);
            }
            match mode {
                CursorModeV1::Native => {
                    let (start, _) = cursor.native_range().ok_or(CursorError::CursorMode)?;
                    let (previous_start, previous_end) =
                        previous.native_range().ok_or(CursorError::CursorMode)?;
                    if start <= previous_end {
                        self.invalidate_recoverable();
                        return Err(if start < previous_start {
                            CursorError::NativeRegression
                        } else {
                            CursorError::NativeOverlap
                        });
                    }
                    if previous_end.checked_add(1) != Some(start) {
                        self.invalidate_recoverable();
                        return Err(CursorError::NativeGap);
                    }
                }
                CursorModeV1::Derived => {
                    if cursor.derived_coordinate().is_none()
                        || previous.derived_coordinate().is_none()
                    {
                        return Err(CursorError::CursorMode);
                    }
                    if cursor < previous {
                        self.invalidate_recoverable();
                        return Err(CursorError::DerivedRegression);
                    }
                }
            }
        } else {
            match mode {
                CursorModeV1::Native if cursor.native_range().is_none() => {
                    return Err(CursorError::CursorMode);
                }
                CursorModeV1::Derived if cursor.derived_coordinate().is_none() => {
                    return Err(CursorError::CursorMode);
                }
                _ => {}
            }
        }
        let first = self.first_available_ns.ok_or_else(|| {
            self.invalidate_recoverable();
            CursorError::TimeOverflow
        })?;
        let elapsed = at_ns.checked_sub(first).ok_or_else(|| {
            self.invalidate_recoverable();
            CursorError::TimeOverflow
        })?;
        self.cursor = Some(cursor.clone());
        self.cursor_mode = Some(mode);
        self.available_at_ns = Some(at_ns);
        self.retained_available_at_ns = Some(at_ns);
        self.payload_hash = Some(payload_hash.to_owned());
        self.observation_present = true;
        if elapsed >= WARMUP_NS {
            self.state = SlotState::Live;
            Ok(IngestOutcome::AcceptedLive)
        } else {
            self.state = SlotState::Warming;
            Ok(IngestOutcome::AcceptedWarming)
        }
    }

    fn view(&self) -> Option<CursorView> {
        Some(CursorView {
            epoch: self.epoch.clone()?,
            epoch_generation: self.generation?,
            cursor: self.cursor.clone()?,
            available_at_ns: self.available_at_ns?,
            payload_hash: self.payload_hash.clone()?,
        })
    }

    fn clear_observation(&mut self) {
        self.observation_present = false;
        self.observation_subject_epoch = None;
    }

    fn retire_cursor(&mut self) {
        if self.invalidity == Some(Invalidity::Terminal) {
            self.state = SlotState::Invalid;
            self.clear_observation();
            return;
        }
        self.state = SlotState::Cold;
        self.invalidity = None;
        self.first_available_ns = None;
        self.cursor = None;
        self.available_at_ns = None;
        self.payload_hash = None;
        self.cursor_mode = None;
        self.clear_observation();
        self.book_eligible = false;
        self.book_snapshot_permitted = false;
    }

    fn preflight_time(&mut self, at_ns: i64) -> Result<(), CursorError> {
        if self
            .retained_available_at_ns
            .is_some_and(|retained| at_ns < retained)
        {
            self.invalidate_recoverable();
            return Err(CursorError::AvailabilityRegression);
        }
        Ok(())
    }

    fn invalidate_recoverable(&mut self) {
        if self.invalidity != Some(Invalidity::Terminal) {
            self.state = SlotState::Invalid;
            self.invalidity = Some(Invalidity::Recoverable);
        }
    }

    fn invalidate_terminal(&mut self) {
        self.state = SlotState::Invalid;
        self.invalidity = Some(Invalidity::Terminal);
    }
}

#[derive(Debug, Clone)]
struct ConnectionSlot {
    state: SlotState,
    invalidity: Option<Invalidity>,
    epoch: Option<String>,
    generation: Option<u8>,
    history: BTreeSet<String>,
}

impl Default for ConnectionSlot {
    fn default() -> Self {
        Self {
            state: SlotState::Cold,
            invalidity: None,
            epoch: None,
            generation: None,
            history: BTreeSet::new(),
        }
    }
}

impl ConnectionSlot {
    fn begin_epoch(&mut self, epoch: &str, generation: u8) -> Result<(), CursorError> {
        if self.invalidity == Some(Invalidity::Terminal) {
            return Err(CursorError::TerminalInvalid);
        }
        if self.history.contains(epoch) {
            self.invalidate_terminal();
            return Err(CursorError::EpochReused);
        }
        if self.history.len() >= 256 {
            self.invalidate_terminal();
            return Err(CursorError::EpochHistoryExhausted);
        }
        self.history.insert(epoch.to_owned());
        self.epoch = Some(epoch.to_owned());
        self.generation = Some(generation);
        self.state = SlotState::Warming;
        self.invalidity = None;
        Ok(())
    }

    fn invalidate_recoverable(&mut self) {
        if self.invalidity != Some(Invalidity::Terminal) {
            self.state = SlotState::Invalid;
            self.invalidity = Some(Invalidity::Recoverable);
        }
    }

    fn invalidate_terminal(&mut self) {
        self.state = SlotState::Invalid;
        self.invalidity = Some(Invalidity::Terminal);
    }
}

#[derive(Debug, Clone)]
struct SystemSlot {
    source: SystemSourceKeyV1,
    slot: Slot,
    chain_head: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SourceStateMachine {
    config: MechanicsConfigV1,
    connections: BTreeMap<ConnectionKeyV1, ConnectionSlot>,
    contributors: BTreeMap<ContributorKeyV1, Slot>,
    clocks: BTreeMap<ClockSourceKeyV1, Slot>,
    coverage: BTreeMap<CoverageSourceKeyV1, Slot>,
    systems: Vec<SystemSlot>,
}

/// Additive V2 cursor boundary keyed by the configured contributor and family.
///
/// This type deliberately does not expose an aggregate contributor cursor and
/// is not consumed by the snapshot processor. V1 state and APIs remain exact.
#[derive(Debug, Clone)]
pub struct SourceStateMachineV2 {
    v1: SourceStateMachine,
    market_families: BTreeMap<(ContributorKeyV1, FamilyV1), MarketFamilySlotV2>,
}

impl SourceStateMachineV2 {
    pub fn new(config: MechanicsConfigV1) -> Self {
        let market_families = config
            .contributors()
            .iter()
            .flat_map(|spec| {
                spec.allowed_families().iter().map(move |family| {
                    (
                        (spec.key().clone(), *family),
                        MarketFamilySlotV2::new(*family),
                    )
                })
            })
            .collect();
        Self {
            v1: SourceStateMachine::new(config),
            market_families,
        }
    }

    pub fn ingest(&mut self, input: &MechanicsInputV2) -> Result<IngestOutcome, CursorError> {
        input
            .validate_static()
            .map_err(|_| CursorError::CursorMode)?;
        let mut candidate = self.clone();
        let result = if let Some(non_market) = input.as_v1_non_market() {
            candidate.v1.ingest(non_market)
        } else {
            candidate.ingest_market_v2(input)
        };
        candidate.synchronize_family_invalidity();
        match &result {
            Ok(_) => *self = candidate,
            Err(error) if error.invalidates_slot() => *self = candidate,
            Err(_) => {}
        }
        result
    }

    pub fn market_state(
        &self,
        contributor: &ContributorKeyV1,
        family: FamilyV1,
    ) -> Option<SlotState> {
        self.market_families
            .get(&(contributor.clone(), family))
            .map(|slot| slot.state)
    }

    pub fn market_invalidity(
        &self,
        contributor: &ContributorKeyV1,
        family: FamilyV1,
    ) -> Option<Invalidity> {
        self.market_families
            .get(&(contributor.clone(), family))?
            .invalidity
    }

    pub fn market_cursor(
        &self,
        contributor: &ContributorKeyV1,
        family: FamilyV1,
    ) -> Option<MarketCursorViewV2> {
        self.family_slot_if_current(contributor, family)?.view()
    }

    fn family_slot_if_current(
        &self,
        contributor: &ContributorKeyV1,
        family: FamilyV1,
    ) -> Option<&MarketFamilySlotV2> {
        let aggregate = self.v1.contributors.get(contributor)?;
        let connection_key = self.v1.config.contributor_connections().get(contributor)?;
        let connection = self.v1.connections.get(connection_key)?;
        let family_slot = self.market_families.get(&(contributor.clone(), family))?;
        if aggregate.state == SlotState::Invalid
            || connection.state == SlotState::Invalid
            || aggregate.epoch != family_slot.epoch
            || aggregate.generation != family_slot.generation
            || connection.epoch != family_slot.epoch
            || connection.generation != family_slot.generation
        {
            return None;
        }
        Some(family_slot)
    }

    fn synchronize_family_invalidity(&mut self) {
        for ((contributor, _), family) in &mut self.market_families {
            let aggregate = self.v1.contributors.get(contributor);
            let connection = self
                .v1
                .config
                .contributor_connections()
                .get(contributor)
                .and_then(|key| self.v1.connections.get(key));
            let invalidity = match (aggregate, connection) {
                (Some(aggregate), Some(connection))
                    if aggregate.invalidity == Some(Invalidity::Terminal)
                        || connection.invalidity == Some(Invalidity::Terminal) =>
                {
                    Some(Invalidity::Terminal)
                }
                (Some(aggregate), Some(connection))
                    if aggregate.state == SlotState::Invalid
                        || connection.state == SlotState::Invalid =>
                {
                    Some(Invalidity::Recoverable)
                }
                _ => None,
            };
            if let Some(invalidity) = invalidity {
                family.retire_cursor();
                if invalidity == Invalidity::Terminal {
                    family.invalidate_terminal();
                } else {
                    family.invalidate_recoverable();
                }
            }
        }
    }

    fn ingest_market_v2(&mut self, input: &MechanicsInputV2) -> Result<IngestOutcome, CursorError> {
        let MechanicsInputRefV2::Market {
            envelope,
            catalog,
            market_cursor,
            source_provenance,
            payload_hash,
            ..
        } = input.view()
        else {
            return Err(CursorError::CursorMode);
        };
        let venue = catalog
            .venue_source(envelope.venue.0)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let instrument_id = envelope
            .instrument
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let instrument = catalog
            .instrument(instrument_id.0)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let contributor = ContributorKeyV1::new(venue.source_id(), instrument.clone())
            .map_err(|_| CursorError::UnconfiguredIdentity)?;
        let family =
            family_for_v2_market(&envelope.payload).ok_or(CursorError::UnconfiguredIdentity)?;
        if !self
            .market_families
            .contains_key(&(contributor.clone(), family))
        {
            return Err(CursorError::UnconfiguredIdentity);
        }
        let epoch = catalog
            .connection_epochs()
            .iter()
            .find(|entry| {
                entry.connection_id() == envelope.connection.0
                    && entry.session_id() == envelope.session.0
            })
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let connection_key = self
            .v1
            .config
            .contributor_connections()
            .get(&contributor)
            .cloned()
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let connection = self
            .v1
            .connections
            .get(&connection_key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let advances = connection.generation.is_none_or(|current| {
            epoch.epoch_generation() > current
                || connection.epoch.as_deref() != Some(epoch.connection_epoch())
        });
        self.prepare_lifecycle(
            &connection_key,
            &contributor,
            epoch.connection_epoch(),
            epoch.epoch_generation(),
            envelope.receive_ts.0,
        )?;
        if advances {
            let bound = self
                .v1
                .config
                .contributor_connections()
                .iter()
                .filter(|(_, connection)| *connection == &connection_key)
                .map(|(key, _)| key)
                .collect::<BTreeSet<_>>();
            for ((key, _), slot) in &mut self.market_families {
                if bound.contains(key) {
                    slot.clear_current();
                }
            }
        }
        let slot = self
            .market_families
            .get_mut(&(contributor.clone(), family))
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let is_book_snapshot = matches!(
            (&envelope.payload, source_provenance),
            (
                marketfeed_model::MarketEvent::BookSnapshot(_),
                crate::SourceProvenanceV2::BinanceBookSnapshot { .. }
            )
        );
        slot.prepare_epoch(
            epoch.connection_epoch(),
            epoch.epoch_generation(),
            envelope.receive_ts.0,
            is_book_snapshot,
        )?;
        let outcome = match (&envelope.payload, source_provenance) {
            (
                marketfeed_model::MarketEvent::BookSnapshot(_),
                crate::SourceProvenanceV2::BinanceBookSnapshot { last_update_id, .. },
            ) => slot.accept_book_snapshot(
                market_cursor,
                *last_update_id,
                envelope.receive_ts.0,
                payload_hash,
            ),
            (
                marketfeed_model::MarketEvent::BookDelta(_),
                crate::SourceProvenanceV2::BinanceBookDelta {
                    first_update_id,
                    final_update_id,
                    previous_final_update_id,
                    ..
                },
            ) => slot.accept_book_delta(
                market_cursor,
                *first_update_id,
                *final_update_id,
                *previous_final_update_id,
                envelope.receive_ts.0,
                payload_hash,
            ),
            _ => slot.accept_cursor(market_cursor, envelope.receive_ts.0, payload_hash),
        }?;
        if outcome != IngestOutcome::IgnoredDuplicate {
            let aggregate = self
                .v1
                .contributors
                .get_mut(&contributor)
                .ok_or(CursorError::UnconfiguredIdentity)?;
            aggregate.preflight_time(envelope.receive_ts.0)?;
            aggregate.retained_available_at_ns = Some(envelope.receive_ts.0);
        }
        Ok(outcome)
    }

    fn prepare_lifecycle(
        &mut self,
        connection_key: &ConnectionKeyV1,
        contributor_key: &ContributorKeyV1,
        epoch: &str,
        generation: u8,
        at_ns: i64,
    ) -> Result<(), CursorError> {
        let connection = self
            .v1
            .connections
            .get(connection_key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        match connection.generation {
            None => self.v1.advance_connection(
                connection_key,
                contributor_key,
                epoch,
                generation,
                at_ns,
            ),
            Some(current) if generation > current => self.v1.advance_connection(
                connection_key,
                contributor_key,
                epoch,
                generation,
                at_ns,
            ),
            Some(current)
                if generation == current && connection.epoch.as_deref() == Some(epoch) =>
            {
                let contributor = self
                    .v1
                    .contributors
                    .get_mut(contributor_key)
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                if contributor.generation.is_none() {
                    contributor.begin_epoch(epoch, generation, at_ns)
                } else if contributor.generation == Some(generation)
                    && contributor.epoch.as_deref() == Some(epoch)
                    && contributor.state != SlotState::Invalid
                {
                    contributor.preflight_time(at_ns)
                } else {
                    contributor.invalidate_recoverable();
                    Err(CursorError::EpochMismatch)
                }
            }
            _ => {
                self.v1
                    .contributors
                    .get_mut(contributor_key)
                    .ok_or(CursorError::UnconfiguredIdentity)?
                    .invalidate_recoverable();
                Err(CursorError::EpochMismatch)
            }
        }
    }
}

fn family_for_v2_market(payload: &marketfeed_model::MarketEvent) -> Option<FamilyV1> {
    match payload {
        marketfeed_model::MarketEvent::Trade(_) => Some(FamilyV1::Trade),
        marketfeed_model::MarketEvent::Quote(_) => Some(FamilyV1::Quote),
        marketfeed_model::MarketEvent::BookSnapshot(_)
        | marketfeed_model::MarketEvent::BookDelta(_) => Some(FamilyV1::Book),
        marketfeed_model::MarketEvent::OpenInterest(_) => Some(FamilyV1::OpenInterest),
        marketfeed_model::MarketEvent::Liquidation(_) => Some(FamilyV1::Liquidation),
        marketfeed_model::MarketEvent::MarkPrice(_)
        | marketfeed_model::MarketEvent::IndexPrice(_) => Some(FamilyV1::ConfirmationPrice),
        _ => None,
    }
}

impl SourceStateMachine {
    pub fn new(config: MechanicsConfigV1) -> Self {
        Self {
            connections: config
                .connections()
                .iter()
                .cloned()
                .map(|key| (key, ConnectionSlot::default()))
                .collect(),
            contributors: config
                .contributors()
                .iter()
                .map(|spec| (spec.key().clone(), Slot::default()))
                .collect(),
            clocks: config
                .clock_sources()
                .iter()
                .cloned()
                .map(|key| (key, Slot::default()))
                .collect(),
            coverage: config
                .coverage_sources()
                .iter()
                .cloned()
                .map(|key| (key, Slot::default()))
                .collect(),
            systems: config
                .system_sources()
                .iter()
                .cloned()
                .map(|source| SystemSlot {
                    source,
                    slot: Slot::default(),
                    chain_head: None,
                })
                .collect(),
            config,
        }
    }

    pub fn ingest(&mut self, input: &MechanicsInputV1) -> Result<IngestOutcome, CursorError> {
        let mut candidate = self.clone();
        let result = candidate.ingest_inner(input);
        match &result {
            Ok(_) => *self = candidate,
            Err(error) if error.invalidates_slot() => *self = candidate,
            Err(_) => {}
        }
        result
    }

    pub fn contributor_state(&self, key: &ContributorKeyV1) -> Option<SlotState> {
        self.contributors.get(key).map(|slot| slot.state)
    }
    pub fn connection_state(&self, key: &ConnectionKeyV1) -> Option<SlotState> {
        self.connections.get(key).map(|slot| slot.state)
    }
    pub fn connection_invalidity(&self, key: &ConnectionKeyV1) -> Option<Invalidity> {
        self.connections.get(key)?.invalidity
    }
    pub fn contributor_invalidity(&self, key: &ContributorKeyV1) -> Option<Invalidity> {
        self.contributors.get(key)?.invalidity
    }
    pub fn contributor_cursor(&self, key: &ContributorKeyV1) -> Option<CursorView> {
        self.contributors.get(key)?.view()
    }
    pub fn clock_state(&self, key: &ClockSourceKeyV1) -> Option<SlotState> {
        self.clocks.get(key).map(|slot| slot.state)
    }
    pub fn clock_invalidity(&self, key: &ClockSourceKeyV1) -> Option<Invalidity> {
        self.clocks.get(key)?.invalidity
    }
    pub fn clock_cursor(&self, key: &ClockSourceKeyV1) -> Option<CursorView> {
        let slot = self.clocks.get(key)?;
        self.observation_matches_subject(slot, key.subject())
            .then(|| slot.view())
            .flatten()
    }
    pub fn coverage_cursor(&self, key: &CoverageSourceKeyV1) -> Option<CursorView> {
        let slot = self.coverage.get(key)?;
        self.observation_matches_subject(slot, key.subject())
            .then(|| slot.view())
            .flatten()
    }
    pub fn coverage_state(&self, key: &CoverageSourceKeyV1) -> Option<SlotState> {
        self.coverage.get(key).map(|slot| slot.state)
    }
    pub fn coverage_invalidity(&self, key: &CoverageSourceKeyV1) -> Option<Invalidity> {
        self.coverage.get(key)?.invalidity
    }
    pub fn system_chain_head(&self, key: &SystemSourceKeyV1) -> Option<&str> {
        self.systems
            .iter()
            .find(|slot| &slot.source == key)?
            .chain_head
            .as_deref()
    }
    pub fn system_cursor(&self, key: &SystemSourceKeyV1) -> Option<CursorView> {
        let system = self.systems.iter().find(|slot| &slot.source == key)?;
        let mut view = system.slot.view()?;
        view.payload_hash = system.chain_head.clone()?;
        Some(view)
    }
    pub fn system_state(&self, key: &SystemSourceKeyV1) -> Option<SlotState> {
        self.systems
            .iter()
            .find(|slot| &slot.source == key)
            .map(|slot| slot.slot.state)
    }
    pub fn system_invalidity(&self, key: &SystemSourceKeyV1) -> Option<Invalidity> {
        self.systems
            .iter()
            .find(|slot| &slot.source == key)?
            .slot
            .invalidity
    }
    pub fn book_eligible(&self, key: &ContributorKeyV1) -> Option<bool> {
        self.contributors.get(key).map(|slot| slot.book_eligible)
    }
    pub fn book_snapshot_permitted(&self, key: &ContributorKeyV1) -> Option<bool> {
        self.contributors
            .get(key)
            .map(|slot| slot.book_snapshot_permitted)
    }

    pub(crate) fn invalidate_contributor_for_queue_drop(
        &mut self,
        key: &ContributorKeyV1,
    ) -> Result<(), CursorError> {
        let slot = self
            .contributors
            .get_mut(key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        slot.invalidate_recoverable();
        slot.retire_cursor();
        slot.invalidate_recoverable();
        Ok(())
    }

    pub(crate) fn invalidate_clock_for_queue_drop(
        &mut self,
        key: &ClockSourceKeyV1,
    ) -> Result<(), CursorError> {
        let slot = self
            .clocks
            .get_mut(key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        invalidate_slot_for_queue_drop(slot);
        Ok(())
    }

    pub(crate) fn invalidate_coverage_for_queue_drop(
        &mut self,
        key: &CoverageSourceKeyV1,
    ) -> Result<(), CursorError> {
        let slot = self
            .coverage
            .get_mut(key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        invalidate_slot_for_queue_drop(slot);
        Ok(())
    }

    pub(crate) fn invalidate_system_for_queue_drop(
        &mut self,
        key: &SystemSourceKeyV1,
    ) -> Result<(), CursorError> {
        let slot = &mut self
            .systems
            .iter_mut()
            .find(|slot| &slot.source == key)
            .ok_or(CursorError::UnconfiguredIdentity)?
            .slot;
        invalidate_slot_for_queue_drop(slot);
        Ok(())
    }

    fn ingest_inner(&mut self, input: &MechanicsInputV1) -> Result<IngestOutcome, CursorError> {
        match input.view() {
            MechanicsInputRefV1::Market {
                envelope,
                action_index,
                catalog,
                payload_hash,
            } => {
                let venue = catalog
                    .venue_source(envelope.venue.0)
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                let instrument_id = envelope
                    .instrument
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                let instrument = catalog
                    .instrument(instrument_id.0)
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                let key = ContributorKeyV1::new(venue.source_id(), instrument.clone())
                    .map_err(|_| CursorError::UnconfiguredIdentity)?;
                let epoch = catalog
                    .connection_epochs()
                    .iter()
                    .find(|entry| {
                        entry.connection_id() == envelope.connection.0
                            && entry.session_id() == envelope.session.0
                    })
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                let cursor = match envelope.source_sequence {
                    Some(sequence) => CursorV1::native(sequence.first, sequence.last),
                    None => CursorV1::derived(
                        envelope.frame_seq,
                        action_index,
                        u32::from(envelope.event_index),
                    ),
                }
                .map_err(|_| CursorError::CursorMode)?;
                self.ingest_market(
                    &key,
                    epoch.connection_epoch(),
                    epoch.epoch_generation(),
                    &cursor,
                    envelope.receive_ts.0,
                    payload_hash,
                    matches!(
                        envelope.payload,
                        marketfeed_model::MarketEvent::BookSnapshot(_)
                    ),
                )
            }
            MechanicsInputRefV1::Coverage {
                contributor,
                coverage_source,
                available_at,
                coverage_cursor,
                payload_hash,
                ..
            } => {
                self.check_subject(
                    contributor.key(),
                    contributor.connection_epoch(),
                    contributor.epoch_generation(),
                )?;
                let slot = self
                    .coverage
                    .get_mut(coverage_source.key())
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                if slot.invalidity == Some(Invalidity::Terminal) {
                    return Err(CursorError::TerminalInvalid);
                }
                let at = match time_ns(available_at) {
                    Ok(at) => at,
                    Err(error) => {
                        slot.invalidate_recoverable();
                        return Err(error);
                    }
                };
                slot.prepare_epoch(
                    coverage_source.epoch(),
                    coverage_source.epoch_generation(),
                    at,
                )?;
                let outcome = slot.accept_cursor(
                    coverage_cursor.cursor(),
                    at,
                    payload_hash,
                    CursorModeV1::Native,
                )?;
                slot.observation_subject_epoch = Some((
                    contributor.connection_epoch().to_owned(),
                    contributor.epoch_generation(),
                ));
                Ok(outcome)
            }
            MechanicsInputRefV1::Clock {
                contributor,
                clock_source,
                available_at,
                clock_cursor,
                payload_hash,
                ..
            } => {
                self.check_subject(
                    contributor.key(),
                    contributor.connection_epoch(),
                    contributor.epoch_generation(),
                )?;
                let slot = self
                    .clocks
                    .get_mut(clock_source.key())
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                if slot.invalidity == Some(Invalidity::Terminal) {
                    return Err(CursorError::TerminalInvalid);
                }
                let at = match time_ns(available_at) {
                    Ok(at) => at,
                    Err(error) => {
                        slot.invalidate_recoverable();
                        return Err(error);
                    }
                };
                slot.prepare_epoch(clock_source.epoch(), clock_source.epoch_generation(), at)?;
                let outcome = slot.accept_cursor(
                    clock_cursor.cursor(),
                    at,
                    payload_hash,
                    CursorModeV1::Native,
                )?;
                slot.observation_subject_epoch = Some((
                    contributor.connection_epoch().to_owned(),
                    contributor.epoch_generation(),
                ));
                Ok(outcome)
            }
            MechanicsInputRefV1::System {
                system_source,
                scope,
                available_at,
                system_cursor,
                fault,
                predecessor_system_chain_hash,
                payload_hash,
                ..
            } => self.ingest_system(
                system_source.key(),
                system_source.epoch(),
                system_source.epoch_generation(),
                scope.view(),
                available_at,
                system_cursor,
                fault.view(),
                predecessor_system_chain_hash,
                payload_hash,
            ),
        }
    }

    fn ingest_market(
        &mut self,
        key: &ContributorKeyV1,
        epoch: &str,
        generation: u8,
        cursor: &CursorV1,
        at_ns: i64,
        payload_hash: &str,
        is_book_snapshot: bool,
    ) -> Result<IngestOutcome, CursorError> {
        let connection_key = self
            .config
            .contributor_connections()
            .get(key)
            .cloned()
            .ok_or(CursorError::UnconfiguredIdentity)?;
        if self
            .connections
            .get(&connection_key)
            .is_some_and(|slot| slot.invalidity == Some(Invalidity::Terminal))
            || self
                .contributors
                .get(key)
                .is_some_and(|slot| slot.invalidity == Some(Invalidity::Terminal))
        {
            return Err(CursorError::TerminalInvalid);
        }
        let connection = self
            .connections
            .get(&connection_key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let contributor = self
            .contributors
            .get(key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let connection_transition = connection.epoch.as_deref() != Some(epoch);
        let contributor_transition = contributor.epoch.as_deref() != Some(epoch);
        let epoch_reused = (connection_transition && connection.history.contains(epoch))
            || (contributor_transition && contributor.history.contains(epoch));
        let history_exhausted = (connection_transition && connection.history.len() >= 256)
            || (contributor_transition && contributor.history.len() >= 256);
        if epoch_reused || history_exhausted {
            self.connections
                .get_mut(&connection_key)
                .expect("preallocated connection")
                .invalidate_terminal();
            self.contributors
                .get_mut(key)
                .expect("preallocated contributor")
                .invalidate_terminal();
            return Err(if epoch_reused {
                CursorError::EpochReused
            } else {
                CursorError::EpochHistoryExhausted
            });
        }
        let contributor_time = self
            .contributors
            .get_mut(key)
            .ok_or(CursorError::UnconfiguredIdentity)?
            .preflight_time(at_ns);
        if contributor_time.is_err() {
            self.contributors
                .get_mut(key)
                .expect("preallocated contributor")
                .invalidate_recoverable();
            return Err(CursorError::AvailabilityRegression);
        }
        let connection = self
            .connections
            .get(&connection_key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let advance = match connection.generation {
            None => true,
            Some(current) if generation > current => true,
            Some(current)
                if generation == current && connection.epoch.as_deref() == Some(epoch) =>
            {
                if connection.state == SlotState::Invalid {
                    return Err(CursorError::EpochMismatch);
                }
                false
            }
            _ => {
                self.contributors
                    .get_mut(key)
                    .ok_or(CursorError::UnconfiguredIdentity)?
                    .invalidate_recoverable();
                return Err(CursorError::EpochMismatch);
            }
        };
        if advance {
            self.advance_connection(&connection_key, key, epoch, generation, at_ns)?;
        } else {
            let contributor = self
                .contributors
                .get_mut(key)
                .ok_or(CursorError::UnconfiguredIdentity)?;
            if contributor.generation.is_none() {
                contributor.begin_epoch(epoch, generation, at_ns)?;
            } else if contributor.generation != Some(generation)
                || contributor.epoch.as_deref() != Some(epoch)
            {
                contributor.invalidate_recoverable();
                return Err(CursorError::EpochMismatch);
            } else if contributor.state == SlotState::Invalid {
                if is_book_snapshot && contributor.book_snapshot_permitted {
                    let mode = if cursor.native_range().is_some() {
                        CursorModeV1::Native
                    } else {
                        CursorModeV1::Derived
                    };
                    let outcome = contributor.accept_cursor(cursor, at_ns, payload_hash, mode)?;
                    if outcome == IngestOutcome::IgnoredDuplicate {
                        return Ok(IngestOutcome::IgnoredDuplicate);
                    }
                    contributor.invalidate_recoverable();
                    contributor.book_eligible = true;
                    contributor.book_snapshot_permitted = false;
                    return Ok(IngestOutcome::Invalidated);
                }
                return Err(CursorError::EpochMismatch);
            }
        }
        let mode = if cursor.native_range().is_some() {
            CursorModeV1::Native
        } else {
            CursorModeV1::Derived
        };
        let outcome = self
            .contributors
            .get_mut(key)
            .ok_or(CursorError::UnconfiguredIdentity)?
            .accept_cursor(cursor, at_ns, payload_hash, mode)?;
        if is_book_snapshot && outcome != IngestOutcome::IgnoredDuplicate {
            let contributor = self
                .contributors
                .get_mut(key)
                .expect("preallocated contributor");
            contributor.book_eligible = true;
            contributor.book_snapshot_permitted = false;
        }
        Ok(outcome)
    }

    fn advance_connection(
        &mut self,
        connection_key: &ConnectionKeyV1,
        trigger: &ContributorKeyV1,
        epoch: &str,
        generation: u8,
        at_ns: i64,
    ) -> Result<(), CursorError> {
        self.connections
            .get_mut(connection_key)
            .ok_or(CursorError::UnconfiguredIdentity)?
            .begin_epoch(epoch, generation)?;
        let bound: Vec<_> = self
            .config
            .contributor_connections()
            .iter()
            .filter(|(_, connection)| *connection == connection_key)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &bound {
            let contributor = self
                .contributors
                .get_mut(key)
                .expect("preallocated contributor");
            if key == trigger {
                contributor.begin_epoch(epoch, generation, at_ns)?;
            } else {
                contributor.clear_current();
            }
            for (clock_key, slot) in &mut self.clocks {
                if clock_key.subject() == key {
                    slot.clear_observation();
                }
            }
            for (coverage_key, slot) in &mut self.coverage {
                if coverage_key.subject() == key {
                    slot.clear_observation();
                }
            }
        }
        for system in &mut self.systems {
            let target = system.source.configured_target_key();
            if target.connection_key() == Some(connection_key)
                || target
                    .contributor_key()
                    .is_some_and(|key| bound.contains(key))
            {
                system.slot.retire_cursor();
                system.chain_head = None;
            }
        }
        Ok(())
    }

    fn check_subject(
        &self,
        key: &ContributorKeyV1,
        epoch: &str,
        generation: u8,
    ) -> Result<(), CursorError> {
        let slot = self
            .contributors
            .get(key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        if slot.generation.is_none() {
            return Err(CursorError::SubjectNotInitialized);
        }
        if slot.generation != Some(generation) || slot.epoch.as_deref() != Some(epoch) {
            return Err(CursorError::SubjectEpochMismatch);
        }
        Ok(())
    }

    fn observation_matches_subject(&self, slot: &Slot, subject: &ContributorKeyV1) -> bool {
        let Some(contributor) = self.contributors.get(subject) else {
            return false;
        };
        slot.observation_present
            && matches!(
                (
                    slot.observation_subject_epoch.as_ref(),
                    contributor.epoch.as_deref(),
                    contributor.generation,
                ),
                (Some((observed_epoch, observed_generation)), Some(current_epoch), Some(current_generation))
                    if observed_epoch == current_epoch && *observed_generation == current_generation
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn ingest_system(
        &mut self,
        key: &SystemSourceKeyV1,
        epoch: &str,
        generation: u8,
        scope: FaultScopeRefV1<'_>,
        available_at: &Rfc3339Time,
        cursor: &CursorV1,
        fault: SystemFaultRefV1,
        predecessor: Option<&str>,
        payload_hash: &str,
    ) -> Result<IngestOutcome, CursorError> {
        self.validate_scope(scope, key.configured_target_key())?;
        let index = self
            .systems
            .iter()
            .position(|slot| &slot.source == key)
            .ok_or(CursorError::UnconfiguredIdentity)?;
        let system = &mut self.systems[index];
        if system.slot.invalidity == Some(Invalidity::Terminal) {
            return Err(CursorError::TerminalInvalid);
        }
        let at = match time_ns(available_at) {
            Ok(at) => at,
            Err(error) => {
                system.slot.invalidate_recoverable();
                return Err(error);
            }
        };
        let same_epoch = system.slot.generation == Some(generation)
            && system.slot.epoch.as_deref() == Some(epoch);
        if same_epoch && system.slot.cursor.as_ref() == Some(cursor) {
            return system
                .slot
                .accept_cursor(cursor, at, payload_hash, key.cursor_mode());
        }
        let old_head = system.chain_head.clone();
        let new_epoch = system.slot.prepare_epoch(epoch, generation, at)?;
        let required = if new_epoch && old_head.is_some() {
            old_head.as_deref()
        } else {
            system.chain_head.as_deref()
        };
        if predecessor != required {
            return Err(CursorError::SystemPredecessor);
        }
        let cursor_result = system
            .slot
            .accept_cursor(cursor, at, payload_hash, key.cursor_mode());
        let outcome = match cursor_result {
            Ok(outcome) => outcome,
            Err(error) => {
                if error.invalidates_slot() {
                    self.invalidate_targets(scope)?;
                }
                return Err(error);
            }
        };
        let head = match system.chain_head.as_deref() {
            None => SystemChainPreimage::hash_first(payload_hash),
            Some(previous) => SystemChainPreimage::hash_next(previous, payload_hash),
        }
        .map_err(|_| CursorError::SystemPredecessor)?;
        system.chain_head = Some(head);
        self.apply_fault(scope, fault)?;
        Ok(match fault {
            SystemFaultRefV1::BookResynchronized => outcome,
            _ => IngestOutcome::Invalidated,
        })
    }

    fn validate_scope(
        &self,
        scope: FaultScopeRefV1<'_>,
        target: &ConfiguredTargetKeyV1,
    ) -> Result<(), CursorError> {
        match (
            scope,
            target.contributor_key(),
            target.connection_key(),
            target.processor_id(),
        ) {
            (FaultScopeRefV1::Contributor { contributor }, Some(key), _, _)
                if contributor.key() == key =>
            {
                self.check_subject(
                    key,
                    contributor.connection_epoch(),
                    contributor.epoch_generation(),
                )
            }
            (
                FaultScopeRefV1::ConnectionEpoch {
                    connection_key,
                    connection_epoch,
                    epoch_generation,
                },
                _,
                Some(key),
                _,
            ) if connection_key == key => {
                let slot = self
                    .connections
                    .get(key)
                    .ok_or(CursorError::UnconfiguredIdentity)?;
                if slot.epoch.as_deref() == Some(connection_epoch)
                    && slot.generation == Some(epoch_generation)
                {
                    Ok(())
                } else {
                    Err(CursorError::FaultScopeMismatch)
                }
            }
            (FaultScopeRefV1::Processor { processor_id }, _, _, Some(id)) if processor_id == id => {
                Ok(())
            }
            _ => Err(CursorError::FaultScopeMismatch),
        }
    }

    fn apply_fault(
        &mut self,
        scope: FaultScopeRefV1<'_>,
        fault: SystemFaultRefV1,
    ) -> Result<(), CursorError> {
        let targets: Vec<ContributorKeyV1> = match scope {
            FaultScopeRefV1::Contributor { contributor } => vec![contributor.key().clone()],
            FaultScopeRefV1::ConnectionEpoch { connection_key, .. } => self
                .config
                .contributor_connections()
                .iter()
                .filter(|(_, connection)| *connection == connection_key)
                .map(|(key, _)| key.clone())
                .collect(),
            FaultScopeRefV1::Processor { .. } => self
                .config
                .contributors()
                .iter()
                .map(|spec| spec.key().clone())
                .collect(),
        };
        if targets.is_empty() {
            return Err(CursorError::EmptyFaultExpansion);
        }
        match fault {
            SystemFaultRefV1::BookResynchronized => {
                for key in targets {
                    let slot = self.contributors.get_mut(&key).expect("configured target");
                    slot.book_eligible = false;
                    slot.book_snapshot_permitted = true;
                }
            }
            SystemFaultRefV1::ClockJump { .. } => {
                for key in &targets {
                    self.contributors
                        .get_mut(key)
                        .expect("configured target")
                        .invalidate_recoverable();
                }
                for clock in self.clocks.values_mut() {
                    clock.invalidate_recoverable();
                    clock.observation_present = false;
                }
            }
            SystemFaultRefV1::BookInvalidated | SystemFaultRefV1::ChecksumMismatch => {
                for key in targets {
                    let slot = self.contributors.get_mut(&key).expect("configured target");
                    slot.invalidate_recoverable();
                    slot.book_eligible = false;
                    slot.book_snapshot_permitted = false;
                }
            }
            SystemFaultRefV1::Disconnected
            | SystemFaultRefV1::SequenceGap { .. }
            | SystemFaultRefV1::EventsDropped {
                category:
                    DropCategoryV1::ActionBuffer
                    | DropCategoryV1::MarketDispatch
                    | DropCategoryV1::SystemDispatch,
                ..
            } => {
                for key in targets {
                    self.contributors
                        .get_mut(&key)
                        .expect("configured target")
                        .invalidate_recoverable();
                }
                if let (
                    SystemFaultRefV1::Disconnected,
                    FaultScopeRefV1::ConnectionEpoch { connection_key, .. },
                ) = (fault, scope)
                {
                    self.connections
                        .get_mut(connection_key)
                        .expect("configured connection")
                        .invalidate_recoverable();
                }
            }
        }
        Ok(())
    }

    fn invalidate_targets(&mut self, scope: FaultScopeRefV1<'_>) -> Result<(), CursorError> {
        let targets: Vec<_> = match scope {
            FaultScopeRefV1::Contributor { contributor } => vec![contributor.key().clone()],
            FaultScopeRefV1::ConnectionEpoch { connection_key, .. } => self
                .config
                .contributor_connections()
                .iter()
                .filter(|(_, connection)| *connection == connection_key)
                .map(|(key, _)| key.clone())
                .collect(),
            FaultScopeRefV1::Processor { .. } => self
                .config
                .contributors()
                .iter()
                .map(|spec| spec.key().clone())
                .collect(),
        };
        if targets.is_empty() {
            return Err(CursorError::EmptyFaultExpansion);
        }
        for target in targets {
            self.contributors
                .get_mut(&target)
                .expect("configured target")
                .invalidate_recoverable();
        }
        Ok(())
    }
}

fn invalidate_slot_for_queue_drop(slot: &mut Slot) {
    slot.invalidate_recoverable();
    slot.retire_cursor();
    slot.invalidate_recoverable();
}

fn time_ns(value: &Rfc3339Time) -> Result<i64, CursorError> {
    value
        .utc_micros()
        .checked_mul(1_000)
        .ok_or(CursorError::TimeOverflow)
}

#[cfg(test)]
mod tests {
    use super::{CursorError, Invalidity, Slot};

    #[test]
    fn fixed_epoch_history_exhaustion_is_terminal() {
        let mut slot = Slot::default();
        for generation in 0..=u8::MAX {
            slot.history.insert(format!("epoch_{generation}"));
        }
        slot.retained_available_at_ns = Some(10);
        assert_eq!(
            slot.begin_epoch("epoch_new", 0, 9),
            Err(CursorError::EpochHistoryExhausted)
        );
        assert_eq!(slot.invalidity, Some(Invalidity::Terminal));
        assert_eq!(
            slot.begin_epoch("epoch_other", 1, 1),
            Err(CursorError::TerminalInvalid)
        );
    }
}
