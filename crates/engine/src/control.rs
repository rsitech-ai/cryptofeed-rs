//! Embedded control plane (Spec §10.4 / §19.2) — sync ownership, no adapter I/O.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use marketfeed_adapter_api::{SessionCommand, SessionMachine, SubscriptionPatch};
use marketfeed_model::{
    BookSnapshot, CatalogView, InstrumentId, PlanVersion, SessionId, SystemEvent, TimestampNs,
    VenueId,
};
use thiserror::Error;

use crate::state::{EngineLifecycle, SessionLifecycle};
use crate::{EngineError, EngineSupervisor, SessionRunnerConfig};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ControlError {
    #[error("engine is stopped")]
    Stopped,
    #[error("session not found")]
    SessionNotFound,
    #[error("venue {0:?} is paused")]
    VenuePaused(VenueId),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("book unavailable for instrument {0:?}")]
    BookUnavailable(InstrumentId),
    #[error("recording rotate not configured")]
    RotateUnavailable,
    #[error("engine: {0}")]
    Engine(String),
}

impl From<EngineError> for ControlError {
    fn from(err: EngineError) -> Self {
        match err {
            EngineError::Stopped => ControlError::Stopped,
            EngineError::SessionNotFound => ControlError::SessionNotFound,
            other => ControlError::Engine(other.to_string()),
        }
    }
}

/// Cross-task request flag for recording segment rotation (§19.2).
#[derive(Debug, Default)]
pub struct RecordingRotateHandle {
    requested: AtomicBool,
}

impl RecordingRotateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request_rotate(&self) {
        self.requested.store(true, Ordering::SeqCst);
    }

    pub fn take_request(&self) -> bool {
        self.requested.swap(false, Ordering::SeqCst)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHealth {
    pub session: SessionId,
    pub venue: VenueId,
    pub lifecycle: SessionLifecycle,
    pub subscribed: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub lifecycle: EngineLifecycle,
    pub plan_version: PlanVersion,
    pub paused_venues: Vec<VenueId>,
    pub sessions: Vec<SessionHealth>,
}

/// Rolling replace bookkeeping (Spec §10.4 skeleton).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollingReplace {
    pub old: SessionId,
    pub new: SessionId,
}

/// Spec §19.2 control surface for dynamic subscriptions (§10.4).
///
/// Sync: engine owns sessions. Async/remote wrappers are YAGNI until gRPC/UDS.
pub trait EngineControl {
    fn apply_subscriptions(
        &mut self,
        patch: SubscriptionPatch,
        now: TimestampNs,
    ) -> Result<PlanVersion, ControlError>;

    fn health(&self) -> Result<HealthSnapshot, ControlError>;

    fn publish_catalog_refresh(
        &mut self,
        session: SessionId,
        catalog: CatalogView,
        now: TimestampNs,
    ) -> Result<(), ControlError>;

    fn begin_rolling_replace(
        &mut self,
        old: SessionId,
        machine: Box<dyn SessionMachine>,
        cfg: SessionRunnerConfig,
        now: TimestampNs,
    ) -> Result<RollingReplace, ControlError>;

    fn complete_rolling_replace(&mut self, replace: RollingReplace) -> Result<(), ControlError>;

    fn book_snapshot(
        &self,
        instrument: InstrumentId,
        depth: Option<u32>,
    ) -> Result<BookSnapshot, ControlError>;

    fn rotate_recordings(&self) -> Result<(), ControlError>;
}

pub(crate) type DesiredMap = HashMap<SessionId, Vec<String>>;
pub(crate) type PausedSet = HashSet<VenueId>;
pub(crate) type RollingMap = HashMap<SessionId, RollingReplace>;

impl EngineSupervisor {
    pub fn plan_version(&self) -> PlanVersion {
        self.plan_version
    }

    pub fn is_venue_paused(&self, venue: VenueId) -> bool {
        self.paused_venues.contains(&venue)
    }

    pub fn desired_symbols(&self, session: SessionId) -> Option<&[String]> {
        self.desired_symbols.get(&session).map(|v| v.as_slice())
    }

    fn bump_plan(&mut self) -> PlanVersion {
        self.plan_version.0 = self.plan_version.0.saturating_add(1);
        self.plan_version
    }

    fn ensure_running(&self) -> Result<(), ControlError> {
        if self.lifecycle != EngineLifecycle::Running {
            Err(ControlError::Stopped)
        } else {
            Ok(())
        }
    }

    fn patch_session(
        &mut self,
        session: SessionId,
        now: TimestampNs,
        cmd: SessionCommand,
        desired: Vec<String>,
    ) -> Result<(), ControlError> {
        let venue = self
            .sessions
            .get(&session)
            .map(|r| r.venue())
            .ok_or(ControlError::SessionNotFound)?;
        if self.paused_venues.contains(&venue) {
            return Err(ControlError::VenuePaused(venue));
        }
        let next = self.plan_version.0.saturating_add(1);
        self.session_mut(session)?.deliver_control(cmd, now)?;
        self.desired_symbols.insert(session, desired);
        // The adapter/wire mutation and desired state are authoritative. A full
        // diagnostic queue must not make a successful control command look
        // rolled back when it cannot be rolled back.
        self.session_mut(session)?
            .push_system_best_effort(SystemEvent::SubscriptionStateChanged {
                state: format!("plan={next}"),
            });
        Ok(())
    }
}

impl EngineControl for EngineSupervisor {
    fn apply_subscriptions(
        &mut self,
        patch: SubscriptionPatch,
        now: TimestampNs,
    ) -> Result<PlanVersion, ControlError> {
        self.ensure_running()?;

        match patch {
            SubscriptionPatch::Add { session, symbols } => {
                let mut desired = self
                    .desired_symbols
                    .get(&session)
                    .cloned()
                    .unwrap_or_default();
                for s in &symbols {
                    if !desired.iter().any(|d| d == s) {
                        desired.push(s.clone());
                    }
                }
                self.patch_session(session, now, SessionCommand::Subscribe(symbols), desired)?;
            }
            SubscriptionPatch::Remove { session, symbols } => {
                let mut desired = self
                    .desired_symbols
                    .get(&session)
                    .cloned()
                    .unwrap_or_default();
                desired.retain(|d| !symbols.iter().any(|s| s == d));
                self.patch_session(session, now, SessionCommand::Unsubscribe(symbols), desired)?;
            }
            SubscriptionPatch::Replace { session, symbols } => {
                self.patch_session(
                    session,
                    now,
                    SessionCommand::Replace(symbols.clone()),
                    symbols,
                )?;
            }
            SubscriptionPatch::PauseVenue { venue } => {
                self.paused_venues.insert(venue);
                let ids: Vec<SessionId> = self
                    .sessions
                    .iter()
                    .filter(|(_, r)| r.venue() == venue)
                    .map(|(id, _)| *id)
                    .collect();
                for id in ids {
                    let runner = self.session_mut(id)?;
                    runner.mark_degraded_with_status("venue paused", now)?;
                    runner.push_system(SystemEvent::SubscriptionStateChanged {
                        state: "paused".into(),
                    })?;
                }
            }
            SubscriptionPatch::ResumeVenue { venue } => {
                self.paused_venues.remove(&venue);
                let ids: Vec<(SessionId, Vec<String>)> = self
                    .sessions
                    .iter()
                    .filter(|(_, r)| r.venue() == venue)
                    .map(|(id, _)| {
                        (
                            *id,
                            self.desired_symbols.get(id).cloned().unwrap_or_default(),
                        )
                    })
                    .collect();
                for (id, symbols) in ids {
                    let runner = self.session_mut(id)?;
                    if !symbols.is_empty() {
                        runner.deliver_control(SessionCommand::Subscribe(symbols), now)?;
                    }
                    runner.push_system(SystemEvent::SubscriptionStateChanged {
                        state: "resumed".into(),
                    })?;
                }
            }
        }

        Ok(self.bump_plan())
    }

    fn health(&self) -> Result<HealthSnapshot, ControlError> {
        let mut sessions = Vec::with_capacity(self.sessions.len());
        for (id, runner) in &self.sessions {
            sessions.push(SessionHealth {
                session: *id,
                venue: runner.venue(),
                lifecycle: runner.lifecycle,
                subscribed: self.desired_symbols.get(id).cloned().unwrap_or_default(),
            });
        }
        sessions.sort_by_key(|s| s.session.0);
        let mut paused: Vec<_> = self.paused_venues.iter().copied().collect();
        paused.sort_by_key(|v| v.0);
        Ok(HealthSnapshot {
            lifecycle: self.lifecycle,
            plan_version: self.plan_version,
            paused_venues: paused,
            sessions,
        })
    }

    fn publish_catalog_refresh(
        &mut self,
        session: SessionId,
        catalog: CatalogView,
        now: TimestampNs,
    ) -> Result<(), ControlError> {
        self.ensure_running()?;
        let runner = self.session_mut(session)?;
        runner.publish_catalog_refresh(catalog, now)?;
        Ok(())
    }

    fn begin_rolling_replace(
        &mut self,
        old: SessionId,
        machine: Box<dyn SessionMachine>,
        cfg: SessionRunnerConfig,
        now: TimestampNs,
    ) -> Result<RollingReplace, ControlError> {
        self.ensure_running()?;
        if !self.sessions.contains_key(&old) {
            return Err(ControlError::SessionNotFound);
        }
        let new_id = cfg.session;
        if new_id == old {
            return Err(ControlError::Unsupported(
                "rolling replace requires a distinct session id".into(),
            ));
        }
        if self.sessions.contains_key(&new_id) {
            return Err(ControlError::Unsupported(
                "replacement session id already exists".into(),
            ));
        }
        if self.rolling.values().any(|replace| replace.old == old) {
            return Err(ControlError::Unsupported(
                "rolling replace is already in progress for the old session".into(),
            ));
        }
        let symbols = self.desired_symbols.get(&old).cloned().unwrap_or_default();
        self.insert_session(machine, cfg)?;
        if !symbols.is_empty() {
            if let Err(error) = self
                .session_mut(new_id)?
                .deliver_control(SessionCommand::Subscribe(symbols.clone()), now)
            {
                self.sessions.remove(&new_id);
                self.desired_symbols.remove(&new_id);
                return Err(error.into());
            }
            self.desired_symbols.insert(new_id, symbols);
        }
        let pair = RollingReplace { old, new: new_id };
        self.rolling.insert(new_id, pair);
        let _ = self.bump_plan();
        Ok(pair)
    }

    fn complete_rolling_replace(&mut self, replace: RollingReplace) -> Result<(), ControlError> {
        if self.lifecycle != EngineLifecycle::Running && self.lifecycle != EngineLifecycle::Draining
        {
            return Err(ControlError::Stopped);
        }
        let tracked =
            self.rolling.get(&replace.new).copied().ok_or_else(|| {
                ControlError::Unsupported("rolling replace is not tracked".into())
            })?;
        if tracked != replace {
            return Err(ControlError::Unsupported(
                "rolling replace pair mismatch".into(),
            ));
        }
        if !self.sessions.contains_key(&replace.old) {
            return Err(ControlError::SessionNotFound);
        }
        let old_desired: HashSet<&str> = self
            .desired_symbols
            .get(&replace.old)
            .into_iter()
            .flatten()
            .map(String::as_str)
            .collect();
        let new_desired: HashSet<&str> = self
            .desired_symbols
            .get(&replace.new)
            .into_iter()
            .flatten()
            .map(String::as_str)
            .collect();
        if old_desired != new_desired {
            return Err(ControlError::Unsupported(
                "replacement desired subscriptions do not match the old session".into(),
            ));
        }
        let replacement = self
            .sessions
            .get(&replace.new)
            .ok_or(ControlError::SessionNotFound)?;
        if replacement.lifecycle != SessionLifecycle::Live {
            return Err(ControlError::Unsupported(
                "replacement session is not live".into(),
            ));
        }
        self.rolling.remove(&replace.new);
        if let Some(runner) = self.sessions.get_mut(&replace.old) {
            runner.request_stop();
            let _ = runner.push_system(SystemEvent::SubscriptionStateChanged {
                state: "replaced".into(),
            });
        }
        self.sessions.remove(&replace.old);
        self.desired_symbols.remove(&replace.old);
        let _ = self.bump_plan();
        Ok(())
    }

    fn book_snapshot(
        &self,
        instrument: InstrumentId,
        depth: Option<u32>,
    ) -> Result<BookSnapshot, ControlError> {
        for runner in self.sessions.values() {
            if let Some(snap) = runner.book_snapshot(instrument, depth) {
                return Ok(snap);
            }
        }
        Err(ControlError::BookUnavailable(instrument))
    }

    fn rotate_recordings(&self) -> Result<(), ControlError> {
        let h = self
            .recording_rotate
            .as_ref()
            .ok_or(ControlError::RotateUnavailable)?;
        h.request_rotate();
        Ok(())
    }
}

#[cfg(test)]
mod r15_tests {
    use super::*;
    use marketfeed_model::{BookLevel, Fixed, Price, Quantity};
    use std::sync::Arc;

    #[test]
    fn rotate_handle_roundtrip() {
        let h = Arc::new(RecordingRotateHandle::new());
        assert!(!h.take_request());
        h.request_rotate();
        assert!(h.take_request());
        assert!(!h.take_request());
    }

    #[test]
    fn book_unavailable_without_sessions() {
        let mut eng = EngineSupervisor::new();
        eng.mark_running();
        assert!(matches!(
            EngineControl::book_snapshot(&eng, InstrumentId(1), None),
            Err(ControlError::BookUnavailable(InstrumentId(1)))
        ));
        assert_eq!(
            EngineControl::rotate_recordings(&eng),
            Err(ControlError::RotateUnavailable)
        );
        eng.set_recording_rotate(Arc::new(RecordingRotateHandle::new()));
        EngineControl::rotate_recordings(&eng).unwrap();
        assert!(eng.recording_rotate.as_ref().unwrap().take_request());
        let _ = BookLevel {
            price: Price(Fixed::new(1, 0)),
            quantity: Quantity(Fixed::new(1, 0)),
        };
    }
}
