//! Private session machine — mirror of public [`SessionMachine`](marketfeed_adapter_api::SessionMachine).
//!
//! Reuses public [`SessionInput`] / [`SessionAction`] for wire I/O. Account
//! payloads go out as [`PrivateSessionAction::Account`].

use marketfeed_adapter_api::{SessionAction, SessionInput};

use crate::PrivateError;
use crate::account::AccountEvent;

/// Deterministic private-account protocol machine (fixture-driven today).
pub trait PrivateSessionMachine: Send + 'static {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError>;
}

/// Actions from a private session: engine wire ops + account events.
// `SessionAction` carries the protocol's full deterministic action payload.
// Boxing it would add heap traffic to every private wire operation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateSessionAction {
    /// Public adapter action (RequestHttp / timers / reconnect / MarkLive / …).
    Session(SessionAction),
    /// Normalized private account payload.
    Account(AccountEvent),
}

/// Default bound for private action sinks (DropNewest when full).
pub const DEFAULT_PRIVATE_ACTION_BUFFER_CAPACITY: usize = 1024;

/// Bounded action sink reused across `on_input` calls.
#[derive(Debug)]
pub struct PrivateActionBuffer {
    actions: Vec<PrivateSessionAction>,
    capacity: usize,
    dropped: u64,
}

impl Default for PrivateActionBuffer {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_PRIVATE_ACTION_BUFFER_CAPACITY)
    }
}

impl PrivateActionBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            actions: Vec::with_capacity(capacity.min(1024)),
            capacity,
            dropped: 0,
        }
    }

    pub fn push(&mut self, action: PrivateSessionAction) {
        if self.actions.len() >= self.capacity {
            self.dropped += 1;
            return;
        }
        self.actions.push(action);
    }

    pub fn push_session(&mut self, action: SessionAction) {
        self.push(PrivateSessionAction::Session(action));
    }

    pub fn push_account(&mut self, event: AccountEvent) {
        self.push(PrivateSessionAction::Account(event));
    }

    pub fn as_slice(&self) -> &[PrivateSessionAction] {
        &self.actions
    }

    pub fn drain(&mut self) -> impl Iterator<Item = PrivateSessionAction> + '_ {
        self.actions.drain(..)
    }

    pub fn take_dropped(&mut self) -> u64 {
        std::mem::replace(&mut self.dropped, 0)
    }

    pub fn clear(&mut self) {
        self.actions.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.actions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use marketfeed_adapter_api::StopReason;

    #[test]
    fn private_action_buffer_drop_newest() {
        let mut buf = PrivateActionBuffer::with_capacity(2);
        buf.push_session(SessionAction::SendText(Bytes::from_static(b"a")));
        buf.push_session(SessionAction::SendText(Bytes::from_static(b"b")));
        buf.push_session(SessionAction::StopSession(StopReason::Control));
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.take_dropped(), 1);
    }
}
