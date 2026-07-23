//! Session and engine lifecycle states.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionLifecycle {
    Planned,
    Connecting,
    Connected,
    Subscribing,
    Synchronizing,
    Live,
    Degraded,
    Backoff,
    Draining,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineLifecycle {
    Starting,
    Running,
    Draining,
    Stopped,
}
