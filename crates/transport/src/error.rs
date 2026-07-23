//! Transport errors.

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    #[error("not connected")]
    NotConnected,
    #[error("connection closed")]
    Closed,
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("timeout")]
    Timeout,
    #[error("tls: {0}")]
    Tls(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("io: {0}")]
    Io(String),
    /// ponytail: stub transport until tokio-tungstenite+rustls is wired.
    #[error("transport stub: {0}")]
    Stub(String),
}
