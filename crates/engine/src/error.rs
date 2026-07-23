//! Engine errors.

use marketfeed_adapter_api::AdapterError;
use marketfeed_dispatch::DispatchError;
use marketfeed_recording::RecordingError;
use marketfeed_transport::TransportError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("engine is stopped")]
    Stopped,
    #[error("session not found")]
    SessionNotFound,
    #[error("internal: {0}")]
    Internal(String),
}
