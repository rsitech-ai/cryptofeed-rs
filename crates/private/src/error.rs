//! Errors for the private-account milestone.

use thiserror::Error;

use crate::credentials::CredentialsError;

/// Failures from private-account adapters (alpha scaffold).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PrivateError {
    #[error("private account streams are not implemented")]
    NotImplemented,
    #[error("private protocol: {0}")]
    Protocol(String),
    #[error("private parse: {0}")]
    Parse(String),
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
    #[error("private transport: {0}")]
    Transport(String),
}
