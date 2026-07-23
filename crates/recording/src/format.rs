//! Binary MFR1 raw-segment layout (little-endian).

use marketfeed_model::SessionId;
use thiserror::Error;

pub const MAGIC: &[u8; 4] = b"MFR1";
/// Current MFR1 schema version. Version 3 adds accepted dynamic-subscription commands.
pub const FORMAT_VERSION: u16 = 3;
/// Oldest MFR1 schema supported by this reader.
pub const MIN_SUPPORTED_FORMAT_VERSION: u16 = 1;
pub const HEADER_SIZE: usize = 4 + 2 + 8 + 8; // magic + ver + start_ts + session_count(placeholder 0)
/// Defensive upper bound for one raw wire record, including its length field.
pub const MAX_RAW_RECORD_LEN: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Direction {
    Inbound = 0,
    Outbound = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FrameOpcode {
    Text = 1,
    Binary = 2,
    Ping = 3,
    Pong = 4,
    Close = 5,
    /// Encoded adapter HTTP response (`request_id`, status, headers, body).
    HttpResponse = 6,
    /// JSON-encoded build or session reproduction metadata.
    Metadata = 7,
    /// Bounded accepted dynamic-subscription mutation.
    SubscriptionCommand = 8,
}

impl FrameOpcode {
    pub fn from_u8_for_version(v: u8, format_version: u16) -> Option<Self> {
        match v {
            1 => Some(Self::Text),
            2 => Some(Self::Binary),
            3 => Some(Self::Ping),
            4 => Some(Self::Pong),
            5 => Some(Self::Close),
            6 if format_version >= 2 => Some(Self::HttpResponse),
            7 if format_version >= 2 => Some(Self::Metadata),
            8 if format_version >= 3 => Some(Self::SubscriptionCommand),
            _ => None,
        }
    }
}

impl Direction {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Inbound),
            1 => Some(Self::Outbound),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRecordHeader {
    pub record_len: u32,
    pub session: SessionId,
    pub frame_seq: u64,
    pub receive_ts_ns: i64,
    pub monotonic_ns: u64,
    pub direction: Direction,
    pub opcode: FrameOpcode,
    pub flags: u8,
    pub payload_len: u32,
    pub payload_crc32c: u32,
}

/// Fixed portion after record_len (session..crc).
pub const RAW_HEADER_BODY_LEN: usize = 8 + 8 + 8 + 8 + 1 + 1 + 1 + 4 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRecord {
    pub header: RawRecordHeader,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecordingError {
    #[error("io: {0}")]
    Io(String),
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported version {0}")]
    UnsupportedVersion(u16),
    #[error("truncated / incomplete record")]
    Truncated,
    #[error("payload crc mismatch")]
    CrcMismatch,
    #[error("invalid header field")]
    InvalidHeader,
    #[error("conflicting recording metadata for {key}")]
    MetadataConflict { key: String },
    #[error("invalid recorded control command: {0}")]
    InvalidControlCommand(String),
    #[error("raw record length {record_len} exceeds maximum {max}")]
    RecordTooLarge { record_len: u32, max: u32 },
    #[error("recording queue full")]
    QueueFull,
    #[error("unsupported overflow policy: {0}")]
    UnsupportedOverflow(String),
    #[error("shutdown drain timed out with {remaining} frames still queued")]
    ShutdownTimeout { remaining: usize },
    #[error("normalized recording bound exceeded ({kind} limit {limit})")]
    NormalizedBoundExceeded { kind: &'static str, limit: u64 },
    #[error("disk full under FailEngine recording policy")]
    DiskFull,
}

impl From<std::io::Error> for RecordingError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}
