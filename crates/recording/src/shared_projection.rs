//! Opaque, indivisible projection groups. Engine owns typed payload semantics;
//! recording validates the physical source-cursor chain before publication.
use super::{FeedCursor, MAX_RECORD, err};
use crate::RecordingError;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

pub const MAX_PROJECTION_PAYLOAD: usize = (MAX_RECORD - 4096) / 4 * 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionEnvelope {
    pub schema_version: u16,
    pub source_before: FeedCursor,
    pub source_after: FeedCursor,
    pub source_sealed: bool,
    pub payload_base64: String,
}
impl ProjectionEnvelope {
    pub fn new(
        source_before: FeedCursor,
        source_after: FeedCursor,
        source_sealed: bool,
        payload: &[u8],
    ) -> Result<Self, RecordingError> {
        if payload.is_empty() || payload.len() > MAX_PROJECTION_PAYLOAD {
            return Err(err("projection payload byte budget"));
        }
        let result = Self {
            schema_version: 1,
            source_before,
            source_after,
            source_sealed,
            payload_base64: STANDARD.encode(payload),
        };
        result.validate()?;
        Ok(result)
    }
    pub fn payload(&self) -> Result<Vec<u8>, RecordingError> {
        self.validate()?;
        STANDARD.decode(&self.payload_base64).map_err(err)
    }
    /// Structural/byte validation only; typed source semantics are engine-owned.
    pub fn validate(&self) -> Result<(), RecordingError> {
        if self.schema_version != 1 {
            return Err(RecordingError::UnsupportedVersion(self.schema_version));
        }
        let before = &self.source_before;
        let after = &self.source_after;
        if before.feed_id.len() != 40
            || !before
                .feed_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || after.feed_id != before.feed_id
            || before.offset < 48
            || (before.sequence == 0 && before.offset != 48)
            || Some(after.sequence) != before.sequence.checked_add(1)
            || after.offset <= before.offset
            || self.payload_base64.is_empty()
            || self.payload_base64.len() > (MAX_RECORD - 4096)
        {
            return Err(err("projection envelope source boundary or byte budget"));
        }
        let mut buffer = [0u8; 3072];
        for (i, chunk) in self.payload_base64.as_bytes().chunks(4096).enumerate() {
            let written = STANDARD.decode_slice(chunk, &mut buffer).map_err(err)?;
            if i * 4096 + chunk.len() < self.payload_base64.len() && written != buffer.len() {
                return Err(err("non-final projection base64 padding"));
            }
        }
        Ok(())
    }
}
