//! Frame and normalized-event recording.
//!
//! - **MFR1** ([`RawSegmentWriter`]): raw frames before normalization.
//! - **MFNE-JSON1** ([`NormalizedEventWriter`]): stamped `EventEnvelope` /
//!   `MarketEvent` as newline-delimited JSON (proto field names; same body
//!   schema as MFPE-JSON1). See [`event_envelope_json`] / [`read_normalized_jsonl`].

#![forbid(unsafe_code)]

mod control;
mod crc32c;
mod envelope_json;
mod format;
mod http;
mod metadata;
mod normalized;
mod pipeline;
mod queue;
mod reader;
mod writer;

pub use control::{decode_subscription_command, encode_subscription_command};
pub use crc32c::crc32c;
pub use envelope_json::{event_envelope_json, read_length_prefixed_json, read_normalized_jsonl};
pub use format::*;
pub use http::{decode_http_response, encode_http_response};
pub use metadata::{
    BuildMetadata, CatalogInstrumentMetadata, FixedMetadata, MetadataRecord,
    SessionRecordingMetadata, SubscriptionMetadata, decode_metadata, encode_metadata,
};
pub use normalized::{NormalizedBounds, NormalizedEventWriter, NormalizedFormat};
pub use pipeline::*;
pub use queue::*;
pub use reader::*;
pub use writer::*;
