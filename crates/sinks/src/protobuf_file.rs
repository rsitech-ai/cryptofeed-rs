//! Length-prefixed JSON file sink aligned with `proto/marketfeed/v1/market_event.proto`.
//!
//! # Framing (MFPE-JSON1)
//!
//! Append-only stream of records:
//!
//! ```text
//! [u32 little-endian byte length][UTF-8 JSON body]
//! ```
//!
//! - **Market:** JSON object matching `EventEnvelope` proto field names
//!   (body schema shared with MFNE-JSON1 via
//!   [`marketfeed_recording::event_envelope_json`]; no prost; Rust model remains SoT).
//! - **System:** `{"kind":"system","event":"<SystemEvent Debug>"}` companion
//!   (systems are not in the MarketEvent proto).
//!
//! See crate `README.md`. Companion binary framing = `ProtobufBinaryFileSink`
//! (`MFPE-PB1`, `type = "protobuf-file-bin"`). Upgrade = prost codegen.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{EventEnvelope, OverflowPolicy, SystemEvent};
use marketfeed_recording::event_envelope_json as encode_envelope;
use serde_json::{Value, json};

use crate::memory::MemorySink;
use crate::sink::{EventSink, SinkError};

pub use marketfeed_recording::read_length_prefixed_json;

/// Bounded sink that appends length-prefixed JSON records (proto field names).
///
/// # ponytail
/// Sync write blocks the caller; ceiling = stall under slow disks.
/// JSON not binary protobuf; binary = [`crate::ProtobufBinaryFileSink`].
/// Upgrade = prost encode + same length prefix.
#[derive(Debug)]
pub struct ProtobufFileSink {
    inner: MemorySink,
    writer: BufWriter<File>,
    path: PathBuf,
    records_written: u64,
}

impl ProtobufFileSink {
    /// Open `path` for append (create if missing).
    pub fn open(
        path: impl AsRef<Path>,
        batch_capacity: usize,
        system_capacity: usize,
        policy: OverflowPolicy,
    ) -> Result<Self, std::io::Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            inner: MemorySink::new(batch_capacity, system_capacity, policy),
            writer: BufWriter::new(file),
            path,
            records_written: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn records_written(&self) -> u64 {
        self.records_written
    }

    pub fn dropped_batches(&self) -> u64 {
        self.inner.dropped_batches()
    }

    pub fn dropped_systems(&self) -> u64 {
        self.inner.dropped_systems()
    }

    fn write_record(&mut self, body: &[u8]) -> Result<(), SinkError> {
        let len = u32::try_from(body.len())
            .map_err(|_| SinkError::Io("record exceeds u32::MAX bytes".into()))?;
        self.writer
            .write_all(&len.to_le_bytes())
            .map_err(|e| SinkError::Io(e.to_string()))?;
        self.writer
            .write_all(body)
            .map_err(|e| SinkError::Io(e.to_string()))?;
        self.records_written += 1;
        Ok(())
    }

    fn flush_accepted_batches(&mut self) -> Result<(), SinkError> {
        while let Some(batch) = self.inner.pop_batch() {
            for env in &batch.events {
                let v = encode_envelope(env);
                let body = serde_json::to_vec(&v).map_err(|e| SinkError::Io(e.to_string()))?;
                self.write_record(&body)?;
            }
        }
        self.writer
            .flush()
            .map_err(|e| SinkError::Io(e.to_string()))?;
        Ok(())
    }

    fn flush_accepted_systems(&mut self) -> Result<(), SinkError> {
        while let Some(ev) = self.inner.pop_system() {
            let v = json!({
                "kind": "system",
                "event": format!("{ev:?}"),
            });
            let body = serde_json::to_vec(&v).map_err(|e| SinkError::Io(e.to_string()))?;
            self.write_record(&body)?;
        }
        self.writer
            .flush()
            .map_err(|e| SinkError::Io(e.to_string()))?;
        Ok(())
    }
}

impl EventSink for ProtobufFileSink {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        let outcome = self.inner.push_batch(batch)?;
        match outcome {
            PushOutcome::Accepted | PushOutcome::DroppedOldest { .. } => {
                self.flush_accepted_batches()?;
            }
            PushOutcome::DroppedNewest => {}
        }
        Ok(outcome)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        let outcome = self.inner.push_system(event)?;
        match outcome {
            PushOutcome::Accepted | PushOutcome::DroppedOldest { .. } => {
                self.flush_accepted_systems()?;
            }
            PushOutcome::DroppedNewest => {}
        }
        Ok(outcome)
    }
}

/// Encode [`EventEnvelope`] using proto3 JSON field names (delegates to recording).
pub fn event_envelope_json(env: &EventEnvelope) -> Value {
    encode_envelope(env)
}

#[cfg(test)]
mod tests {
    use marketfeed_model::{
        AggressorSide, ConnectionId, EventFlags, Fixed, MarketEvent, Price, Quantity, SessionId,
        TimestampNs, Trade, VenueId,
    };

    use super::*;

    fn sample_trade_envelope() -> EventEnvelope {
        EventEnvelope {
            schema_version: 1,
            venue: VenueId(2),
            instrument: Some(marketfeed_model::InstrumentId(7)),
            connection: ConnectionId(3),
            session: SessionId(4),
            frame_seq: 9,
            event_index: 0,
            exchange_ts: Some(TimestampNs(1_000)),
            receive_ts: TimestampNs(1_100),
            source_sequence: None,
            flags: EventFlags(0),
            payload: MarketEvent::Trade(Trade {
                price: Price(Fixed::new(100_00, 2)),
                quantity: Quantity(Fixed::new(1_5, 1)),
                aggressor: AggressorSide::Buy,
                trade_id: Some(marketfeed_model::SourceId("t1".into())),
            }),
        }
    }

    #[test]
    fn writes_length_prefixed_trade_and_system() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.mfpe");

        let mut sink = ProtobufFileSink::open(&path, 4, 4, OverflowPolicy::FailEngine).unwrap();
        let batch = EventBatch {
            session: SessionId(4),
            frame_seq: 9,
            events: vec![sample_trade_envelope()],
        };
        assert_eq!(sink.push_batch(batch).unwrap(), PushOutcome::Accepted);
        assert_eq!(
            sink.push_system(SystemEvent::ShutdownStarted).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(sink.records_written(), 2);

        let bytes = std::fs::read(&path).unwrap();
        let records = read_length_prefixed_json(&bytes).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["venue_id"], 2);
        assert_eq!(
            records[0]["payload"]["trade"]["aggressor"],
            "AGGRESSOR_SIDE_BUY"
        );
        assert_eq!(
            records[0]["payload"]["trade"]["price"]["value"]["coefficient_lo"],
            100_00
        );
        assert_eq!(records[0]["payload"]["trade"]["price"]["value"]["scale"], 2);
        assert_eq!(records[1]["kind"], "system");
        assert!(
            records[1]["event"]
                .as_str()
                .unwrap()
                .contains("ShutdownStarted")
        );
    }

    #[test]
    fn empty_batch_writes_no_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.mfpe");
        let mut sink = ProtobufFileSink::open(&path, 4, 4, OverflowPolicy::FailEngine).unwrap();
        let batch = EventBatch {
            session: SessionId(1),
            frame_seq: 1,
            events: Vec::new(),
        };
        assert_eq!(sink.push_batch(batch).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.records_written(), 0);
        assert!(std::fs::read(&path).unwrap().is_empty());
    }
}
