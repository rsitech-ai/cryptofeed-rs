//! Length-prefixed binary protobuf file sink (`MFPE-PB1`).
//!
//! # Framing (MFPE-PB1)
//!
//! Append-only stream of records:
//!
//! ```text
//! [u32 little-endian byte length][record body]
//! ```
//!
//! - **Market:** protobuf3 `EventEnvelope` body (hand-encoded; tags match
//!   `proto/marketfeed/v1/market_event.proto`). No prost.
//! - **System:** UTF-8 JSON companion
//!   `{"kind":"system","event":"<SystemEvent Debug>"}` (same as MFPE-JSON1;
//!   systems are not in the MarketEvent proto). Detect via body starting with `{`.
//!
//! See crate `README.md`. `type = "protobuf-file"` remains MFPE-JSON1 unchanged.
//! Upgrade = prost-codegen when an external consumer needs generated stubs.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{OverflowPolicy, SystemEvent};
use serde_json::json;

use crate::memory::MemorySink;
use crate::protobuf_wire::encode_event_envelope;
use crate::sink::{EventSink, SinkError};

/// Bounded sink that appends length-prefixed protobuf `EventEnvelope` records.
///
/// # ponytail
/// Sync write blocks the caller; ceiling = stall under slow disks.
/// Hand wire encoder (not prost); upgrade = prost feature + same length prefix.
#[derive(Debug)]
pub struct ProtobufBinaryFileSink {
    inner: MemorySink,
    writer: BufWriter<File>,
    path: PathBuf,
    records_written: u64,
}

impl ProtobufBinaryFileSink {
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
                let body = encode_event_envelope(env);
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

impl EventSink for ProtobufBinaryFileSink {
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

#[cfg(test)]
mod tests {
    use marketfeed_model::{
        AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, MarketEvent, Price,
        Quantity, SessionId, TimestampNs, Trade, VenueId,
    };

    use super::*;
    use crate::protobuf_wire::read_length_prefixed_records;

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
        let path = dir.path().join("events.mfpeb");

        let mut sink =
            ProtobufBinaryFileSink::open(&path, 4, 4, OverflowPolicy::FailEngine).unwrap();
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
        let records = read_length_prefixed_records(&bytes).unwrap();
        assert_eq!(records.len(), 2);

        let market = &records[0];
        assert_ne!(market.first(), Some(&b'{'));
        // schema_version=1 is field 1 varint → tag 0x08, value 0x01
        assert!(
            market.windows(2).any(|w| w == [0x08, 0x01]),
            "expected schema_version=1 in protobuf body: {market:?}"
        );
        // trade_id "t1" length-delimited somewhere in payload
        assert!(
            market.windows(2).any(|w| w == b"t1"),
            "expected trade_id bytes in body"
        );

        let system = &records[1];
        assert_eq!(system.first(), Some(&b'{'));
        let v: serde_json::Value = serde_json::from_slice(system).unwrap();
        assert_eq!(v["kind"], "system");
        assert!(v["event"].as_str().unwrap().contains("ShutdownStarted"));
    }

    #[test]
    fn empty_batch_writes_no_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.mfpeb");
        let mut sink =
            ProtobufBinaryFileSink::open(&path, 4, 4, OverflowPolicy::FailEngine).unwrap();
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
