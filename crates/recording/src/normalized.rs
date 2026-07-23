//! Normalized event segment writer / reader (beyond raw MFR1).
//!
//! # Format: MFNE-JSON1 (§18.5 / Wave-4 W4-P0a)
//!
//! Separately versioned from MFR1. Default encoding is **newline-delimited JSON**
//! whose object shape matches `proto/marketfeed/v1/market_event.proto`
//! (`EventEnvelope` + `MarketEvent` oneof) — the same body schema as MFPE-JSON1
//! (length-prefixed companion in `marketfeed-sinks`).
//!
//! ```text
//! {"schema_version":1,"venue_id":1,"session_id":7,...,"payload":{"trade":{...}}}\n
//! ```
//!
//! See [`event_envelope_json`](crate::event_envelope_json) for the field map.
//! Reader: [`crate::read_normalized_jsonl`].
//!
//! # ponytail
//! JSONL not length-delimited protobuf. Ceiling = larger on-disk / no random
//! access index. Upgrade = MFNE-PB1 (reuse MFPE-PB1 wire) + segment index.

use std::io::Write;

use marketfeed_adapter_api::EventBatch;
use marketfeed_model::EventEnvelope;

use crate::envelope_json::event_envelope_json;
use crate::format::RecordingError;

/// Encoding for stamped market-event batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NormalizedFormat {
    /// One JSON object per line (MFNE-JSON1; proto field names). Stable schema.
    #[default]
    Jsonl,
    /// Legacy Debug text line (fixtures / human grepping only).
    DebugJsonl,
}

/// Hard limits for a single writer instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NormalizedBounds {
    /// Max envelopes written (0 = unlimited).
    pub max_records: u64,
    /// Max bytes written including newlines / length prefixes (0 = unlimited).
    pub max_bytes: u64,
}

/// Append-only writer for stamped [`EventBatch`] / [`EventEnvelope`] streams.
#[derive(Debug)]
pub struct NormalizedEventWriter<W: Write> {
    writer: W,
    format: NormalizedFormat,
    bounds: NormalizedBounds,
    pub records_written: u64,
    pub bytes_written: u64,
    pub batches_written: u64,
}

impl<W: Write> NormalizedEventWriter<W> {
    pub fn create(writer: W, format: NormalizedFormat, bounds: NormalizedBounds) -> Self {
        Self {
            writer,
            format,
            bounds,
            records_written: 0,
            bytes_written: 0,
            batches_written: 0,
        }
    }

    /// Write every envelope in `batch` (stamped via [`EventEnvelope`] fields).
    pub fn write_batch(&mut self, batch: &EventBatch) -> Result<(), RecordingError> {
        for env in &batch.events {
            self.write_envelope(env)?;
        }
        self.batches_written += 1;
        Ok(())
    }

    pub fn write_envelope(&mut self, env: &EventEnvelope) -> Result<(), RecordingError> {
        if self.bounds.max_records > 0 && self.records_written >= self.bounds.max_records {
            return Err(RecordingError::NormalizedBoundExceeded {
                kind: "records",
                limit: self.bounds.max_records,
            });
        }

        let line = match self.format {
            NormalizedFormat::Jsonl => {
                let v = event_envelope_json(env);
                serde_json::to_string(&v).map_err(|e| RecordingError::Io(e.to_string()))?
            }
            // ponytail: legacy Debug — keep for grepping; not schema-stable.
            NormalizedFormat::DebugJsonl => format!(
                "session={} frame_seq={} event_index={} receive_ts={} exchange_ts={:?} {:?}",
                env.session.0,
                env.frame_seq,
                env.event_index,
                env.receive_ts.0,
                env.exchange_ts.map(|t| t.0),
                env.payload
            ),
        };
        let nbytes = (line.len() + 1) as u64;
        if self.bounds.max_bytes > 0
            && self.bytes_written.saturating_add(nbytes) > self.bounds.max_bytes
        {
            return Err(RecordingError::NormalizedBoundExceeded {
                kind: "bytes",
                limit: self.bounds.max_bytes,
            });
        }

        writeln!(self.writer, "{line}")?;
        self.records_written += 1;
        self.bytes_written += nbytes;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), RecordingError> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    pub fn get_ref(&self) -> &W {
        &self.writer
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use marketfeed_adapter_api::EventBatch;
    use marketfeed_model::{
        AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, MarketEvent, Price,
        PricePoint, Quantity, SessionId, TimestampNs, Trade, VenueId,
    };
    use tempfile::NamedTempFile;

    use super::{NormalizedBounds, NormalizedEventWriter, NormalizedFormat};
    use crate::format::RecordingError;
    use crate::read_normalized_jsonl;

    fn envelope(idx: u16, frame_seq: u64) -> EventEnvelope {
        EventEnvelope {
            schema_version: 1,
            venue: VenueId(1),
            instrument: None,
            connection: ConnectionId(1),
            session: SessionId(7),
            frame_seq,
            event_index: idx,
            exchange_ts: Some(TimestampNs(100)),
            receive_ts: TimestampNs(200),
            source_sequence: None,
            flags: EventFlags::default(),
            payload: MarketEvent::Trade(Trade {
                price: Price(Fixed::new(100, 0)),
                quantity: Quantity(Fixed::new(1, 0)),
                aggressor: AggressorSide::Buy,
                trade_id: None,
            }),
        }
    }

    #[test]
    fn jsonl_tempfile_roundtrip() {
        let mut tmp = NamedTempFile::new().unwrap();
        {
            let mut w = NormalizedEventWriter::create(
                &mut tmp,
                NormalizedFormat::Jsonl,
                NormalizedBounds::default(),
            );
            let batch = EventBatch {
                session: SessionId(7),
                frame_seq: 3,
                events: vec![envelope(0, 3), envelope(1, 3)],
            };
            w.write_batch(&batch).unwrap();
            w.flush().unwrap();
            assert_eq!(w.records_written, 2);
        }
        tmp.flush().unwrap();

        let bytes = fs::read(tmp.path()).unwrap();
        let records = read_normalized_jsonl(&bytes).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["schema_version"], 1);
        assert_eq!(records[0]["venue_id"], 1);
        assert_eq!(records[0]["session_id"], 7);
        assert_eq!(records[0]["frame_seq"], 3);
        assert_eq!(records[0]["event_index"], 0);
        assert_eq!(records[0]["receive_ts"]["ns"], 200);
        assert_eq!(records[0]["exchange_ts"]["ns"], 100);
        assert_eq!(
            records[0]["payload"]["trade"]["aggressor"],
            "AGGRESSOR_SIDE_BUY"
        );
        assert_eq!(
            records[0]["payload"]["trade"]["price"]["value"]["coefficient_lo"],
            100
        );
        assert_eq!(records[1]["event_index"], 1);
    }

    #[test]
    fn writes_debug_lines_when_requested() {
        let mut w = NormalizedEventWriter::create(
            Vec::new(),
            NormalizedFormat::DebugJsonl,
            NormalizedBounds::default(),
        );
        w.write_envelope(&envelope(0, 3)).unwrap();
        let text = String::from_utf8(w.into_inner()).unwrap();
        assert!(text.contains("session=7"));
        assert!(text.contains("Trade"));
    }

    #[test]
    fn respects_max_records_bound() {
        let mut w = NormalizedEventWriter::create(
            Vec::new(),
            NormalizedFormat::Jsonl,
            NormalizedBounds {
                max_records: 1,
                max_bytes: 0,
            },
        );
        w.write_envelope(&envelope(0, 1)).unwrap();
        let err = w.write_envelope(&envelope(1, 1)).unwrap_err();
        assert!(matches!(
            err,
            RecordingError::NormalizedBoundExceeded {
                kind: "records",
                limit: 1
            }
        ));
        assert_eq!(w.records_written, 1);
    }

    #[test]
    fn respects_max_bytes_bound() {
        let mut w = NormalizedEventWriter::create(
            Vec::new(),
            NormalizedFormat::Jsonl,
            NormalizedBounds {
                max_records: 0,
                max_bytes: 40,
            },
        );
        let err = w
            .write_envelope(&EventEnvelope {
                payload: MarketEvent::MarkPrice(PricePoint {
                    price: Price(Fixed::new(1, 0)),
                }),
                ..envelope(0, 1)
            })
            .unwrap_err();
        assert!(matches!(
            err,
            RecordingError::NormalizedBoundExceeded { kind: "bytes", .. }
        ));
        assert_eq!(w.records_written, 0);
        assert_eq!(w.bytes_written, 0);
    }
}
