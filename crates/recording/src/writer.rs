//! Append-only raw segment writer.

use std::io::Write;

use marketfeed_model::SessionId;

use crate::crc32c::crc32c;
use crate::format::{
    Direction, FORMAT_VERSION, FrameOpcode, MAGIC, MAX_RAW_RECORD_LEN, RAW_HEADER_BODY_LEN,
    RawRecord, RawRecordHeader, RecordingError,
};
use crate::{MetadataRecord, encode_metadata};

#[derive(Debug)]
pub struct RawSegmentWriter<W: Write> {
    writer: W,
    pub records_written: u64,
    started: bool,
}

impl<W: Write> RawSegmentWriter<W> {
    pub fn create(mut writer: W, start_ts_ns: i64) -> Result<Self, RecordingError> {
        writer.write_all(MAGIC)?;
        writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
        writer.write_all(&start_ts_ns.to_le_bytes())?;
        writer.write_all(&0u64.to_le_bytes())?; // reserved session table count
        Ok(Self {
            writer,
            records_written: 0,
            started: true,
        })
    }

    #[allow(clippy::too_many_arguments)] // wire-format record fields; keep flat for CRC layout clarity
    pub fn write_record(
        &mut self,
        session: SessionId,
        frame_seq: u64,
        receive_ts_ns: i64,
        monotonic_ns: u64,
        direction: Direction,
        opcode: FrameOpcode,
        flags: u8,
        payload: &[u8],
    ) -> Result<(), RecordingError> {
        if !self.started {
            return Err(RecordingError::Io("writer not started".into()));
        }
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| RecordingError::InvalidHeader)?;
        let payload_crc = crc32c(payload);
        let record_len = u32::try_from(4 + RAW_HEADER_BODY_LEN + payload.len())
            .map_err(|_| RecordingError::InvalidHeader)?;
        if record_len > MAX_RAW_RECORD_LEN {
            return Err(RecordingError::RecordTooLarge {
                record_len,
                max: MAX_RAW_RECORD_LEN,
            });
        }

        let header = RawRecordHeader {
            record_len,
            session,
            frame_seq,
            receive_ts_ns,
            monotonic_ns,
            direction,
            opcode,
            flags,
            payload_len,
            payload_crc32c: payload_crc,
        };
        self.write_header(&header)?;
        self.writer.write_all(payload)?;
        self.records_written += 1;
        Ok(())
    }

    pub fn write_raw(&mut self, record: &RawRecord) -> Result<(), RecordingError> {
        self.write_record(
            record.header.session,
            record.header.frame_seq,
            record.header.receive_ts_ns,
            record.header.monotonic_ns,
            record.header.direction,
            record.header.opcode,
            record.header.flags,
            &record.payload,
        )
    }

    pub fn write_metadata(
        &mut self,
        metadata: &MetadataRecord,
        start_ts_ns: i64,
    ) -> Result<(), RecordingError> {
        let payload = encode_metadata(metadata)?;
        self.write_record(
            metadata.session_id(),
            0,
            start_ts_ns,
            0,
            Direction::Inbound,
            FrameOpcode::Metadata,
            0,
            &payload,
        )
    }

    fn write_header(&mut self, h: &RawRecordHeader) -> Result<(), RecordingError> {
        self.writer.write_all(&h.record_len.to_le_bytes())?;
        self.writer.write_all(&h.session.0.to_le_bytes())?;
        self.writer.write_all(&h.frame_seq.to_le_bytes())?;
        self.writer.write_all(&h.receive_ts_ns.to_le_bytes())?;
        self.writer.write_all(&h.monotonic_ns.to_le_bytes())?;
        self.writer.write_all(&[h.direction as u8])?;
        self.writer.write_all(&[h.opcode as u8])?;
        self.writer.write_all(&[h.flags])?;
        self.writer.write_all(&h.payload_len.to_le_bytes())?;
        self.writer.write_all(&h.payload_crc32c.to_le_bytes())?;
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    pub fn flush(&mut self) -> Result<(), RecordingError> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_rejects_records_the_reader_cannot_accept() {
        let mut writer = RawSegmentWriter::create(Vec::new(), 0).unwrap();
        let oversized_payload_len = MAX_RAW_RECORD_LEN as usize - 4 - RAW_HEADER_BODY_LEN + 1;
        let payload = vec![0u8; oversized_payload_len];
        let err = writer
            .write_record(
                SessionId(1),
                1,
                1,
                1,
                Direction::Inbound,
                FrameOpcode::Binary,
                0,
                &payload,
            )
            .unwrap_err();
        assert_eq!(
            err,
            RecordingError::RecordTooLarge {
                record_len: MAX_RAW_RECORD_LEN + 1,
                max: MAX_RAW_RECORD_LEN,
            }
        );
        assert_eq!(writer.records_written, 0);
    }
}
