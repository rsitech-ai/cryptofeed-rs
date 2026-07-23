//! Raw segment reader with CRC validation and truncated-tail recovery.

use std::io::{Cursor, Read};

use marketfeed_model::SessionId;

use crate::crc32c::crc32c;
use crate::format::{
    Direction, FORMAT_VERSION, FrameOpcode, MAGIC, MAX_RAW_RECORD_LEN,
    MIN_SUPPORTED_FORMAT_VERSION, RAW_HEADER_BODY_LEN, RawRecord, RawRecordHeader, RecordingError,
};

#[derive(Debug)]
pub struct RawSegmentReader<R: Read> {
    reader: R,
    pub format_version: u16,
    pub start_ts_ns: i64,
    pub records_read: u64,
}

impl RawSegmentReader<Cursor<Vec<u8>>> {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, RecordingError> {
        Self::open(Cursor::new(bytes))
    }
}

impl<R: Read> RawSegmentReader<R> {
    pub fn open(mut reader: R) -> Result<Self, RecordingError> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(RecordingError::BadMagic);
        }
        let version = read_u16(&mut reader)?;
        if !(MIN_SUPPORTED_FORMAT_VERSION..=FORMAT_VERSION).contains(&version) {
            return Err(RecordingError::UnsupportedVersion(version));
        }
        let start_ts_ns = read_i64(&mut reader)?;
        let _session_count = read_u64(&mut reader)?;
        Ok(Self {
            reader,
            format_version: version,
            start_ts_ns,
            records_read: 0,
        })
    }

    pub fn read_record(&mut self) -> Result<Option<RawRecord>, RecordingError> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let record_len = u32::from_le_bytes(len_buf);
        if record_len < (4 + RAW_HEADER_BODY_LEN) as u32 {
            return Err(RecordingError::InvalidHeader);
        }
        if record_len > MAX_RAW_RECORD_LEN {
            return Err(RecordingError::RecordTooLarge {
                record_len,
                max: MAX_RAW_RECORD_LEN,
            });
        }
        let body_and_payload = (record_len as usize) - 4;
        let mut rest = vec![0u8; body_and_payload];
        match self.reader.read_exact(&mut rest) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Crash recovery: truncate incomplete tail.
                return Err(RecordingError::Truncated);
            }
            Err(e) => return Err(e.into()),
        }

        let session = SessionId(u64::from_le_bytes(rest[0..8].try_into().unwrap()));
        let frame_seq = u64::from_le_bytes(rest[8..16].try_into().unwrap());
        let receive_ts_ns = i64::from_le_bytes(rest[16..24].try_into().unwrap());
        let monotonic_ns = u64::from_le_bytes(rest[24..32].try_into().unwrap());
        let direction = Direction::from_u8(rest[32]).ok_or(RecordingError::InvalidHeader)?;
        let opcode = FrameOpcode::from_u8_for_version(rest[33], self.format_version)
            .ok_or(RecordingError::InvalidHeader)?;
        let flags = rest[34];
        let payload_len = u32::from_le_bytes(rest[35..39].try_into().unwrap());
        let payload_crc = u32::from_le_bytes(rest[39..43].try_into().unwrap());
        if rest.len() != RAW_HEADER_BODY_LEN + payload_len as usize {
            return Err(RecordingError::InvalidHeader);
        }
        let payload = rest[RAW_HEADER_BODY_LEN..].to_vec();
        if crc32c(&payload) != payload_crc {
            return Err(RecordingError::CrcMismatch);
        }

        self.records_read += 1;
        Ok(Some(RawRecord {
            header: RawRecordHeader {
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
            },
            payload,
        }))
    }

    pub fn read_all(&mut self) -> Result<Vec<RawRecord>, RecordingError> {
        let mut out = Vec::new();
        loop {
            match self.read_record() {
                Ok(Some(r)) => out.push(r),
                Ok(None) => return Ok(out),
                Err(RecordingError::Truncated) => return Ok(out), // drop incomplete tail
                Err(e) => return Err(e),
            }
        }
    }
}

fn read_u16(r: &mut impl Read) -> Result<u16, RecordingError> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(u16::from_le_bytes(b))
}

fn read_u64(r: &mut impl Read) -> Result<u64, RecordingError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_i64(r: &mut impl Read) -> Result<i64, RecordingError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(i64::from_le_bytes(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FORMAT_VERSION, HEADER_SIZE, RawSegmentWriter};

    fn write_two_records() -> Vec<u8> {
        let mut buf = Vec::new();
        let mut w = RawSegmentWriter::create(&mut buf, 42).unwrap();
        w.write_record(
            SessionId(7),
            1,
            100,
            200,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"SUB BTC-USD",
        )
        .unwrap();
        w.write_record(
            SessionId(7),
            2,
            101,
            201,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"TRADE 1 1.00 1.000 BUY",
        )
        .unwrap();
        buf
    }

    #[test]
    fn write_read_roundtrip() {
        let buf = write_two_records();
        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        let recs = r.read_all().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].payload, b"SUB BTC-USD");
        assert_eq!(recs[1].header.frame_seq, 2);
    }

    /// Crash mid-write: incomplete final record is dropped; prior records remain.
    #[test]
    fn truncated_tail_recovers_complete_prefix() {
        let mut buf = write_two_records();
        // Chop the last record mid-payload (keep segment header + first record intact).
        let keep = HEADER_SIZE
            + 4
            + RAW_HEADER_BODY_LEN
            + b"SUB BTC-USD".len()
            + 4
            + RAW_HEADER_BODY_LEN
            + 4; // partial second payload
        buf.truncate(keep);
        assert!(buf.len() < write_two_records().len());

        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        let recs = r.read_all().unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].payload, b"SUB BTC-USD");
        assert_eq!(r.records_read, 1);
    }

    #[test]
    fn truncated_after_record_len_only_yields_empty() {
        let mut buf = write_two_records();
        // Keep file header + first 2 bytes of record_len — not enough for a full record.
        buf.truncate(HEADER_SIZE + 2);
        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        let recs = r.read_all().unwrap();
        assert!(recs.is_empty());
    }

    #[test]
    fn header_only_segment_is_empty() {
        let mut buf = Vec::new();
        RawSegmentWriter::create(&mut buf, 99).unwrap();
        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        assert!(r.read_all().unwrap().is_empty());
        assert_eq!(r.start_ts_ns, 99);
    }

    #[test]
    fn schema_compat_rejects_bad_magic() {
        let err = RawSegmentReader::from_bytes(b"XXXX\x01\x00".to_vec()).unwrap_err();
        assert_eq!(err, RecordingError::BadMagic);
    }

    #[test]
    fn schema_compat_rejects_unsupported_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        buf.extend_from_slice(&0i64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        let err = RawSegmentReader::from_bytes(buf).unwrap_err();
        assert_eq!(err, RecordingError::UnsupportedVersion(FORMAT_VERSION + 1));
    }

    #[test]
    fn schema_compat_accepts_current_version_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&123i64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        let r = RawSegmentReader::from_bytes(buf).unwrap();
        assert_eq!(r.format_version, FORMAT_VERSION);
        assert_eq!(r.start_ts_ns, 123);
    }

    #[test]
    fn schema_compat_accepts_previous_version_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&(FORMAT_VERSION - 1).to_le_bytes());
        buf.extend_from_slice(&123i64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        let r = RawSegmentReader::from_bytes(buf).unwrap();
        assert_eq!(r.format_version, FORMAT_VERSION - 1);
        assert_eq!(r.start_ts_ns, 123);
    }

    #[test]
    fn schema_compat_accepts_original_v1_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&MIN_SUPPORTED_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&123i64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        let r = RawSegmentReader::from_bytes(buf).unwrap();
        assert_eq!(r.format_version, 1);
        assert_eq!(r.start_ts_ns, 123);
    }

    #[test]
    fn schema_compat_reads_previous_version_records() {
        let mut buf = write_two_records();
        buf[4..6].copy_from_slice(&(FORMAT_VERSION - 1).to_le_bytes());
        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        assert_eq!(r.format_version, FORMAT_VERSION - 1);
        assert_eq!(r.read_all().unwrap().len(), 2);
    }

    #[test]
    fn schema_compat_reads_original_v1_records() {
        let mut buf = write_two_records();
        buf[4..6].copy_from_slice(&MIN_SUPPORTED_FORMAT_VERSION.to_le_bytes());
        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        assert_eq!(r.format_version, 1);
        assert_eq!(r.read_all().unwrap().len(), 2);
    }

    #[test]
    fn crc_mismatch_is_hard_error_not_truncation() {
        let mut buf = write_two_records();
        // Flip one byte in the first payload so CRC fails (complete frame still present).
        let payload_off = HEADER_SIZE + 4 + RAW_HEADER_BODY_LEN;
        buf[payload_off] ^= 0xff;
        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        let err = r.read_all().unwrap_err();
        assert_eq!(err, RecordingError::CrcMismatch);
    }

    #[test]
    fn read_record_reports_truncated_before_read_all_recovers() {
        let mut buf = write_two_records();
        buf.truncate(HEADER_SIZE + 4 + RAW_HEADER_BODY_LEN + b"SUB BTC-USD".len() + 8);
        let mut r = RawSegmentReader::from_bytes(buf).unwrap();
        assert!(r.read_record().unwrap().is_some());
        assert_eq!(r.read_record().unwrap_err(), RecordingError::Truncated);
    }

    #[test]
    fn rejects_oversized_record_before_allocating_payload_buffer() {
        let mut buf = Vec::new();
        RawSegmentWriter::create(&mut buf, 0).unwrap();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());

        let mut reader = RawSegmentReader::from_bytes(buf).unwrap();
        assert_eq!(
            reader.read_record().unwrap_err(),
            RecordingError::RecordTooLarge {
                record_len: u32::MAX,
                max: MAX_RAW_RECORD_LEN,
            }
        );
    }

    /// Lightweight no-panic corpus for CI; full coverage lives in `fuzz/recording_reader`.
    #[test]
    fn recording_reader_fuzz_smoke_no_panic() {
        let seeds: &[&[u8]] = &[b"", b"MFR1", b"XXXX\x01\x00", MAGIC, &[0xff; 64]];
        for s in seeds {
            if let Ok(mut r) = RawSegmentReader::from_bytes(s.to_vec()) {
                let _ = r.read_all();
            }
        }
        let mut state: u64 = 0xC0FF_EE42_u64;
        let mut buf = [0u8; 128];
        for _ in 0..512 {
            for b in &mut buf {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                *b = (state >> 33) as u8;
            }
            let len = (state as usize % buf.len()) + 1;
            if let Ok(mut r) = RawSegmentReader::from_bytes(buf[..len].to_vec()) {
                let _ = r.read_all();
            }
        }
        let good = write_two_records();
        let mut r = RawSegmentReader::from_bytes(good).unwrap();
        assert_eq!(r.read_all().unwrap().len(), 2);
    }
}
