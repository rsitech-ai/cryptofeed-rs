//! MFSF1 local authoritative feed. Readers see only the atomically published,
//! synced prefix. Payloads preserve original bytes; cursors bind to a feed ID.
//! A fresh directory is required for each writer lifetime (no silent append or
//! takeover). Slow consumers replay from disk rather than a lossy UI ring.
use crate::{MetadataRecord, RecordingError, crc32c};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use marketfeed_adapter_api::EventBatch;
use marketfeed_model::{SessionId, SystemEvent};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
const MAGIC: &[u8; 8] = b"MFSF1\0\0\0";
const HEADER_LEN: u64 = 48;
const MAX_RECORD: usize = 32 * 1024 * 1024;
const MAX_TRANSACTION: usize = 64 * 1024 * 1024;
fn err(s: impl ToString) -> RecordingError {
    RecordingError::Io(s.to_string())
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FeedCursor {
    pub feed_id: String,
    pub sequence: u64,
    pub offset: u64,
}
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum FeedRecord {
    /// Public reproduction inputs, embedded once and content hashed by readers.
    Artifact {
        role: String,
        payload_base64: String,
    },
    Seal {
        schema_version: u16,
        now_ns: i64,
        reason: String,
    },
    Metadata(MetadataRecord),
    /// Catalog HTTP bytes are evidence, including native token IDs and indices
    /// that a generic InstrumentDefinition alone cannot express.
    CatalogResponse {
        venue: u16,
        request_id: u64,
        request_body: String,
        status: u16,
        body_base64: String,
        provenance: Option<CatalogHttpProvenance>,
    },
    Frame {
        session: SessionId,
        frame_seq: u64,
        receive_ts_ns: i64,
        monotonic_ns: u64,
        direction: u8,
        opcode: u8,
        payload_base64: String,
    },
    FrameAccepted {
        session: SessionId,
        frame_seq: u64,
    },
    Timer {
        session: SessionId,
        timer_id: u64,
        now_ns: i64,
    },
    Batch(EventBatch),
    Connection {
        session: SessionId,
        connection: marketfeed_model::ConnectionId,
        now_ns: i64,
        connected: bool,
        reason: Option<String>,
    },
    Lifecycle {
        session: SessionId,
        now_ns: i64,
        state: String,
    },
    System {
        session: SessionId,
        event: SystemEvent,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogHttpProvenance {
    pub url: String,
    pub method: String,
    pub request_body_base64: String,
    pub request_headers: Vec<(String, String)>,
    pub response_headers: Vec<(String, String)>,
    pub received_at_ns: i64,
}
// Preserve raw variant content until its schema is known. Serde's generic
// buffered enum content can route large integers through f64 without this
// boundary. Do not enable a workspace-wide JSON numeric feature to fix it.
impl<'de> Deserialize<'de> for FeedRecord {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            kind: String,
            data: Box<serde_json::value::RawValue>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let data = raw.data.get();
        macro_rules! fields {
            ($variant:ident {$($name:ident:$ty:ty),* $(,)?}) => {{
                #[derive(Deserialize)] #[serde(deny_unknown_fields)] struct Fields {$($name:$ty),*}
                let Fields {$($name),*}=serde_json::from_str::<Fields>(data).map_err(serde::de::Error::custom)?;
                Self::$variant {$($name),*}
            }};
        }
        Ok(match raw.kind.as_str() {
            "artifact" => fields!(Artifact {
                role: String,
                payload_base64: String
            }),
            "metadata" => {
                Self::Metadata(serde_json::from_str(data).map_err(serde::de::Error::custom)?)
            }
            "batch" => Self::Batch(serde_json::from_str(data).map_err(serde::de::Error::custom)?),
            "seal" => fields!(Seal {
                schema_version: u16,
                now_ns: i64,
                reason: String
            }),
            "catalog_response" => fields!(CatalogResponse {
                venue: u16,
                request_id: u64,
                request_body: String,
                status: u16,
                body_base64: String
                ,provenance: Option<CatalogHttpProvenance>
            }),
            "frame" => fields!(Frame {
                session: SessionId,
                frame_seq: u64,
                receive_ts_ns: i64,
                monotonic_ns: u64,
                direction: u8,
                opcode: u8,
                payload_base64: String
            }),
            "frame_accepted" => fields!(FrameAccepted {
                session: SessionId,
                frame_seq: u64
            }),
            "timer" => fields!(Timer {
                session: SessionId,
                timer_id: u64,
                now_ns: i64
            }),
            "connection" => {
                fields!(Connection {session:SessionId,connection:marketfeed_model::ConnectionId,now_ns:i64,connected:bool,reason:Option<String>})
            }
            "lifecycle" => fields!(Lifecycle {
                session: SessionId,
                now_ns: i64,
                state: String
            }),
            "system" => fields!(System {
                session: SessionId,
                event: SystemEvent
            }),
            _ => {
                return Err(serde::de::Error::custom(
                    "unsupported shared feed record kind",
                ));
            }
        })
    }
}

impl FeedRecord {
    pub fn artifact(role: &str, bytes: &[u8]) -> Self {
        Self::Artifact {
            role: role.into(),
            payload_base64: STANDARD.encode(bytes),
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub fn frame(
        session: SessionId,
        frame_seq: u64,
        receive_ts_ns: i64,
        monotonic_ns: u64,
        direction: u8,
        opcode: u8,
        payload: &[u8],
    ) -> Self {
        Self::Frame {
            session,
            frame_seq,
            receive_ts_ns,
            monotonic_ns,
            direction,
            opcode,
            payload_base64: STANDARD.encode(payload),
        }
    }
    pub fn payload_bytes(&self) -> Result<Option<Vec<u8>>, RecordingError> {
        match self {
            Self::Artifact { payload_base64, .. } => {
                STANDARD.decode(payload_base64).map(Some).map_err(err)
            }
            Self::Frame { payload_base64, .. } => {
                STANDARD.decode(payload_base64).map(Some).map_err(err)
            }
            Self::CatalogResponse { body_base64, .. } => {
                STANDARD.decode(body_base64).map(Some).map_err(err)
            }
            _ => Ok(None),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Watermark {
    schema_version: u16,
    cursor: FeedCursor,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    sequence: u64,
    record: FeedRecord,
}
#[derive(Debug)]
pub struct FeedWriter {
    directory: PathBuf,
    file: File,
    cursor: FeedCursor,
    max_bytes: u64,
    poisoned: bool,
    sealed: bool,
    #[cfg(test)]
    fail_publication_barrier: bool,
}
impl FeedWriter {
    /// Refuses an existing directory, preventing overwrite and concurrent owners.
    /// max_bytes includes headers and framing and must be explicitly bounded.
    pub fn create(directory: impl AsRef<Path>, max_bytes: u64) -> Result<Self, RecordingError> {
        if max_bytes < HEADER_LEN {
            return Err(err("shared feed byte budget too small"));
        }
        let directory = directory.as_ref().to_path_buf();
        if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir(&directory)?;
        // Globally scoped across hosts and concurrent collectors. Timestamp
        // plus PID can collide across machines or coarse-clock threads.
        let mut random = [0u8; 20];
        getrandom::fill(&mut random).map_err(err)?;
        let id: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("events.mfsf"))?;
        file.write_all(MAGIC)?;
        file.write_all(id.as_bytes())?;
        file.sync_all()?;
        let mut writer = Self {
            directory,
            file,
            cursor: FeedCursor {
                feed_id: id,
                sequence: 0,
                offset: HEADER_LEN,
            },
            max_bytes,
            poisoned: false,
            sealed: false,
            #[cfg(test)]
            fail_publication_barrier: false,
        };
        writer.publish()?;
        crate::durability::sync_directory_ancestry(&writer.directory)
            .map_err(|e| RecordingError::PublicationIndeterminate(e.to_string()))?;
        Ok(writer)
    }
    /// Last successfully acknowledged durable cursor. After an indeterminate
    /// publication, readers may see a complete later transaction. The poisoned
    /// writer cannot acknowledge or append anything further.
    pub fn cursor(&self) -> &FeedCursor {
        &self.cursor
    }
    /// Acceptance means both data and the new commit watermark were synced.
    /// Any I/O failure poisons the writer; later appends cannot bless a torn tail.
    pub fn append(&mut self, record: FeedRecord) -> Result<FeedCursor, RecordingError> {
        self.append_many(vec![record])
    }
    /// Publish all records under one durable watermark. Failure before watermark
    /// replacement leaves an uncommitted tail. Failure of the subsequent
    /// directory barrier is indeterminate: readers may see the complete synced
    /// transaction, but no durability acknowledgment is issued. Both failures
    /// poison the writer permanently; neither exposes a partial transaction.
    pub fn append_many(&mut self, records: Vec<FeedRecord>) -> Result<FeedCursor, RecordingError> {
        if self.poisoned || self.sealed {
            return Err(err("shared feed writer poisoned or sealed"));
        }
        let count = records.len();
        if count == 0 || count > 65536 {
            return Err(err("invalid shared feed transaction count"));
        }
        let mut bytes = Vec::new();
        let mut sequence = self.cursor.sequence;
        let mut sealing = false;
        for (index, record) in records.into_iter().enumerate() {
            if let FeedRecord::Seal { schema_version, .. } = &record {
                if *schema_version != 1 || index + 1 != count {
                    return Err(err("unsupported or non-final feed seal"));
                }
                sealing = true;
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| err("feed sequence overflow"))?;
            let body = serde_json::to_vec(&WireRecord { sequence, record }).map_err(err)?;
            if body.len() > MAX_RECORD
                || bytes.len().saturating_add(body.len() + 8) > MAX_TRANSACTION
            {
                return Err(err("shared feed record or transaction too large"));
            }
            bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&crc32c(&body).to_le_bytes());
            bytes.extend_from_slice(&body);
        }
        let end = self
            .cursor
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| err("feed byte overflow"))?;
        if end > self.max_bytes {
            self.poisoned = true;
            return Err(err("shared feed byte budget exhausted"));
        }
        let previous = self.cursor.clone();
        let result = (|| -> Result<(), RecordingError> {
            self.file.write_all(&bytes)?;
            self.file.sync_data()?;
            self.cursor.sequence = sequence;
            self.cursor.offset = end;
            self.publish()?;
            Ok(())
        })();
        if let Err(e) = result {
            self.cursor = previous;
            self.poisoned = true;
            return Err(e);
        }
        self.sealed = sealing;
        Ok(self.cursor.clone())
    }
    fn publish(&mut self) -> Result<(), RecordingError> {
        let bytes = serde_json::to_vec(&Watermark {
            schema_version: 1,
            cursor: self.cursor.clone(),
        })
        .map_err(err)?;
        let temp = self.directory.join("committed.tmp");
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        drop(f);
        fs::rename(temp, self.directory.join("committed.json"))?;
        self.sync_publication_directory()
            .map_err(|e| RecordingError::PublicationIndeterminate(e.to_string()))?;
        Ok(())
    }
    fn sync_publication_directory(&self) -> std::io::Result<()> {
        #[cfg(test)]
        if self.fail_publication_barrier {
            return Err(std::io::Error::other(
                "injected publication barrier failure",
            ));
        }
        crate::durability::sync_directory(&self.directory)
    }
}
#[derive(Debug, Clone)]
pub struct FeedHandle(Arc<Mutex<FeedWriter>>);
impl FeedHandle {
    pub fn new(writer: FeedWriter) -> Self {
        Self(Arc::new(Mutex::new(writer)))
    }
    pub fn append(&self, record: FeedRecord) -> Result<FeedCursor, RecordingError> {
        self.0
            .lock()
            .map_err(|_| err("shared feed lock poisoned"))?
            .append(record)
    }
    pub fn append_many(&self, records: Vec<FeedRecord>) -> Result<FeedCursor, RecordingError> {
        self.0
            .lock()
            .map_err(|_| err("shared feed lock poisoned"))?
            .append_many(records)
    }
}
#[derive(Debug)]
pub struct FeedReader {
    directory: PathBuf,
    file: File,
    cursor: FeedCursor,
    seen_commit: FeedCursor,
    sealed: bool,
}
/// Read an immutable, completely sealed journal against its pinned checkpoint.
/// Unlike the live reader this never consults a mutable filesystem watermark.
pub struct SealedFeedReader<'a> {
    bytes: &'a [u8],
    checkpoint: FeedCursor,
    cursor: FeedCursor,
    sealed: bool,
}
impl<'a> SealedFeedReader<'a> {
    pub fn new(bytes: &'a [u8], checkpoint: FeedCursor) -> Result<Self, RecordingError> {
        if bytes.len() < HEADER_LEN as usize
            || &bytes[..8] != MAGIC
            || bytes[8..48] != *checkpoint.feed_id.as_bytes()
            || checkpoint.offset != bytes.len() as u64
        {
            return Err(err("invalid sealed feed identity or length"));
        }
        Ok(Self {
            bytes,
            cursor: FeedCursor {
                feed_id: checkpoint.feed_id.clone(),
                sequence: 0,
                offset: HEADER_LEN,
            },
            checkpoint,
            sealed: false,
        })
    }
    pub fn read_next(&mut self) -> Result<Option<FeedRecord>, RecordingError> {
        if self.cursor.offset == self.checkpoint.offset {
            if !self.sealed || self.cursor != self.checkpoint {
                return Err(err("incomplete sealed journal"));
            }
            return Ok(None);
        }
        if self.sealed {
            return Err(err("journal grew after seal"));
        }
        let start = usize::try_from(self.cursor.offset).map_err(err)?;
        let header = self
            .bytes
            .get(start..start + 8)
            .ok_or_else(|| err("truncated journal header"))?;
        let length = u32::from_le_bytes(header[..4].try_into().expect("length")) as usize;
        if length > MAX_RECORD {
            return Err(err("journal record byte budget"));
        }
        let end = start
            .checked_add(8 + length)
            .ok_or_else(|| err("journal offset overflow"))?;
        let payload = self
            .bytes
            .get(start + 8..end)
            .ok_or_else(|| err("truncated journal record"))?;
        if crc32c(payload) != u32::from_le_bytes(header[4..].try_into().expect("CRC")) {
            return Err(err("journal CRC mismatch"));
        }
        let wire: WireRecord = serde_json::from_slice(payload).map_err(err)?;
        if wire.sequence != self.cursor.sequence + 1 || wire.sequence > self.checkpoint.sequence {
            return Err(err("journal sequence gap"));
        }
        if let FeedRecord::Seal { schema_version, .. } = &wire.record {
            if *schema_version != 1
                || end as u64 != self.checkpoint.offset
                || wire.sequence != self.checkpoint.sequence
            {
                return Err(err("invalid journal seal"));
            }
            self.sealed = true;
        }
        self.cursor.sequence = wire.sequence;
        self.cursor.offset = end as u64;
        Ok(Some(wire.record))
    }
}
impl FeedReader {
    pub fn open(
        directory: impl AsRef<Path>,
        cursor: Option<FeedCursor>,
    ) -> Result<Self, RecordingError> {
        let directory = directory.as_ref().to_path_buf();
        let mut file = File::open(directory.join("events.mfsf"))?;
        let mut header = [0; 48];
        file.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            return Err(err("unsupported shared feed header"));
        }
        let id = String::from_utf8(header[8..].to_vec()).map_err(err)?;
        let watermark = Self::watermark(&directory)?;
        if watermark.feed_id != id {
            return Err(err("shared feed ID mismatch"));
        }
        let target = cursor.unwrap_or_else(|| FeedCursor {
            feed_id: id.clone(),
            sequence: 0,
            offset: HEADER_LEN,
        });
        if target.feed_id != id
            || target.offset < HEADER_LEN
            || target.offset > watermark.offset
            || target.sequence > watermark.sequence
        {
            return Err(err("invalid shared feed cursor"));
        }
        // Validate the requested boundary and prefix CRCs before trusting a
        // persisted cursor. A fabricated offset cannot silently skip corruption.
        let mut reader = Self {
            directory,
            file,
            cursor: FeedCursor {
                feed_id: id,
                sequence: 0,
                offset: HEADER_LEN,
            },
            seen_commit: watermark,
            sealed: false,
        };
        while reader.cursor.sequence < target.sequence {
            reader
                .read_next()?
                .ok_or_else(|| err("cursor exceeds committed records"))?;
        }
        if reader.cursor != target {
            return Err(err("cursor not on a committed record boundary"));
        }
        Ok(reader)
    }
    pub fn cursor(&self) -> &FeedCursor {
        &self.cursor
    }
    fn watermark(directory: &Path) -> Result<FeedCursor, RecordingError> {
        let f = File::open(directory.join("committed.json"))?;
        let w: Watermark = serde_json::from_reader(f.take(4097)).map_err(err)?;
        if w.schema_version != 1 || w.cursor.offset < HEADER_LEN {
            return Err(err("invalid shared feed watermark"));
        }
        Ok(w.cursor)
    }
    /// None means caught up to the durable prefix, not end of live collection.
    pub fn read_next(&mut self) -> Result<Option<FeedRecord>, RecordingError> {
        let w = Self::watermark(&self.directory)?;
        if w.feed_id != self.cursor.feed_id
            || w.offset < self.seen_commit.offset
            || w.sequence < self.seen_commit.sequence
        {
            return Err(err("shared feed commit rewound or changed identity"));
        }
        if self.sealed && w != self.seen_commit {
            return Err(err("sealed shared feed grew"));
        }
        self.seen_commit = w;
        if self.cursor.offset == self.seen_commit.offset {
            if self.cursor.sequence != self.seen_commit.sequence {
                return Err(err("inconsistent shared feed watermark"));
            }
            return Ok(None);
        }
        self.file.seek(SeekFrom::Start(self.cursor.offset))?;
        let mut header = [0; 8];
        self.file.read_exact(&mut header)?;
        let len = u32::from_le_bytes(header[..4].try_into().expect("fixed header")) as usize;
        let end = self
            .cursor
            .offset
            .checked_add(len as u64 + 8)
            .ok_or_else(|| err("feed record overflow"))?;
        if len > MAX_RECORD || end > self.seen_commit.offset {
            return Err(err("record exceeds committed shared feed prefix"));
        }
        let mut bytes = vec![0; len];
        self.file.read_exact(&mut bytes)?;
        if crc32c(&bytes) != u32::from_le_bytes(header[4..].try_into().expect("fixed CRC")) {
            return Err(err("shared feed CRC mismatch"));
        }
        let wire: WireRecord = serde_json::from_slice(&bytes).map_err(err)?;
        if wire.sequence != self.cursor.sequence + 1 || wire.sequence > self.seen_commit.sequence {
            return Err(err("shared feed sequence gap"));
        }
        if let FeedRecord::Seal { schema_version, .. } = &wire.record {
            if *schema_version != 1
                || end != self.seen_commit.offset
                || wire.sequence != self.seen_commit.sequence
            {
                return Err(err("invalid or non-final shared feed seal"));
            }
            self.sealed = true;
        }
        self.cursor.sequence = wire.sequence;
        self.cursor.offset = end;
        Ok(Some(wire.record))
    }
}

#[cfg(test)]
mod publication_failure_tests {
    use super::*;

    fn state(name: &str) -> FeedRecord {
        FeedRecord::Lifecycle {
            session: marketfeed_model::SessionId(7),
            now_ns: 1,
            state: name.into(),
        }
    }

    #[test]
    fn post_replace_barrier_failure_is_indeterminate_and_permanently_poisoned() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("feed");
        let mut writer = FeedWriter::create(&directory, 1_000_000).unwrap();
        let mut existing_reader = FeedReader::open(&directory, None).unwrap();
        let acknowledged = writer.append(state("acknowledged")).unwrap();
        assert!(existing_reader.read_next().unwrap().is_some());
        writer.fail_publication_barrier = true;
        let failure = writer
            .append_many(vec![state("first"), state("second")])
            .unwrap_err();
        assert!(matches!(
            failure,
            RecordingError::PublicationIndeterminate(_)
        ));
        assert_eq!(writer.cursor(), &acknowledged);

        // Rename has happened and data was synced. Existing and new readers can
        // observe the entire transaction despite the missing durability ACK.
        for mut reader in [
            existing_reader,
            FeedReader::open(&directory, Some(acknowledged.clone())).unwrap(),
        ] {
            assert!(
                matches!(reader.read_next().unwrap(), Some(FeedRecord::Lifecycle { state, .. }) if state == "first")
            );
            assert!(
                matches!(reader.read_next().unwrap(), Some(FeedRecord::Lifecycle { state, .. }) if state == "second")
            );
            assert!(reader.read_next().unwrap().is_none());
            assert_eq!(reader.cursor().sequence, acknowledged.sequence + 2);
        }
        let data = fs::read(directory.join("events.mfsf")).unwrap();
        let watermark = fs::read(directory.join("committed.json")).unwrap();
        writer.fail_publication_barrier = false;
        assert!(writer.append(state("must-not-bless-tail")).is_err());
        assert_eq!(writer.cursor(), &acknowledged);
        assert_eq!(fs::read(directory.join("events.mfsf")).unwrap(), data);
        assert_eq!(
            fs::read(directory.join("committed.json")).unwrap(),
            watermark
        );
    }
}
