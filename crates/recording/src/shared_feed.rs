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
        let id = format!(
            "{:032x}{:08x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(err)?
                .as_nanos(),
            std::process::id()
        );
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
        };
        writer.publish()?;
        if let Some(parent) = writer
            .directory
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            File::open(parent)?.sync_all()?;
        }
        Ok(writer)
    }
    pub fn cursor(&self) -> &FeedCursor {
        &self.cursor
    }
    /// Acceptance means both data and the new commit watermark were synced.
    /// Any I/O failure poisons the writer; later appends cannot bless a torn tail.
    pub fn append(&mut self, record: FeedRecord) -> Result<FeedCursor, RecordingError> {
        if self.poisoned || self.sealed {
            return Err(err("shared feed writer poisoned or sealed"));
        }
        let sealing = matches!(
            record,
            FeedRecord::Seal {
                schema_version: 1,
                ..
            }
        );
        if matches!(record, FeedRecord::Seal { schema_version, .. } if schema_version != 1) {
            return Err(err("unsupported feed seal"));
        }
        let sequence = self
            .cursor
            .sequence
            .checked_add(1)
            .ok_or_else(|| err("feed sequence overflow"))?;
        let bytes = serde_json::to_vec(&WireRecord { sequence, record }).map_err(err)?;
        if bytes.len() > MAX_RECORD {
            return Err(err("shared feed record too large"));
        }
        let end = self
            .cursor
            .offset
            .checked_add(bytes.len() as u64 + 8)
            .ok_or_else(|| err("feed byte overflow"))?;
        if end > self.max_bytes {
            self.poisoned = true;
            return Err(err("shared feed byte budget exhausted"));
        }
        let previous = self.cursor.clone();
        let result = (|| -> Result<(), RecordingError> {
            self.file.write_all(&(bytes.len() as u32).to_le_bytes())?;
            self.file.write_all(&crc32c(&bytes).to_le_bytes())?;
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
        File::open(&self.directory)?.sync_all()?;
        Ok(())
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
}
#[derive(Debug)]
pub struct FeedReader {
    directory: PathBuf,
    file: File,
    cursor: FeedCursor,
    seen_commit: FeedCursor,
    sealed: bool,
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
