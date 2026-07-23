//! Append-only file sink with a bounded ingress queue.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{OverflowPolicy, SystemEvent};

use crate::memory::MemorySink;
use crate::sink::{EventSink, SinkError};
use crate::wire_json::{batch_json, system_json};

/// Bounded sink that appends accepted items as complete JSON lines.
///
/// # ponytail
/// Sync line write blocks the caller; ceiling = stall under slow disks / NFS.
/// Batch event envelopes use the MFPE-JSON1 field map; system events are typed
/// serde JSON. Upgrade = background writer when synchronous I/O is too costly.
#[derive(Debug)]
pub struct FileSink {
    inner: MemorySink,
    writer: BufWriter<File>,
    path: PathBuf,
    lines_written: u64,
}

impl FileSink {
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
            lines_written: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn lines_written(&self) -> u64 {
        self.lines_written
    }

    pub fn dropped_batches(&self) -> u64 {
        self.inner.dropped_batches()
    }

    pub fn dropped_systems(&self) -> u64 {
        self.inner.dropped_systems()
    }

    fn flush_accepted_batches(&mut self) -> Result<(), SinkError> {
        while let Some(b) = self.inner.pop_batch() {
            let line = batch_json(&b)?;
            self.writer
                .write_all(&line)
                .and_then(|()| self.writer.write_all(b"\n"))
                .map_err(|e| SinkError::Io(e.to_string()))?;
            self.lines_written += 1;
        }
        self.writer
            .flush()
            .map_err(|e| SinkError::Io(e.to_string()))?;
        Ok(())
    }

    fn flush_accepted_systems(&mut self) -> Result<(), SinkError> {
        while let Some(ev) = self.inner.pop_system() {
            let line = system_json(&ev)?;
            self.writer
                .write_all(&line)
                .and_then(|()| self.writer.write_all(b"\n"))
                .map_err(|e| SinkError::Io(e.to_string()))?;
            self.lines_written += 1;
        }
        self.writer
            .flush()
            .map_err(|e| SinkError::Io(e.to_string()))?;
        Ok(())
    }
}

impl EventSink for FileSink {
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
