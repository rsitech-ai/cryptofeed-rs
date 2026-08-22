//! Strict input-only EPIN-JSON1 persistence for deterministic offline replay.

use std::io::{BufRead, Read, Write};

use thiserror::Error;

use crate::{
    window::PROCESSOR_RECORD_CAPACITY,
    wire::{CursorV1, MAX_INPUT_BYTES, MechanicsInputRefV1, MechanicsInputV1, Rfc3339Time},
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReplayInputError {
    #[error("EPIN-JSON1 I/O failed: {0}")]
    Io(String),
    #[error("EPIN-JSON1 input is invalid: {0}")]
    InvalidInput(String),
    #[error("EPIN-JSON1 line exceeds 16 MiB")]
    LineTooLarge,
    #[error("EPIN-JSON1 record is not terminated by a newline")]
    MissingNewline,
    #[error("EPIN-JSON1 input order regressed")]
    OrderViolation,
    #[error("EPIN-JSON1 contains input after the admitted decision bound")]
    FutureInput,
    #[error("EPIN-JSON1 read_all exceeds the bounded processor record capacity")]
    RecordCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayOrderKey {
    available_micros: i64,
    source_id: String,
    epoch: String,
    sequence_start: u64,
    sequence_end: u64,
    payload_hash: String,
}

/// Streaming canonical EPIN-JSON1 writer.
pub struct EpinJson1Writer<W: Write> {
    writer: W,
    last_order: Option<ReplayOrderKey>,
}

impl<W: Write> EpinJson1Writer<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            last_order: None,
        }
    }

    pub fn write_input(&mut self, input: &MechanicsInputV1) -> Result<(), ReplayInputError> {
        input
            .validate_static()
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
        let order = replay_order(input)?;
        if self.last_order.as_ref().is_some_and(|last| order < *last) {
            return Err(ReplayInputError::OrderViolation);
        }
        let bytes = serde_json::to_vec(input)
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(ReplayInputError::LineTooLarge);
        }
        self.writer
            .write_all(&bytes)
            .and_then(|()| self.writer.write_all(b"\n"))
            .map_err(|error| ReplayInputError::Io(error.to_string()))?;
        self.last_order = Some(order);
        Ok(())
    }

    pub fn finish(self) -> W {
        self.writer
    }
}

/// Streaming strict EPIN-JSON1 reader with an immutable future-input bound.
pub struct EpinJson1Reader<R: BufRead> {
    reader: R,
    not_after_micros: i64,
    last_order: Option<ReplayOrderKey>,
}

impl<R: BufRead> EpinJson1Reader<R> {
    pub fn new(reader: R, not_after: Rfc3339Time) -> Self {
        Self {
            reader,
            not_after_micros: not_after.utc_micros(),
            last_order: None,
        }
    }

    pub fn read_input(&mut self) -> Result<Option<MechanicsInputV1>, ReplayInputError> {
        let mut line = Vec::with_capacity(4_096);
        let limit = u64::try_from(MAX_INPUT_BYTES)
            .map_err(|_| ReplayInputError::LineTooLarge)?
            .checked_add(2)
            .ok_or(ReplayInputError::LineTooLarge)?;
        let read = self
            .reader
            .by_ref()
            .take(limit)
            .read_until(b'\n', &mut line)
            .map_err(|error| ReplayInputError::Io(error.to_string()))?;
        if read == 0 {
            return Ok(None);
        }
        if line.last() != Some(&b'\n') {
            return Err(if line.len() > MAX_INPUT_BYTES {
                ReplayInputError::LineTooLarge
            } else {
                ReplayInputError::MissingNewline
            });
        }
        line.pop();
        if line.len() > MAX_INPUT_BYTES {
            return Err(ReplayInputError::LineTooLarge);
        }
        let input = MechanicsInputV1::from_epin_json(&line)
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
        let order = replay_order(&input)?;
        if order.available_micros > self.not_after_micros {
            return Err(ReplayInputError::FutureInput);
        }
        if self.last_order.as_ref().is_some_and(|last| order < *last) {
            return Err(ReplayInputError::OrderViolation);
        }
        self.last_order = Some(order);
        Ok(Some(input))
    }

    pub fn read_all(mut self) -> Result<Vec<MechanicsInputV1>, ReplayInputError> {
        let mut inputs = Vec::with_capacity(PROCESSOR_RECORD_CAPACITY.min(4_096));
        while let Some(input) = self.read_input()? {
            if inputs.len() == PROCESSOR_RECORD_CAPACITY {
                return Err(ReplayInputError::RecordCapacity);
            }
            inputs.push(input);
        }
        Ok(inputs)
    }
}

fn replay_order(input: &MechanicsInputV1) -> Result<ReplayOrderKey, ReplayInputError> {
    let (available_micros, source_id, epoch, cursor) = match input.view() {
        MechanicsInputRefV1::Market {
            envelope,
            action_index,
            catalog,
            ..
        } => {
            let venue = catalog
                .venue_source(envelope.venue.0)
                .ok_or_else(|| ReplayInputError::InvalidInput("venue mapping".into()))?;
            let epoch = catalog
                .connection_epochs()
                .iter()
                .find(|entry| {
                    entry.connection_id() == envelope.connection.0
                        && entry.session_id() == envelope.session.0
                })
                .ok_or_else(|| ReplayInputError::InvalidInput("epoch mapping".into()))?;
            let cursor = match envelope.source_sequence {
                Some(range) => CursorV1::native(range.first, range.last),
                None => CursorV1::derived(
                    envelope.frame_seq,
                    action_index,
                    u32::from(envelope.event_index),
                ),
            }
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
            (
                envelope.receive_ts.0.div_euclid(1_000),
                venue.source_id(),
                epoch.connection_epoch(),
                cursor,
            )
        }
        MechanicsInputRefV1::System {
            system_source,
            available_at,
            system_cursor,
            ..
        } => (
            available_at.utc_micros(),
            system_source.key().source_id(),
            system_source.epoch(),
            system_cursor.clone(),
        ),
        MechanicsInputRefV1::Coverage {
            coverage_source,
            available_at,
            coverage_cursor,
            ..
        } => (
            available_at.utc_micros(),
            coverage_source.key().source_id(),
            coverage_source.epoch(),
            coverage_cursor.cursor().clone(),
        ),
        MechanicsInputRefV1::Clock {
            clock_source,
            available_at,
            clock_cursor,
            ..
        } => (
            available_at.utc_micros(),
            clock_source.key().source_id(),
            clock_source.epoch(),
            clock_cursor.cursor().clone(),
        ),
    };
    let sequence = cursor
        .display_sequence()
        .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
    let (sequence_start, sequence_end) = cursor.native_range().unwrap_or((sequence, sequence));
    Ok(ReplayOrderKey {
        available_micros,
        source_id: source_id.to_owned(),
        epoch: epoch.to_owned(),
        sequence_start,
        sequence_end,
        payload_hash: input.payload_hash().to_owned(),
    })
}
