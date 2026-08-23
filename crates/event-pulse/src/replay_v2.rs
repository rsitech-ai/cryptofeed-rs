//! Strict canonical JSONL replay for MechanicsInput V2.

use std::io::{BufRead, Read, Write};

use crate::{
    replay::ReplayInputError,
    window::PROCESSOR_RECORD_CAPACITY,
    wire::{CursorV1, MAX_INPUT_BYTES, MechanicsInputRefV1, Rfc3339Time},
    wire_v2::{MechanicsInputRefV2, MechanicsInputV2},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReplayOrderKeyV2 {
    available_micros: i64,
    source_id: String,
    epoch: String,
    cursor: ReplayCursorOrderV2,
    payload_hash: String,
}

impl ReplayOrderKeyV2 {
    pub(crate) fn available_micros(&self) -> i64 {
        self.available_micros
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ReplayCursorOrderV2 {
    Market {
        raw_frame_seq: u64,
        action_index: u32,
        item_index: u32,
    },
    V1(CursorV1),
}

pub struct MechanicsInputV2JsonlWriter<W: Write> {
    writer: W,
    last_order: Option<ReplayOrderKeyV2>,
}

impl<W: Write> MechanicsInputV2JsonlWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            last_order: None,
        }
    }

    pub fn write_input(&mut self, input: &MechanicsInputV2) -> Result<(), ReplayInputError> {
        input
            .validate_static()
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
        let order = replay_order_v2(input)?;
        if self.last_order.as_ref().is_some_and(|last| order < *last) {
            return Err(ReplayInputError::OrderViolation);
        }
        let bytes = serde_json::to_vec(input)
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(ReplayInputError::LineTooLarge);
        }
        let decoded = MechanicsInputV2::from_json_line(&bytes)
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
        if decoded != *input {
            return Err(ReplayInputError::InvalidInput(
                "serialized V2 input differs after strict readback".to_owned(),
            ));
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

pub struct MechanicsInputV2JsonlReader<R: BufRead> {
    reader: R,
    not_after_micros: i64,
    last_order: Option<ReplayOrderKeyV2>,
}

impl<R: BufRead> MechanicsInputV2JsonlReader<R> {
    pub fn new(reader: R, not_after: Rfc3339Time) -> Self {
        Self {
            reader,
            not_after_micros: not_after.utc_micros(),
            last_order: None,
        }
    }

    pub fn read_input(&mut self) -> Result<Option<MechanicsInputV2>, ReplayInputError> {
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
        let input = MechanicsInputV2::from_json_line(&line)
            .map_err(|error| ReplayInputError::InvalidInput(error.to_string()))?;
        let order = replay_order_v2(&input)?;
        if order.available_micros > self.not_after_micros {
            return Err(ReplayInputError::FutureInput);
        }
        if self.last_order.as_ref().is_some_and(|last| order < *last) {
            return Err(ReplayInputError::OrderViolation);
        }
        self.last_order = Some(order);
        Ok(Some(input))
    }

    pub fn read_all(mut self) -> Result<Vec<MechanicsInputV2>, ReplayInputError> {
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

pub(crate) fn replay_order_v2(
    input: &MechanicsInputV2,
) -> Result<ReplayOrderKeyV2, ReplayInputError> {
    let invalid = || ReplayInputError::InvalidInput("invalid V2 replay coordinate".to_owned());
    let (available_micros, source_id, epoch, cursor) = match input.view() {
        MechanicsInputRefV2::Market {
            envelope,
            catalog,
            action_index,
            ..
        } => {
            let venue = catalog.venue_source(envelope.venue.0).ok_or_else(invalid)?;
            let epoch = catalog
                .connection_epochs()
                .iter()
                .find(|entry| {
                    entry.connection_id() == envelope.connection.0
                        && entry.session_id() == envelope.session.0
                })
                .ok_or_else(invalid)?;
            (
                envelope.receive_ts.0.div_euclid(1_000),
                venue.source_id(),
                epoch.connection_epoch(),
                ReplayCursorOrderV2::Market {
                    raw_frame_seq: envelope.frame_seq,
                    action_index,
                    item_index: u32::from(envelope.event_index),
                },
            )
        }
        MechanicsInputRefV2::NonMarket(view) => match view {
            MechanicsInputRefV1::System {
                system_source,
                available_at,
                system_cursor,
                ..
            } => (
                available_at.utc_micros(),
                system_source.key().source_id(),
                system_source.epoch(),
                ReplayCursorOrderV2::V1(system_cursor.clone()),
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
                ReplayCursorOrderV2::V1(coverage_cursor.cursor().clone()),
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
                ReplayCursorOrderV2::V1(clock_cursor.cursor().clone()),
            ),
            MechanicsInputRefV1::Market { .. } => return Err(invalid()),
        },
    };
    Ok(ReplayOrderKeyV2 {
        available_micros,
        source_id: source_id.to_owned(),
        epoch: epoch.to_owned(),
        cursor,
        payload_hash: input.payload_hash().to_owned(),
    })
}
