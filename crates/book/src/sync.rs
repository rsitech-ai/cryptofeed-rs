//! Book synchronization lifecycle.

use marketfeed_model::InstrumentId;

use crate::{BookError, BookValidity, OrderBook};

/// Sync state machine for one instrument's L2 book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncState {
    Idle,
    BufferingDeltas,
    SnapshotRequested,
    SnapshotReceived,
    ApplyingBufferedDeltas,
    Live,
    GapDetected,
    Invalid,
    Resynchronizing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncLimits {
    pub max_buffered_messages: usize,
    pub max_buffered_bytes: usize,
    pub max_buffered_span_ns: u64,
}

impl Default for SyncLimits {
    fn default() -> Self {
        Self {
            max_buffered_messages: 10_000,
            max_buffered_bytes: 4 * 1024 * 1024,
            max_buffered_span_ns: 5_000_000_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BufferedDelta {
    pub sequence: u64,
    pub bytes_len: usize,
    pub received_mono_ns: u64,
    pub side: marketfeed_model::BookSide,
    pub operation: marketfeed_model::BookOperation,
    pub price: marketfeed_model::Price,
    pub quantity: Option<marketfeed_model::Quantity>,
}

/// Owns sync state + book for one instrument.
#[derive(Debug)]
pub struct BookSynchronizer {
    pub instrument: InstrumentId,
    pub state: SyncState,
    pub book: OrderBook,
    pub limits: SyncLimits,
    buffer: Vec<BufferedDelta>,
    buffered_bytes: usize,
    buffered_mono_min: Option<u64>,
    buffered_mono_max: Option<u64>,
    pub expected_sequence: Option<u64>,
}

impl BookSynchronizer {
    pub fn new(instrument: InstrumentId, book: OrderBook, limits: SyncLimits) -> Self {
        Self {
            instrument,
            state: SyncState::Idle,
            book,
            limits,
            buffer: Vec::new(),
            buffered_bytes: 0,
            buffered_mono_min: None,
            buffered_mono_max: None,
            expected_sequence: None,
        }
    }

    pub fn begin_resync(&mut self) {
        self.state = SyncState::Resynchronizing;
        self.book.set_validity(BookValidity::Synchronizing);
        self.book.clear();
        self.buffer.clear();
        self.buffered_bytes = 0;
        self.buffered_mono_min = None;
        self.buffered_mono_max = None;
        self.expected_sequence = None;
    }

    pub fn invalidate(&mut self, _reason: &str) {
        self.state = SyncState::Invalid;
        self.book.set_validity(BookValidity::Invalid);
        self.buffer.clear();
        self.buffered_bytes = 0;
        self.buffered_mono_min = None;
        self.buffered_mono_max = None;
    }

    pub fn request_snapshot(&mut self) {
        self.state = SyncState::SnapshotRequested;
        self.book.set_validity(BookValidity::Synchronizing);
    }

    pub fn note_gap(&mut self) {
        self.state = SyncState::GapDetected;
        self.book.set_validity(BookValidity::Invalid);
        self.buffer.clear();
        self.buffered_bytes = 0;
        self.buffered_mono_min = None;
        self.buffered_mono_max = None;
    }

    /// Buffer a delta that arrived before snapshot application.
    pub fn buffer_delta(&mut self, delta: BufferedDelta) -> Result<(), BookError> {
        let mono_min = self
            .buffered_mono_min
            .map_or(delta.received_mono_ns, |value| {
                value.min(delta.received_mono_ns)
            });
        let mono_max = self
            .buffered_mono_max
            .map_or(delta.received_mono_ns, |value| {
                value.max(delta.received_mono_ns)
            });
        if self.buffer.len() >= self.limits.max_buffered_messages
            || self.buffered_bytes + delta.bytes_len > self.limits.max_buffered_bytes
            || mono_max.saturating_sub(mono_min) > self.limits.max_buffered_span_ns
        {
            self.invalidate("delta buffer overflow");
            return Err(BookError::NotValid(BookValidity::Invalid));
        }
        self.buffered_bytes += delta.bytes_len;
        self.buffered_mono_min = Some(mono_min);
        self.buffered_mono_max = Some(mono_max);
        self.buffer.push(delta);
        if self.state == SyncState::Idle || self.state == SyncState::SnapshotRequested {
            self.state = SyncState::BufferingDeltas;
        }
        Ok(())
    }

    pub fn apply_snapshot_and_drain(
        &mut self,
        bids: &[(marketfeed_model::Price, marketfeed_model::Quantity)],
        asks: &[(marketfeed_model::Price, marketfeed_model::Quantity)],
        sequence: u64,
    ) -> Result<(), BookError> {
        self.state = SyncState::SnapshotReceived;
        self.book.apply_snapshot(bids, asks, Some(sequence))?;
        self.state = SyncState::ApplyingBufferedDeltas;
        // Apply buffered deltas with sequence > snapshot sequence.
        let pending: Vec<_> = self
            .buffer
            .drain(..)
            .filter(|d| d.sequence > sequence)
            .collect();
        self.buffered_bytes = 0;
        self.buffered_mono_min = None;
        self.buffered_mono_max = None;
        let mut last = sequence;
        for d in pending {
            if d.sequence <= last {
                continue; // duplicate / already covered
            }
            if d.sequence != last + 1 {
                self.note_gap();
                return Err(BookError::NotValid(BookValidity::Invalid));
            }
            self.book
                .apply_change(d.side, d.operation, d.price, d.quantity)?;
            last = d.sequence;
        }
        self.expected_sequence = Some(last + 1);
        self.book.set_sequence(last);
        self.book.set_validity(BookValidity::Valid);
        self.state = SyncState::Live;
        Ok(())
    }

    pub fn on_live_delta(
        &mut self,
        sequence: u64,
        side: marketfeed_model::BookSide,
        operation: marketfeed_model::BookOperation,
        price: marketfeed_model::Price,
        quantity: Option<marketfeed_model::Quantity>,
    ) -> Result<(), BookError> {
        if self.state != SyncState::Live {
            return Err(BookError::NotValid(self.book.validity()));
        }
        let expected = self.expected_sequence.unwrap_or(sequence);
        if sequence < expected {
            return Ok(()); // duplicate
        }
        if sequence > expected {
            self.note_gap();
            return Err(BookError::NotValid(BookValidity::Invalid));
        }
        self.book.apply_change(side, operation, price, quantity)?;
        self.expected_sequence = Some(sequence + 1);
        self.book.set_sequence(sequence);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OrderBook;
    use marketfeed_model::{BookOperation, BookSide, Fixed, InstrumentId, Price, Quantity};

    fn px(c: i128) -> Price {
        Price(Fixed::new(c, 2))
    }
    fn qty(c: i128) -> Quantity {
        Quantity(Fixed::new(c, 2))
    }

    #[test]
    fn gap_invalidates_and_overflow_invalidates() {
        let mut sync = BookSynchronizer::new(
            InstrumentId(1),
            OrderBook::new(2, 2, None),
            SyncLimits {
                max_buffered_messages: 2,
                max_buffered_bytes: 10_000,
                max_buffered_span_ns: 100,
            },
        );
        sync.request_snapshot();
        sync.buffer_delta(BufferedDelta {
            sequence: 2,
            bytes_len: 8,
            received_mono_ns: 1,
            side: BookSide::Bid,
            operation: BookOperation::Upsert,
            price: px(100_00),
            quantity: Some(qty(1_00)),
        })
        .unwrap();
        sync.buffer_delta(BufferedDelta {
            sequence: 3,
            bytes_len: 8,
            received_mono_ns: 2,
            side: BookSide::Bid,
            operation: BookOperation::Upsert,
            price: px(99_00),
            quantity: Some(qty(1_00)),
        })
        .unwrap();
        let err = sync
            .buffer_delta(BufferedDelta {
                sequence: 4,
                bytes_len: 8,
                received_mono_ns: 3,
                side: BookSide::Ask,
                operation: BookOperation::Upsert,
                price: px(101_00),
                quantity: Some(qty(1_00)),
            })
            .unwrap_err();
        assert!(matches!(err, BookError::NotValid(BookValidity::Invalid)));
        assert_eq!(sync.state, SyncState::Invalid);
    }

    #[test]
    fn delta_buffer_time_span_overflow_invalidates() {
        let mut sync = BookSynchronizer::new(
            InstrumentId(1),
            OrderBook::new(2, 2, None),
            SyncLimits {
                max_buffered_messages: 10,
                max_buffered_bytes: 10_000,
                max_buffered_span_ns: 100,
            },
        );
        sync.request_snapshot();
        sync.buffer_delta(BufferedDelta {
            sequence: 2,
            bytes_len: 8,
            received_mono_ns: 1_000,
            side: BookSide::Bid,
            operation: BookOperation::Upsert,
            price: px(100_00),
            quantity: Some(qty(1_00)),
        })
        .unwrap();
        let error = sync
            .buffer_delta(BufferedDelta {
                sequence: 3,
                bytes_len: 8,
                received_mono_ns: 1_101,
                side: BookSide::Ask,
                operation: BookOperation::Upsert,
                price: px(101_00),
                quantity: Some(qty(1_00)),
            })
            .unwrap_err();
        assert!(matches!(error, BookError::NotValid(BookValidity::Invalid)));
        assert_eq!(sync.state, SyncState::Invalid);
    }
}
