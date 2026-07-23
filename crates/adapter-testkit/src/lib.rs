//! Shared offline assertions for adapter decode / book / reconnect tests (§11.7).
//!
//! # ponytail
//! Helpers only — no harness, no HTTP, no corpus IO. Ceiling: adapters still own
//! fixture strings and session construction. Upgrade: optional fixture loaders
//! if duplication across venues grows again.

#![forbid(unsafe_code)]

use marketfeed_adapter_api::{ActionBuffer, EventBatch, SessionAction};
use marketfeed_model::{AggressorSide, BookSnapshot, Fixed, MarketEvent, SystemEvent, Trade};

/// Collect market payloads from `EmitBatch` actions (order preserved).
pub fn markets(buf: &ActionBuffer) -> Vec<MarketEvent> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitBatch(EventBatch { events, .. }) => Some(events),
            _ => None,
        })
        .flatten()
        .map(|e| e.payload.clone())
        .collect()
}

/// Collect `EmitSystem` payloads (order preserved).
pub fn systems(buf: &ActionBuffer) -> Vec<SystemEvent> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitSystem(ev) => Some(ev.clone()),
            _ => None,
        })
        .collect()
}

/// True when any action requests reconnect.
pub fn has_reconnect(buf: &ActionBuffer) -> bool {
    buf.as_slice()
        .iter()
        .any(|a| matches!(a, SessionAction::Reconnect(_)))
}

/// Assert `actual` parses equal to `expected` decimal string (exact Fixed).
pub fn assert_fixed_eq(actual: &Fixed, expected: &str) {
    let want = Fixed::parse_str(expected).unwrap_or_else(|e| {
        panic!("assert_fixed_eq: bad expected {expected:?}: {e}");
    });
    assert_eq!(
        actual, &want,
        "fixed mismatch: got {actual:?} want {expected}"
    );
}

/// Assert market event is a trade with the given aggressor.
pub fn assert_trade_aggressor(ev: &MarketEvent, side: AggressorSide) -> &Trade {
    match ev {
        MarketEvent::Trade(t) if t.aggressor == side => t,
        other => panic!("expected Trade({side:?}), got {other:?}"),
    }
}

/// Assert reconnect was requested (sequence gap / protocol path).
pub fn assert_reconnect(buf: &ActionBuffer) {
    assert!(
        has_reconnect(buf),
        "expected SessionAction::Reconnect, actions={:?}",
        buf.as_slice()
    );
}

/// Assert market event is a book snapshot; return it.
pub fn assert_book_snapshot(ev: &MarketEvent) -> &BookSnapshot {
    match ev {
        MarketEvent::BookSnapshot(s) => s,
        other => panic!("expected BookSnapshot, got {other:?}"),
    }
}

/// Assert snapshot has at least `min_bids` / `min_asks` levels.
pub fn assert_book_depth(snap: &BookSnapshot, min_bids: usize, min_asks: usize) {
    assert!(
        snap.bids.len() >= min_bids,
        "bids {} < {min_bids}",
        snap.bids.len()
    );
    assert!(
        snap.asks.len() >= min_asks,
        "asks {} < {min_asks}",
        snap.asks.len()
    );
}

/// Assert a book-invalidated / gap-style system event is present.
pub fn assert_book_invalidated(buf: &ActionBuffer) {
    let found = systems(buf)
        .iter()
        .any(|e| matches!(e, SystemEvent::BookInvalidated { .. }));
    assert!(
        found,
        "expected BookInvalidated system event, systems={:?}",
        systems(buf)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::{EventBatch, ReconnectReason};
    use marketfeed_model::{
        BookLevel, ConnectionId, EventEnvelope, EventFlags, InstrumentId, Price, Quantity,
        SessionId, TimestampNs, VenueId,
    };

    fn trade_buy() -> MarketEvent {
        MarketEvent::Trade(Trade {
            price: Price(Fixed::new(10000, 2)),
            quantity: Quantity(Fixed::new(1, 3)),
            aggressor: AggressorSide::Buy,
            trade_id: None,
        })
    }

    fn envelope(payload: MarketEvent) -> EventEnvelope {
        EventEnvelope {
            schema_version: 1,
            venue: VenueId(1),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(1),
            session: SessionId(1),
            frame_seq: 1,
            event_index: 0,
            exchange_ts: None,
            receive_ts: TimestampNs(1),
            source_sequence: None,
            flags: EventFlags::empty(),
            payload,
        }
    }

    #[test]
    fn markets_and_trade_aggressor() {
        let mut buf = ActionBuffer::new();
        buf.push(SessionAction::EmitBatch(EventBatch {
            session: SessionId(1),
            frame_seq: 1,
            events: vec![envelope(trade_buy())],
        }));
        let m = markets(&buf);
        assert_eq!(m.len(), 1);
        let t = assert_trade_aggressor(&m[0], AggressorSide::Buy);
        assert_fixed_eq(&t.price.0, "100.00");
    }

    #[test]
    fn book_snapshot_depth_and_reconnect() {
        let snap = BookSnapshot {
            bids: vec![BookLevel {
                price: Price(Fixed::new(10000, 2)),
                quantity: Quantity(Fixed::new(1, 0)),
            }],
            asks: vec![BookLevel {
                price: Price(Fixed::new(10100, 2)),
                quantity: Quantity(Fixed::new(2, 0)),
            }],
            depth: Some(50),
            checksum: None,
        };
        assert_book_depth(&snap, 1, 1);
        let mut buf = ActionBuffer::new();
        buf.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
        assert_reconnect(&buf);
    }
}
