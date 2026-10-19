//! Stable JSON payloads shared by text and broker sinks.

use marketfeed_adapter_api::EventBatch;
use marketfeed_model::SystemEvent;
use marketfeed_recording::event_envelope_json;
use serde_json::json;

use crate::SinkError;

pub(crate) fn batch_json(batch: &EventBatch) -> Result<Vec<u8>, SinkError> {
    let events: Vec<_> = batch.events.iter().map(event_envelope_json).collect();
    serde_json::to_vec(&json!({
        "kind": "batch",
        "session": batch.session.0,
        "frame_seq": batch.frame_seq,
        "events": events,
    }))
    .map_err(|error| SinkError::Io(error.to_string()))
}

pub(crate) fn system_json(event: &SystemEvent) -> Result<Vec<u8>, SinkError> {
    serde_json::to_vec(&json!({
        "kind": "system",
        "event": event,
    }))
    .map_err(|error| SinkError::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use marketfeed_adapter_api::EventBatch;
    use marketfeed_model::{
        AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
        Price, Quantity, SessionId, TimestampNs, Trade, VenueId,
    };

    use super::*;

    #[test]
    fn invalidations_with_the_same_instrument_keep_distinct_runtime_scopes() {
        let event = |venue, session| SystemEvent::QuoteInvalidated {
            venue: Some(VenueId(venue)),
            session: Some(SessionId(session)),
            connection: Some(ConnectionId(session)),
            instrument: InstrumentId(1),
            reason: "native side unavailable".into(),
        };
        let left: serde_json::Value =
            serde_json::from_slice(&system_json(&event(21, 1)).unwrap()).unwrap();
        let right: serde_json::Value =
            serde_json::from_slice(&system_json(&event(22, 2)).unwrap()).unwrap();
        assert_eq!(
            left["event"]["QuoteInvalidated"]["instrument"],
            right["event"]["QuoteInvalidated"]["instrument"]
        );
        assert_ne!(
            left["event"]["QuoteInvalidated"]["venue"],
            right["event"]["QuoteInvalidated"]["venue"]
        );
        assert_ne!(
            left["event"]["QuoteInvalidated"]["session"],
            right["event"]["QuoteInvalidated"]["session"]
        );
    }

    #[test]
    fn batch_json_contains_complete_normalized_trade() {
        let batch = EventBatch {
            session: SessionId(1),
            frame_seq: 9,
            events: vec![EventEnvelope {
                schema_version: 1,
                venue: VenueId(2),
                instrument: Some(InstrumentId(3)),
                connection: ConnectionId(4),
                session: SessionId(1),
                frame_seq: 9,
                event_index: 0,
                exchange_ts: None,
                receive_ts: TimestampNs(5),
                source_sequence: None,
                flags: EventFlags::empty(),
                payload: MarketEvent::Trade(Trade {
                    price: Price(Fixed::new(12345, 2)),
                    quantity: Quantity(Fixed::new(25, 1)),
                    aggressor: AggressorSide::Buy,
                    trade_id: None,
                }),
            }],
        };

        let value: serde_json::Value =
            serde_json::from_slice(&batch_json(&batch).unwrap()).unwrap();
        let trade = &value["events"][0]["payload"]["trade"];
        assert_eq!(trade["price"]["value"]["coefficient_lo"], 12345);
        assert_eq!(trade["price"]["value"]["scale"], 2);
        assert_eq!(trade["quantity"]["value"]["coefficient_lo"], 25);
        assert_eq!(trade["aggressor"], "AGGRESSOR_SIDE_BUY");
    }
}
