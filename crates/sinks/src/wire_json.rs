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
