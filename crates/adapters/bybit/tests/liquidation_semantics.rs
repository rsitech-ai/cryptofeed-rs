//! Synthetic/adversarial liquidation records; no live delivery qualification.
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, SessionAction, SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_bybit::{
    BybitCategory, BybitSession, BybitSessionConfig, DecodedEvent, decode_text_serde,
};
use marketfeed_model::*;
use serde_json::{Value, json};
fn frame() -> Value {
    json!({"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1700000000001_i64,"data":[{"T":1700000000000_i64,"s":"BTCUSDT","S":"Buy","v":"0.000000000000000001","p":"65000.123456789012345678"}]})
}
fn session(category: BybitCategory) -> BybitSession {
    BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(category.venue_id(), CatalogVersion(1)),
        BybitSessionConfig {
            category,
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: [("BTCUSDT".into(), InstrumentId(1))].into(),
            ..Default::default()
        },
    )
}
fn drive(s: &mut BybitSession, value: &Value) -> ActionBuffer {
    let mut bytes = serde_json::to_vec(value).unwrap();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: FrameStamp {
                receive_ts: TimestampNs(1700000000002000000),
                mono_ns: 1,
            },
        },
        &mut out,
    )
    .unwrap();
    out
}
fn records(out: &ActionBuffer) -> Vec<&EventEnvelope> {
    out.as_slice()
        .iter()
        .filter_map(|a| {
            if let SessionAction::EmitBatch(batch) = a {
                Some(batch.events.iter())
            } else {
                None
            }
        })
        .flatten()
        .collect()
}
fn rejects(value: &Value, category: BybitCategory) {
    let out = drive(&mut session(category), value);
    assert!(records(&out).is_empty());
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::EmitSystem(SystemEvent::ParseError { .. })))
    );
}
#[test]
fn native_position_price_basis_units_and_both_times_survive_without_aggressor_inference() {
    for category in [BybitCategory::Linear, BybitCategory::Inverse] {
        let mut value = frame();
        for side in ["Sell", "new-side"] {
            let mut row = value["data"][0].clone();
            row["S"] = json!(side);
            value["data"].as_array_mut().unwrap().push(row);
        }
        let out = drive(&mut session(category), &value);
        let events = records(&out);
        assert_eq!(events.len(), 3);
        for (index, (event, position)) in events
            .iter()
            .zip([
                LiquidatedPositionSide::Long,
                LiquidatedPositionSide::Short,
                LiquidatedPositionSide::Unknown,
            ])
            .enumerate()
        {
            assert_eq!(event.event_index, index as u16);
            assert_eq!(event.instrument, Some(InstrumentId(1)));
            assert_eq!(event.exchange_ts, Some(TimestampNs(1700000000000000000)));
            assert!(event.source_sequence.is_none());
            assert_eq!(event.flags, EventFlags::empty());
            let MarketEvent::Liquidation(row) = &event.payload else {
                panic!("liquidation")
            };
            assert_eq!(row.side, AggressorSide::Unknown);
            assert_eq!(row.price.0, Fixed::new(65000123456789012345678, 18));
            assert_eq!(row.quantity.0, Fixed::new(1, 18));
            let source = row.provenance.as_ref().unwrap();
            assert_eq!(source.position_side, Some(position));
            assert_eq!(source.price_basis, LiquidationPriceBasis::Bankruptcy);
            assert_eq!(source.quantity_unit, LiquidationQuantityUnit::Unverified);
            assert_eq!(source.native_attributes["T"], "1700000000000");
            assert_eq!(source.native_attributes["ts"], "1700000000001");
            assert_eq!(
                source.native_attributes["S"],
                value["data"][index]["S"].as_str().unwrap()
            );
            assert_eq!(source.source_channel, "allLiquidation.BTCUSDT");
        }
    }
}
#[test]
fn malformed_late_row_never_emits_a_prefix_or_saturates_updated_time() {
    for field in ["p", "v", "T", "s", "S"] {
        for invalid in [json!(-1), json!(i64::MAX), json!(null), json!(true)] {
            let mut value = frame();
            let mut row = value["data"][0].clone();
            row[field] = invalid;
            value["data"].as_array_mut().unwrap().push(row);
            rejects(&value, BybitCategory::Linear);
        }
    }
    for field in ["p", "v"] {
        for invalid in [json!("-1"), json!("0"), json!("1e1000"), json!(1.5)] {
            let mut value = frame();
            value["data"][0][field] = invalid;
            rejects(&value, BybitCategory::Linear);
        }
    }
}
#[test]
fn source_topic_type_publication_scope_and_product_are_checked_atomically() {
    for field in ["topic", "type", "ts", "data", "op"] {
        let mut value = frame();
        value[field] = match field {
            "topic" => json!("allLiquidation.ETHUSDT"),
            "type" => json!("delta"),
            "ts" => json!(i64::MAX),
            "op" => json!("subscribe"),
            _ => json!({}),
        };
        rejects(&value, BybitCategory::Linear);
    }
    let mut missing = frame();
    missing.as_object_mut().unwrap().remove("ts");
    rejects(&missing, BybitCategory::Linear);
    let mut foreign = frame();
    foreign["topic"] = json!("allLiquidation.ETHUSDT");
    foreign["data"][0]["s"] = json!("ETHUSDT");
    rejects(&foreign, BybitCategory::Linear);
    rejects(&frame(), BybitCategory::Spot);
}
#[test]
fn zero_updated_time_is_preserved_instead_of_being_replaced_by_publication_time() {
    let mut value = frame();
    value["data"][0]["T"] = json!(0);
    let out = drive(&mut session(BybitCategory::Linear), &value);
    assert_eq!(records(&out)[0].exchange_ts, Some(TimestampNs(0)));
}
#[test]
fn empty_known_batch_is_not_an_unknown_protocol_message_and_rows_are_bounded() {
    let mut value = frame();
    value["data"] = json!([]);
    let out = drive(&mut session(BybitCategory::Linear), &value);
    assert!(out.as_slice().is_empty());
    rejects(&value, BybitCategory::Spot);
    for topic in ["allLiquidation.ETHUSDT", "allLiquidation."] {
        let mut foreign = value.clone();
        foreign["topic"] = json!(topic);
        rejects(&foreign, BybitCategory::Linear);
    }
    let row = frame()["data"][0].clone();
    value["data"] = Value::Array(vec![row; 4097]);
    rejects(&value, BybitCategory::Linear);
}
#[test]
fn lexical_duplicate_keys_and_native_control_types_are_not_coerced() {
    let source = serde_json::to_string(&frame()).unwrap();
    for bad in [
        source.replace("\"S\":\"Buy\"", "\"S\":\"Buy\",\"S\":\"Sell\""),
        source.replace("1700000000000,", "1700000000000.0,"),
    ] {
        assert!(decode_text_serde(bad.as_bytes()).is_err());
    }
    let mut value = frame();
    value["data"][0]["T"] = json!("1700000000000");
    assert!(decode_text_serde(&serde_json::to_vec(&value).unwrap()).is_err());
    let DecodedEvent::Liquidations(batch) =
        decode_text_serde(&serde_json::to_vec(&frame()).unwrap()).unwrap()
    else {
        panic!("liquidations")
    };
    assert_eq!(batch.symbol, "BTCUSDT");
    assert_eq!(batch.rows.len(), 1);
}
