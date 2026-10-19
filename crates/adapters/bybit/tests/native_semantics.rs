//! Explicit native expectations; fixtures are synthetic/adversarial, not live evidence.
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, SessionAction, SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_bybit::{BybitCategory, BybitSession, BybitSessionConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, EventEnvelope, FrameStamp, InstrumentId, MarketEvent, SystemEvent,
    TimestampNs, TradeGranularity, VenueId,
};
use serde_json::{Value, json};

fn session(category: BybitCategory) -> BybitSession {
    let venue = match category {
        BybitCategory::Spot => 6,
        BybitCategory::Linear => 5,
        BybitCategory::Inverse => 11,
    };
    let mut session = BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(venue), CatalogVersion(1)),
        BybitSessionConfig {
            category,
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: [("BTCUSDT".into(), InstrumentId(1))].into(),
            ..Default::default()
        },
    );
    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1700000001000000000),
            },
            &mut ActionBuffer::new(),
        )
        .unwrap();
    session
}
fn drive(session: &mut BybitSession, value: &Value) -> ActionBuffer {
    let mut bytes = serde_json::to_vec(value).unwrap();
    let mut out = ActionBuffer::new();
    session
        .on_input(
            SessionInput::TextFrame {
                bytes: &mut bytes,
                received: FrameStamp {
                    receive_ts: TimestampNs(1_700_000_001_000_000_000),
                    mono_ns: 1,
                },
            },
            &mut out,
        )
        .unwrap();
    out
}
fn events(out: &ActionBuffer) -> Vec<&EventEnvelope> {
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
fn rejected(value: &Value) {
    let out = drive(&mut session(BybitCategory::Linear), value);
    assert!(events(&out).is_empty());
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::EmitSystem(SystemEvent::ParseError { .. })))
    );
}
fn trade() -> Value {
    json!({"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1700000000001_i64,"data":[{"T":1700000000000_i64,"s":"BTCUSDT","S":"Buy","v":"0.00000001","p":"65000.123456789012345678","i":"9007199254740993","seq":18446744073709551615_u64,"L":"PlusTick","BT":true,"RPI":false}]})
}
fn candle() -> Value {
    json!({"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1700000061000_i64,"data":[{"start":1699999980000_i64,"end":1700000039999_i64,"interval":"1","open":"10","high":"12","low":"9","close":"11","volume":"2.1","turnover":"23.000000000000000001","confirm":false,"timestamp":1700000000000_i64}]})
}
#[test]
fn native_trade_flags_and_repeated_cross_sequence_are_preserved_without_deduplication() {
    let mut value = trade();
    let mut second = value["data"][0].clone();
    second["i"] = json!("different-same-economics");
    value["data"].as_array_mut().unwrap().push(second);
    let mut s = session(BybitCategory::Linear);
    let out = drive(&mut s, &value);
    let records = events(&out);
    assert_eq!(records.len(), 2);
    for (index, event) in records.iter().enumerate() {
        assert_eq!(event.event_index, index as u16);
        assert!(event.source_sequence.is_none());
        let MarketEvent::Trade(t) = &event.payload else {
            panic!("trade")
        };
        let p = t.provenance.as_ref().expect("native provenance");
        assert_eq!(p.granularity, TradeGranularity::IndividualExecution);
        assert_eq!(p.source_channel, "publicTrade.BTCUSDT");
        assert_eq!(p.native_attributes["seq"], "18446744073709551615");
        assert_eq!(p.native_attributes["BT"], "true");
        assert_eq!(p.native_attributes["RPI"], "false");
        assert_eq!(p.native_attributes["ts"], "1700000000001");
        assert_eq!(p.native_attributes["L"], "PlusTick");
        assert_eq!(
            t.price.0,
            marketfeed_model::Fixed::new(65000123456789012345678, 18)
        );
        assert!(p.native_count.is_none());
    }
    let next = drive(&mut s, &value);
    assert_eq!(
        events(&next).len(),
        2,
        "repeated seq does not suppress distinct deliveries"
    );
}
#[test]
fn malformed_trade_batch_never_emits_a_prefix_or_saturates_time() {
    for key in ["T", "v", "p", "BT", "RPI", "s", "i"] {
        let mut value = trade();
        let mut row = value["data"][0].clone();
        row[key] = match key {
            "T" => json!(i64::MAX),
            "v" | "p" => json!("-1"),
            "BT" | "RPI" => json!("true"),
            "s" => json!("ETHUSDT"),
            _ => json!(""),
        };
        value["data"].as_array_mut().unwrap().push(row);
        rejected(&value);
    }
    let mut value = trade();
    value["ts"] = json!(i64::MAX);
    rejected(&value);
    let mut value = trade();
    let row = value["data"][0].clone();
    value["data"] = json!(vec![row; 1025]);
    rejected(&value);
}
#[test]
fn every_candle_row_retains_finality_and_native_turnover_with_publication_timestamp() {
    for category in [
        BybitCategory::Spot,
        BybitCategory::Linear,
        BybitCategory::Inverse,
    ] {
        let mut value = candle();
        let mut row = value["data"][0].clone();
        row["confirm"] = json!(true);
        value["data"].as_array_mut().unwrap().push(row);
        let out = drive(&mut session(category), &value);
        let rows = events(&out);
        assert_eq!(rows.len(), 2);
        for (index, event) in rows.iter().enumerate() {
            assert_eq!(event.event_index, index as u16);
            assert_eq!(
                event.exchange_ts,
                Some(TimestampNs(1_700_000_061_000_000_000))
            );
            let MarketEvent::Candle(c) = &event.payload else {
                panic!("candle")
            };
            assert_eq!(c.is_closed, Some(index == 1));
            assert_eq!(
                c.native_volume_ccy.unwrap().0,
                marketfeed_model::Fixed::new(23000000000000000001, 18)
            );
            assert!(
                c.quote_volume.is_none(),
                "WS turnover unit is retained natively pending channel qualification"
            );
            let p = c.provenance.as_ref().unwrap();
            assert_eq!(p.native_attributes["timestamp"], "1700000000000");
            assert_eq!(p.native_attributes["end"], "1700000039999");
            assert_eq!(p.source_channel, "kline.1.BTCUSDT");
        }
    }
}
#[test]
fn malformed_late_candle_and_interval_mismatch_are_atomic_failures() {
    for (key, bad) in [
        ("confirm", json!("false")),
        ("start", json!(i64::MAX)),
        ("timestamp", json!(i64::MAX)),
        ("turnover", json!("-1")),
        ("interval", json!("5")),
        ("high", json!("8")),
    ] {
        let mut value = candle();
        let mut row = value["data"][0].clone();
        row[key] = bad;
        value["data"].as_array_mut().unwrap().push(row);
        rejected(&value);
    }
}

#[test]
fn native_limits_unknown_side_optional_flags_and_catalog_scope_are_explicit() {
    let mut value = trade();
    value["data"][0]["S"] = json!("FutureSide");
    value["data"][0].as_object_mut().unwrap().remove("BT");
    value["data"][0].as_object_mut().unwrap().remove("RPI");
    let row = value["data"][0].clone();
    value["data"] = json!(vec![row; 1024]);
    let out = drive(&mut session(BybitCategory::Linear), &value);
    let records = events(&out);
    assert_eq!(records.len(), 1024);
    let MarketEvent::Trade(t) = &records[1023].payload else {
        panic!("trade")
    };
    assert_eq!(t.aggressor, marketfeed_model::AggressorSide::Unknown);
    assert!(
        !t.provenance
            .as_ref()
            .unwrap()
            .native_attributes
            .contains_key("BT")
    );
    assert!(
        !records[0]
            .flags
            .contains(marketfeed_model::EventFlags::INITIAL_DELIVERY)
    );
    let mut value = trade();
    value["topic"] = json!("publicTrade.ETHUSDT");
    value["data"][0]["s"] = json!("ETHUSDT");
    rejected(&value);
    let mut value = candle();
    value["topic"] = json!("kline.1.ETHUSDT");
    rejected(&value);
    let mut value = candle();
    let row = value["data"][0].clone();
    value["data"] = json!(vec![row; 4096]);
    let out = drive(&mut session(BybitCategory::Linear), &value);
    assert_eq!(events(&out).len(), 4096);
    let extra = value["data"][0].clone();
    value["data"].as_array_mut().unwrap().push(extra);
    rejected(&value);
    for mut value in [trade(), candle()] {
        value.as_object_mut().unwrap().remove("ts");
        rejected(&value);
    }
}
#[test]
fn duplicate_native_keys_and_oversized_input_are_rejected_by_every_backend() {
    let raw = br#"{"topic":"publicTrade.BTCUSDT","topic":"ignored","ts":1,"data":[]}"#;
    assert!(marketfeed_adapter_bybit::decode_text(raw).is_err());
    assert!(marketfeed_adapter_bybit::decode_text_serde(raw).is_err());
    assert!(marketfeed_adapter_bybit::decode_text(&vec![b' '; 16 * 1024 * 1024 + 1]).is_err());
}
