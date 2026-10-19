//! Explicit update-ID semantics; no assertion of complete source delivery.
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, ReconnectReason, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_bybit::{BybitSession, BybitSessionConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, MarketEvent, SystemEvent, TimestampNs, VenueId,
};
use serde_json::{Value, json};
fn session() -> BybitSession {
    BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(5), CatalogVersion(1)),
        BybitSessionConfig {
            enable_l2: true,
            ..Default::default()
        },
    )
}
fn message(kind: &str, u: u64, seq: u64, quantity: &str) -> Value {
    json!({"topic":"orderbook.50.BTCUSDT","type":kind,"ts":1700000000001_i64,"cts":1700000000000_i64,"data":{"s":"BTCUSDT","b":[["100.00",quantity]],"a":if kind=="snapshot"{json!([["101.00","1.000"]])}else{json!([])},"u":u,"seq":seq}})
}
fn drive(s: &mut BybitSession, v: &Value) -> ActionBuffer {
    let mut bytes = serde_json::to_vec(v).unwrap();
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
fn market_count(out: &ActionBuffer) -> usize {
    out.as_slice()
        .iter()
        .filter_map(|a| {
            if let SessionAction::EmitBatch(b) = a {
                Some(b.events.len())
            } else {
                None
            }
        })
        .sum()
}
fn protocol_resync(out: &ActionBuffer) {
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(ReconnectReason::Protocol)))
    );
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
    assert!(!out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::SequenceGap { .. })
    )));
    assert_eq!(market_count(out), 0);
}
#[test]
fn increasing_nonunit_update_ids_are_applied_without_inventing_a_gap() {
    let mut s = session();
    drive(&mut s, &message("snapshot", 10, 100, "1.000"));
    for (u, seq) in [(30, 1000), (u64::MAX, u64::MAX)] {
        let out = drive(&mut s, &message("delta", u, seq, "2.000"));
        assert_eq!(market_count(&out), 1);
        assert!(
            !out.as_slice()
                .iter()
                .any(|a| matches!(a, SessionAction::Reconnect(_)))
        );
        for action in out.as_slice() {
            if let SessionAction::EmitBatch(batch) = action {
                assert!(
                    batch.events[0].source_sequence.is_none(),
                    "u/seq are provenance, not consecutive source sequences"
                );
            }
        }
    }
}
#[test]
fn conflicting_repeat_is_visible_and_requires_fresh_snapshot() {
    let mut s = session();
    drive(&mut s, &message("snapshot", 10, 100, "1.000"));
    let delta = message("delta", 11, 101, "2.000");
    drive(&mut s, &delta);
    let duplicate = drive(&mut s, &delta);
    assert_eq!(market_count(&duplicate), 0);
    protocol_resync(&drive(&mut s, &message("delta", 11, 101, "9.000")));
    assert_eq!(
        market_count(&drive(&mut s, &message("delta", 12, 102, "3.000"))),
        0
    );
    assert_eq!(
        market_count(&drive(&mut s, &message("snapshot", 2, 1, "4.000"))),
        1
    );
    assert_eq!(
        market_count(&drive(&mut s, &message("delta", 20, 10, "5.000"))),
        1
    );
}
#[test]
fn regression_is_an_ordering_anomaly_not_confirmed_missing_interval() {
    let mut s = session();
    drive(&mut s, &message("snapshot", 10, 100, "1.000"));
    protocol_resync(&drive(&mut s, &message("delta", 5, 50, "2.000")));
}
#[test]
fn maximum_update_id_snapshot_never_overflows_and_new_snapshot_resets_it() {
    let mut s = session();
    assert_eq!(
        market_count(&drive(
            &mut s,
            &message("snapshot", u64::MAX, u64::MAX, "1.000")
        )),
        1
    );
    assert_eq!(
        market_count(&drive(&mut s, &message("snapshot", 2, 1, "2.000"))),
        1
    );
    assert_eq!(
        market_count(&drive(&mut s, &message("delta", 3, 2, "3.000"))),
        1
    );
}
#[test]
fn mismatched_topic_and_unconfigured_book_symbol_are_visible_parse_failures() {
    for native in ["BTCUSDT", "ETHUSDT"] {
        let mut s = session();
        let mut value = message("snapshot", 10, 100, "1.000");
        value["topic"] = json!("orderbook.50.ETHUSDT");
        value["data"]["s"] = json!(native);
        let out = drive(&mut s, &value);
        assert_eq!(market_count(&out), 0);
        assert!(
            out.as_slice()
                .iter()
                .any(|a| matches!(a, SessionAction::EmitSystem(SystemEvent::ParseError { .. })))
        );
    }
}

#[test]
fn malformed_native_frame_latches_unavailable_until_a_fresh_snapshot() {
    for field in [
        "price",
        "timestamp",
        "cts",
        "type",
        "missing_ts",
        "scope",
        "unknown",
    ] {
        let mut s = session();
        drive(&mut s, &message("snapshot", 10, 100, "1.000"));
        let mut bad = message("delta", 20, 200, "2.000");
        match field {
            "price" => bad["data"]["b"][0][0] = json!("bad"),
            "timestamp" => bad["ts"] = json!(i64::MAX),
            "cts" => bad["cts"] = json!(i64::MAX),
            "type" => bad["type"] = json!("new-unverified-type"),
            "missing_ts" => {
                bad.as_object_mut().unwrap().remove("ts");
            }
            "scope" => bad["data"]["s"] = json!("ETHUSDT"),
            _ => bad["topic"] = json!("unknown.book"),
        }
        protocol_resync(&drive(&mut s, &bad));
        assert_eq!(
            market_count(&drive(&mut s, &message("delta", 30, 300, "3.000"))),
            0,
            "{field}"
        );
        assert_eq!(
            market_count(&drive(&mut s, &message("snapshot", 40, 400, "4.000"))),
            1
        );
        assert_eq!(
            market_count(&drive(&mut s, &message("delta", 90, 900, "5.000"))),
            1
        );
    }
}
#[test]
fn unexpected_binary_frame_invalidates_books_and_quotes_until_a_fresh_snapshot() {
    let mut s = session();
    drive(&mut s, &message("snapshot", 10, 100, "1.000"));
    let mut quote = message("snapshot", 10, 100, "1.000");
    quote["topic"] = json!("orderbook.1.BTCUSDT");
    assert_eq!(market_count(&drive(&mut s, &quote)), 1);
    let mut bytes = [0x01, 0x02];
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::BinaryFrame {
            bytes: &mut bytes,
            received: FrameStamp {
                receive_ts: TimestampNs(1700000000002000000),
                mono_ns: 1,
            },
        },
        &mut out,
    )
    .unwrap();
    protocol_resync(&out);
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::QuoteInvalidated { .. })
    )));
    assert_eq!(
        market_count(&drive(&mut s, &message("delta", 30, 300, "2.000"))),
        0
    );
    assert_eq!(
        market_count(&drive(&mut s, &message("snapshot", 40, 400, "3.000"))),
        1
    );
    assert_eq!(
        market_count(&drive(&mut s, &message("delta", 90, 900, "4.000"))),
        1
    );
}
#[test]
fn same_update_id_binds_native_metadata_and_ordered_levels() {
    for field in ["seq", "ts", "cts", "cts_presence", "price", "quantity"] {
        let mut s = session();
        drive(&mut s, &message("snapshot", 10, 100, "1.000"));
        let original = message("delta", 20, 200, "2.000");
        drive(&mut s, &original);
        let mut changed = original.clone();
        match field {
            "seq" => changed["data"]["seq"] = json!(201),
            "ts" => changed["ts"] = json!(1700000000002_i64),
            "cts" => changed["cts"] = json!(1700000000001_i64),
            "cts_presence" => {
                changed.as_object_mut().unwrap().remove("cts");
            }
            "price" => changed["data"]["b"][0][0] = json!("99.00"),
            _ => changed["data"]["b"][0][1] = json!("9.000"),
        }
        protocol_resync(&drive(&mut s, &changed));
    }
    let mut s = session();
    drive(&mut s, &message("snapshot", 10, 100, "1.000"));
    protocol_resync(&drive(&mut s, &message("delta", 30, 50, "2.000")));
}
#[test]
fn native_provenance_and_action_order_feed_the_real_downstream_projection() {
    use marketfeed_event_pulse::features::BookProjection;
    let mut s = session();
    let mut projection = BookProjection::new(2, 3, Some(50));
    let mut projected = 0;
    for message in [
        message("snapshot", 10, 100, "1.000"),
        message("delta", 30, 900, "2.000"),
        message("snapshot", 2, 1, "3.000"),
        message("delta", u64::MAX, u64::MAX, "4.000"),
    ] {
        let out = drive(&mut s, &message);
        for action in out.as_slice() {
            match action {
                SessionAction::EmitSystem(SystemEvent::BookResynchronized { .. }) => {
                    projection.permit_resnapshot()
                }
                SessionAction::EmitBatch(batch) => {
                    for event in &batch.events {
                        assert!(event.source_sequence.is_none());
                        match &event.payload {
                            MarketEvent::BookSnapshot(book) => {
                                let source = book.provenance.as_ref().unwrap();
                                assert_eq!(source.native_attributes["type"], "snapshot");
                                projection
                                    .snapshot_derived(book, event.receive_ts.0)
                                    .unwrap();
                            }
                            MarketEvent::BookDelta(book) => {
                                let source = book.provenance.as_ref().unwrap();
                                assert_eq!(
                                    source.native_attributes["u"],
                                    message["data"]["u"].to_string()
                                );
                                assert_eq!(
                                    source.native_attributes["seq"],
                                    message["data"]["seq"].to_string()
                                );
                                assert_eq!(source.native_attributes["cts"], "1700000000000");
                                assert_eq!(source.source_channel, "orderbook.50.BTCUSDT");
                                projection.delta_derived(book, event.receive_ts.0).unwrap();
                            }
                            _ => panic!("book"),
                        }
                        projected += 1;
                    }
                }
                _ => {}
            }
        }
    }
    assert_eq!(projected, 4);
}
#[test]
fn level1_idle_snapshots_refresh_but_missing_sides_invalidate_the_quote() {
    let mut s = session();
    let mut value = message("snapshot", 9, 99, "1.000");
    value["topic"] = json!("orderbook.1.BTCUSDT");
    assert_eq!(market_count(&drive(&mut s, &value)), 1);
    value["ts"] = json!(1700000003001_i64);
    let out = drive(&mut s, &value);
    assert_eq!(market_count(&out), 1);
    for action in out.as_slice() {
        if let SessionAction::EmitBatch(batch) = action {
            let event = &batch.events[0];
            assert!(event.source_sequence.is_none());
            let MarketEvent::Quote(q) = &event.payload else {
                panic!("quote")
            };
            assert_eq!(q.provenance.as_ref().unwrap().native_attributes["u"], "9");
        }
    }
    value["data"]["b"] = json!([]);
    let out = drive(&mut s, &value);
    assert_eq!(market_count(&out), 0);
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::QuoteInvalidated { .. })
    )));
}

#[test]
fn reset_metadata_and_failed_atomic_apply_are_not_silently_reused() {
    let mut s = session();
    drive(&mut s, &message("snapshot", 10, 100, "1.000"));
    let mut reset = message("snapshot", 1, 1, "2.000");
    reset["type"] = json!("delta");
    let out = drive(&mut s, &reset);
    assert_eq!(market_count(&out), 1);
    for action in out.as_slice() {
        if let SessionAction::EmitBatch(batch) = action {
            let MarketEvent::BookSnapshot(book) = &batch.events[0].payload else {
                panic!("effective snapshot")
            };
            assert_eq!(
                book.provenance.as_ref().unwrap().native_attributes["type"],
                "delta"
            );
        }
    }
    let mut crossed = message("delta", 20, 200, "3.000");
    crossed["data"]["b"][0][0] = json!("102.00");
    protocol_resync(&drive(&mut s, &crossed));
    assert_eq!(
        market_count(&drive(&mut s, &message("delta", 30, 300, "4.000"))),
        0
    );
    assert_eq!(
        market_count(&drive(&mut s, &message("snapshot", 3, 3, "5.000"))),
        1
    );
}
#[test]
fn connection_restart_clears_snapshot_and_exact_repeat_identity() {
    use marketfeed_adapter_api::DisconnectReason;
    let mut s = session();
    drive(&mut s, &message("snapshot", 10, 100, "1.000"));
    let delta = message("delta", 20, 200, "2.000");
    drive(&mut s, &delta);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Disconnected {
            reason: DisconnectReason::RemoteClose,
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(2),
        },
        &mut out,
    )
    .unwrap();
    assert_eq!(market_count(&drive(&mut s, &delta)), 0);
    assert_eq!(
        market_count(&drive(&mut s, &message("snapshot", 10, 100, "1.000"))),
        1
    );
    assert_eq!(market_count(&drive(&mut s, &delta)), 1);
}
