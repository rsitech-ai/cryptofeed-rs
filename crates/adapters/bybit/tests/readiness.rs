//! Synthetic native control challenges; startup is not source continuity proof.
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, DisconnectReason, SessionAction, SessionCommand,
    SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_bybit::{
    BybitCategory, BybitSession, BybitSessionConfig, DecodedEvent, decode_text_serde,
};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, SystemEvent, TimestampNs,
};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
const NS: i64 = 1_000_000_000;
fn configured(category: BybitCategory, n: usize, l2: bool) -> BybitSession {
    let symbols = (0..n).map(|i| format!("COIN{i}USDT")).collect::<Vec<_>>();
    let instrument_ids = symbols
        .iter()
        .enumerate()
        .map(|(i, s)| (s.clone(), InstrumentId(u32::try_from(i + 1).unwrap())))
        .collect::<HashMap<_, _>>();
    BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(category.venue_id(), CatalogVersion(1)),
        BybitSessionConfig {
            category,
            symbols,
            instrument_ids,
            enable_l2: l2,
            ..Default::default()
        },
    )
}
fn connect(s: &mut BybitSession, at: i64) -> Vec<Value> {
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(at),
        },
        &mut out,
    )
    .unwrap();
    assert_eq!(live(&out), 0);
    out.as_slice()
        .iter()
        .filter_map(|a| {
            if let SessionAction::SendText(b) = a {
                Some(serde_json::from_slice(b).unwrap())
            } else {
                None
            }
        })
        .collect()
}
fn drive(s: &mut BybitSession, v: &Value, at: i64) -> ActionBuffer {
    let mut bytes = serde_json::to_vec(v).unwrap();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: FrameStamp {
                receive_ts: TimestampNs(at),
                mono_ns: at.max(0) as u64,
            },
        },
        &mut out,
    )
    .unwrap();
    out
}
fn timer(s: &mut BybitSession, id: u64, at: i64) -> ActionBuffer {
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Timer {
            timer_id: id,
            now: TimestampNs(at),
        },
        &mut out,
    )
    .unwrap();
    out
}
fn ack(request: &Value) -> Value {
    json!({"op":"subscribe","success":true,"ret_msg":"","req_id":request["req_id"],"conn_id":"native-connection-a"})
}
fn book(symbol: &str, depth: u32) -> Value {
    json!({"topic":format!("orderbook.{depth}.{symbol}"),"type":"snapshot","ts":1700000000000_i64,"data":{"s":symbol,"u":10,"seq":100,"b":[["100.00","1.000"]],"a":[["101.00","1.000"]]}})
}
fn live(out: &ActionBuffer) -> usize {
    out.as_slice()
        .iter()
        .filter(|a| matches!(a, SessionAction::MarkLive))
        .count()
}
fn market(out: &ActionBuffer) -> usize {
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
fn rejected(out: &ActionBuffer) {
    assert_eq!(live(out), 0);
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(_)))
    );
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkDegraded))
    );
}
fn complete_books(s: &mut BybitSession, n: usize, l2: bool, at: i64) -> usize {
    let mut transitions = 0;
    for i in 0..n {
        for depth in if l2 { vec![1, 50] } else { vec![1] } {
            transitions += live(&drive(s, &book(&format!("COIN{i}USDT"), depth), at));
        }
    }
    transitions
}
#[test]
fn every_request_and_initial_requested_book_is_required_in_either_arrival_order() {
    for category in [
        BybitCategory::Spot,
        BybitCategory::Linear,
        BybitCategory::Inverse,
    ] {
        for l2 in [false, true] {
            for books_first in [false, true] {
                let mut s = configured(category, 4, l2);
                let requests = connect(&mut s, 0);
                assert!(requests.len() > 1);
                let mut topics = BTreeSet::new();
                let mut ids = BTreeSet::new();
                for request in &requests {
                    assert_eq!(request["op"], "subscribe");
                    assert!(ids.insert(request["req_id"].as_str().unwrap()));
                    let args = request["args"].as_array().unwrap();
                    assert!(!args.is_empty() && args.len() <= 10);
                    for topic in args {
                        assert!(topics.insert(topic.as_str().unwrap()));
                    }
                }
                if books_first {
                    assert_eq!(complete_books(&mut s, 4, l2, NS), 0);
                }
                let mut transitions = 0;
                for (index, request) in requests.iter().enumerate() {
                    let out = drive(&mut s, &ack(request), NS);
                    if index + 1 < requests.len() {
                        assert_eq!(live(&out), 0);
                    }
                    transitions += live(&out);
                    assert_eq!(live(&drive(&mut s, &ack(request), NS)), 0);
                }
                if !books_first {
                    transitions += complete_books(&mut s, 4, l2, NS);
                }
                assert_eq!(transitions, 1);
                assert_eq!(live(&timer(&mut s, 2, 21 * NS)), 0);
            }
        }
    }
}
#[test]
fn a_ping_response_is_not_a_subscription_ack_and_duplicates_do_not_fill_other_chunks() {
    let mut s = configured(BybitCategory::Linear, 5, true);
    let requests = connect(&mut s, 0);
    complete_books(&mut s, 5, true, NS);
    for _ in 0..4 {
        let out = drive(
            &mut s,
            &json!({"op":"ping","success":true,"ret_msg":"pong","req_id":""}),
            NS,
        );
        assert_eq!(live(&out), 0);
        assert!(!out.as_slice().iter().any(|a| matches!(
            a,
            SessionAction::EmitSystem(SystemEvent::SubscriptionStateChanged { .. })
        )));
    }
    for _ in 0..4 {
        assert_eq!(live(&drive(&mut s, &ack(&requests[0]), NS)), 0);
    }
    assert_eq!(live(&drive(&mut s, &ack(&requests[1]), NS)), 0);
    assert_eq!(live(&drive(&mut s, &ack(&requests[2]), NS)), 1);
}
#[test]
fn malformed_negative_foreign_and_unsubscribe_responses_are_terminal_until_reconnect() {
    for kind in [
        "missing_id",
        "empty_id",
        "foreign_id",
        "numeric_id",
        "missing_conn",
        "empty_conn",
        "numeric_conn",
        "negative",
        "missing_success",
        "string_success",
        "unsubscribe",
        "auth",
        "missing_op",
        "null_op",
        "mixed",
        "retmsg",
    ] {
        let mut s = configured(BybitCategory::Linear, 1, true);
        let requests = connect(&mut s, 0);
        complete_books(&mut s, 1, true, NS);
        let mut bad = ack(&requests[0]);
        match kind {
            "missing_id" => {
                bad.as_object_mut().unwrap().remove("req_id");
            }
            "empty_id" => bad["req_id"] = json!(""),
            "foreign_id" => bad["req_id"] = json!("foreign"),
            "numeric_id" => bad["req_id"] = json!(123),
            "missing_conn" => {
                bad.as_object_mut().unwrap().remove("conn_id");
            }
            "empty_conn" => bad["conn_id"] = json!(""),
            "numeric_conn" => bad["conn_id"] = json!(123),
            "negative" => bad["success"] = json!(false),
            "missing_success" => {
                bad.as_object_mut().unwrap().remove("success");
            }
            "string_success" => bad["success"] = json!("true"),
            "unsubscribe" => bad["op"] = json!("unsubscribe"),
            "auth" => bad["op"] = json!("auth"),
            "missing_op" => {
                bad.as_object_mut().unwrap().remove("op");
            }
            "null_op" => bad["op"] = Value::Null,
            "mixed" => bad["topic"] = json!("orderbook.1.COIN0USDT"),
            _ => bad["ret_msg"] = json!("pong"),
        }
        rejected(&drive(&mut s, &bad, NS));
        assert_eq!(market(&drive(&mut s, &ack(&requests[0]), NS)), 0);
        assert_eq!(complete_books(&mut s, 1, true, NS), 0);
        let new = connect(&mut s, 2 * NS);
        assert_ne!(new[0]["req_id"], requests[0]["req_id"]);
        assert_eq!(live(&drive(&mut s, &ack(&new[0]), 3 * NS)), 0);
        assert_eq!(complete_books(&mut s, 1, true, 3 * NS), 1);
    }
}
#[test]
fn conflicting_connection_or_ack_result_cannot_readmit_an_already_live_session() {
    for change in ["connection", "negative", "message"] {
        let mut s = configured(BybitCategory::Spot, 1, false);
        let requests = connect(&mut s, 0);
        drive(&mut s, &ack(&requests[0]), NS);
        assert_eq!(complete_books(&mut s, 1, false, NS), 1);
        let mut bad = ack(&requests[0]);
        match change {
            "connection" => bad["conn_id"] = json!("other-connection"),
            "negative" => bad["success"] = json!(false),
            _ => bad["ret_msg"] = json!("subscribe"),
        };
        rejected(&drive(&mut s, &bad, 2 * NS));
        assert_eq!(complete_books(&mut s, 1, false, 3 * NS), 0);
    }
    let mut s = configured(BybitCategory::Linear, 3, false);
    let requests = connect(&mut s, 0);
    drive(&mut s, &ack(&requests[0]), NS);
    let mut other = ack(&requests[1]);
    other["conn_id"] = json!("other-connection");
    rejected(&drive(&mut s, &other, NS));
}
#[test]
fn deadline_applies_to_received_ack_or_last_snapshot_before_timer_delivery() {
    for at in [20 * NS - 1, 20 * NS, 20 * NS + 1] {
        for last_ack in [false, true] {
            let mut s = configured(BybitCategory::Linear, 1, true);
            let requests = connect(&mut s, 0);
            if last_ack {
                complete_books(&mut s, 1, true, NS);
            } else {
                drive(&mut s, &ack(&requests[0]), NS);
                drive(&mut s, &book("COIN0USDT", 1), NS);
            }
            let out = drive(
                &mut s,
                &if last_ack {
                    ack(&requests[0])
                } else {
                    book("COIN0USDT", 50)
                },
                at,
            );
            if at < 20 * NS {
                assert_eq!(live(&out), 1);
            } else {
                rejected(&out);
                assert_eq!(market(&out), 0);
            }
        }
    }
}
#[test]
fn missing_quotes_and_books_both_timeout_and_failure_cannot_be_healed_by_late_data() {
    for missing in ["ack", "quote", "l2"] {
        let mut s = configured(BybitCategory::Linear, 1, true);
        let requests = connect(&mut s, 0);
        if missing != "ack" {
            drive(&mut s, &ack(&requests[0]), NS);
        }
        if missing != "quote" {
            drive(&mut s, &book("COIN0USDT", 1), NS);
        }
        if missing != "l2" {
            drive(&mut s, &book("COIN0USDT", 50), NS);
        }
        rejected(&timer(&mut s, 2, 20 * NS));
        assert_eq!(market(&drive(&mut s, &book("COIN0USDT", 1), 21 * NS)), 0);
        assert_eq!(live(&drive(&mut s, &ack(&requests[0]), 21 * NS)), 0);
    }
}
#[test]
fn reconnect_resets_all_caches_and_old_ack_or_timer_cannot_complete_new_startup() {
    let mut s = configured(BybitCategory::Linear, 1, true);
    let old = connect(&mut s, 0);
    drive(&mut s, &ack(&old[0]), NS);
    assert_eq!(complete_books(&mut s, 1, true, NS), 1);
    let new = connect(&mut s, 10 * NS);
    assert_ne!(old[0]["req_id"], new[0]["req_id"]);
    assert!(
        !timer(&mut s, 2, 20 * NS)
            .as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(_)))
    );
    assert_eq!(live(&drive(&mut s, &ack(&new[0]), 21 * NS)), 0);
    assert_eq!(live(&drive(&mut s, &book("COIN0USDT", 50), 21 * NS)), 0);
    assert_eq!(live(&drive(&mut s, &book("COIN0USDT", 1), 21 * NS)), 1);
    let fresh = connect(&mut s, 22 * NS);
    assert_ne!(fresh[0]["req_id"], new[0]["req_id"]);
    rejected(&drive(&mut s, &ack(&old[0]), 23 * NS));
}
#[test]
fn loss_of_a_requested_bbo_degrades_and_requires_a_new_valid_bbo_before_readmission() {
    let mut s = configured(BybitCategory::Spot, 1, false);
    let requests = connect(&mut s, 0);
    drive(&mut s, &ack(&requests[0]), NS);
    assert_eq!(complete_books(&mut s, 1, false, NS), 1);
    let mut empty = book("COIN0USDT", 1);
    empty["data"]["b"] = json!([]);
    let out = drive(&mut s, &empty, 2 * NS);
    assert_eq!(live(&out), 0);
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkDegraded))
    );
    assert!(
        !out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(_)))
    );
    assert_eq!(live(&drive(&mut s, &book("COIN0USDT", 1), 3 * NS)), 1);
    assert!(
        !timer(&mut s, 2, 22 * NS)
            .as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(_)))
    );
}
#[test]
fn mixed_nonstring_control_ops_cannot_be_decoded_as_market_snapshots() {
    for op in [Value::Null, json!(1), json!(true), json!({}), json!([])] {
        for depth in [1, 50] {
            let mut value = book("COIN0USDT", depth);
            value["op"] = op.clone();
            assert!(decode_text_serde(&serde_json::to_vec(&value).unwrap()).is_err());
            let mut s = configured(BybitCategory::Linear, 1, true);
            connect(&mut s, 0);
            rejected(&drive(&mut s, &value, NS));
        }
    }
    assert!(matches!(
        decode_text_serde(br#"{"op":"ping","success":true,"ret_msg":"pong"}"#).unwrap(),
        DecodedEvent::Pong
    ));
}
#[test]
fn invalid_configuration_and_cumulative_argument_budget_fail_before_any_send() {
    for cfg in [
        BybitSessionConfig {
            symbols: vec![],
            ..Default::default()
        },
        BybitSessionConfig {
            symbols: vec!["BTCUSDT".into(), "BTCUSDT".into()],
            ..Default::default()
        },
        BybitSessionConfig {
            enable_l2: true,
            l2_depth: 1,
            ..Default::default()
        },
        BybitSessionConfig {
            candle_intervals: vec![marketfeed_adapter_api::CandleInterval::M1; 2],
            ..Default::default()
        },
    ] {
        let mut s = BybitSession::new(
            SessionSpec {
                endpoint_name: "ws".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(BybitCategory::Linear.venue_id(), CatalogVersion(1)),
            cfg,
        );
        let mut out = ActionBuffer::new();
        assert!(
            s.on_input(
                SessionInput::Connected {
                    now: TimestampNs(0)
                },
                &mut out
            )
            .is_err()
        );
        assert!(
            !out.as_slice()
                .iter()
                .any(|a| matches!(a, SessionAction::SendText(_) | SessionAction::MarkLive))
        );
    }
    let mut s = configured(BybitCategory::Linear, 400, true);
    let mut out = ActionBuffer::new();
    assert!(
        s.on_input(
            SessionInput::Connected {
                now: TimestampNs(0)
            },
            &mut out
        )
        .is_err()
    );
    assert!(
        !out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::SendText(_)))
    );
}
#[test]
fn disconnected_stopped_and_overflowed_startups_do_not_emit_or_readmit() {
    for stopped in [false, true] {
        let mut s = configured(BybitCategory::Spot, 1, false);
        let requests = connect(&mut s, 0);
        let mut out = ActionBuffer::new();
        if stopped {
            s.on_input(
                SessionInput::Control {
                    command: &SessionCommand::Stop,
                },
                &mut out,
            )
            .unwrap();
        } else {
            s.on_input(
                SessionInput::Disconnected {
                    reason: DisconnectReason::RemoteClose,
                    now: TimestampNs(NS),
                },
                &mut out,
            )
            .unwrap();
        }
        assert!(
            out.as_slice()
                .iter()
                .any(|a| matches!(a, SessionAction::CancelTimer(2)))
        );
        assert_eq!(market(&drive(&mut s, &book("COIN0USDT", 1), NS)), 0);
        assert_eq!(live(&drive(&mut s, &ack(&requests[0]), NS)), 0);
        assert!(
            !timer(&mut s, 1, 20 * NS)
                .as_slice()
                .iter()
                .any(|a| matches!(a, SessionAction::SendText(_)))
        );
    }
    let mut s = configured(BybitCategory::Spot, 1, false);
    let mut out = ActionBuffer::new();
    assert!(
        s.on_input(
            SessionInput::Connected {
                now: TimestampNs(i64::MAX)
            },
            &mut out
        )
        .is_err()
    );
    assert!(
        !out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::SendText(_) | SessionAction::MarkLive))
    );
}

#[test]
fn engine_readiness_signal_is_revoked_on_a_new_connection_without_prior_disconnect() {
    use marketfeed_engine::{SessionLifecycle, SessionRunner, SessionRunnerConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let signal = Arc::new(AtomicBool::new(false));
    let mut runner = SessionRunner::new(
        Box::new(configured(BybitCategory::Spot, 1, false)),
        SessionRunnerConfig {
            venue: BybitCategory::Spot.venue_id(),
            live_signal: Some(signal.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(0)).unwrap();
    let initial = runner.take_pending_writes();
    let request: Value = serde_json::from_slice(&initial[0].payload).unwrap();
    for value in [ack(&request), book("COIN0USDT", 1)] {
        runner
            .on_text_frame(
                &mut serde_json::to_vec(&value).unwrap(),
                FrameStamp {
                    receive_ts: TimestampNs(NS),
                    mono_ns: 1,
                },
            )
            .unwrap();
    }
    assert!(signal.load(Ordering::Acquire));
    assert_eq!(runner.lifecycle, SessionLifecycle::Live);
    runner.on_connected(TimestampNs(2 * NS)).unwrap();
    assert!(!signal.load(Ordering::Acquire));
    assert_ne!(runner.lifecycle, SessionLifecycle::Live);
    let requests = runner.take_pending_writes();
    let request: Value = serde_json::from_slice(&requests[0].payload).unwrap();
    runner
        .on_text_frame(
            &mut serde_json::to_vec(&ack(&request)).unwrap(),
            FrameStamp {
                receive_ts: TimestampNs(3 * NS),
                mono_ns: 2,
            },
        )
        .unwrap();
    assert!(!signal.load(Ordering::Acquire));
    runner
        .on_text_frame(
            &mut serde_json::to_vec(&book("COIN0USDT", 1)).unwrap(),
            FrameStamp {
                receive_ts: TimestampNs(3 * NS),
                mono_ns: 3,
            },
        )
        .unwrap();
    assert!(signal.load(Ordering::Acquire));
}

#[test]
fn cold_sessions_ignore_financial_and_binary_frames_until_connected() {
    for category in [
        BybitCategory::Spot,
        BybitCategory::Linear,
        BybitCategory::Inverse,
    ] {
        let mut session = configured(category, 1, true);
        for value in [
            book("COIN0USDT", 1),
            book("COIN0USDT", 50),
            json!({"topic":"publicTrade.COIN0USDT","type":"snapshot","ts":1700000000000_i64,"data":[{"T":1700000000000_i64,"s":"COIN0USDT","S":"Buy","v":"1","p":"100","i":"1"}]}),
        ] {
            let out = drive(&mut session, &value, NS);
            assert_eq!(market(&out), 0);
            assert_eq!(live(&out), 0);
            assert!(out.as_slice().is_empty());
        }
        let mut bytes = [1, 2];
        let mut out = ActionBuffer::new();
        session
            .on_input(
                SessionInput::BinaryFrame {
                    bytes: &mut bytes,
                    received: FrameStamp {
                        receive_ts: TimestampNs(NS),
                        mono_ns: 1,
                    },
                },
                &mut out,
            )
            .unwrap();
        assert!(out.as_slice().is_empty());
        let requests = connect(&mut session, 2 * NS);
        for request in &requests {
            assert_eq!(live(&drive(&mut session, &ack(request), 3 * NS)), 0);
        }
        assert_eq!(complete_books(&mut session, 1, true, 3 * NS), 1);
    }
}

#[test]
fn failed_reconnect_and_frame_callbacks_revoke_previously_live_engine_signal() {
    use marketfeed_engine::{SessionLifecycle, SessionRunner, SessionRunnerConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    for reconnect in [false, true] {
        let signal = Arc::new(AtomicBool::new(false));
        let mut runner = SessionRunner::new(
            Box::new(configured(BybitCategory::Spot, 1, false)),
            SessionRunnerConfig {
                venue: BybitCategory::Spot.venue_id(),
                live_signal: Some(signal.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        runner.on_connected(TimestampNs(0)).unwrap();
        let writes = runner.take_pending_writes();
        let request: Value = serde_json::from_slice(&writes[0].payload).unwrap();
        for value in [ack(&request), book("COIN0USDT", 1)] {
            runner
                .on_text_frame(
                    &mut serde_json::to_vec(&value).unwrap(),
                    FrameStamp {
                        receive_ts: TimestampNs(NS),
                        mono_ns: 1,
                    },
                )
                .unwrap();
        }
        assert!(signal.load(Ordering::Acquire));
        assert_eq!(runner.lifecycle, SessionLifecycle::Live);
        if reconnect {
            assert!(runner.on_connected(TimestampNs(i64::MAX)).is_err());
        } else {
            let mut missing_bid = book("COIN0USDT", 1);
            missing_bid["data"]["b"] = json!([]);
            assert!(
                runner
                    .on_text_frame(
                        &mut serde_json::to_vec(&missing_bid).unwrap(),
                        FrameStamp {
                            receive_ts: TimestampNs(i64::MAX),
                            mono_ns: 2
                        }
                    )
                    .is_err()
            );
        }
        assert!(!signal.load(Ordering::Acquire), "reconnect={reconnect}");
        assert_ne!(runner.lifecycle, SessionLifecycle::Live);
    }
}
