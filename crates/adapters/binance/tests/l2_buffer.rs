//! Offline L2: buffer depth → HTTP snapshot → drain → live → gap reconnect.

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, HttpResponse, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{BinanceSessionConfig, BinanceSpotSession};
use marketfeed_engine::{SessionLifecycle, SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, OverflowPolicy, SessionId,
    TimestampNs, VenueId,
};
use marketfeed_recording::{Direction, FrameOpcode, RawSegmentReader, decode_http_response};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn l2_session() -> BinanceSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    BinanceSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(2), CatalogVersion(1)),
        BinanceSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 2,
            qty_scale: 8,
            ..BinanceSessionConfig::default()
        },
    )
}

#[test]
fn buffer_then_snapshot_drains_and_goes_live() {
    let mut s = l2_session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    // Pre-snapshot depth events (will be buffered).
    // After snapshot lastUpdateId=100: drop u<=100, keep [96,102] (bridges) then [103,103].
    for (text, ts) in [
        (
            r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":90,"u":95,"b":[["100.00","1"]],"a":[["101.00","1"]]}"#,
            2i64,
        ),
        (
            r#"{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":96,"u":102,"b":[["100.00","1.1"]],"a":[]}"#,
            3,
        ),
        (
            r#"{"e":"depthUpdate","E":3,"s":"BTCUSDT","U":103,"u":103,"b":[["99.00","2"]],"a":[]}"#,
            4,
        ),
    ] {
        let mut bytes = text.as_bytes().to_vec();
        s.on_input(
            SessionInput::TextFrame {
                bytes: &mut bytes,
                received: stamp(ts),
            },
            &mut out,
        )
        .unwrap();
    }

    // Snapshot lastUpdateId=100 bridges event [96,102]; first discarded (u=95<=100).
    let snap = br#"{"lastUpdateId":100,"bids":[["100.00","1.0"],["99.50","1.0"]],"asks":[["101.00","1.5"]]}"#;
    out.clear();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: 1,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(snap),
            },
            received: stamp(3),
        },
        &mut out,
    )
    .unwrap();

    let payloads: Vec<_> = out
        .as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitBatch(b) => Some(b.events.iter().map(|e| e.payload.clone())),
            _ => None,
        })
        .flatten()
        .collect();
    assert!(
        payloads
            .iter()
            .any(|p| matches!(p, MarketEvent::BookSnapshot(_)))
    );
    // Buffered U=101 event should drain as delta.
    assert!(
        payloads
            .iter()
            .any(|p| matches!(p, MarketEvent::BookDelta(_)))
    );
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
}

#[test]
fn stale_snapshot_requests_again() {
    let mut s = l2_session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    // First buffered U=200.
    let mut bytes =
        br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":200,"u":201,"b":[["100.00","1"]],"a":[["101.00","1"]]}"#
            .to_vec();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp(2),
        },
        &mut out,
    )
    .unwrap();

    // Snapshot lastUpdateId=50 < first.U=200 → re-request (id=2).
    out.clear();
    let snap = br#"{"lastUpdateId":50,"bids":[["100.00","1"]],"asks":[["101.00","1"]]}"#;
    s.on_input(
        SessionInput::HttpResponse {
            request_id: 1,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(snap),
            },
            received: stamp(3),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::RequestHttp(r) if r.id == 2))
    );
}

#[tokio::test]
async fn runner_http_snapshot_then_live() {
    let mut runner = SessionRunner::new(
        Box::new(l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            record: true,
            overflow: OverflowPolicy::FailEngine,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();
    let reqs = runner.take_pending_http();
    assert_eq!(reqs.len(), 1);
    assert!(reqs[0].url.contains("depth"));

    // Buffer a bridging depth event before snapshot response.
    let mut depth =
        br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":8,"u":12,"b":[["100.00","1.2"]],"a":[]}"#
            .to_vec();
    runner.on_text_frame(&mut depth, stamp(2)).unwrap();

    let resp = HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: Bytes::from_static(
            br#"{"lastUpdateId":10,"bids":[["100.00","1"]],"asks":[["101.00","1"]]}"#,
        ),
    };
    runner
        .on_http_response(reqs[0].id, &resp, stamp(3))
        .unwrap();
    assert_eq!(runner.lifecycle, SessionLifecycle::Live);
    let snapshot = runner
        .market_batches
        .iter()
        .flat_map(|b| &b.events)
        .find(|e| matches!(e.payload, MarketEvent::BookSnapshot(_)))
        .expect("book snapshot");
    assert_eq!(snapshot.receive_ts, TimestampNs(3));
    assert!(
        runner
            .market_batches
            .iter()
            .flat_map(|b| &b.events)
            .any(|e| matches!(e.payload, MarketEvent::BookDelta(_)))
    );

    let mut recording =
        RawSegmentReader::from_bytes(runner.recording_bytes().expect("recording enabled")).unwrap();
    let records = recording.read_all().unwrap();
    let http_record = records
        .iter()
        .find(|record| {
            record.header.direction == Direction::Inbound
                && record.header.opcode == FrameOpcode::HttpResponse
        })
        .expect("recorded HTTP snapshot");
    assert_eq!(http_record.header.receive_ts_ns, 3);
    assert_eq!(http_record.header.monotonic_ns, 3);
    let (recorded_request_id, recorded_response) =
        decode_http_response(&http_record.payload).unwrap();
    assert_eq!(recorded_request_id, reqs[0].id);
    assert_eq!(recorded_response, resp);
}
