//! Checked-in raw replay corpus for Gemini spot L2 + REST candles (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-gemini --test corpus_replay regen_spot_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-gemini --test corpus_replay regen_spot_candles_rest_corpus -- --ignored`

use bytes::Bytes;
use marketfeed_adapter_api::{CandleInterval, ConcreteSubscriptionSet, HttpResponse, SessionSpec};
use marketfeed_adapter_gemini::{GeminiSession, GeminiSessionConfig};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, OverflowPolicy, SessionId,
    TimestampNs, VenueId,
};
use marketfeed_recording::{Direction, FrameOpcode, RawSegmentWriter, encode_http_response};
use marketfeed_replay::ReplayRunner;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_l2_book.mfr")
}

fn l2_session() -> GeminiSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSD".into(), InstrumentId(1));
    GeminiSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(15), CatalogVersion(1)),
        GeminiSessionConfig {
            symbols: vec!["BTCUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            ..GeminiSessionConfig::default()
        },
    )
}

fn record_frames(frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::FailEngine,
            record: true,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    live.on_connected(TimestampNs(100)).unwrap();
    let mut ts = 100i64;
    for f in frames {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    let events: Vec<_> = live
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .filter(|e| !matches!(e, MarketEvent::VenueStatus(_)))
        .collect();
    (live.recording_bytes().unwrap(), events)
}

fn replay_events(bytes: Vec<u8>) -> Vec<MarketEvent> {
    let mut machine = l2_session();
    let mut replay = ReplayRunner::new(64);
    let outcome = replay
        .replay_bytes(&mut machine, bytes, TimestampNs(100))
        .unwrap();
    outcome
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .collect()
}

/// Current full depth snapshot + trade + book ticker + two incremental diffs.
fn l2_corpus_frames() -> [&'static str; 5] {
    [
        r#"{"e":"depthUpdate","E":2000000000,"s":"btcusd","U":10,"u":10,"b":[["29000.12","1.50000000"]],"a":[["29001.00","2.00000000"]]}"#,
        r#"{"E":2100000000,"s":"btcusd","t":99,"p":"29000.12","q":"0.10000000","m":false}"#,
        r#"{"u":11,"E":2200000000,"s":"btcusd","b":"29000.12","B":"1.50000000","a":"29001.00","A":"2.00000000"}"#,
        r#"{"e":"depthUpdate","E":2300000000,"s":"btcusd","U":11,"u":11,"b":[["29000.12","1.80000000"]],"a":[]}"#,
        r#"{"e":"depthUpdate","E":2400000000,"s":"btcusd","U":12,"u":12,"b":[],"a":[["29001.00","0"]]}"#,
    ]
}

#[test]
fn corpus_spot_l2_book_replays_identically() {
    let bytes = std::fs::read(l2_corpus_path()).expect(
        "missing tests/corpus/spot_l2_book.mfr — run regen_spot_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "L2 corpus file empty");
    let events = replay_events(bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in L2 corpus replay"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta events in L2 corpus replay"
    );
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert_eq!(events, replay_events(bytes));
}

#[test]
fn corpus_spot_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames(&l2_corpus_frames());
    let corpus =
        std::fs::read(l2_corpus_path()).expect("missing L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-gemini --test corpus_replay regen_spot_l2_book_corpus -- --ignored"]
fn regen_spot_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(&l2_corpus_frames());
    let path = l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

// --- REST candles: MFR1 v2 records the exact HTTP response. ---

fn candle_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_candles_rest.mfr")
}

fn candle_rest_body() -> &'static [u8] {
    br#"[[1609459200000,"0.0010","0.0025","0.0015","0.0020","1000"]]"#
}

fn wrap_rest_body_mfr(body: &[u8]) -> Vec<u8> {
    let response = HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: Bytes::copy_from_slice(body),
    };
    let payload = encode_http_response(1, &response).unwrap();
    let mut w = RawSegmentWriter::create(Cursor::new(Vec::new()), 0).unwrap();
    w.write_record(
        SessionId(1),
        1,
        101,
        101,
        Direction::Inbound,
        FrameOpcode::HttpResponse,
        0,
        &payload,
    )
    .unwrap();
    w.into_inner().into_inner()
}

fn candle_session() -> GeminiSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSD".into(), InstrumentId(1));
    GeminiSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(15), CatalogVersion(1)),
        GeminiSessionConfig {
            symbols: vec!["BTCUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: vec![CandleInterval::M1],
            ..GeminiSessionConfig::default()
        },
    )
}

fn drive_candle_http(body: &[u8]) -> Vec<MarketEvent> {
    let mut live = SessionRunner::new(
        Box::new(candle_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::FailEngine,
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    live.on_connected(TimestampNs(100)).unwrap();
    let req_id = live
        .take_pending_http()
        .into_iter()
        .find(|r| r.url.contains("/candles/"))
        .map(|r| r.id)
        .expect("gemini candles request");
    live.on_http_response(
        req_id,
        &HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::copy_from_slice(body),
        },
        stamp(101),
    )
    .unwrap();
    live.market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .filter(|e| !matches!(e, MarketEvent::VenueStatus(_)))
        .collect()
}

fn replay_candle_corpus(bytes: &[u8]) -> Vec<MarketEvent> {
    let mut session = candle_session();
    ReplayRunner::new(1024)
        .replay_bytes(&mut session, bytes.to_vec(), TimestampNs(100))
        .unwrap()
        .market_batches
        .iter()
        .flat_map(|batch| batch.events.iter().map(|event| event.payload.clone()))
        .filter(|event| !matches!(event, MarketEvent::VenueStatus(_)))
        .collect()
}

#[test]
fn corpus_spot_candles_rest_replays_identically() {
    let bytes = std::fs::read(candle_corpus_path()).expect(
        "missing tests/corpus/spot_candles_rest.mfr — run regen_spot_candles_rest_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "candle corpus file empty");
    let events = replay_candle_corpus(&bytes);
    assert!(
        events.iter().any(|e| matches!(e, MarketEvent::Candle(_))),
        "expected Candle in REST candle corpus replay"
    );
    assert_eq!(events, replay_candle_corpus(&bytes));
}

#[test]
fn corpus_spot_candles_rest_matches_live_http() {
    let corpus = wrap_rest_body_mfr(candle_rest_body());
    let checked = std::fs::read(candle_corpus_path())
        .expect("missing candle corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        corpus.as_slice(),
        checked.as_slice(),
        "checked-in candle corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(
        drive_candle_http(candle_rest_body()),
        replay_candle_corpus(&checked)
    );
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-gemini --test corpus_replay regen_spot_candles_rest_corpus -- --ignored"]
fn regen_spot_candles_rest_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let bytes = wrap_rest_body_mfr(candle_rest_body());
    let path = candle_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}
