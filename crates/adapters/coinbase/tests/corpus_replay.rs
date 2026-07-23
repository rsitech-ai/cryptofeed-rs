//! Checked-in raw replay corpus for Coinbase Exchange spot L2 + REST candles
//! and Advanced Trade L2 (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-coinbase --test corpus_replay regen_spot_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-coinbase --test corpus_replay regen_spot_candles_rest_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-coinbase --test corpus_replay regen_adv_l2_book_corpus -- --ignored`

use bytes::Bytes;
use marketfeed_adapter_api::{CandleInterval, ConcreteSubscriptionSet, HttpResponse, SessionSpec};
use marketfeed_adapter_coinbase::{
    CoinbaseAdvSession, CoinbaseAdvSessionConfig, CoinbaseSessionConfig, CoinbaseSpotSession,
};
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

fn l2_session() -> CoinbaseSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USD".into(), InstrumentId(1));
    CoinbaseSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(16), CatalogVersion(1)),
        CoinbaseSessionConfig {
            products: vec!["BTC-USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 2,
            qty_scale: 8,
            ..CoinbaseSessionConfig::default()
        },
    )
}

fn fixture_recording(frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut writer = RawSegmentWriter::create(Cursor::new(Vec::new()), 0).unwrap();
    let subscribe = br#"{"channels":["matches","ticker","heartbeat","status","level2"],"product_ids":["BTC-USD"],"type":"subscribe"}"#;
    writer
        .write_record(
            SessionId(1),
            0,
            100,
            100,
            Direction::Outbound,
            FrameOpcode::Text,
            0,
            subscribe,
        )
        .unwrap();
    for (index, frame) in frames.iter().enumerate() {
        let sequence = index as u64 + 1;
        let timestamp = 101 + index as i64;
        writer
            .write_record(
                SessionId(1),
                sequence,
                timestamp,
                timestamp as u64,
                Direction::Inbound,
                FrameOpcode::Text,
                0,
                frame.as_bytes(),
            )
            .unwrap();
    }
    let bytes = writer.into_inner().into_inner();
    let events = replay_events(bytes.clone());
    (bytes, events)
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

/// Snapshot + two l2update deltas (upsert/delete then second upsert).
fn l2_corpus_frames() -> [&'static str; 3] {
    [
        r#"{"type":"snapshot","product_id":"BTC-USD","bids":[["101.10","1.5"],["101.00","2.0"]],"asks":[["101.20","3.0"],["101.30","0.5"]]}"#,
        r#"{"type":"l2update","product_id":"BTC-USD","time":"2014-11-07T08:19:27.028459Z","changes":[["buy","101.10","0"],["sell","101.25","1.25"]]}"#,
        r#"{"type":"l2update","product_id":"BTC-USD","time":"2014-11-07T08:19:28.000000Z","changes":[["buy","101.00","2.5"]]}"#,
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
    assert_eq!(events, replay_events(bytes));
}

#[test]
fn corpus_spot_l2_book_matches_fixture_recording() {
    let (fixture_bytes, fixture_events) = fixture_recording(&l2_corpus_frames());
    let corpus =
        std::fs::read(l2_corpus_path()).expect("missing L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        fixture_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(fixture_events, replay_events(corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-coinbase --test corpus_replay regen_spot_l2_book_corpus -- --ignored"]
fn regen_spot_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = fixture_recording(&l2_corpus_frames());
    let path = l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

// --- REST candles: MFR1 v2 records the exact HTTP response. ---

fn candle_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_candles_rest.mfr")
}

fn candle_rest_body() -> &'static [u8] {
    br#"[[1609459200,"0.0015","0.0025","0.0010","0.0020","1000"]]"#
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

fn candle_session() -> CoinbaseSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USD".into(), InstrumentId(1));
    CoinbaseSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(16), CatalogVersion(1)),
        CoinbaseSessionConfig {
            products: vec!["BTC-USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: vec![CandleInterval::M1],
            price_scale: 2,
            qty_scale: 8,
            ..CoinbaseSessionConfig::default()
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
        .find(|r| r.url.contains("/candles"))
        .map(|r| r.id)
        .expect("coinbase candles request");
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
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-coinbase --test corpus_replay regen_spot_candles_rest_corpus -- --ignored"]
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

// --- Advanced Trade VenueId 18 L2 corpus ---

fn adv_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/adv_l2_book.mfr")
}

fn adv_l2_session() -> CoinbaseAdvSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USD".into(), InstrumentId(1));
    CoinbaseAdvSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(18), CatalogVersion(1)),
        CoinbaseAdvSessionConfig {
            products: vec!["BTC-USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 2,
            qty_scale: 8,
            ..CoinbaseAdvSessionConfig::default()
        },
    )
}

fn adv_record_frames(frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(adv_l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            venue: VenueId(18),
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

fn adv_replay_events(bytes: Vec<u8>) -> Vec<MarketEvent> {
    let mut machine = adv_l2_session();
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

/// Adv wire: snapshot + two l2_data updates (delete/upsert then second upsert).
fn adv_l2_corpus_frames() -> [&'static str; 3] {
    [
        r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:50.714964855Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"101.10","new_quantity":"1.5"},{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"101.00","new_quantity":"2.0"},{"side":"ask","event_time":"1970-01-01T00:00:00Z","price_level":"101.20","new_quantity":"3.0"},{"side":"ask","event_time":"1970-01-01T00:00:00Z","price_level":"101.30","new_quantity":"0.5"}]}]}"#,
        r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:51Z","sequence_num":1,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2014-11-07T08:19:27.028459Z","price_level":"101.10","new_quantity":"0"},{"side":"ask","event_time":"2014-11-07T08:19:27.028459Z","price_level":"101.25","new_quantity":"1.25"}]}]}"#,
        r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:52Z","sequence_num":2,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2014-11-07T08:19:28.000000Z","price_level":"101.00","new_quantity":"2.5"}]}]}"#,
    ]
}

#[test]
fn corpus_adv_l2_book_replays_identically() {
    let bytes = std::fs::read(adv_l2_corpus_path()).expect(
        "missing tests/corpus/adv_l2_book.mfr — run regen_adv_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "Adv L2 corpus file empty");
    let events = adv_replay_events(bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in Adv L2 corpus replay"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta events in Adv L2 corpus replay"
    );
    assert_eq!(events, adv_replay_events(bytes));
}

#[test]
fn corpus_adv_l2_book_matches_live_record() {
    let (live_bytes, live_events) = adv_record_frames(&adv_l2_corpus_frames());
    let corpus = std::fs::read(adv_l2_corpus_path())
        .expect("missing Adv L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in Adv L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, adv_replay_events(corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-coinbase --test corpus_replay regen_adv_l2_book_corpus -- --ignored"]
fn regen_adv_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = adv_record_frames(&adv_l2_corpus_frames());
    let path = adv_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}
