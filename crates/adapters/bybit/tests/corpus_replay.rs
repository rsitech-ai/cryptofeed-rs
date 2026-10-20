//! Versioned synthetic correlated-request corpora plus frozen legacy quarantine.
//! Original .mfr files are immutable. Creation commands below only create new
//! *_correlated_v1.mfr files and refuse to overwrite existing files.

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionAction, SessionSpec};
use marketfeed_adapter_bybit::{BybitCategory, BybitSession, BybitSessionConfig};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, EventEnvelope, FrameStamp, InstrumentId, MarketEvent,
    OverflowPolicy, SessionId, SystemEvent, TimestampNs, VenueId,
};
use marketfeed_replay::ReplayRunner;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn trade_quote_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/corpus/linear_trade_quote_correlated_v1.mfr")
}

fn l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/linear_l2_book_correlated_v1.mfr")
}

fn inverse_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/inverse_l2_book_correlated_v1.mfr")
}

fn category_session(
    category: BybitCategory,
    venue: VenueId,
    symbol: &str,
    enable_l2: bool,
) -> BybitSession {
    let mut ids = HashMap::new();
    ids.insert(symbol.into(), InstrumentId(1));
    BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(venue, CatalogVersion(1)),
        BybitSessionConfig {
            category,
            symbols: vec![symbol.into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..BybitSessionConfig::default()
        },
    )
}

fn linear_session(enable_l2: bool) -> BybitSession {
    category_session(BybitCategory::Linear, VenueId(5), "BTCUSDT", enable_l2)
}

fn inverse_session(enable_l2: bool) -> BybitSession {
    category_session(BybitCategory::Inverse, VenueId(11), "BTCUSD", enable_l2)
}

fn record_frames_with(
    session: BybitSession,
    frames: &[&str],
    venue: VenueId,
) -> (Vec<u8>, Vec<EventEnvelope>) {
    let mut live = SessionRunner::new(
        Box::new(session),
        SessionRunnerConfig {
            venue,
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
    assert_eq!(live.lifecycle, marketfeed_engine::SessionLifecycle::Live);
    assert_eq!(
        live.other_actions
            .iter()
            .filter(|a| matches!(a, SessionAction::MarkLive))
            .count(),
        1
    );
    let events: Vec<_> = live
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().cloned())
        // SessionRunner injects VenueStatus; ReplayRunner does not — compare MD only.
        .filter(|e| !matches!(e.payload, MarketEvent::VenueStatus(_)))
        .collect();
    (live.recording_bytes().unwrap(), events)
}

fn record_frames(enable_l2: bool, frames: &[&str]) -> (Vec<u8>, Vec<EventEnvelope>) {
    record_frames_with(linear_session(enable_l2), frames, VenueId(5))
}

fn replay_events_with(mut machine: BybitSession, bytes: Vec<u8>) -> Vec<EventEnvelope> {
    let mut replay = ReplayRunner::new(64);
    let outcome = replay
        .replay_bytes(&mut machine, bytes, TimestampNs(100))
        .unwrap();
    assert_eq!(
        outcome
            .other_actions
            .iter()
            .filter(|a| matches!(a, SessionAction::MarkLive))
            .count(),
        1
    );
    assert!(
        !outcome
            .other_actions
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(_)))
    );
    assert!(
        outcome.system_events.iter().any(
            |e| matches!(e,SystemEvent::SubscriptionStateChanged{state} if state=="subscribed")
        )
    );
    outcome
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().cloned())
        .collect()
}

fn replay_events(enable_l2: bool, bytes: Vec<u8>) -> Vec<EventEnvelope> {
    replay_events_with(linear_session(enable_l2), bytes)
}

// MFR1 alone omits the collector's physical connection identity. The current
// adapter ReplayRunner also retains adapter ordinals rather than raw ordinals.
// This challenge verifies semantics and all remaining envelope fields, exact
// raw/outbound bytes, and real startup admission. It does NOT qualify complete
// runtime-envelope replay; the authoritative shared-feed boundary owns that.
fn assert_mfr_semantic_envelopes(live: &[EventEnvelope], replay: &[EventEnvelope], venue: VenueId) {
    assert!(!live.is_empty());
    assert_eq!(live.len(), replay.len());
    for (index, (captured, replayed)) in live.iter().zip(replay).enumerate() {
        assert_eq!(captured.venue, venue);
        assert_eq!(replayed.venue, venue);
        assert_eq!(captured.session, SessionId(1));
        assert!(captured.connection.0 > 0);
        assert_eq!(replayed.connection.0, 1);
        assert_eq!(replayed.session, SessionId(1));
        assert_eq!(replayed.frame_seq, u64::try_from(index + 1).unwrap());
        // Each fixture begins with one ACK, then one event per raw input.
        assert_eq!(captured.frame_seq, u64::try_from(index + 2).unwrap());
        let mut semantic_capture = captured.clone();
        semantic_capture.connection = replayed.connection;
        semantic_capture.frame_seq = replayed.frame_seq;
        assert_eq!(&semantic_capture, replayed);
    }
}

fn trade_quote_frames() -> [&'static str; 3] {
    [
        r#"{"success":true,"ret_msg":"","op":"subscribe","req_id":"b0000000000000001-0000","conn_id":"synthetic-correlated-v1"}"#,
        r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Sell","v":"1","p":"1.00","L":"MinusTick","i":"c-1","seq":1}]}"#,
        r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":2,"data":{"s":"BTCUSDT","b":[["1.00","1"]],"a":[["1.01","1"]],"u":1,"seq":1}}"#,
    ]
}

/// Snapshot + two contiguous `u` deltas (`u == previous_u + 1`).
fn l2_corpus_frames() -> [&'static str; 5] {
    [
        r#"{"success":true,"ret_msg":"","op":"subscribe","req_id":"b0000000000000001-0000","conn_id":"synthetic-correlated-v1"}"#,
        r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"],["99.50","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","1.2"]],"a":[],"u":11,"seq":101}}"#,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":3,"data":{"s":"BTCUSDT","b":[["99.50","0"]],"a":[["101.50","0.5"]],"u":12,"seq":102}}"#,
    ]
}

#[test]
fn corpus_linear_trade_quote_replays_identically() {
    let bytes = std::fs::read(trade_quote_corpus_path())
        .expect("missing tests/corpus/linear_trade_quote_correlated_v1.mfr — run create separately named correlated fixture with CREATE_CORRELATED_CORPUS=1");
    assert!(!bytes.is_empty());
    let events = replay_events(false, bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e.payload, MarketEvent::Trade(_)))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.payload, MarketEvent::Quote(_)))
    );
    assert_eq!(events, replay_events(false, bytes));
}

#[test]
fn corpus_linear_trade_quote_matches_live_record() {
    let (live_bytes, live_events) = record_frames(false, &trade_quote_frames());
    let corpus = std::fs::read(trade_quote_corpus_path())
        .expect("missing corpus — create separately named correlated fixture with CREATE_CORRELATED_CORPUS=1");
    assert_eq!(
        live_bytes, corpus,
        "checked-in corpus drifted; create separately named correlated fixture with CREATE_CORRELATED_CORPUS=1"
    );
    assert_mfr_semantic_envelopes(&live_events, &replay_events(false, corpus), VenueId(5));
}

#[test]
#[ignore = "regen only: CREATE_CORRELATED_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_linear_trade_quote_corpus -- --ignored"]
fn regen_linear_trade_quote_corpus() {
    assert_eq!(
        std::env::var("CREATE_CORRELATED_CORPUS").ok().as_deref(),
        Some("1"),
        "set CREATE_CORRELATED_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(false, &trade_quote_frames());
    let path = trade_quote_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
}

#[test]
fn corpus_linear_l2_book_replays_identically() {
    let bytes = std::fs::read(l2_corpus_path()).expect(
        "missing tests/corpus/linear_l2_book_correlated_v1.mfr — run regen_linear_l2_book_corpus with CREATE_CORRELATED_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "L2 corpus file empty");
    let events = replay_events(true, bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e.payload, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in L2 corpus replay"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e.payload, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta events in L2 corpus replay"
    );
    assert_eq!(events, replay_events(true, bytes));
}

#[test]
fn corpus_linear_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames(true, &l2_corpus_frames());
    let corpus =
        std::fs::read(l2_corpus_path()).expect("missing L2 corpus — create separately named correlated fixture with CREATE_CORRELATED_CORPUS=1");
    assert_eq!(
        live_bytes, corpus,
        "checked-in L2 corpus drifted; create separately named correlated fixture with CREATE_CORRELATED_CORPUS=1"
    );
    assert_mfr_semantic_envelopes(&live_events, &replay_events(true, corpus), VenueId(5));
}

#[test]
#[ignore = "regen only: CREATE_CORRELATED_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_linear_l2_book_corpus -- --ignored"]
fn regen_linear_l2_book_corpus() {
    assert_eq!(
        std::env::var("CREATE_CORRELATED_CORPUS").ok().as_deref(),
        Some("1"),
        "set CREATE_CORRELATED_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(true, &l2_corpus_frames());
    let path = l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
}

/// Inverse uses the same `orderbook.{depth}.{symbol}` shape as linear; corpus
/// proves VenueId(11) / BTCUSD path has L2 parity (R7).
fn inverse_l2_corpus_frames() -> [&'static str; 5] {
    [
        r#"{"success":true,"ret_msg":"","op":"subscribe","req_id":"b0000000000000001-0000","conn_id":"synthetic-correlated-v1"}"#,
        r#"{"topic":"orderbook.1.BTCUSD","type":"snapshot","ts":1,"data":{"s":"BTCUSD","b":[["100.00","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        r#"{"topic":"orderbook.50.BTCUSD","type":"snapshot","ts":1,"data":{"s":"BTCUSD","b":[["100.00","1.0"],["99.50","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        r#"{"topic":"orderbook.50.BTCUSD","type":"delta","ts":2,"data":{"s":"BTCUSD","b":[["100.00","1.2"]],"a":[],"u":11,"seq":101}}"#,
        r#"{"topic":"orderbook.50.BTCUSD","type":"delta","ts":3,"data":{"s":"BTCUSD","b":[["99.50","0"]],"a":[["101.50","0.5"]],"u":12,"seq":102}}"#,
    ]
}

#[test]
fn corpus_inverse_l2_book_replays_identically() {
    let bytes = std::fs::read(inverse_l2_corpus_path()).expect(
        "missing tests/corpus/inverse_l2_book_correlated_v1.mfr — run regen_inverse_l2_book_corpus with CREATE_CORRELATED_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "inverse L2 corpus file empty");
    let events = replay_events_with(inverse_session(true), bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e.payload, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in inverse L2 corpus replay"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e.payload, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta events in inverse L2 corpus replay"
    );
    assert_eq!(events, replay_events_with(inverse_session(true), bytes));
}

#[test]
fn corpus_inverse_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames_with(
        inverse_session(true),
        &inverse_l2_corpus_frames(),
        VenueId(11),
    );
    let corpus = std::fs::read(inverse_l2_corpus_path())
        .expect("missing inverse L2 corpus — create separately named correlated fixture with CREATE_CORRELATED_CORPUS=1");
    assert_eq!(
        live_bytes, corpus,
        "checked-in inverse L2 corpus drifted; create separately named correlated fixture with CREATE_CORRELATED_CORPUS=1"
    );
    assert_mfr_semantic_envelopes(
        &live_events,
        &replay_events_with(inverse_session(true), corpus),
        VenueId(11),
    );
}

#[test]
#[ignore = "regen only: CREATE_CORRELATED_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_inverse_l2_book_corpus -- --ignored"]
fn regen_inverse_l2_book_corpus() {
    assert_eq!(
        std::env::var("CREATE_CORRELATED_CORPUS").ok().as_deref(),
        Some("1"),
        "set CREATE_CORRELATED_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames_with(
        inverse_session(true),
        &inverse_l2_corpus_frames(),
        VenueId(11),
    );
    let path = inverse_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
}

#[test]
fn frozen_uncorrelated_corpora_remain_unchanged_and_are_explicitly_quarantined() {
    for (name, hash, l2, inverse) in [
        (
            "linear_trade_quote.mfr",
            "d35c1831b3f080aa668e5450901cac67df0351d1842449ef4cd8517412f88f8b",
            false,
            false,
        ),
        (
            "linear_l2_book.mfr",
            "a069e5755a5ddcbc621898933c132cdb13fa66c768ca506a28cf5d36e7bd8bb0",
            true,
            false,
        ),
        (
            "inverse_l2_book.mfr",
            "d6d519e9ed5d19c9108da2393fd9b0acb42fe682044ce023952a5728779e3351",
            true,
            true,
        ),
    ] {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/corpus")
            .join(name);
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), hash);
        let mut machine = if inverse {
            inverse_session(l2)
        } else {
            linear_session(l2)
        };
        let outcome = ReplayRunner::new(64)
            .replay_bytes(&mut machine, bytes, TimestampNs(100))
            .unwrap();
        assert!(outcome.market_batches.iter().all(|b| b.events.is_empty()));
        assert!(
            !outcome
                .other_actions
                .iter()
                .any(|a| matches!(a, SessionAction::MarkLive))
        );
        assert!(
            outcome
                .other_actions
                .iter()
                .any(|a| matches!(a, SessionAction::MarkDegraded))
        );
        assert!(
            outcome
                .other_actions
                .iter()
                .any(|a| matches!(a, SessionAction::Reconnect(_)))
        );
        assert!(outcome.system_events.iter().any(|e|matches!(e,SystemEvent::ParseError{detail} if detail.contains("subscription response"))));
    }
}
