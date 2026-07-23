//! Checked-in raw replay corpus for Bybit linear + inverse (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_linear_trade_quote_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_linear_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_inverse_l2_book_corpus -- --ignored`

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
use marketfeed_adapter_bybit::{BybitCategory, BybitSession, BybitSessionConfig};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, OverflowPolicy, SessionId,
    TimestampNs, VenueId,
};
use marketfeed_replay::ReplayRunner;
use std::collections::HashMap;
use std::path::PathBuf;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn trade_quote_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/linear_trade_quote.mfr")
}

fn l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/linear_l2_book.mfr")
}

fn inverse_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/inverse_l2_book.mfr")
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

fn record_frames_with(session: BybitSession, frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(session),
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
        // SessionRunner injects VenueStatus; ReplayRunner does not — compare MD only.
        .filter(|e| !matches!(e, MarketEvent::VenueStatus(_)))
        .collect();
    (live.recording_bytes().unwrap(), events)
}

fn record_frames(enable_l2: bool, frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    record_frames_with(linear_session(enable_l2), frames)
}

fn replay_events_with(mut machine: BybitSession, bytes: Vec<u8>) -> Vec<MarketEvent> {
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

fn replay_events(enable_l2: bool, bytes: Vec<u8>) -> Vec<MarketEvent> {
    replay_events_with(linear_session(enable_l2), bytes)
}

fn trade_quote_frames() -> [&'static str; 3] {
    [
        r#"{"success":true,"ret_msg":"","op":"subscribe"}"#,
        r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Sell","v":"1","p":"1.00","L":"MinusTick","i":"c-1","seq":1}]}"#,
        r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":2,"data":{"s":"BTCUSDT","b":[["1.00","1"]],"a":[["1.01","1"]],"u":1,"seq":1}}"#,
    ]
}

/// Snapshot + two contiguous `u` deltas (`u == previous_u + 1`).
fn l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"success":true,"ret_msg":"","op":"subscribe"}"#,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"],["99.50","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","1.2"]],"a":[],"u":11,"seq":101}}"#,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":3,"data":{"s":"BTCUSDT","b":[["99.50","0"]],"a":[["101.50","0.5"]],"u":12,"seq":102}}"#,
    ]
}

#[test]
fn corpus_linear_trade_quote_replays_identically() {
    let bytes = std::fs::read(trade_quote_corpus_path())
        .expect("missing tests/corpus/linear_trade_quote.mfr — run regen with REGEN_CORPUS=1");
    assert!(!bytes.is_empty());
    let events = replay_events(false, bytes.clone());
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert_eq!(events, replay_events(false, bytes));
}

#[test]
fn corpus_linear_trade_quote_matches_live_record() {
    let (live_bytes, live_events) = record_frames(false, &trade_quote_frames());
    let corpus = std::fs::read(trade_quote_corpus_path())
        .expect("missing corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(false, corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_linear_trade_quote_corpus -- --ignored"]
fn regen_linear_trade_quote_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(false, &trade_quote_frames());
    let path = trade_quote_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

#[test]
fn corpus_linear_l2_book_replays_identically() {
    let bytes = std::fs::read(l2_corpus_path()).expect(
        "missing tests/corpus/linear_l2_book.mfr — run regen_linear_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "L2 corpus file empty");
    let events = replay_events(true, bytes.clone());
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
    assert_eq!(events, replay_events(true, bytes));
}

#[test]
fn corpus_linear_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames(true, &l2_corpus_frames());
    let corpus =
        std::fs::read(l2_corpus_path()).expect("missing L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(true, corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_linear_l2_book_corpus -- --ignored"]
fn regen_linear_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(true, &l2_corpus_frames());
    let path = l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

/// Inverse uses the same `orderbook.{depth}.{symbol}` shape as linear; corpus
/// proves VenueId(11) / BTCUSD path has L2 parity (R7).
fn inverse_l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"success":true,"ret_msg":"","op":"subscribe"}"#,
        r#"{"topic":"orderbook.50.BTCUSD","type":"snapshot","ts":1,"data":{"s":"BTCUSD","b":[["100.00","1.0"],["99.50","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        r#"{"topic":"orderbook.50.BTCUSD","type":"delta","ts":2,"data":{"s":"BTCUSD","b":[["100.00","1.2"]],"a":[],"u":11,"seq":101}}"#,
        r#"{"topic":"orderbook.50.BTCUSD","type":"delta","ts":3,"data":{"s":"BTCUSD","b":[["99.50","0"]],"a":[["101.50","0.5"]],"u":12,"seq":102}}"#,
    ]
}

#[test]
fn corpus_inverse_l2_book_replays_identically() {
    let bytes = std::fs::read(inverse_l2_corpus_path()).expect(
        "missing tests/corpus/inverse_l2_book.mfr — run regen_inverse_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "inverse L2 corpus file empty");
    let events = replay_events_with(inverse_session(true), bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in inverse L2 corpus replay"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta events in inverse L2 corpus replay"
    );
    assert_eq!(events, replay_events_with(inverse_session(true), bytes));
}

#[test]
fn corpus_inverse_l2_book_matches_live_record() {
    let (live_bytes, live_events) =
        record_frames_with(inverse_session(true), &inverse_l2_corpus_frames());
    let corpus = std::fs::read(inverse_l2_corpus_path())
        .expect("missing inverse L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in inverse L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(
        live_events,
        replay_events_with(inverse_session(true), corpus)
    );
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bybit --test corpus_replay regen_inverse_l2_book_corpus -- --ignored"]
fn regen_inverse_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames_with(inverse_session(true), &inverse_l2_corpus_frames());
    let path = inverse_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}
