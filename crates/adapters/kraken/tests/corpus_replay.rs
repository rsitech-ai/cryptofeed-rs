//! Checked-in raw replay corpus for Kraken Spot + Futures (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_spot_trade_quote_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_spot_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_futures_ticker_liq_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_futures_l2_book_corpus -- --ignored`

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
use marketfeed_adapter_kraken::{
    KrakenFuturesSession, KrakenFuturesSessionConfig, KrakenSessionConfig, KrakenSpotSession,
};
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_trade_quote.mfr")
}

fn l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_l2_book.mfr")
}

fn spot_session(enable_l2: bool) -> KrakenSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC/USD".into(), InstrumentId(1));
    KrakenSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(7), CatalogVersion(1)),
        KrakenSessionConfig {
            symbols: vec!["BTC/USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..KrakenSessionConfig::default()
        },
    )
}

fn record_frames(enable_l2: bool, frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(spot_session(enable_l2)),
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

fn replay_events(enable_l2: bool, bytes: Vec<u8>) -> Vec<MarketEvent> {
    let mut machine = spot_session(enable_l2);
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

fn trade_quote_frames() -> [&'static str; 3] {
    [
        r#"{"method":"subscribe","success":true,"result":{"channel":"trade","symbol":"BTC/USD"}}"#,
        r#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"sell","price":1.00,"qty":1,"ord_type":"market","trade_id":1,"timestamp":"2023-09-25T07:49:37.708706Z"}]}"#,
        r#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":1.00,"bid_qty":1,"ask":1.01,"ask_qty":1,"last":1,"volume":1,"vwap":1,"low":1,"high":1,"change":0,"change_pct":0}]}"#,
    ]
}

/// Golden snapshot + two CRC32-verified updates (second checksum computed offline).
fn l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"method":"subscribe","success":true,"result":{"channel":"book","symbol":"BTC/USD"}}"#,
        include_str!("fixtures/l2_snapshot.json").trim_end(),
        include_str!("fixtures/l2_update.json").trim_end(),
        include_str!("fixtures/l2_update2.json").trim_end(),
    ]
}

#[test]
fn corpus_spot_trade_quote_replays_identically() {
    let bytes = std::fs::read(trade_quote_corpus_path())
        .expect("missing tests/corpus/spot_trade_quote.mfr — run regen with REGEN_CORPUS=1");
    assert!(!bytes.is_empty());
    let events = replay_events(false, bytes.clone());
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert_eq!(events, replay_events(false, bytes));
}

#[test]
fn corpus_spot_trade_quote_matches_live_record() {
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
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_spot_trade_quote_corpus -- --ignored"]
fn regen_spot_trade_quote_corpus() {
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
fn corpus_spot_l2_book_replays_identically() {
    let bytes = std::fs::read(l2_corpus_path()).expect(
        "missing tests/corpus/spot_l2_book.mfr — run regen_spot_l2_book_corpus with REGEN_CORPUS=1",
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
fn corpus_spot_l2_book_matches_live_record() {
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
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_spot_l2_book_corpus -- --ignored"]
fn regen_spot_l2_book_corpus() {
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

// --- Kraken Futures (VenueId 13): ticker enrich + liq, L2 ---

fn futures_ticker_liq_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/futures_ticker_liq.mfr")
}

fn futures_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/futures_l2_book.mfr")
}

fn futures_session(enable_l2: bool) -> KrakenFuturesSession {
    let mut ids = HashMap::new();
    ids.insert("PF_XBTUSD".into(), InstrumentId(1));
    KrakenFuturesSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(13), CatalogVersion(1)),
        KrakenFuturesSessionConfig {
            symbols: vec!["PF_XBTUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..KrakenFuturesSessionConfig::default()
        },
    )
}

fn record_futures_frames(enable_l2: bool, frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(futures_session(enable_l2)),
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

fn replay_futures_events(enable_l2: bool, bytes: Vec<u8>) -> Vec<MarketEvent> {
    let mut machine = futures_session(enable_l2);
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

/// Liquidation trade + ticker carrying mark/index/funding/OI (W2-P0a offline proof).
fn futures_ticker_liq_frames() -> [&'static str; 2] {
    [
        r#"{"feed":"trade","product_id":"PF_XBTUSD","uid":"liq-uid-1","side":"buy","type":"liquidation","seq":42,"time":1612269657781,"qty":100,"price":35000}"#,
        r#"{"time":1676393235406,"product_id":"PF_XBTUSD","funding_rate":0.0001,"next_funding_rate_time":1676394000000,"feed":"ticker","bid":21978.5,"ask":21987.0,"bid_size":2536.0,"ask_size":13948.0,"index":21984.54,"openInterest":30072580.0,"markPrice":21979.5}"#,
    ]
}

fn futures_l2_corpus_frames() -> [&'static str; 3] {
    [
        r#"{"feed":"book_snapshot","product_id":"PF_XBTUSD","timestamp":1612269825817,"seq":10,"bids":[{"price":34892.5,"qty":6385}],"asks":[{"price":34911.5,"qty":20598}]}"#,
        r#"{"feed":"book","product_id":"PF_XBTUSD","side":"buy","seq":11,"price":34892.5,"qty":7000,"timestamp":1612269953629}"#,
        r#"{"feed":"book","product_id":"PF_XBTUSD","side":"sell","seq":12,"price":34911.5,"qty":0,"timestamp":1612269953630}"#,
    ]
}

#[test]
fn corpus_futures_ticker_liq_replays_identically() {
    let bytes = std::fs::read(futures_ticker_liq_corpus_path()).expect(
        "missing tests/corpus/futures_ticker_liq.mfr — run regen_futures_ticker_liq_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty());
    let events = replay_futures_events(false, bytes.clone());
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::Liquidation(_))),
        "expected Liquidation in futures ticker/liq corpus"
    );
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::MarkPrice(_)))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::IndexPrice(_)))
    );
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Funding(_))));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::OpenInterest(_)))
    );
    assert_eq!(events, replay_futures_events(false, bytes));
}

#[test]
fn corpus_futures_ticker_liq_matches_live_record() {
    let (live_bytes, live_events) = record_futures_frames(false, &futures_ticker_liq_frames());
    let corpus = std::fs::read(futures_ticker_liq_corpus_path())
        .expect("missing corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in futures ticker/liq corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_futures_events(false, corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_futures_ticker_liq_corpus -- --ignored"]
fn regen_futures_ticker_liq_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_futures_frames(false, &futures_ticker_liq_frames());
    let path = futures_ticker_liq_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

#[test]
fn corpus_futures_l2_book_replays_identically() {
    let bytes = std::fs::read(futures_l2_corpus_path()).expect(
        "missing tests/corpus/futures_l2_book.mfr — run regen_futures_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "futures L2 corpus file empty");
    let events = replay_futures_events(true, bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in futures L2 corpus replay"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta events in futures L2 corpus replay"
    );
    assert_eq!(events, replay_futures_events(true, bytes));
}

#[test]
fn corpus_futures_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_futures_frames(true, &futures_l2_corpus_frames());
    let corpus = std::fs::read(futures_l2_corpus_path())
        .expect("missing futures L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in futures L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_futures_events(true, corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-kraken --test corpus_replay regen_futures_l2_book_corpus -- --ignored"]
fn regen_futures_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_futures_frames(true, &futures_l2_corpus_frames());
    let path = futures_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}
