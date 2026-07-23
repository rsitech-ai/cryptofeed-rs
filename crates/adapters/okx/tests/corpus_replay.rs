//! Checked-in raw replay corpus for OKX Spot + SWAP/Futures L2 (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_spot_trade_quote_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_spot_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_swap_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_futures_l2_book_corpus -- --ignored`

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
use marketfeed_adapter_okx::{OkxSession, OkxSessionConfig};
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

fn corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_trade_quote.mfr")
}

fn l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_l2_book.mfr")
}

fn swap_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/swap_l2_book.mfr")
}

fn futures_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/futures_l2_book.mfr")
}

fn okx_session(venue: VenueId, symbol: &str, enable_l2: bool) -> OkxSession {
    let mut ids = HashMap::new();
    ids.insert(symbol.into(), InstrumentId(1));
    OkxSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(venue, CatalogVersion(1)),
        OkxSessionConfig {
            symbols: vec![symbol.into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            venue,
            subscribe_mark_funding: venue == VenueId(9) || venue == VenueId(10),
            // Derivatives use coarser lot sizes than spot.
            price_scale: 1,
            qty_scale: if venue == VenueId(4) { 8 } else { 0 },
            ..OkxSessionConfig::default()
        },
    )
}

fn spot_session(enable_l2: bool) -> OkxSession {
    okx_session(VenueId(4), "BTC-USDT", enable_l2)
}

fn record_frames_with(session: OkxSession, frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
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
    record_frames_with(spot_session(enable_l2), frames)
}

fn replay_events_with(mut machine: OkxSession, bytes: Vec<u8>) -> Vec<MarketEvent> {
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
    replay_events_with(spot_session(enable_l2), bytes)
}

fn corpus_frames() -> [&'static str; 3] {
    [
        r#"{"id":"1","event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"},"connId":"x"}"#,
        r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"1.0","sz":"1","side":"sell","ts":"1","seqId":1}]}"#,
        r#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","last":"1.0","lastSz":"1","askPx":"1.1","askSz":"1","bidPx":"1.0","bidSz":"1","open24h":"0","high24h":"0","low24h":"0","volCcy24h":"0","vol24h":"0","sodUtc0":"0","sodUtc8":"0","ts":"2"}]}"#,
    ]
}

/// Snapshot + two continuous deltas (`prevSeqId` chain) for Spot books.
fn l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"id":"1","event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"},"connId":"x"}"#,
        include_str!("fixtures/l2_snapshot.json").trim_end(),
        include_str!("fixtures/l2_update.json").trim_end(),
        include_str!("fixtures/l2_update2.json").trim_end(),
    ]
}

#[test]
fn corpus_spot_trade_quote_replays_identically() {
    let bytes = std::fs::read(corpus_path())
        .expect("missing tests/corpus/spot_trade_quote.mfr — run regen with REGEN_CORPUS=1");
    assert!(!bytes.is_empty());
    let events = replay_events(false, bytes.clone());
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::Statistics24h(_)))
    );
    assert_eq!(events, replay_events(false, bytes));
}

#[test]
fn corpus_spot_trade_quote_matches_live_record() {
    let (live_bytes, live_events) = record_frames(false, &corpus_frames());
    let corpus = std::fs::read(corpus_path()).expect("missing corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(false, corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_spot_trade_quote_corpus -- --ignored"]
fn regen_spot_trade_quote_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(false, &corpus_frames());
    let path = corpus_path();
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
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_spot_l2_book_corpus -- --ignored"]
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

/// SWAP/Futures books share the Spot `books` protocol; dedicated corpora prove
/// VenueId(9)/VenueId(10) instrument ids (R16).
fn swap_l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"id":"1","event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"connId":"x"}"#,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"asks":[["65010.0","2"],["65011.0","1"]],"bids":[["65009.0","1"],["65008.0","3"]],"ts":"1700000000000","checksum":0,"prevSeqId":-1,"seqId":1000}]}"#,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"asks":[["65010.0","2"]],"bids":[["65009.0","2"]],"ts":"1700000001000","checksum":0,"prevSeqId":1000,"seqId":1001}]}"#,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"asks":[],"bids":[["65008.0","0"]],"ts":"1700000002000","checksum":0,"prevSeqId":1001,"seqId":1002}]}"#,
    ]
}

fn futures_l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"id":"1","event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT-250328"},"connId":"x"}"#,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT-250328"},"action":"snapshot","data":[{"asks":[["65010.0","2"],["65011.0","1"]],"bids":[["65009.0","1"],["65008.0","3"]],"ts":"1700000000000","checksum":0,"prevSeqId":-1,"seqId":2000}]}"#,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT-250328"},"action":"update","data":[{"asks":[["65010.0","2"]],"bids":[["65009.0","2"]],"ts":"1700000001000","checksum":0,"prevSeqId":2000,"seqId":2001}]}"#,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT-250328"},"action":"update","data":[{"asks":[],"bids":[["65008.0","0"]],"ts":"1700000002000","checksum":0,"prevSeqId":2001,"seqId":2002}]}"#,
    ]
}

#[test]
fn corpus_swap_l2_book_replays_identically() {
    let bytes = std::fs::read(swap_l2_corpus_path()).expect(
        "missing tests/corpus/swap_l2_book.mfr — run regen_swap_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty());
    let events = replay_events_with(
        okx_session(VenueId(9), "BTC-USDT-SWAP", true),
        bytes.clone(),
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2
    );
    assert_eq!(
        events,
        replay_events_with(okx_session(VenueId(9), "BTC-USDT-SWAP", true), bytes)
    );
}

#[test]
fn corpus_swap_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames_with(
        okx_session(VenueId(9), "BTC-USDT-SWAP", true),
        &swap_l2_corpus_frames(),
    );
    let corpus = std::fs::read(swap_l2_corpus_path())
        .expect("missing swap L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(live_bytes.as_slice(), corpus.as_slice());
    assert_eq!(
        live_events,
        replay_events_with(okx_session(VenueId(9), "BTC-USDT-SWAP", true), corpus)
    );
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_swap_l2_book_corpus -- --ignored"]
fn regen_swap_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames_with(
        okx_session(VenueId(9), "BTC-USDT-SWAP", true),
        &swap_l2_corpus_frames(),
    );
    let path = swap_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

#[test]
fn corpus_futures_l2_book_replays_identically() {
    let bytes = std::fs::read(futures_l2_corpus_path()).expect(
        "missing tests/corpus/futures_l2_book.mfr — run regen_futures_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty());
    let events = replay_events_with(
        okx_session(VenueId(10), "BTC-USDT-250328", true),
        bytes.clone(),
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2
    );
    assert_eq!(
        events,
        replay_events_with(okx_session(VenueId(10), "BTC-USDT-250328", true), bytes)
    );
}

#[test]
fn corpus_futures_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames_with(
        okx_session(VenueId(10), "BTC-USDT-250328", true),
        &futures_l2_corpus_frames(),
    );
    let corpus = std::fs::read(futures_l2_corpus_path())
        .expect("missing futures L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(live_bytes.as_slice(), corpus.as_slice());
    assert_eq!(
        live_events,
        replay_events_with(okx_session(VenueId(10), "BTC-USDT-250328", true), corpus)
    );
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-okx --test corpus_replay regen_futures_l2_book_corpus -- --ignored"]
fn regen_futures_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames_with(
        okx_session(VenueId(10), "BTC-USDT-250328", true),
        &futures_l2_corpus_frames(),
    );
    let path = futures_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}
