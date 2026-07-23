//! Checked-in raw replay corpus for Deribit (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-deribit --test corpus_replay regen_perp_trade_ticker_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-deribit --test corpus_replay regen_perp_l2_book_corpus -- --ignored`

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
use marketfeed_adapter_deribit::{DeribitSession, DeribitSessionConfig};
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

fn trade_ticker_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/perp_trade_ticker.mfr")
}

fn l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/perp_l2_book.mfr")
}

fn perp_session(enable_l2: bool) -> DeribitSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-PERPETUAL".into(), InstrumentId(1));
    DeribitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(8), CatalogVersion(1)),
        DeribitSessionConfig {
            instruments: vec!["BTC-PERPETUAL".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..DeribitSessionConfig::default()
        },
    )
}

fn record_frames(enable_l2: bool, frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(perp_session(enable_l2)),
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
    let mut machine = perp_session(enable_l2);
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

fn trade_ticker_frames() -> [&'static str; 3] {
    [
        r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#,
        r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.raw","data":[{"trade_seq":1,"trade_id":"1","timestamp":1,"price":1.0,"amount":1,"direction":"buy","instrument_name":"BTC-PERPETUAL"}]}}"#,
        r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"ticker.BTC-PERPETUAL.100ms","data":{"timestamp":2,"instrument_name":"BTC-PERPETUAL","best_bid_price":1.0,"best_bid_amount":1,"best_ask_price":1.1,"best_ask_amount":1,"mark_price":1.05,"index_price":1.04,"funding_8h":0.0001,"open_interest":10}}}"#,
    ]
}

/// Snapshot + two `prev_change_id` / `change_id` contiguous deltas.
fn l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#,
        include_str!("fixtures/l2_snapshot.json").trim_end(),
        include_str!("fixtures/l2_update.json").trim_end(),
        include_str!("fixtures/l2_update2.json").trim_end(),
    ]
}

#[test]
fn corpus_perp_trade_ticker_replays_identically() {
    let bytes = std::fs::read(trade_ticker_corpus_path())
        .expect("missing tests/corpus/perp_trade_ticker.mfr — run regen with REGEN_CORPUS=1");
    assert!(!bytes.is_empty());
    let events = replay_events(false, bytes.clone());
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert_eq!(events, replay_events(false, bytes));
}

#[test]
fn corpus_perp_trade_ticker_matches_live_record() {
    let (live_bytes, live_events) = record_frames(false, &trade_ticker_frames());
    let corpus = std::fs::read(trade_ticker_corpus_path())
        .expect("missing corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(false, corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-deribit --test corpus_replay regen_perp_trade_ticker_corpus -- --ignored"]
fn regen_perp_trade_ticker_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(false, &trade_ticker_frames());
    let path = trade_ticker_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

#[test]
fn corpus_perp_l2_book_replays_identically() {
    let bytes = std::fs::read(l2_corpus_path()).expect(
        "missing tests/corpus/perp_l2_book.mfr — run regen_perp_l2_book_corpus with REGEN_CORPUS=1",
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
fn corpus_perp_l2_book_matches_live_record() {
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
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-deribit --test corpus_replay regen_perp_l2_book_corpus -- --ignored"]
fn regen_perp_l2_book_corpus() {
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
