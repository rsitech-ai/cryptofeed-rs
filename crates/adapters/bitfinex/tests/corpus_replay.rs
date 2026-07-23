//! Checked-in raw replay corpora for Bitfinex spot (**17**) + deriv (**20**) L2 (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bitfinex --test corpus_replay regen_ -- --ignored`

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
use marketfeed_adapter_bitfinex::{
    BITFINEX_DERIV_VENUE_ID, BITFINEX_VENUE_ID, BitfinexSession, BitfinexSessionConfig,
};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, OverflowPolicy, SessionId,
    TimestampNs,
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

fn spot_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_l2_book.mfr")
}

fn deriv_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/deriv_l2_book.mfr")
}

fn spot_l2_session() -> BitfinexSession {
    let mut ids = HashMap::new();
    ids.insert("tBTCUSD".into(), InstrumentId(1));
    BitfinexSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(BITFINEX_VENUE_ID, CatalogVersion(1)),
        BitfinexSessionConfig {
            venue: BITFINEX_VENUE_ID,
            symbols: vec!["tBTCUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            ..BitfinexSessionConfig::default()
        },
    )
}

fn deriv_l2_session() -> BitfinexSession {
    let mut ids = HashMap::new();
    ids.insert("tBTCF0:USTF0".into(), InstrumentId(1));
    BitfinexSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(BITFINEX_DERIV_VENUE_ID, CatalogVersion(1)),
        BitfinexSessionConfig {
            venue: BITFINEX_DERIV_VENUE_ID,
            symbols: vec!["tBTCF0:USTF0".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            poll_deriv_status: true,
            ..BitfinexSessionConfig::default()
        },
    )
}

fn record_frames(
    mut make: impl FnMut() -> BitfinexSession,
    frames: &[&str],
) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(make()),
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

fn replay_events(mut make: impl FnMut() -> BitfinexSession, bytes: Vec<u8>) -> Vec<MarketEvent> {
    let mut machine = make();
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

/// subscribed + book snapshot + upsert delta + delete delta.
fn spot_l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"event":"subscribed","channel":"book","chanId":20,"symbol":"tBTCUSD","pair":"BTCUSD"}"#,
        r#"[20,[[29000.0,2,1.5],[29001.0,1,-2.0]]]"#,
        r#"[20,[29000.0,3,1.8]]"#,
        r#"[20,[29001.0,0,-1]]"#,
    ]
}

fn deriv_l2_corpus_frames() -> [&'static str; 4] {
    [
        r#"{"event":"subscribed","channel":"book","chanId":40,"symbol":"tBTCF0:USTF0","pair":"BTCF0:USTF0"}"#,
        r#"[40,[[65000.0,2,1.5],[65001.0,1,-2.0]]]"#,
        r#"[40,[65000.0,3,1.8]]"#,
        r#"[40,[65001.0,0,-1]]"#,
    ]
}

fn assert_l2_corpus(events: &[MarketEvent]) {
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
}

#[test]
fn corpus_spot_l2_book_replays_identically() {
    let bytes = std::fs::read(spot_l2_corpus_path()).expect(
        "missing tests/corpus/spot_l2_book.mfr — run regen_spot_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "L2 corpus file empty");
    let events = replay_events(spot_l2_session, bytes.clone());
    assert_l2_corpus(&events);
    assert_eq!(events, replay_events(spot_l2_session, bytes));
}

#[test]
fn corpus_spot_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames(spot_l2_session, &spot_l2_corpus_frames());
    let corpus = std::fs::read(spot_l2_corpus_path())
        .expect("missing L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(spot_l2_session, corpus));
}

#[test]
fn corpus_deriv_l2_book_replays_identically() {
    let bytes = std::fs::read(deriv_l2_corpus_path()).expect(
        "missing tests/corpus/deriv_l2_book.mfr — run regen_deriv_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "deriv L2 corpus file empty");
    let events = replay_events(deriv_l2_session, bytes.clone());
    assert_l2_corpus(&events);
    assert_eq!(events, replay_events(deriv_l2_session, bytes));
}

#[test]
fn corpus_deriv_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_frames(deriv_l2_session, &deriv_l2_corpus_frames());
    let corpus = std::fs::read(deriv_l2_corpus_path())
        .expect("missing deriv L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in deriv L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(deriv_l2_session, corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bitfinex --test corpus_replay regen_spot_l2_book_corpus -- --ignored"]
fn regen_spot_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(spot_l2_session, &spot_l2_corpus_frames());
    let path = spot_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-bitfinex --test corpus_replay regen_deriv_l2_book_corpus -- --ignored"]
fn regen_deriv_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(deriv_l2_session, &deriv_l2_corpus_frames());
    let path = deriv_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}
