//! Checked-in raw replay corpus for Binance Spot, USD-M, and Coin-M (offline CI).
//!
//! Regenerate with:
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_spot_trade_quote_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_usdm_mark_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_spot_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_usdm_l2_book_corpus -- --ignored`
//! `REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_coinm_l2_book_corpus -- --ignored`
//!
//! Trade/quote/mark corpora record WebSocket inputs. L2 book corpora record the
//! pre-snapshot WebSocket updates, exact REST snapshot response, and post-snapshot
//! updates in MFR1 v2. All paths replay through `ReplayRunner`. Gap/overflow stay
//! in `l2_buffer.rs` / `usdm_l2_buffer.rs` / `coinm_l2_buffer.rs`.

use bytes::Bytes;
use marketfeed_adapter_api::{ConcreteSubscriptionSet, HttpResponse, SessionSpec};
use marketfeed_adapter_binance::{
    BinanceCoinmSession, BinanceCoinmSessionConfig, BinanceSessionConfig, BinanceSpotSession,
    BinanceUsdmSession, BinanceUsdmSessionConfig,
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

fn corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_trade_quote.mfr")
}

fn spot_session() -> BinanceSpotSession {
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
            enable_l2: false,
            ..BinanceSessionConfig::default()
        },
    )
}

fn record_frames(frames: &[&str]) -> (Vec<u8>, Vec<marketfeed_model::MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(spot_session()),
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

fn replay_events(bytes: Vec<u8>) -> Vec<marketfeed_model::MarketEvent> {
    let mut machine = spot_session();
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

fn corpus_frames() -> [&'static str; 3] {
    [
        r#"{"result":null,"id":1}"#,
        r#"{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1.00","q":"1","T":1,"m":true,"M":true}"#,
        r#"{"u":1,"s":"BTCUSDT","b":"1.00","B":"1","a":"1.01","A":"1"}"#,
    ]
}

#[test]
fn corpus_spot_trade_quote_replays_identically() {
    let bytes = std::fs::read(corpus_path()).expect(
        "missing tests/corpus/spot_trade_quote.mfr — run regen_spot_trade_quote_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "corpus file empty");
    let events = replay_events(bytes.clone());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, marketfeed_model::MarketEvent::Trade(_)))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, marketfeed_model::MarketEvent::Quote(_)))
    );
    assert_eq!(events, replay_events(bytes));
}

#[test]
fn corpus_spot_trade_quote_matches_live_record() {
    let (live_bytes, live_events) = record_frames(&corpus_frames());
    let corpus = std::fs::read(corpus_path()).expect("missing corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_events(corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_spot_trade_quote_corpus -- --ignored"]
fn regen_spot_trade_quote_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_frames(&corpus_frames());
    let path = corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

// --- USD-M: aggTrade + quote + markPriceUpdate bundle (mark/index/funding). ---

fn usdm_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/usdm_mark.mfr")
}

fn usdm_session() -> BinanceUsdmSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    BinanceUsdmSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(3), CatalogVersion(1)),
        BinanceUsdmSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: false,
            price_scale: 1,
            qty_scale: 1,
            ..BinanceUsdmSessionConfig::default()
        },
    )
}

fn record_usdm_frames(frames: &[&str]) -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(usdm_session()),
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

fn replay_usdm_events(bytes: Vec<u8>) -> Vec<MarketEvent> {
    let mut machine = usdm_session();
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

fn corpus_usdm_frames() -> [&'static str; 3] {
    [
        r#"{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"1.0","q":"1","f":1,"l":1,"T":1,"m":true}"#,
        r#"{"u":1,"s":"BTCUSDT","b":"1.0","B":"1","a":"1.1","A":"1"}"#,
        r#"{"e":"markPriceUpdate","E":10,"s":"BTCUSDT","p":"1.0","i":"1.0","P":"1.0","r":"0.0001","T":20}"#,
    ]
}

#[test]
fn corpus_usdm_mark_replays_identically() {
    let bytes = std::fs::read(usdm_corpus_path()).expect(
        "missing tests/corpus/usdm_mark.mfr — run regen_usdm_mark_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "corpus file empty");
    let events = replay_usdm_events(bytes.clone());
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
    assert!(events.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::MarkPrice(_)))
    );
    assert_eq!(events, replay_usdm_events(bytes));
}

#[test]
fn corpus_usdm_mark_matches_live_record() {
    let (live_bytes, live_events) = record_usdm_frames(&corpus_usdm_frames());
    let corpus =
        std::fs::read(usdm_corpus_path()).expect("missing corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_usdm_events(corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_usdm_mark_corpus -- --ignored"]
fn regen_usdm_mark_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_usdm_frames(&corpus_usdm_frames());
    let path = usdm_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

// --- Spot L2: buffer → recorded REST snapshot → live deltas. ---

fn spot_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/spot_l2_book.mfr")
}

fn spot_l2_snapshot_body() -> &'static [u8] {
    include_str!("fixtures/spot_l2_snapshot.json")
        .trim_end()
        .as_bytes()
}

fn spot_l2_session() -> BinanceSpotSession {
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

/// Pre-snapshot buffered depths + post-snapshot live deltas.
fn spot_l2_pre_frames() -> [&'static str; 2] {
    [
        // Dropped after snap (u <= lastUpdateId).
        r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":90,"u":95,"b":[["100.00","1"]],"a":[["101.00","1"]]}"#,
        // Bridges snap lastUpdateId=100; drained as BookDelta with snapshot.
        r#"{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":96,"u":102,"b":[["100.00","1.1"]],"a":[]}"#,
    ]
}

fn spot_l2_post_frames() -> [&'static str; 2] {
    [
        r#"{"e":"depthUpdate","E":3,"s":"BTCUSDT","U":103,"u":103,"b":[["99.00","2"]],"a":[]}"#,
        r#"{"e":"depthUpdate","E":4,"s":"BTCUSDT","U":104,"u":105,"b":[["98.50","1"]],"a":[["101.50","0.5"]]}"#,
    ]
}

fn record_spot_l2() -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(spot_l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::FailEngine,
            record: true,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    live.on_connected(TimestampNs(100)).unwrap();
    let depth_id = live
        .take_pending_http()
        .into_iter()
        .find(|r| r.url.contains("/depth?"))
        .map(|r| r.id)
        .expect("spot depth snapshot request");

    let mut ts = 100i64;
    for f in spot_l2_pre_frames() {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    ts += 1;
    live.on_http_response(
        depth_id,
        &HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::copy_from_slice(spot_l2_snapshot_body()),
        },
        stamp(ts),
    )
    .unwrap();
    for f in spot_l2_post_frames() {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    let events: Vec<_> = live
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .filter(|event| !matches!(event, MarketEvent::VenueStatus(_)))
        .collect();
    (live.recording_bytes().unwrap(), events)
}

fn replay_spot_l2(bytes: &[u8]) -> Vec<MarketEvent> {
    let mut session = spot_l2_session();
    ReplayRunner::new(1024)
        .replay_bytes(&mut session, bytes.to_vec(), TimestampNs(100))
        .unwrap()
        .market_batches
        .iter()
        .flat_map(|batch| batch.events.iter().map(|event| event.payload.clone()))
        .collect()
}

#[test]
fn corpus_spot_l2_book_replays_identically() {
    let bytes = std::fs::read(spot_l2_corpus_path()).expect(
        "missing tests/corpus/spot_l2_book.mfr — run regen_spot_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "spot L2 corpus empty");
    let events = replay_spot_l2(&bytes);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in spot L2 corpus"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta in spot L2 corpus, got {events:?}"
    );
    assert_eq!(events, replay_spot_l2(&bytes));
}

#[test]
fn corpus_spot_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_spot_l2();
    let corpus = std::fs::read(spot_l2_corpus_path())
        .expect("missing spot L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in spot L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_spot_l2(&corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_spot_l2_book_corpus -- --ignored"]
fn regen_spot_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_spot_l2();
    let path = spot_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

// --- USD-M L2: buffer → REST snapshot (`pu` bridge) → live deltas. ---

fn usdm_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/usdm_l2_book.mfr")
}

fn usdm_l2_snapshot_body() -> &'static [u8] {
    include_str!("fixtures/usdm_l2_snapshot.json")
        .trim_end()
        .as_bytes()
}

fn usdm_l2_session() -> BinanceUsdmSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    BinanceUsdmSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(3), CatalogVersion(1)),
        BinanceUsdmSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 1,
            qty_scale: 1,
            ..BinanceUsdmSessionConfig::default()
        },
    )
}

fn usdm_l2_pre_frames() -> [&'static str; 1] {
    [
        // Bridges snap lastUpdateId=100 via pu; drained with snapshot.
        r#"{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":100,"u":102,"pu":100,"b":[["100.0","1.0"]],"a":[["101.0","2.0"]]}"#,
    ]
}

fn usdm_l2_post_frames() -> [&'static str; 2] {
    [
        r#"{"e":"depthUpdate","E":3,"T":3,"s":"BTCUSDT","U":103,"u":104,"pu":102,"b":[["99.0","0.5"]],"a":[]}"#,
        r#"{"e":"depthUpdate","E":4,"T":4,"s":"BTCUSDT","U":105,"u":106,"pu":104,"b":[["98.0","1"]],"a":[["102.0","1"]]}"#,
    ]
}

fn record_usdm_l2() -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(usdm_l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::FailEngine,
            record: true,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    live.on_connected(TimestampNs(100)).unwrap();
    let depth_id = live
        .take_pending_http()
        .into_iter()
        .find(|r| r.url.contains("/depth?"))
        .map(|r| r.id)
        .expect("usdm depth snapshot request");

    let mut ts = 100i64;
    for f in usdm_l2_pre_frames() {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    ts += 1;
    live.on_http_response(
        depth_id,
        &HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::copy_from_slice(usdm_l2_snapshot_body()),
        },
        stamp(ts),
    )
    .unwrap();
    for f in usdm_l2_post_frames() {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    let events: Vec<_> = live
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .filter(|event| !matches!(event, MarketEvent::VenueStatus(_)))
        .collect();
    (live.recording_bytes().unwrap(), events)
}

fn replay_usdm_l2(bytes: &[u8]) -> Vec<MarketEvent> {
    let mut session = usdm_l2_session();
    ReplayRunner::new(1024)
        .replay_bytes(&mut session, bytes.to_vec(), TimestampNs(100))
        .unwrap()
        .market_batches
        .iter()
        .flat_map(|batch| batch.events.iter().map(|event| event.payload.clone()))
        .collect()
}

#[test]
fn corpus_usdm_l2_book_replays_identically() {
    let bytes = std::fs::read(usdm_l2_corpus_path()).expect(
        "missing tests/corpus/usdm_l2_book.mfr — run regen_usdm_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "usdm L2 corpus empty");
    let events = replay_usdm_l2(&bytes);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in usdm L2 corpus"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta in usdm L2 corpus, got {events:?}"
    );
    assert_eq!(events, replay_usdm_l2(&bytes));
}

#[test]
fn corpus_usdm_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_usdm_l2();
    let corpus = std::fs::read(usdm_l2_corpus_path())
        .expect("missing usdm L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in usdm L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_usdm_l2(&corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_usdm_l2_book_corpus -- --ignored"]
fn regen_usdm_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_usdm_l2();
    let path = usdm_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

// --- Coin-M L2: buffer → REST snapshot (`pu` on dapi depth) → live deltas. ---

fn coinm_l2_corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/coinm_l2_book.mfr")
}

fn coinm_l2_snapshot_body() -> &'static [u8] {
    include_str!("fixtures/coinm_l2_snapshot.json")
        .trim_end()
        .as_bytes()
}

fn coinm_l2_session() -> BinanceCoinmSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSD_PERP".into(), InstrumentId(1));
    BinanceCoinmSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(12), CatalogVersion(1)),
        BinanceCoinmSessionConfig {
            symbols: vec!["BTCUSD_PERP".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 1,
            qty_scale: 0,
            ..BinanceCoinmSessionConfig::default()
        },
    )
}

fn coinm_l2_pre_frames() -> [&'static str; 1] {
    [
        // Bridges snap lastUpdateId=100 via pu; drained with snapshot.
        r#"{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSD_PERP","U":100,"u":102,"pu":100,"b":[["100.0","1"]],"a":[["101.0","2"]]}"#,
    ]
}

fn coinm_l2_post_frames() -> [&'static str; 2] {
    [
        r#"{"e":"depthUpdate","E":3,"T":3,"s":"BTCUSD_PERP","U":103,"u":104,"pu":102,"b":[["99.0","1"]],"a":[]}"#,
        r#"{"e":"depthUpdate","E":4,"T":4,"s":"BTCUSD_PERP","U":105,"u":106,"pu":104,"b":[["98.0","1"]],"a":[["102.0","1"]]}"#,
    ]
}

fn record_coinm_l2() -> (Vec<u8>, Vec<MarketEvent>) {
    let mut live = SessionRunner::new(
        Box::new(coinm_l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::FailEngine,
            record: true,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    live.on_connected(TimestampNs(100)).unwrap();
    let depth_id = live
        .take_pending_http()
        .into_iter()
        .find(|r| r.url.contains("/dapi/v1/depth?"))
        .map(|r| r.id)
        .expect("coinm dapi depth snapshot request");

    let mut ts = 100i64;
    for f in coinm_l2_pre_frames() {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    ts += 1;
    live.on_http_response(
        depth_id,
        &HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::copy_from_slice(coinm_l2_snapshot_body()),
        },
        stamp(ts),
    )
    .unwrap();
    for f in coinm_l2_post_frames() {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    let events: Vec<_> = live
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .filter(|event| !matches!(event, MarketEvent::VenueStatus(_)))
        .collect();
    (live.recording_bytes().unwrap(), events)
}

fn replay_coinm_l2(bytes: &[u8]) -> Vec<MarketEvent> {
    let mut session = coinm_l2_session();
    ReplayRunner::new(1024)
        .replay_bytes(&mut session, bytes.to_vec(), TimestampNs(100))
        .unwrap()
        .market_batches
        .iter()
        .flat_map(|batch| batch.events.iter().map(|event| event.payload.clone()))
        .collect()
}

#[test]
fn corpus_coinm_l2_book_replays_identically() {
    let bytes = std::fs::read(coinm_l2_corpus_path()).expect(
        "missing tests/corpus/coinm_l2_book.mfr — run regen_coinm_l2_book_corpus with REGEN_CORPUS=1",
    );
    assert!(!bytes.is_empty(), "coinm L2 corpus empty");
    let events = replay_coinm_l2(&bytes);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
        "expected BookSnapshot in coinm L2 corpus"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, MarketEvent::BookDelta(_)))
            .count()
            >= 2,
        "expected ≥2 BookDelta in coinm L2 corpus, got {events:?}"
    );
    assert_eq!(events, replay_coinm_l2(&bytes));
}

#[test]
fn corpus_coinm_l2_book_matches_live_record() {
    let (live_bytes, live_events) = record_coinm_l2();
    let corpus = std::fs::read(coinm_l2_corpus_path())
        .expect("missing coinm L2 corpus — regen with REGEN_CORPUS=1");
    assert_eq!(
        live_bytes.as_slice(),
        corpus.as_slice(),
        "checked-in coinm L2 corpus drifted; regen with REGEN_CORPUS=1"
    );
    assert_eq!(live_events, replay_coinm_l2(&corpus));
}

#[test]
#[ignore = "regen only: REGEN_CORPUS=1 cargo test -p marketfeed-adapter-binance --test corpus_replay regen_coinm_l2_book_corpus -- --ignored"]
fn regen_coinm_l2_book_corpus() {
    assert_eq!(
        std::env::var("REGEN_CORPUS").ok().as_deref(),
        Some("1"),
        "set REGEN_CORPUS=1 to write corpus"
    );
    let (bytes, _) = record_coinm_l2();
    let path = coinm_l2_corpus_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}
