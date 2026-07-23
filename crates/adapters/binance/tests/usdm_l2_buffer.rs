//! USD-M L2: depth buffer overflow while still buffering pre-snapshot →
//! invalidate + reconnect (mirrors Spot's `l2_depth_buffer_overflow_reconnects`
//! in `fixtures.rs`; USD-M's per-symbol buffer caps are not currently exposed
//! via `BinanceUsdmSessionConfig`, so this drives the fixed 4 MiB byte cap
//! with one oversized event instead of a configurable event-count cap).

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, ReconnectReason, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{BinanceUsdmSession, BinanceUsdmSessionConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, SessionId, SystemEvent, TimestampNs,
    VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn l2_session() -> BinanceUsdmSession {
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

/// USD-M rule: the pre-snapshot depth buffer is bounded (bytes cap, currently
/// 4 MiB per symbol); an oversized event while still buffering must invalidate
/// the book and reconnect rather than growing memory unbounded.
#[test]
fn depth_buffer_overflow_invalidates_and_reconnects() {
    let mut s = l2_session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    // Snapshot requested but not yet answered -> session is still buffering.

    let level = r#"["100.0","1"]"#;
    let bids_json = std::iter::repeat_n(level, 180_000)
        .collect::<Vec<_>>()
        .join(",");
    let mut huge = format!(
        r#"{{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[{bids_json}],"a":[]}}"#
    )
    .into_bytes();

    out.clear();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut huge,
            received: stamp(2),
        },
        &mut out,
    )
    .unwrap();

    assert!(
        out.as_slice().iter().any(|a| matches!(
            a,
            SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
        )),
        "overflow must invalidate the book: {:?}",
        out.as_slice()
    );
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(ReconnectReason::SequenceGap)))
    );
}

#[test]
fn depth_buffer_time_span_overflow_invalidates_and_reconnects() {
    let mut s = l2_session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let mut first =
        br#"{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]}"#
            .to_vec();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut first,
            received: stamp(2),
        },
        &mut out,
    )
    .unwrap();

    let mut second =
        br#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":3,"u":4,"pu":2,"b":[],"a":[]}"#
            .to_vec();
    out.clear();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut second,
            received: FrameStamp {
                receive_ts: TimestampNs(3),
                mono_ns: 5_000_000_003,
            },
        },
        &mut out,
    )
    .unwrap();

    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::Reconnect(ReconnectReason::SequenceGap)
    )));
}
