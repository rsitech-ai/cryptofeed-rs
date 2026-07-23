//! Coin-M L2: depth buffer overflow while still buffering pre-snapshot →
//! invalidate + reconnect (mirrors `usdm_l2_buffer.rs`).

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, ReconnectReason, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{BinanceCoinmSession, BinanceCoinmSessionConfig};
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

fn l2_session() -> BinanceCoinmSession {
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

    let level = r#"["100.0","1"]"#;
    let bids_json = std::iter::repeat_n(level, 180_000)
        .collect::<Vec<_>>()
        .join(",");
    let mut huge = format!(
        r#"{{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSD_PERP","U":1,"u":2,"pu":0,"b":[{bids_json}],"a":[]}}"#
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
        br#"{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSD_PERP","U":1,"u":2,"pu":0,"b":[],"a":[]}"#
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
        br#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSD_PERP","U":3,"u":4,"pu":2,"b":[],"a":[]}"#
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
