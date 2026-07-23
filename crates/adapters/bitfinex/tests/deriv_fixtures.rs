//! Offline fixtures for Bitfinex derivatives (VenueId **20**).
use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, Capability, ConcreteSubscriptionSet, HttpResponse, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_bitfinex::{
    BITFINEX_DERIV_SPEC, BITFINEX_DERIV_VENUE_ID, BitfinexSession, BitfinexSessionConfig,
    STATUS_POLL_INTERVAL_MS, STATUS_TIMER_ID,
};
use marketfeed_model::{
    AggressorSide, CatalogVersion, CatalogView, Fixed, FrameStamp, InstrumentId, MarketEvent,
    SessionId, TimestampNs,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session() -> BitfinexSession {
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
            poll_deriv_status: true,
            ..BitfinexSessionConfig::default()
        },
    )
}

fn drive(s: &mut BitfinexSession, text: &str, ts: i64) -> ActionBuffer {
    let mut out = ActionBuffer::new();
    let mut bytes = text.as_bytes().to_vec();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp(ts),
        },
        &mut out,
    )
    .unwrap();
    out
}

#[test]
fn deriv_spec_lists_liquidations() {
    assert!(
        BITFINEX_DERIV_SPEC
            .capabilities
            .contains(&Capability::Liquidations)
    );
}

#[test]
fn status_deriv_fixture_exact() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let req_id = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::RequestHttp(r) if r.url.contains("status/deriv") => Some(r.id),
            _ => None,
        })
        .expect("status/deriv request");
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::ScheduleTimer(t)
            if t.timer_id == STATUS_TIMER_ID
                && t.fire_at.0 == 1 + STATUS_POLL_INTERVAL_MS * 1_000_000
    )));
    assert!(
        out.as_slice().iter().any(|a| match a {
            SessionAction::SendText(b) => {
                let s = std::str::from_utf8(b).unwrap_or("");
                s.contains("liq:global") && s.contains("\"channel\":\"status\"")
            }
            _ => false,
        }),
        "must subscribe status/liq:global: {out:?}"
    );
    let mut status_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: req_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"[["tBTCF0:USTF0",1700000000000,null,65924.12,65889,null,0,null,1700010000000,0,0,null,0.00006854,null,null,65885.8908,null,null,8875.366]]"#,
                ),
            },
            received: stamp(9),
        },
        &mut status_out,
    )
    .unwrap();
    let events: Vec<_> = status_out
        .as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitBatch(b) => Some(b),
            _ => None,
        })
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .collect();
    assert!(matches!(
        &events[0],
        MarketEvent::MarkPrice(p) if p.price.0 == Fixed::parse_str("65885.8908").unwrap()
    ));
    assert!(matches!(
        &events[2],
        MarketEvent::Funding(f) if f.rate.0 == Fixed::parse_str("0.00006854").unwrap()
    ));
    let venue = status_out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::EmitBatch(b) => b.events.first().map(|e| e.venue),
            _ => None,
        })
        .unwrap();
    assert_eq!(venue, BITFINEX_DERIV_VENUE_ID);
}

#[test]
fn liq_global_fixture_exact() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let _ = drive(
        &mut s,
        r#"{"event":"subscribed","channel":"status","chanId":91684,"key":"liq:global"}"#,
        2,
    );
    let liq_out = drive(
        &mut s,
        r#"[91684,[["pos",142397657,1574697680828.2002,null,"tBTCF0:USTF0",-2.62932,91.583875238719,null,1,1,null,112.27]]]"#,
        3,
    );
    let events: Vec<_> = liq_out
        .as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitBatch(b) => Some(b),
            _ => None,
        })
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .collect();
    assert_eq!(events.len(), 1);
    let MarketEvent::Liquidation(l) = &events[0] else {
        panic!("expected Liquidation: {events:?}");
    };
    assert_eq!(l.price.0, Fixed::parse_str("112.27").unwrap());
    assert_eq!(l.quantity.0, Fixed::parse_str("2.62932").unwrap());
    assert_eq!(l.side, AggressorSide::Buy);

    // Unsubscribed symbol filtered out.
    let filtered = drive(
        &mut s,
        r#"[91684,[["pos",1,1574697680828,null,"tETHF0:USTF0",1.0,100.0,null,1,1,null,99.0]]]"#,
        4,
    );
    assert!(!filtered.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitBatch(b) if b.events.iter().any(|e| matches!(e.payload, MarketEvent::Liquidation(_)))
    )));
}
