//! Fixture-driven Binance Spot user-data private session (offline, no live keys).

use marketfeed_adapter_api::{SessionAction, SessionInput, StopReason};
use marketfeed_model::{Fixed, InstrumentId, Quantity, SessionId, TimestampNs};
use marketfeed_private::{
    AccountEvent, BINANCE_SPOT_VENUE_ID, BinanceSpotUserDataConfig, BinanceSpotUserDataSession,
    PrivateActionBuffer, PrivateSessionAction, PrivateSessionMachine,
};
use std::collections::HashMap;

fn session() -> BinanceSpotUserDataSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    BinanceSpotUserDataSession::new(BinanceSpotUserDataConfig {
        session: SessionId(1),
        venue: BINANCE_SPOT_VENUE_ID,
        instrument_ids: ids,
    })
}

fn drive_text(s: &mut BinanceSpotUserDataSession, text: &str) -> PrivateActionBuffer {
    let mut buf = PrivateActionBuffer::new();
    let mut bytes = text.as_bytes().to_vec();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: marketfeed_model::FrameStamp {
                receive_ts: TimestampNs(2),
                mono_ns: 2,
            },
        },
        &mut buf,
    )
    .unwrap();
    buf
}

fn accounts(buf: &PrivateActionBuffer) -> Vec<AccountEvent> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            PrivateSessionAction::Account(e) => Some(e.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn retired_listen_key_bootstrap_fails_closed_without_http() {
    let mut s = session();
    let mut out = PrivateActionBuffer::new();

    let err = s
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut out,
        )
        .expect_err("retired listen-key protocol must not start");

    assert_eq!(err, marketfeed_private::PrivateError::NotImplemented);
    assert!(!s.is_live());
    assert!(out.as_slice().iter().all(|action| !matches!(
        action,
        PrivateSessionAction::Session(SessionAction::RequestHttp(_))
    )));
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        PrivateSessionAction::Session(SessionAction::StopSession(StopReason::FatalProtocol))
    )));
}

#[test]
fn outbound_account_position_balances() {
    let mut s = session();
    let raw = include_str!("fixtures/outbound_account_position.json");
    let ev = accounts(&drive_text(&mut s, raw));
    assert_eq!(ev.len(), 2);
    match &ev[0] {
        AccountEvent::Balance(b) => {
            assert_eq!(b.asset, "BTC");
            assert_eq!(b.free.0, Fixed::parse_str("0.09905000").unwrap());
            assert_eq!(b.locked.0, Fixed::parse_str("0.00000000").unwrap());
        }
        other => panic!("expected Balance, got {other:?}"),
    }
    match &ev[1] {
        AccountEvent::Balance(b) => {
            assert_eq!(b.asset, "USDT");
            assert_eq!(b.locked.0, Fixed::parse_str("50.00000000").unwrap());
        }
        other => panic!("expected Balance, got {other:?}"),
    }
}

#[test]
fn current_websocket_api_event_wrapper_is_decoded() {
    let mut s = session();
    let raw = include_str!("fixtures/balance_update.json");
    let wrapped = format!(r#"{{"subscriptionId":0,"event":{raw}}}"#);
    let ev = accounts(&drive_text(&mut s, &wrapped));

    assert!(matches!(
        ev.as_slice(),
        [AccountEvent::BalanceDelta(delta)] if delta.asset == "USDT"
    ));
}

#[test]
fn balance_update_signed_delta() {
    let mut s = session();
    let raw = include_str!("fixtures/balance_update.json");
    let ev = accounts(&drive_text(&mut s, raw));
    assert_eq!(ev.len(), 1);
    match &ev[0] {
        AccountEvent::BalanceDelta(d) => {
            assert_eq!(d.asset, "USDT");
            assert_eq!(
                d.delta,
                Quantity(Fixed::parse_str("-100.00000000").unwrap())
            );
        }
        other => panic!("expected BalanceDelta, got {other:?}"),
    }
}

#[test]
fn execution_report_new_and_trade_fill() {
    let mut s = session();

    let new_ev = accounts(&drive_text(
        &mut s,
        include_str!("fixtures/execution_report_new.json"),
    ));
    assert_eq!(new_ev.len(), 1);
    match &new_ev[0] {
        AccountEvent::Order(o) => {
            assert_eq!(o.instrument, InstrumentId(1));
            assert_eq!(o.execution_type.as_deref(), Some("NEW"));
            assert_eq!(o.status.as_deref(), Some("NEW"));
            assert_eq!(o.client_order_id.as_deref(), Some("client-1"));
        }
        other => panic!("expected Order, got {other:?}"),
    }

    let trade_ev = accounts(&drive_text(
        &mut s,
        include_str!("fixtures/execution_report_trade.json"),
    ));
    assert_eq!(trade_ev.len(), 2);
    assert!(
        matches!(&trade_ev[0], AccountEvent::Order(o) if o.execution_type.as_deref() == Some("TRADE"))
    );
    match &trade_ev[1] {
        AccountEvent::Fill(f) => {
            assert_eq!(f.price.0, Fixed::parse_str("0.10264410").unwrap());
            assert_eq!(f.quantity.0, Fixed::parse_str("0.10000000").unwrap());
            assert_eq!(f.trade_id.as_deref(), Some("12345"));
        }
        other => panic!("expected Fill, got {other:?}"),
    }
}
