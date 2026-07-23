//! Fixture-driven OKX private account session (offline, no live keys).

use marketfeed_adapter_api::{ReconnectReason, SessionAction, SessionInput};
use marketfeed_model::{Fixed, InstrumentId, Quantity, SessionId, TimestampNs};
use marketfeed_private::{
    AccountEvent, OKX_SPOT_VENUE_ID, OkxPrivateConfig, OkxPrivateSession, PrivateActionBuffer,
    PrivateSessionAction, PrivateSessionMachine,
};
use std::collections::HashMap;

fn session() -> OkxPrivateSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USDT".into(), InstrumentId(1));
    OkxPrivateSession::new(OkxPrivateConfig {
        session: SessionId(1),
        venue: OKX_SPOT_VENUE_ID,
        login_payload: marketfeed_private::FIXTURE_LOGIN_PAYLOAD.into(),
        instrument_ids: ids,
        ..OkxPrivateConfig::default()
    })
}

fn connect_and_auth(s: &mut OkxPrivateSession) {
    let mut out = PrivateActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        PrivateSessionAction::Session(SessionAction::SendSensitiveText(_))
    )));

    let out = drive_text(s, include_str!("fixtures/okx_login_ok.json"));
    assert!(s.is_live());
    assert!(s.is_authed());
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, PrivateSessionAction::Session(SessionAction::MarkLive)))
    );
    let sub = out.as_slice().iter().find_map(|a| match a {
        PrivateSessionAction::Session(SessionAction::SendText(b)) => {
            Some(std::str::from_utf8(b).unwrap().to_string())
        }
        _ => None,
    });
    let sub = sub.expect("subscribe SendText");
    assert!(sub.contains(r#""channel":"account""#));
    assert!(sub.contains(r#""channel":"orders""#));
}

fn drive_text(s: &mut OkxPrivateSession, text: &str) -> PrivateActionBuffer {
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
fn login_fail_reconnects() {
    let mut s = session();
    let mut out = PrivateActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let out = drive_text(&mut s, include_str!("fixtures/okx_login_fail.json"));
    assert!(!s.is_live());
    assert!(!s.is_authed());
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        PrivateSessionAction::Session(SessionAction::Reconnect(ReconnectReason::Protocol))
    )));
}

#[test]
fn account_balances() {
    let mut s = session();
    connect_and_auth(&mut s);
    let ev = accounts(&drive_text(
        &mut s,
        include_str!("fixtures/okx_account.json"),
    ));
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
fn orders_new_and_fill() {
    let mut s = session();
    connect_and_auth(&mut s);

    let new_ev = accounts(&drive_text(
        &mut s,
        include_str!("fixtures/okx_order_new.json"),
    ));
    assert_eq!(new_ev.len(), 1);
    match &new_ev[0] {
        AccountEvent::Order(o) => {
            assert_eq!(o.instrument, InstrumentId(1));
            assert_eq!(o.status.as_deref(), Some("live"));
            assert_eq!(o.client_order_id.as_deref(), Some("client-1"));
        }
        other => panic!("expected Order, got {other:?}"),
    }

    let fill_ev = accounts(&drive_text(
        &mut s,
        include_str!("fixtures/okx_order_fill.json"),
    ));
    assert_eq!(fill_ev.len(), 2);
    assert!(
        matches!(&fill_ev[0], AccountEvent::Order(o) if o.execution_type.as_deref() == Some("TRADE"))
    );
    match &fill_ev[1] {
        AccountEvent::Fill(f) => {
            assert_eq!(f.price.0, Fixed::parse_str("0.10264410").unwrap());
            assert_eq!(f.quantity.0, Fixed::parse_str("0.05000000").unwrap());
            assert_eq!(f.trade_id.as_deref(), Some("12345"));
            assert_eq!(
                f.fee,
                Some(Quantity(Fixed::parse_str("-0.00000100").unwrap()))
            );
        }
        other => panic!("expected Fill, got {other:?}"),
    }
}
