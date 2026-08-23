use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, HttpMethod, HttpResponse, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{
    BinanceUsdmRouteV4, BinanceUsdmSession, BinanceUsdmSessionConfig, UsdmDecoded, decode_usdm_text,
};
use marketfeed_model::{
    CatalogVersion, CatalogView, ConnectionId, EventEnvelope, FrameStamp, InstrumentId,
    MarketEvent, SessionId, TimestampNs, VenueId,
};

const PUBLIC_WS: &str = "wss://fstream.binance.com/public/ws";
const MARKET_WS: &str = "wss://fstream.binance.com/market/ws";

fn stamp(value: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(value),
        mono_ns: value as u64,
    }
}

fn routed(route: BinanceUsdmRouteV4) -> BinanceUsdmSession {
    let mut instrument_ids = HashMap::new();
    instrument_ids.insert("BNBUSDT".to_owned(), InstrumentId(7));
    let (endpoint_name, connection, session, enable_l2) = match route {
        BinanceUsdmRouteV4::Public => (PUBLIC_WS, ConnectionId(11), SessionId(21), true),
        BinanceUsdmRouteV4::Market => (MARKET_WS, ConnectionId(12), SessionId(22), false),
    };
    BinanceUsdmSession::try_new_routed_v4(
        SessionSpec {
            endpoint_name: endpoint_name.to_owned(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(3), CatalogVersion(1)),
        BinanceUsdmSessionConfig {
            symbols: vec!["BNBUSDT".to_owned()],
            instrument_ids,
            connection,
            session,
            enable_l2,
            price_scale: 2,
            qty_scale: 3,
            ..BinanceUsdmSessionConfig::default()
        },
        route,
    )
    .expect("valid routed session")
}

fn emitted(out: &ActionBuffer) -> Vec<&EventEnvelope> {
    out.as_slice()
        .iter()
        .filter_map(|action| match action {
            SessionAction::EmitBatch(batch) => Some(batch.events.as_slice()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn drive_text(
    session: &mut BinanceUsdmSession,
    payload: &str,
) -> Result<ActionBuffer, marketfeed_adapter_api::AdapterError> {
    let mut bytes = payload.as_bytes().to_vec();
    let mut out = ActionBuffer::new();
    session.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp(9_000_000_000),
        },
        &mut out,
    )?;
    Ok(out)
}

#[test]
fn routed_sessions_bind_exact_identity_endpoint_and_family() {
    for (route, endpoint) in [
        (BinanceUsdmRouteV4::Public, PUBLIC_WS),
        (BinanceUsdmRouteV4::Market, MARKET_WS),
    ] {
        let mut session = routed(route);
        let mut out = ActionBuffer::new();
        session
            .on_input(
                SessionInput::Connected {
                    now: TimestampNs(1),
                },
                &mut out,
            )
            .unwrap();
        assert!(
            !out.as_slice()
                .iter()
                .any(|action| matches!(action, SessionAction::EmitSystem(_))),
            "routed replay start must be mechanics-safe"
        );
        let subscription = out
            .as_slice()
            .iter()
            .find_map(|action| match action {
                SessionAction::SendText(bytes) => String::from_utf8(bytes.to_vec()).ok(),
                _ => None,
            })
            .unwrap();
        match route {
            BinanceUsdmRouteV4::Public => {
                assert!(subscription.contains("bnbusdt@bookTicker"));
                assert!(subscription.contains("bnbusdt@depth@100ms"));
                assert!(!subscription.contains("aggTrade"));
                assert!(!subscription.contains("forceOrder"));
                assert!(out.as_slice().iter().any(|action| matches!(
                    action,
                    SessionAction::RequestHttp(request)
                        if request.method == HttpMethod::Get
                            && request.url == "https://fapi.binance.com/fapi/v1/depth?symbol=BNBUSDT&limit=1000"
                )));
            }
            BinanceUsdmRouteV4::Market => {
                assert!(subscription.contains("bnbusdt@aggTrade"));
                assert!(subscription.contains("bnbusdt@forceOrder"));
                assert!(!subscription.contains("bookTicker"));
                assert!(!subscription.contains("depth"));
                assert!(out.as_slice().iter().any(|action| matches!(
                    action,
                    SessionAction::RequestHttp(request)
                        if request.method == HttpMethod::Get
                            && request.url == "https://fapi.binance.com/fapi/v1/openInterest?symbol=BNBUSDT"
                )));
            }
        }

        let bad = SessionSpec {
            endpoint_name: format!("{endpoint}/wrong"),
            subscriptions: ConcreteSubscriptionSet::default(),
        };
        let cfg = BinanceUsdmSessionConfig {
            symbols: vec!["BNBUSDT".into()],
            instrument_ids: HashMap::from([("BNBUSDT".into(), InstrumentId(7))]),
            connection: ConnectionId(11),
            session: SessionId(21),
            enable_l2: route == BinanceUsdmRouteV4::Public,
            ..BinanceUsdmSessionConfig::default()
        };
        assert!(
            BinanceUsdmSession::try_new_routed_v4(
                bad,
                CatalogView::new(VenueId(3), CatalogVersion(1)),
                cfg,
                route,
            )
            .is_err()
        );
    }
}

#[test]
fn routed_quote_requires_e_and_t_and_uses_derived_cursor_despite_noncontiguous_u() {
    let mut session = routed(BinanceUsdmRouteV4::Public);
    for (u, transaction_ms) in [(100, 1_784_817_230_005_i64), (105, 1_784_817_230_006)] {
        let raw = format!(
            r#"{{"e":"bookTicker","E":1784817230130,"T":{transaction_ms},"u":{u},"s":"BNBUSDT","b":"650.0","B":"1.2","a":"650.1","A":"0.8"}}"#
        );
        assert!(matches!(
            decode_usdm_text(raw.as_bytes()).unwrap(),
            UsdmDecoded::Quote {
                update_id,
                event_time_ms: Some(1_784_817_230_130),
                transaction_time_ms: Some(value),
                ..
            } if update_id == u && value == transaction_ms
        ));
        let out = drive_text(&mut session, &raw).unwrap();
        let event = emitted(&out)[0];
        assert!(matches!(event.payload, MarketEvent::Quote(_)));
        assert_eq!(
            event.exchange_ts,
            Some(TimestampNs(transaction_ms * 1_000_000))
        );
        assert_eq!(
            event.source_sequence, None,
            "bookTicker u is provenance, not continuity"
        );
    }
    assert!(drive_text(
        &mut session,
        r#"{"e":"bookTicker","E":1784817230130,"u":106,"s":"BNBUSDT","b":"650.0","B":"1.2","a":"650.1","A":"0.8"}"#,
    )
    .is_err());
    assert!(
        drive_text(
            &mut session,
            r#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":9,"p":"650","q":"1","T":2,"m":false}"#,
        )
        .is_err()
    );
    assert!(drive_text(
        &mut session,
        r#"{"e":"bookTicker","E":1,"T":2,"u":107,"s":"BNBUSDC","b":"650","B":"1","a":"651","A":"1"}"#,
    )
    .is_err());
}

#[test]
fn routed_market_preserves_family_source_times_and_native_trade_cursor() {
    let mut session = routed(BinanceUsdmRouteV4::Market);
    let mut start = ActionBuffer::new();
    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut start,
        )
        .unwrap();
    let oi_request_id = start
        .as_slice()
        .iter()
        .find_map(|action| match action {
            SessionAction::RequestHttp(request) if request.url.contains("openInterest") => {
                Some(request.id)
            }
            _ => None,
        })
        .unwrap();
    let trade = drive_text(
        &mut session,
        r#"{"e":"aggTrade","E":1000,"s":"BNBUSDT","a":42,"p":"650.1","q":"0.01","T":1001,"m":false}"#,
    )
    .unwrap();
    let event = emitted(&trade)[0];
    assert!(matches!(event.payload, MarketEvent::Trade(_)));
    assert_eq!(event.exchange_ts, Some(TimestampNs(1_001_000_000)));
    assert_eq!(event.source_sequence.unwrap().first, 42);

    let liquidation = drive_text(
        &mut session,
        r#"{"e":"forceOrder","E":2002,"o":{"s":"BNBUSDT","S":"SELL","ap":"649","l":"0.5"}}"#,
    )
    .unwrap();
    assert_eq!(
        emitted(&liquidation)[0].exchange_ts,
        Some(TimestampNs(2_002_000_000))
    );
    assert!(drive_text(
        &mut session,
        r#"{"e":"bookTicker","E":1,"T":2,"u":3,"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}"#,
    )
    .is_err());

    let mut rejected = ActionBuffer::new();
    assert!(
        session
            .on_input(
                SessionInput::HttpResponse {
                    request_id: oi_request_id,
                    response: &HttpResponse {
                        status: 200,
                        headers: Vec::new(),
                        body: Bytes::from_static(
                            br#"{"symbol":"BNBUSDT","openInterest":"10659.509"}"#,
                        ),
                    },
                    received: stamp(9_000_000_001),
                },
                &mut rejected,
            )
            .is_err()
    );
    assert!(rejected.is_empty());

    let mut oi = ActionBuffer::new();
    session
        .on_input(
            SessionInput::HttpResponse {
                request_id: oi_request_id,
                response: &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from_static(
                        br#"{"symbol":"BNBUSDT","openInterest":"10659.509","time":1589437530011}"#,
                    ),
                },
                received: stamp(9_000_000_002),
            },
            &mut oi,
        )
        .unwrap();
    assert_eq!(
        emitted(&oi)[0].exchange_ts,
        Some(TimestampNs(1_589_437_530_011_000_000))
    );
}

#[test]
fn routed_snapshot_requires_official_e_t_shape_and_authors_e() {
    let decoded = decode_usdm_text(
        br#"{"lastUpdateId":100,"E":1784841086945,"T":1784841086836,"bids":[["650.0","1"]],"asks":[["651.0","2"]]}"#,
    )
    .unwrap();
    assert!(matches!(
        decoded,
        UsdmDecoded::DepthSnapshot {
            event_time_ms: Some(1_784_841_086_945),
            transaction_time_ms: Some(1_784_841_086_836),
            ..
        }
    ));
    let mut session = routed(BinanceUsdmRouteV4::Public);
    let mut start = ActionBuffer::new();
    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut start,
        )
        .unwrap();
    let request_id = start
        .as_slice()
        .iter()
        .find_map(|action| match action {
            SessionAction::RequestHttp(request) if request.url.contains("/depth?") => {
                Some(request.id)
            }
            _ => None,
        })
        .unwrap();
    let buffered = drive_text(
        &mut session,
        r#"{"e":"depthUpdate","E":1784841086946,"T":1784841086837,"s":"BNBUSDT","U":99,"u":101,"pu":98,"b":[["650.0","1.5"]],"a":[["651.0","1.5"]]}"#,
    )
    .unwrap();
    assert!(emitted(&buffered).is_empty());

    let mut out = ActionBuffer::new();
    session
        .on_input(
            SessionInput::HttpResponse {
                request_id,
                response: &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from_static(
                        br#"{"lastUpdateId":100,"E":1784841086945,"T":1784841086836,"bids":[["650.0","1"]],"asks":[["651.0","2"]]}"#,
                    ),
                },
                received: stamp(9_000_000_000),
            },
            &mut out,
        )
        .unwrap();
    let snapshot = emitted(&out)
        .into_iter()
        .find(|event| matches!(event.payload, MarketEvent::BookSnapshot(_)))
        .unwrap();
    assert_eq!(
        snapshot.exchange_ts,
        Some(TimestampNs(1_784_841_086_945_000_000))
    );
    assert!(
        emitted(&out)
            .iter()
            .any(|event| matches!(event.payload, MarketEvent::BookDelta(_))),
        "inclusive U<=lastUpdateId<=u bridge must drain"
    );
    let next = drive_text(
        &mut session,
        r#"{"e":"depthUpdate","E":1784841086947,"T":1784841086838,"s":"BNBUSDT","U":102,"u":102,"pu":101,"b":[["650.0","2"]],"a":[]}"#,
    )
    .unwrap();
    assert!(matches!(
        emitted(&next)[0].payload,
        MarketEvent::BookDelta(_)
    ));
    let mismatch = drive_text(
        &mut session,
        r#"{"e":"depthUpdate","E":1784841086948,"T":1784841086839,"s":"BNBUSDT","U":103,"u":103,"pu":99,"b":[["650.0","3"]],"a":[]}"#,
    )
    .unwrap();
    assert!(emitted(&mismatch).is_empty());
    assert!(
        mismatch
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(_)))
    );

    let mut missing = routed(BinanceUsdmRouteV4::Public);
    let mut start = ActionBuffer::new();
    missing
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut start,
        )
        .unwrap();
    let request_id = start
        .as_slice()
        .iter()
        .find_map(|action| match action {
            SessionAction::RequestHttp(request) if request.url.contains("/depth?") => {
                Some(request.id)
            }
            _ => None,
        })
        .unwrap();
    let mut rejected = ActionBuffer::new();
    assert!(missing
        .on_input(
            SessionInput::HttpResponse {
                request_id,
                response: &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from_static(
                        br#"{"lastUpdateId":100,"bids":[["650.0","1"]],"asks":[["651.0","2"]]}"#,
                    ),
                },
                received: stamp(9_000_000_000),
            },
            &mut rejected,
        )
        .is_err());
    assert!(rejected.is_empty());

    let mut retry = ActionBuffer::new();
    missing
        .on_input(
            SessionInput::HttpResponse {
                request_id,
                response: &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from_static(
                        br#"{"lastUpdateId":100,"E":1784841086945,"T":1784841086836,"bids":[["650.0","1"]],"asks":[["651.0","2"]]}"#,
                    ),
                },
                received: stamp(9_000_000_001),
            },
            &mut retry,
        )
        .unwrap();
    assert!(
        emitted(&retry)
            .iter()
            .any(|event| matches!(event.payload, MarketEvent::BookSnapshot(_)))
    );
}
