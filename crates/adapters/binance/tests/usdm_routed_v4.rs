use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, HttpMethod, HttpResponse, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{
    BinanceUsdmRouteV4, BinanceUsdmSession, BinanceUsdmSessionConfig, UsdmDecoded,
    decode_usdm_routed_v4_text,
};
use marketfeed_model::{
    AssetCode, CatalogVersion, CatalogView, ConnectionId, EventEnvelope, Fixed, FrameStamp,
    Instrument, InstrumentId, InstrumentKey, InstrumentKind, InstrumentStatus, MarketEvent, Price,
    Quantity, SessionId, TimestampNs, VenueCode, VenueId,
};

const PUBLIC_WS: &str = "wss://fstream.binance.com/public/ws";
const MARKET_WS: &str = "wss://fstream.binance.com/market/ws";

fn routed_catalog(id: InstrumentId) -> CatalogView {
    let version = CatalogVersion(1);
    CatalogView::with_instruments(
        VenueId(3),
        version,
        vec![Instrument {
            id,
            key: InstrumentKey {
                venue: VenueCode("binance-usdm".to_owned()),
                native_symbol: "BNBUSDT".to_owned(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: Some(AssetCode("USDT".to_owned())),
                expiry_ns: None,
            },
            base: AssetCode("BNB".to_owned()),
            quote: AssetCode("USDT".to_owned()),
            settlement: Some(AssetCode("USDT".to_owned())),
            price_scale: 2,
            quantity_scale: 3,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 3),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
            catalog_version: version,
        }],
    )
}

fn mutated_routed_catalog(mutate: fn(&mut Instrument)) -> CatalogView {
    let mut instruments = routed_catalog(InstrumentId(7)).instruments.to_vec();
    mutate(&mut instruments[0]);
    CatalogView::with_instruments(VenueId(3), CatalogVersion(1), instruments)
}

fn stamp(value: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(value),
        mono_ns: value as u64,
    }
}

fn routed(route: BinanceUsdmRouteV4) -> BinanceUsdmSession {
    let (public, market) = BinanceUsdmSession::try_new_routed_pair_v4(
        SessionSpec {
            endpoint_name: PUBLIC_WS.to_owned(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        SessionSpec {
            endpoint_name: MARKET_WS.to_owned(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        routed_catalog(InstrumentId(7)),
        routed_config(ConnectionId(11), SessionId(21), true),
        routed_config(ConnectionId(12), SessionId(22), false),
    )
    .expect("valid routed pair");
    match route {
        BinanceUsdmRouteV4::Public => public,
        BinanceUsdmRouteV4::Market => market,
    }
}

fn routed_config(
    connection: ConnectionId,
    session: SessionId,
    enable_l2: bool,
) -> BinanceUsdmSessionConfig {
    BinanceUsdmSessionConfig {
        symbols: vec!["BNBUSDT".to_owned()],
        instrument_ids: HashMap::from([("BNBUSDT".to_owned(), InstrumentId(7))]),
        connection,
        session,
        enable_l2,
        price_scale: 2,
        qty_scale: 3,
        ..BinanceUsdmSessionConfig::default()
    }
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

        let public_spec = SessionSpec {
            endpoint_name: if route == BinanceUsdmRouteV4::Public {
                format!("{endpoint}/wrong")
            } else {
                PUBLIC_WS.to_owned()
            },
            subscriptions: ConcreteSubscriptionSet::default(),
        };
        let market_spec = SessionSpec {
            endpoint_name: if route == BinanceUsdmRouteV4::Market {
                format!("{endpoint}/wrong")
            } else {
                MARKET_WS.to_owned()
            },
            subscriptions: ConcreteSubscriptionSet::default(),
        };
        assert!(
            BinanceUsdmSession::try_new_routed_pair_v4(
                public_spec,
                market_spec,
                routed_catalog(InstrumentId(7)),
                routed_config(ConnectionId(11), SessionId(21), true),
                routed_config(ConnectionId(12), SessionId(22), false),
            )
            .is_err()
        );
    }
}

#[test]
fn routed_pair_requires_distinct_connection_and_session_ids() {
    let result = BinanceUsdmSession::try_new_routed_pair_v4(
        SessionSpec {
            endpoint_name: PUBLIC_WS.to_owned(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        SessionSpec {
            endpoint_name: MARKET_WS.to_owned(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        routed_catalog(InstrumentId(7)),
        routed_config(ConnectionId(11), SessionId(21), true),
        routed_config(ConnectionId(11), SessionId(21), false),
    );
    assert!(result.is_err());
}

#[test]
fn routed_pair_requires_exact_catalog_row() {
    let construct = |catalog| {
        BinanceUsdmSession::try_new_routed_pair_v4(
            SessionSpec {
                endpoint_name: PUBLIC_WS.to_owned(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            SessionSpec {
                endpoint_name: MARKET_WS.to_owned(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            catalog,
            routed_config(ConnectionId(11), SessionId(21), true),
            routed_config(ConnectionId(12), SessionId(22), false),
        )
    };
    assert!(construct(CatalogView::new(VenueId(3), CatalogVersion(1))).is_err());
    assert!(construct(routed_catalog(InstrumentId(8))).is_err());
    let mut wrong_venue = routed_catalog(InstrumentId(7));
    wrong_venue.venue = VenueId(2);
    assert!(construct(wrong_venue).is_err());
    let mut wrong_version = routed_catalog(InstrumentId(7));
    wrong_version.version = CatalogVersion(2);
    assert!(construct(wrong_version).is_err());
    for catalog in [
        mutated_routed_catalog(|instrument| instrument.base = AssetCode("BTC".to_owned())),
        mutated_routed_catalog(|instrument| instrument.quote = AssetCode("USDC".to_owned())),
        mutated_routed_catalog(|instrument| instrument.key.kind = InstrumentKind::FutureLinear),
        mutated_routed_catalog(|instrument| {
            instrument.settlement = Some(AssetCode("USDC".to_owned()));
            instrument.key.settlement = Some(AssetCode("USDC".to_owned()));
        }),
        mutated_routed_catalog(|instrument| instrument.inverse = true),
        mutated_routed_catalog(|instrument| {
            instrument.expiry_ns = Some(1);
            instrument.key.expiry_ns = Some(1);
        }),
    ] {
        assert!(construct(catalog).is_err());
    }
}

#[test]
fn legacy_usdm_decoded_variant_shape_remains_source_compatible() {
    let price = Price(Fixed::new(1, 0));
    let quantity = Quantity(Fixed::new(1, 0));
    let _ = UsdmDecoded::AggTrade {
        symbol: "BNBUSDT".to_owned(),
        agg_id: 1,
        price,
        quantity,
        aggressor: marketfeed_model::AggressorSide::Buy,
        exchange_ts_ms: 1,
    };
    let _ = UsdmDecoded::Quote {
        symbol: "BNBUSDT".to_owned(),
        update_id: 1,
        bid_price: price,
        bid_qty: quantity,
        ask_price: price,
        ask_qty: quantity,
    };
    let _ = UsdmDecoded::DepthUpdate {
        symbol: "BNBUSDT".to_owned(),
        first_update_id: 1,
        final_update_id: 1,
        prev_final_update_id: 0,
        bids: vec![],
        asks: vec![],
        exchange_ts_ms: 1,
    };
    let _ = UsdmDecoded::ForceOrder {
        symbol: "BNBUSDT".to_owned(),
        price,
        quantity,
        side: marketfeed_model::AggressorSide::Sell,
        exchange_ts_ms: 1,
    };
    let _ = UsdmDecoded::DepthSnapshot {
        last_update_id: 1,
        bids: vec![],
        asks: vec![],
    };
}

#[test]
fn routed_quote_requires_e_and_t_and_uses_derived_cursor_despite_noncontiguous_u() {
    let mut session = routed(BinanceUsdmRouteV4::Public);
    for (u, transaction_ms) in [(100, 1_784_817_230_005_i64), (105, 1_784_817_230_006)] {
        let raw = format!(
            r#"{{"e":"bookTicker","E":1784817230130,"T":{transaction_ms},"u":{u},"s":"BNBUSDT","b":"650.0","B":"1.2","a":"650.1","A":"0.8"}}"#
        );
        assert!(matches!(
            decode_usdm_routed_v4_text(raw.as_bytes()).unwrap(),
            marketfeed_adapter_binance::UsdmRoutedV4Decoded {
                decoded: UsdmDecoded::Quote { update_id, .. },
                source_times: marketfeed_adapter_binance::UsdmRoutedV4SourceTimes {
                    event_time_ms: Some(1_784_817_230_130),
                    transaction_time_ms: Some(value),
                },
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
    for now in [TimestampNs(2), TimestampNs(3)] {
        let mut timer = ActionBuffer::new();
        session
            .on_input(
                SessionInput::Timer {
                    timer_id: marketfeed_adapter_binance::OI_TIMER_ID,
                    now,
                },
                &mut timer,
            )
            .unwrap();
        assert!(
            !timer
                .as_slice()
                .iter()
                .any(|action| matches!(action, SessionAction::RequestHttp(_)))
        );
    }
    let trade = drive_text(
        &mut session,
        r#"{"e":"aggTrade","E":1000,"s":"BNBUSDT","a":42,"p":"650.1","q":"0.01","T":1001,"m":false}"#,
    )
    .unwrap();
    let event = emitted(&trade)[0];
    assert!(matches!(event.payload, MarketEvent::Trade(_)));
    assert_eq!(event.exchange_ts, Some(TimestampNs(1_001_000_000)));
    assert_eq!(event.source_sequence.unwrap().first, 42);
    assert!(matches!(
        decode_usdm_routed_v4_text(
            br#"{"e":"aggTrade","E":1000,"s":"BNBUSDT","a":42,"p":"650.1","q":"0.01","T":1001,"m":false}"#
        )
        .unwrap(),
        marketfeed_adapter_binance::UsdmRoutedV4Decoded {
            decoded: UsdmDecoded::AggTrade { exchange_ts_ms: 1001, .. },
            source_times: marketfeed_adapter_binance::UsdmRoutedV4SourceTimes {
                event_time_ms: Some(1000),
                transaction_time_ms: Some(1001),
            },
        }
    ));

    let liquidation = drive_text(
        &mut session,
        r#"{"e":"forceOrder","E":2002,"o":{"s":"BNBUSDT","S":"SELL","ap":"649","l":"0.5","T":1999}}"#,
    )
    .unwrap();
    assert_eq!(
        emitted(&liquidation)[0].exchange_ts,
        Some(TimestampNs(1_999_000_000))
    );
    assert!(matches!(
        decode_usdm_routed_v4_text(
            br#"{"e":"forceOrder","E":2002,"o":{"s":"BNBUSDT","S":"SELL","ap":"649","l":"0.5","T":1999}}"#
        )
        .unwrap(),
        marketfeed_adapter_binance::UsdmRoutedV4Decoded {
            decoded: UsdmDecoded::ForceOrder { exchange_ts_ms: 2002, .. },
            source_times: marketfeed_adapter_binance::UsdmRoutedV4SourceTimes {
                event_time_ms: Some(2002),
                transaction_time_ms: Some(1999),
            },
        }
    ));
    assert!(
        drive_text(
            &mut session,
            r#"{"e":"forceOrder","E":2003,"o":{"s":"BNBUSDT","S":"SELL","ap":"649","l":"0.5"}}"#,
        )
        .is_err()
    );
    assert!(drive_text(
        &mut session,
        r#"{"e":"bookTicker","E":1,"T":2,"u":3,"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}"#,
    )
    .is_err());

    let mut rejected = ActionBuffer::new();
    assert!(session
        .on_input(
            SessionInput::HttpResponse {
                request_id: oi_request_id + 100,
                response: &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from_static(
                        br#"{"symbol":"BNBUSDT","openInterest":"10659.509","time":1589437530011}"#,
                    ),
                },
                received: stamp(9_000_000_001),
            },
            &mut rejected,
        )
        .is_err());
    assert!(rejected.is_empty());

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
    let mut duplicate = ActionBuffer::new();
    assert!(session
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
                received: stamp(9_000_000_003),
            },
            &mut duplicate,
        )
        .is_err());
    assert!(duplicate.is_empty());
    let mut next_poll = ActionBuffer::new();
    session
        .on_input(
            SessionInput::Timer {
                timer_id: marketfeed_adapter_binance::OI_TIMER_ID,
                now: TimestampNs(4),
            },
            &mut next_poll,
        )
        .unwrap();
    assert_eq!(
        next_poll
            .as_slice()
            .iter()
            .filter(|action| matches!(action, SessionAction::RequestHttp(_)))
            .count(),
        1
    );
}

#[test]
fn routed_frames_fail_closed_before_authorship() {
    let mut market = routed(BinanceUsdmRouteV4::Market);
    for rejected in [
        r#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":1,"p":"0","q":"1","T":2,"m":false}"#,
        r#"{"e":"aggTrade","E":-1,"s":"BNBUSDT","a":1,"p":"650","q":"1","T":2,"m":false}"#,
        r#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":1,"p":"650","q":"1","T":9223372036855,"m":false}"#,
        r#"{"result":null,"id":2}"#,
        r#"{"result":null}"#,
    ] {
        assert!(drive_text(&mut market, rejected).is_err(), "{rejected}");
    }
    let trade = drive_text(
        &mut market,
        r#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":2,"p":"650","q":"1","T":2,"m":false}"#,
    )
    .unwrap();
    assert_eq!(emitted(&trade)[0].frame_seq, 1);

    let mut ack = routed(BinanceUsdmRouteV4::Public);
    assert!(drive_text(&mut ack, r#"{"result":null,"id":1}"#).is_ok());
}

#[test]
fn routed_native_ids_are_bounded_before_output_or_state_mutation() {
    const MAX: u64 = i64::MAX as u64;
    const OVER: u64 = MAX + 1;

    let mut market = routed(BinanceUsdmRouteV4::Market);
    let first = drive_text(
        &mut market,
        &format!(
            r#"{{"e":"aggTrade","E":1,"s":"BNBUSDT","a":{MAX},"p":"650","q":"1","T":2,"m":false}}"#
        ),
    )
    .unwrap();
    assert_eq!(emitted(&first)[0].source_sequence.unwrap().first, MAX);
    assert!(drive_text(
        &mut market,
        &format!(
            r#"{{"e":"aggTrade","E":1,"s":"BNBUSDT","a":{OVER},"p":"650","q":"1","T":2,"m":false}}"#
        ),
    )
    .is_err());
    let second = drive_text(
        &mut market,
        r#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":1,"p":"650","q":"1","T":2,"m":false}"#,
    )
    .unwrap();
    assert_eq!(emitted(&second)[0].frame_seq, 2);

    let mut public = routed(BinanceUsdmRouteV4::Public);
    let quote = drive_text(
        &mut public,
        &format!(
            r#"{{"e":"bookTicker","E":1,"T":2,"u":{MAX},"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}}"#
        ),
    )
    .unwrap();
    assert_eq!(emitted(&quote)[0].frame_seq, 1);
    let max_quote = drive_text(
        &mut public,
        &format!(
            r#"{{"e":"bookTicker","E":1,"T":2,"u":{},"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}}"#,
            u64::MAX
        ),
    )
    .unwrap();
    assert_eq!(emitted(&max_quote)[0].source_sequence, None);
    let next_quote = drive_text(
        &mut public,
        r#"{"e":"bookTicker","E":1,"T":2,"u":1,"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}"#,
    )
    .unwrap();
    assert_eq!(emitted(&next_quote)[0].frame_seq, 3);
    assert!(drive_text(
        &mut public,
        &format!(
            r#"{{"e":"depthUpdate","E":1,"T":2,"s":"BNBUSDT","U":{MAX},"u":{MAX},"pu":{},"b":[],"a":[]}}"#,
            MAX - 1
        ),
    )
    .is_ok());
    assert!(drive_text(
        &mut public,
        &format!(
            r#"{{"e":"depthUpdate","E":1,"T":2,"s":"BNBUSDT","U":{OVER},"u":{OVER},"pu":0,"b":[],"a":[]}}"#
        ),
    )
    .is_err());

    let mut snapshot_session = routed(BinanceUsdmRouteV4::Public);
    let mut start = ActionBuffer::new();
    snapshot_session
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
    assert!(
        snapshot_session
            .on_input(
                SessionInput::HttpResponse {
                    request_id,
                    response: &HttpResponse {
                        status: 200,
                        headers: Vec::new(),
                        body: Bytes::from(format!(
                            r#"{{"lastUpdateId":{OVER},"E":1,"T":2,"bids":[],"asks":[]}}"#
                        )),
                    },
                    received: stamp(3),
                },
                &mut rejected,
            )
            .is_err()
    );
    assert!(rejected.is_empty());
    let mut accepted = ActionBuffer::new();
    snapshot_session
        .on_input(
            SessionInput::HttpResponse {
                request_id,
                response: &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from(format!(
                        r#"{{"lastUpdateId":{MAX},"E":1,"T":2,"bids":[],"asks":[]}}"#
                    )),
                },
                received: stamp(4),
            },
            &mut accepted,
        )
        .unwrap();
    assert_eq!(emitted(&accepted)[0].source_sequence.unwrap().first, MAX);
}

#[test]
fn routed_snapshot_requires_official_e_t_shape_and_authors_t_without_system() {
    let decoded = decode_usdm_routed_v4_text(
        br#"{"lastUpdateId":100,"E":1784841086945,"T":1784841086836,"bids":[["650.0","1"]],"asks":[["651.0","2"]]}"#,
    )
    .unwrap();
    assert!(matches!(
        decoded,
        marketfeed_adapter_binance::UsdmRoutedV4Decoded {
            decoded: UsdmDecoded::DepthSnapshot { .. },
            source_times: marketfeed_adapter_binance::UsdmRoutedV4SourceTimes {
                event_time_ms: Some(1_784_841_086_945),
                transaction_time_ms: Some(1_784_841_086_836),
            },
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
        Some(TimestampNs(1_784_841_086_836_000_000))
    );
    assert!(
        !out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::EmitSystem(_)))
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
    assert_eq!(
        emitted(&next)[0].exchange_ts,
        Some(TimestampNs(1_784_841_086_838_000_000))
    );
    assert!(matches!(
        decode_usdm_routed_v4_text(
            br#"{"e":"depthUpdate","E":1784841086947,"T":1784841086838,"s":"BNBUSDT","U":102,"u":102,"pu":101,"b":[["650.0","2"]],"a":[]}"#
        )
        .unwrap(),
        marketfeed_adapter_binance::UsdmRoutedV4Decoded {
            decoded: UsdmDecoded::DepthUpdate { exchange_ts_ms: 1_784_841_086_947, .. },
            source_times: marketfeed_adapter_binance::UsdmRoutedV4SourceTimes {
                event_time_ms: Some(1_784_841_086_947),
                transaction_time_ms: Some(1_784_841_086_838),
            },
        }
    ));
    assert!(drive_text(
        &mut session,
        r#"{"e":"depthUpdate","E":1784841086948,"s":"BNBUSDT","U":103,"u":103,"pu":102,"b":[["650.0","3"]],"a":[]}"#,
    )
    .is_err());
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
