use bytes::Bytes;
use marketfeed_adapter_api::*;
use marketfeed_adapter_hyperliquid::{
    HyperliquidFactory,
    protocol::{self, Message},
};
use marketfeed_model::*;

const SPOT: &str = r#"{"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"},{"name":"DUP","szDecimals":2,"weiDecimals":8,"index":9,"tokenId":"0x9"}],"universe":[{"name":"PURR/USDC","tokens":[7,0],"index":0},{"name":"@12","tokens":[9,0],"index":12}]}"#;
const PERPS: &str = r#"{"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5},{"name":"OLD","szDecimals":3,"isDelisted":true}]}"#;

fn observed_session(
    spot: bool,
    limits: ObservedBatchLimits,
) -> marketfeed_adapter_hyperliquid::HyperliquidSession {
    let mut channels = vec![
        Channel::Trades,
        Channel::Quote,
        Channel::L2Book {
            depth: Some(1),
            cadence: None,
        },
        Channel::Candles {
            interval: CandleInterval::M1,
        },
        Channel::MarkPrice,
        Channel::Statistics24h,
    ];
    if !spot {
        channels.extend([Channel::Funding, Channel::OpenInterest, Channel::IndexPrice]);
    }
    let mut session = marketfeed_adapter_hyperliquid::HyperliquidSession::new_with_observations(
        spot,
        ConcreteSubscriptionSet {
            items: channels.into_iter().map(request).collect(),
        },
        catalog(spot),
        limits,
    )
    .unwrap();
    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut ActionBuffer::new(),
        )
        .unwrap();
    session
}
fn observed_frame(session: &mut dyn SessionMachine, frame: &str) -> ObservedEventBatch {
    let mut out = ActionBuffer::new();
    send(session, frame, &mut out).unwrap();
    let batches = out
        .drain()
        .filter_map(|action| match action {
            SessionAction::EmitObservedBatch(batch) => Some(batch),
            SessionAction::EmitBatch(_) => panic!("native capability lost"),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), 1);
    batches.into_iter().next().unwrap()
}

#[test]
fn observed_spot_and_perps_preserve_five_native_families_from_one_decode() {
    for spot in [true, false] {
        let coin = if spot { "PURR/USDC" } else { "BTC" };
        let mut session = observed_session(spot, ObservedBatchLimits::HARD_MAX);
        let frame = r#"{"channel":"trades","data":[{"coin":"$C","side":"B","px":"9007199254740993.123456789012345678","sz":"0.00000000000000000001","time":1000,"tid":18446744073709551615,"hash":"h1","users":["u1","u2"]},{"coin":"$C","side":"A","px":"2.00","sz":"3","time":1001,"tid":18446744073709551615,"hash":"h2"}]}"#.replace("$C", coin);
        let batch = observed_frame(&mut session, &frame);
        assert!(
            ObservedEventBatch::try_new(
                batch.canonical().clone(),
                batch.native().clone(),
                ObservedBatchSource {
                    venue: VenueId(999),
                    ..batch.source()
                },
                ObservedBatchLimits::HARD_MAX
            )
            .is_err()
        );
        assert_eq!(batch.canonical().events.len(), 2);
        assert_eq!(batch.native().rows.len(), 2);
        let expected = HyperliquidTradeObservation {
            coin: coin.into(),
            side: HyperliquidTradeSide::Buy,
            price: Fixed::new(9007199254740993123456789012345678, 18),
            size: Fixed::new(1, 20),
            hash: "h1".into(),
            time_ms: 1000,
            tid: u64::MAX,
            users: vec!["u1".into(), "u2".into()],
        };
        assert_eq!(
            batch.native().rows[0],
            NativeObservationRow {
                canonical_positions: vec![0],
                observation: NativeObservation::HyperliquidTrade(expected.clone()),
            }
        );
        let NativeObservation::HyperliquidTrade(second) = &batch.native().rows[1].observation
        else {
            panic!()
        };
        assert_ne!(second.unique_id(), expected.unique_id());
        assert_eq!(second.side, HyperliquidTradeSide::Sell);
        assert_eq!(second.hash, "h2");
        assert!(second.users.is_empty());
        assert_eq!(batch.native().rows[1].canonical_positions, [1]);
        let mut unit = MarketBatch::Observed(batch);
        assert_eq!(
            unit.stamp_runtime(
                VenueId(90),
                SessionId(77),
                ConnectionId(42),
                123,
                Some(FrameStamp {
                    receive_ts: TimestampNs(i64::MIN),
                    mono_ns: u64::MAX
                }),
                7
            )
            .unwrap(),
            9
        );
        assert_eq!(unit.events[0].event_index, 7);
        assert_eq!(unit.events[1].event_index, 8);
        assert!(unit.events.iter().all(|e| e.venue == VenueId(90)
            && e.session == SessionId(77)
            && e.connection == ConnectionId(42)
            && e.frame_seq == 123
            && e.receive_ts == TimestampNs(i64::MIN)));

        let batch = observed_frame(&mut session, &r#"{"channel":"bbo","data":{"coin":"$C","time":1000,"bbo":[{"px":"1","sz":"2","n":18446744073709551615},{"px":"3","sz":"4","n":0}]}}"#.replace("$C", coin));
        assert_eq!(batch.canonical().events.len(), 1);
        let NativeObservation::HyperliquidBbo(b) = &batch.native().rows[0].observation else {
            panic!()
        };
        assert_eq!(b.sides[0].as_ref().unwrap().orders, u64::MAX);
        assert_eq!(b.sides[1].as_ref().unwrap().orders, 0);
        let batch = observed_frame(&mut session, &r#"{"channel":"l2Book","data":{"coin":"$C","time":1002,"levels":[[{"px":"2","sz":"3","n":4},{"px":"1","sz":"5","n":6}],[{"px":"3","sz":"7","n":8},{"px":"4","sz":"9","n":10}]]}}"#.replace("$C", coin));
        let MarketEvent::BookSnapshot(canonical) = &batch.canonical().events[0].payload else {
            panic!()
        };
        assert_eq!(
            (canonical.bids.len(), canonical.asks.len(), canonical.depth),
            (1, 1, Some(1))
        );
        let NativeObservation::HyperliquidBook(native) = &batch.native().rows[0].observation else {
            panic!()
        };
        assert_eq!((native.sides[0].len(), native.sides[1].len()), (2, 2));
        assert_eq!(native.sides[0][1].orders, 6);
        assert_eq!(native.sides[1][1].orders, 10);
        let batch = observed_frame(&mut session, &r#"{"channel":"candle","data":{"s":"$C","i":"1m","t":1000,"T":1001,"o":"1.00","h":"3","l":"1","c":"2","v":"0.00","n":18446744073709551615}}"#.replace("$C", coin));
        assert_eq!(
            batch.native().rows[0].observation,
            NativeObservation::HyperliquidCandle(HyperliquidCandleObservation {
                coin: coin.into(),
                interval: "1m".into(),
                open_ms: 1000,
                end_ms: 1001,
                open: Fixed::new(100, 2),
                high: Fixed::new(3, 0),
                low: Fixed::new(1, 0),
                close: Fixed::new(2, 0),
                volume: Fixed::new(0, 2),
                trades: u64::MAX,
            })
        );
        let batch = observed_frame(&mut session, &r#"{"channel":"activeAssetCtx","data":{"coin":"$C","ctx":{"dayNtlVlm":"12.00","prevDayPx":"2","markPx":"3","midPx":null,"circulatingSupply":"0.00","funding":"-0.0001","openInterest":"4","oraclePx":"5","dayBaseVlm":"6","premium":"-0.25","impactPxs":["7","8"]}}}"#.replace("$C", coin));
        assert_eq!(batch.native().rows.len(), 1);
        assert_eq!(
            batch.native().rows[0].canonical_positions,
            if spot {
                vec![0, 1]
            } else {
                vec![0, 1, 2, 3, 4]
            }
        );
        assert_eq!(
            batch.native().rows[0].observation,
            NativeObservation::HyperliquidContext(Box::new(HyperliquidContextObservation {
                coin: coin.into(),
                day_notional_volume: Fixed::new(1200, 2),
                previous_day_price: Fixed::new(2, 0),
                mark_price: Fixed::new(3, 0),
                mid_price: None,
                circulating_supply: Some(Fixed::new(0, 2)),
                funding: Some(Fixed::new(-1, 4)),
                open_interest: Some(Fixed::new(4, 0)),
                oracle_price: Some(Fixed::new(5, 0)),
                day_base_volume: Some(Fixed::new(6, 0)),
                premium: Some(Fixed::new(-25, 2)),
                impact_prices: Some([Fixed::new(7, 0), Fixed::new(8, 0)]),
            }))
        );
    }
}

#[test]
fn observed_one_sided_bbo_occupies_a_unit_and_keeps_ordered_invalidation() {
    for sides in [
        r#"[null,{"px":"3","sz":"4","n":5}]"#,
        r#"[{"px":"1","sz":"2","n":3},null]"#,
        "[null,null]",
    ] {
        let mut session = observed_session(false, ObservedBatchLimits::HARD_MAX);
        let mut out = ActionBuffer::new();
        send(
            &mut session,
            &format!(r#"{{"channel":"bbo","data":{{"coin":"BTC","time":1000,"bbo":{sides}}}}}"#),
            &mut out,
        )
        .unwrap();
        assert!(matches!(
            out.as_slice()[0],
            SessionAction::EmitSystem(SystemEvent::QuoteInvalidated { .. })
        ));
        let SessionAction::EmitObservedBatch(batch) = &out.as_slice()[1] else {
            panic!()
        };
        assert!(batch.canonical().events.is_empty());
        assert!(batch.native().rows[0].canonical_positions.is_empty());
        assert_eq!(
            batch.source(),
            ObservedBatchSource {
                venue: VenueId(22),
                connection: ConnectionId(0),
                received: FrameStamp {
                    receive_ts: TimestampNs(1_000_000_000),
                    mono_ns: 1
                }
            }
        );
        let NativeObservation::HyperliquidBbo(native) = &batch.native().rows[0].observation else {
            panic!()
        };
        assert_eq!(native.sides[0].is_none(), sides.starts_with("[null"));
        assert_eq!(native.sides[1].is_none(), sides.ends_with("null]"));
    }
}

#[test]
fn observed_oversize_malformed_and_unsubscribed_units_fail_whole() {
    let mut session = observed_session(
        false,
        ObservedBatchLimits {
            layout: NativeObservationLimits {
                rows: 1,
                ..ObservedBatchLimits::HARD_MAX.layout
            },
            ..ObservedBatchLimits::HARD_MAX
        },
    );
    let trade = r#"{"coin":"BTC","side":"B","px":"1","sz":"2","time":1000,"tid":1,"hash":"h"}"#;
    for frame in [
        format!(r#"{{"channel":"trades","data":[{trade},{trade}]}}"#),
        format!(
            r#"{{"channel":"trades","data":[{trade},{{"coin":"BTC","side":"?","px":"1","sz":"2","time":1000,"tid":1,"hash":"h"}}]}}"#
        ),
        r#"{"channel":"allMids","data":{"mids":{"BTC":"1"}}}"#.into(),
    ] {
        let mut out = ActionBuffer::new();
        assert!(send(&mut session, &frame, &mut out).is_err());
        assert!(!out.as_slice().iter().any(|a| matches!(
            a,
            SessionAction::EmitObservedBatch(_) | SessionAction::EmitBatch(_)
        )));
    }
}

#[test]
fn refused_native_book_and_trade_units_do_not_advance_retained_market_state() {
    let mut session = observed_session(
        false,
        ObservedBatchLimits {
            layout: NativeObservationLimits {
                book_levels: 0,
                rows: 1,
                ..ObservedBatchLimits::HARD_MAX.layout
            },
            ..ObservedBatchLimits::HARD_MAX
        },
    );
    let mut out = ActionBuffer::new();
    assert!(send(&mut session, r#"{"channel":"l2Book","data":{"coin":"BTC","time":1000,"levels":[[{"px":"1","sz":"2","n":3}],[]]}}"#, &mut out).is_err());
    assert!(session.book_snapshot(InstrumentId(1), None).is_none());
    assert!(!out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::BookResynchronized { .. })
    )));
    let trade = r#"{"coin":"BTC","side":"B","px":"1","sz":"2","time":1000,"tid":1,"hash":"h"}"#;
    assert!(
        send(
            &mut session,
            &format!(r#"{{"channel":"trades","data":[{trade},{trade}]}}"#),
            &mut ActionBuffer::new()
        )
        .is_err()
    );
    let batch = observed_frame(
        &mut session,
        &format!(r#"{{"channel":"trades","data":[{trade}]}}"#),
    );
    assert!(
        batch.canonical().events[0]
            .flags
            .contains(EventFlags::INITIAL_DELIVERY)
    );
}
fn response(s: &str) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: vec![],
        body: Bytes::copy_from_slice(s.as_bytes()),
    }
}
fn catalog(spot: bool) -> CatalogView {
    let factory = if spot {
        HyperliquidFactory::spot()
    } else {
        HyperliquidFactory::perps()
    };
    let mut defs = vec![];
    factory
        .parse_instruments(&[response(SPOT), response(PERPS)], &mut defs)
        .unwrap();
    CatalogView::with_instruments(
        factory.specification().id,
        CatalogVersion(1),
        defs.into_iter()
            .enumerate()
            .map(|(i, d)| d.into_instrument(InstrumentId(i as u32 + 1), CatalogVersion(1)))
            .collect::<Vec<_>>(),
    )
}
fn request(channel: Channel) -> ConcreteSubscription {
    ConcreteSubscription {
        instrument: InstrumentId(1),
        channel,
        delivery: DeliveryOptions::default(),
    }
}
fn send(
    session: &mut dyn SessionMachine,
    s: &str,
    out: &mut ActionBuffer,
) -> Result<(), AdapterError> {
    session.on_input(
        SessionInput::TextFrame {
            bytes: &mut s.as_bytes().to_vec(),
            received: FrameStamp {
                receive_ts: TimestampNs(1_000_000_000),
                mono_ns: 1,
            },
        },
        out,
    )
}
#[test]
fn metadata_uses_token_indices_and_separate_product_namespaces() {
    let spot = catalog(true);
    let perps = catalog(false);
    assert_eq!(spot.venue, VenueId(21));
    assert_eq!(perps.venue, VenueId(22));
    assert_eq!(spot.instruments[0].key.native_symbol, "PURR/USDC");
    assert_eq!(spot.instruments[0].base.0, "PURR"); // token index 7 is not array position 7
    assert_eq!(spot.instruments[1].key.native_symbol, "@12");
    assert_eq!(spot.instruments[1].price_scale, 6);
    assert_eq!(
        perps.instruments[0].settlement,
        Some(AssetCode("USDC".into()))
    );
    assert_eq!(
        perps.instruments[0].key.kind,
        InstrumentKind::PerpetualLinear
    );
    assert_eq!(perps.instruments[1].status, InstrumentStatus::Delisted);
    let f = HyperliquidFactory::perps();
    let mut defs = vec![];
    assert!(
        f.parse_instruments(
            &[
                response(SPOT),
                response(r#"{"collateralToken":99,"universe":[]}"#)
            ],
            &mut defs
        )
        .is_err()
    );
    assert!(defs.is_empty());
}
#[test]
fn all_trade_rows_keep_exact_decimals_and_scoped_identity() {
    let frame = br#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"9007199254740993.123456789012345678","sz":"0.00000000000000000001","time":1700000000000,"tid":1125899906842623,"hash":"h1"},{"coin":"ETH","side":"A","px":"2","sz":"3","time":1700000000000,"tid":1125899906842623,"hash":"h2"}]}"#;
    let Message::Trades(t) = protocol::decode(frame).unwrap() else {
        panic!()
    };
    assert_eq!(t.len(), 2);
    assert_ne!(t[0].unique_id(), t[1].unique_id());
    assert_eq!(t[0].unique_id(), "BTC:1700000000000:1125899906842623");
    assert_eq!(
        t[0].px.0,
        Fixed::new(9007199254740993123456789012345678, 18)
    );
    assert_eq!(t[0].sz.0, Fixed::new(1, 20));
    assert_eq!(t[1].side, "A");
}
#[test]
fn malformed_late_trade_rejects_entire_frame() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::Trades)],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    assert!(send(&mut *m, r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","hash":"h","time":1,"tid":1},{"coin":"BTC","side":"?","px":"1","sz":"1","hash":"h","time":1,"tid":2}]}"#, &mut out).is_err());
    assert!(out.is_empty());
}
#[test]
fn numeric_json_and_string_decimal_tokens_agree_without_float() {
    for d in [
        "9007199254740993.123456789012345678",
        "1.234567890123456789e-20",
        "170141183460469231731687303715884105727",
    ] {
        let json =
            format!(r#"{{"channel":"allMids","data":{{"mids":{{"BTC":{d},"ETH":"{d}"}}}}}}"#);
        let Message::AllMids(m) = protocol::decode(json.as_bytes()).unwrap() else {
            panic!()
        };
        assert_eq!(m["BTC"], m["ETH"]);
    }
}
#[test]
fn invalid_values_and_unknown_channels_fail_closed() {
    for frame in [
        r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"NaN","sz":"1","hash":"h","time":1,"tid":1}]}"#,
        r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"0","hash":"h","time":1,"tid":1}]}"#,
        r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","hash":"h","time":9223372036854775807,"tid":1}]}"#,
        r#"{"channel":"userFills","data":{}}"#,
        r#"{"channel":"error","data":"subscription rejected"}"#,
    ] {
        assert!(protocol::decode(frame.as_bytes()).is_err(), "{frame}");
    }
}
#[test]
fn book_is_snapshot_and_unsorted_crossed_books_are_rejected() {
    let ok = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"10","sz":"2","n":3},{"px":"9","sz":"1","n":1}],[{"px":"11","sz":"1","n":2}]]}}"#;
    assert!(matches!(
        protocol::decode(ok.as_bytes()).unwrap(),
        Message::Book(_)
    ));
    assert!(protocol::decode(ok.replace("\"9\"", "\"12\"").as_bytes()).is_err());
    assert!(protocol::decode(ok.replace("\"11\"", "\"8\"").as_bytes()).is_err());
    let c = catalog(false);
    let f = HyperliquidFactory::perps();
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::L2Book {
            depth: Some(20),
            cadence: None,
        })],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    send(&mut *m, ok, &mut out).unwrap();
    let SessionAction::EmitBatch(b) = out
        .as_slice()
        .iter()
        .find(|a| matches!(a, SessionAction::EmitBatch(_)))
        .unwrap()
    else {
        panic!()
    };
    assert!(matches!(&b.events[0].payload,MarketEvent::BookSnapshot(b) if b.bids.len()==2));
}
#[test]
fn readiness_needs_all_acks_and_reconnect_exposes_gap() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::Trades), request(Channel::Quote)],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    send(&mut *m,r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#,&mut out).unwrap();
    assert!(!out.as_slice().contains(&SessionAction::MarkLive));
    send(&mut *m,r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}}"#,&mut out).unwrap();
    assert!(out.as_slice().contains(&SessionAction::MarkLive));
    out.clear();
    m.on_input(
        SessionInput::Disconnected {
            reason: DisconnectReason::RemoteClose,
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    assert!(out.as_slice().contains(&SessionAction::MarkDegraded));
    out.clear();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(2),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    m.on_input(
        SessionInput::Timer {
            timer_id: 1,
            now: TimestampNs(20_000_000_002),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        out.as_slice()
            .contains(&SessionAction::Reconnect(ReconnectReason::Heartbeat))
    );
}
#[test]
fn shared_context_is_one_subscription_and_derivative_fields_are_exact() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![
            request(Channel::Funding),
            request(Channel::OpenInterest),
            request(Channel::IndexPrice),
            request(Channel::MarkPrice),
        ],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    assert_eq!(
        out.as_slice()
            .iter()
            .filter(|a| matches!(a, SessionAction::SendText(_)))
            .count(),
        1
    );
    out.clear();
    send(&mut *m,r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"dayNtlVlm":"100.01","prevDayPx":"90","markPx":"100.02","funding":"-0.00000125","openInterest":"12345.12345","oraclePx":"100.03"}}}"#,&mut out).unwrap();
    let SessionAction::EmitBatch(b) = out
        .as_slice()
        .iter()
        .find(|a| matches!(a, SessionAction::EmitBatch(_)))
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(b.events.len(), 4);
    assert!(
        b.events
            .iter()
            .any(|e| matches!(&e.payload,MarketEvent::Funding(f) if f.rate.0==Fixed::new(-125,8)))
    );
    assert_eq!(b.events[3].event_index, 3);
    let spot = HyperliquidFactory::spot();
    assert!(spot.plan(&r, &catalog(true)).is_err());
}

#[test]
fn live_book_ack_defaults_are_equivalent_but_precision_changes_are_not() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::L2Book {
            depth: None,
            cadence: None,
        })],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    let ack = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}}}"#;
    assert!(
        send(
            &mut *m,
            &ack.replace("\"nSigFigs\":null", "\"nSigFigs\":3"),
            &mut out
        )
        .is_err()
    );
    send(&mut *m, ack, &mut out).unwrap();
    assert!(!out.as_slice().contains(&SessionAction::MarkLive));
}

#[test]
fn builder_perps_use_native_namespace_and_their_own_collateral() {
    let f = HyperliquidFactory::perps();
    let mut defs = vec![];
    let all = format!(
        "[{PERPS},{{\"collateralToken\":0,\"universe\":[{{\"name\":\"xyz:TSLA\",\"szDecimals\":3}}]}}]"
    );
    f.parse_instruments(&[response(SPOT), response(&all)], &mut defs)
        .unwrap();
    assert_eq!(defs[2].key.native_symbol, "xyz:TSLA");
    assert_eq!(defs[2].settlement, Some(AssetCode("USDC".into())));
    assert_eq!(defs[2].quantity_scale, 3);
    assert!(
        f.instrument_requests(Environment::Production).unwrap()[1]
            .body
            .as_ref()
            .unwrap()
            .windows(12)
            .any(|w| w == b"allPerpMetas")
    );
}

#[test]
fn protocol_pong_does_not_require_a_data_field() {
    assert!(matches!(
        protocol::decode(br#"{"channel":"pong"}"#).unwrap(),
        Message::Pong
    ));
}
#[test]
fn large_plan_splits_below_engine_capacity_without_duplicate_context_feeds() {
    let f = HyperliquidFactory::perps();
    let base = catalog(false).instruments[0].clone();
    let instruments: Vec<_> = (1..=205)
        .map(|id| {
            let mut i = base.clone();
            i.id = InstrumentId(id);
            i.key.native_symbol = format!("C{id}");
            i
        })
        .collect();
    let c = CatalogView::with_instruments(VenueId(22), CatalogVersion(1), instruments);
    let r = ConcreteSubscriptionSet {
        items: c
            .instruments
            .iter()
            .flat_map(|i| {
                [Channel::Funding, Channel::MarkPrice]
                    .into_iter()
                    .map(|channel| ConcreteSubscription {
                        instrument: i.id,
                        channel,
                        delivery: DeliveryOptions::default(),
                    })
            })
            .collect(),
    };
    let plan = f.plan(&r, &c).unwrap();
    assert_eq!(plan.len(), 2);
    let mut messages = std::collections::BTreeSet::new();
    for spec in plan {
        assert_eq!(spec.endpoint_name, "wss://api.hyperliquid.xyz/ws");
        let mut m = f.create_session(spec, c.clone()).unwrap();
        let mut out = ActionBuffer::new();
        m.on_input(
            SessionInput::Connected {
                now: TimestampNs(0),
            },
            &mut out,
        )
        .unwrap();
        let count = out
            .as_slice()
            .iter()
            .filter_map(|a| {
                if let SessionAction::SendText(t) = a {
                    assert!(messages.insert(t.to_vec()));
                    Some(())
                } else {
                    None
                }
            })
            .count();
        assert!(count <= 200);
        assert_eq!(out.take_dropped(), 0);
    }
    assert_eq!(messages.len(), 205);
}

#[test]
fn snapshots_register_valid_books_and_disconnect_invalidates_control_queries() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::L2Book {
            depth: None,
            cadence: None,
        })],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    send(&mut *m,r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"10","sz":"2","n":1}],[{"px":"11","sz":"3","n":2}]]}}"#,&mut out).unwrap();
    assert!(out.as_slice().contains(&SessionAction::EmitSystem(
        SystemEvent::BookResynchronized {
            instrument: InstrumentId(1)
        }
    )));
    assert_eq!(
        m.book_snapshot(InstrumentId(1), Some(1)).unwrap().bids[0]
            .quantity
            .0,
        Fixed::new(2, 0)
    );
    out.clear();
    m.on_input(
        SessionInput::Disconnected {
            reason: DisconnectReason::RemoteClose,
            now: TimestampNs(2),
        },
        &mut out,
    )
    .unwrap();
    assert!(m.book_snapshot(InstrumentId(1), None).is_none());
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
}

#[test]
fn initial_trade_origin_resets_each_connection_and_preserves_overlap() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::Trades)],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let frame = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"10","sz":"1","hash":"h","time":1,"tid":1}]}"#;
    let mut out = ActionBuffer::new();
    for generation in 0..2 {
        m.on_input(
            SessionInput::Connected {
                now: TimestampNs(generation),
            },
            &mut out,
        )
        .unwrap();
        for delivery in 0..2 {
            out.clear();
            send(&mut *m, frame, &mut out).unwrap();
            let batch = out
                .as_slice()
                .iter()
                .find_map(|a| {
                    if let SessionAction::EmitBatch(b) = a {
                        Some(b)
                    } else {
                        None
                    }
                })
                .unwrap();
            let event = &batch.events[0];
            assert_eq!(
                event.flags.contains(EventFlags::INITIAL_DELIVERY),
                delivery == 0
            );
            assert!(
                event
                    .flags
                    .contains(EventFlags::ACQUISITION_ORIGIN_UNVERIFIED)
            );
        }
    }
}
#[test]
fn acknowledgements_and_pongs_cannot_prove_required_stream_progress() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::L2Book {
            depth: None,
            cadence: None,
        })],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    send(&mut *m, r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}"#, &mut out).unwrap();
    send(&mut *m, r#"{"channel":"pong"}"#, &mut out).unwrap();
    assert!(!out.as_slice().contains(&SessionAction::MarkLive));
    out.clear();
    m.on_input(
        SessionInput::Timer {
            timer_id: 1,
            now: TimestampNs(21_000_000_000),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        out.as_slice()
            .contains(&SessionAction::Reconnect(ReconnectReason::Heartbeat))
    );
}
#[test]
fn disappearing_bbo_side_invalidates_the_previous_canonical_quote() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![request(Channel::Quote)],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    let level = r#"{"px":"10","sz":"1","n":1}"#;
    let ask = r#"{"px":"11","sz":"1","n":1}"#;
    for (bid, ask, unavailable) in [
        (level, ask, false),
        ("null", ask, true),
        ("null", "null", true),
        (level, ask, false),
    ] {
        out.clear();
        send(
            &mut *m,
            &format!(r#"{{"channel":"bbo","data":{{"coin":"BTC","time":1,"bbo":[{bid},{ask}]}}}}"#),
            &mut out,
        )
        .unwrap();
        assert_eq!(
            out.as_slice().iter().any(|a| matches!(
                a,
                SessionAction::EmitSystem(SystemEvent::QuoteInvalidated { .. })
            )),
            unavailable
        );
        assert_eq!(
            out.as_slice()
                .iter()
                .any(|a| matches!(a, SessionAction::EmitBatch(_))),
            !unavailable
        );
    }
    assert_eq!(catalog(false).instruments[0].price_increment.coefficient, 0);
}

#[test]
fn silent_candle_is_unverified_while_required_book_freshness_expires_despite_pongs() {
    let f = HyperliquidFactory::perps();
    let c = catalog(false);
    let r = ConcreteSubscriptionSet {
        items: vec![
            request(Channel::L2Book {
                depth: None,
                cadence: None,
            }),
            request(Channel::Candles {
                interval: CandleInterval::M1,
            }),
        ],
    };
    let mut m = f
        .create_session(f.plan(&r, &c).unwrap().remove(0), c)
        .unwrap();
    let mut out = ActionBuffer::new();
    m.on_input(
        SessionInput::Connected {
            now: TimestampNs(0),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    for subscription in [
        r#"{"type":"l2Book","coin":"BTC"}"#,
        r#"{"type":"candle","coin":"BTC","interval":"1m"}"#,
    ] {
        send(&mut *m,&format!(r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{subscription}}}}}"#),&mut out).unwrap();
    }
    assert!(!out.as_slice().contains(&SessionAction::MarkLive));
    out.clear();
    send(&mut *m,r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"10","sz":"1","n":1}],[{"px":"11","sz":"1","n":1}]]}}"#,&mut out).unwrap();
    assert!(out.as_slice().contains(&SessionAction::MarkLive));
    assert!(out.as_slice().iter().any(|a|matches!(a,SessionAction::EmitSystem(SystemEvent::SubscriptionStateChanged {state}) if state.contains("silence is unverified"))));
    out.clear();
    let mut pong = br#"{"channel":"pong"}"#.to_vec();
    m.on_input(
        SessionInput::TextFrame {
            bytes: &mut pong,
            received: FrameStamp {
                receive_ts: TimestampNs(60_000_000_000),
                mono_ns: 60_000_000_000,
            },
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    m.on_input(
        SessionInput::Timer {
            timer_id: 1,
            now: TimestampNs(61_000_000_000),
        },
        &mut out,
    )
    .unwrap();
    assert!(out.as_slice().contains(&SessionAction::MarkDegraded));
    assert!(
        out.as_slice()
            .contains(&SessionAction::Reconnect(ReconnectReason::Heartbeat))
    );
}

#[test]
fn direct_session_and_bootstrap_topics_preserve_native_capacity_and_order() {
    let factory = HyperliquidFactory::perps();
    let mut catalog = catalog(false);
    let template = catalog.instruments[0].clone();
    catalog.instruments = (1..=201)
        .map(|id| {
            let mut inst = template.clone();
            inst.id = InstrumentId(id);
            inst.key.native_symbol = format!("asset{id}");
            inst
        })
        .collect::<Vec<_>>()
        .into();
    let request = ConcreteSubscriptionSet {
        items: catalog
            .instruments
            .iter()
            .map(|inst| ConcreteSubscription {
                instrument: inst.id,
                channel: Channel::Trades,
                delivery: DeliveryOptions::default(),
            })
            .collect(),
    };
    let plans = factory.plan(&request, &catalog).unwrap();
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].subscriptions.items.len(), 200);
    assert_eq!(plans[1].subscriptions.items.len(), 1);
    for plan in &plans {
        let topics = HyperliquidFactory::public_plan_topics(false, plan, &catalog).unwrap();
        let mut machine = factory
            .create_session(plan.clone(), catalog.clone())
            .unwrap();
        let mut out = ActionBuffer::new();
        machine
            .on_input(
                SessionInput::Connected {
                    now: TimestampNs(1),
                },
                &mut out,
            )
            .unwrap();
        let actual = out
            .as_slice()
            .iter()
            .filter_map(|action| match action {
                SessionAction::SendText(bytes) => {
                    let control: serde_json::Value = serde_json::from_slice(bytes).unwrap();
                    Some(control["subscription"].to_string())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, topics);
    }
    let oversized = SessionSpec {
        endpoint_name: marketfeed_adapter_hyperliquid::WS_URL.into(),
        subscriptions: request,
    };
    assert!(HyperliquidFactory::public_plan_topics(false, &oversized, &catalog).is_err());
    assert!(factory.create_session(oversized, catalog).is_err());
}
