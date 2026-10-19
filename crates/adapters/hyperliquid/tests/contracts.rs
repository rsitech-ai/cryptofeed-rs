use bytes::Bytes;
use marketfeed_adapter_api::*;
use marketfeed_adapter_hyperliquid::{
    HyperliquidFactory,
    protocol::{self, Message},
};
use marketfeed_model::*;

const SPOT: &str = r#"{"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"},{"name":"DUP","szDecimals":2,"weiDecimals":8,"index":9,"tokenId":"0x9"}],"universe":[{"name":"PURR/USDC","tokens":[7,0],"index":0},{"name":"@12","tokens":[9,0],"index":12}]}"#;
const PERPS: &str = r#"{"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5},{"name":"OLD","szDecimals":3,"isDelisted":true}]}"#;
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
