use marketfeed_adapter_api::*;
use marketfeed_adapter_binance::*;
use marketfeed_model::*;

fn machine(venue: u16) -> Box<dyn SessionMachine> {
    let spec = SessionSpec {
        endpoint_name: "fixture".into(),
        subscriptions: Default::default(),
    };
    let catalog = CatalogView::new(VenueId(venue), CatalogVersion(1));
    match venue {
        2 => Box::new(BinanceSpotSession::new(spec, catalog, Default::default())),
        3 => Box::new(BinanceUsdmSession::new(spec, catalog, Default::default())),
        12 => Box::new(BinanceCoinmSession::new(spec, catalog, Default::default())),
        _ => unreachable!(),
    }
}
fn drive(session: &mut dyn SessionMachine, raw: &str) -> Vec<SessionAction> {
    let mut output = ActionBuffer::new();
    session
        .on_input(
            SessionInput::TextFrame {
                bytes: &mut raw.as_bytes().to_vec(),
                received: FrameStamp {
                    receive_ts: TimestampNs(99),
                    mono_ns: 7,
                },
            },
            &mut output,
        )
        .unwrap();
    output.drain().collect()
}
fn trade(actions: &[SessionAction]) -> &EventEnvelope {
    actions
        .iter()
        .find_map(|action| match action {
            SessionAction::EmitBatch(batch) => batch
                .events
                .iter()
                .find(|e| matches!(e.payload, MarketEvent::Trade(_))),
            _ => None,
        })
        .expect("one native trade")
}

#[test]
fn native_aggregate_is_exact_and_does_not_invent_constituent_count() {
    for (venue, symbol, st) in [(3, "BTCUSDT", 1), (12, "BTCUSD_PERP", 2)] {
        let mut session = machine(venue);
        let raw = format!(
            r#"{{"e":"aggTrade","E":2,"T":1,"s":"{symbol}","a":9007199254740993,
            "p":"12345678901234567890.123456789012345678","q":"2.000000000000000001",
            "nq":"1.000000000000000001","f":9007199254740995,"l":9007199254741005,"m":true,"st":{st}}}"#
        );
        let actions = drive(session.as_mut(), &raw);
        let event = trade(&actions);
        assert_eq!(event.exchange_ts, Some(TimestampNs(1_000_000)));
        assert_eq!(event.source_sequence, None);
        let MarketEvent::Trade(trade) = &event.payload else {
            unreachable!()
        };
        assert_eq!(
            trade.price.0,
            Fixed::new(12345678901234567890123456789012345678, 18)
        );
        assert_eq!(trade.quantity.0, Fixed::new(2000000000000000001, 18));
        assert_eq!(trade.aggressor, AggressorSide::Sell);
        assert_eq!(
            trade.trade_id.as_ref().unwrap().0,
            "aggTrade:9007199254740993"
        );
        let source = trade.provenance.as_ref().unwrap();
        assert_eq!(source.granularity, TradeGranularity::NativeAggregate);
        assert_eq!(source.native_count, None, "f/l are not an execution count");
        assert_eq!(
            source.constituent_first_id.as_ref().unwrap().0,
            "9007199254740995"
        );
        assert_eq!(
            source.constituent_last_id.as_ref().unwrap().0,
            "9007199254741005"
        );
        assert_eq!(source.native_attributes["nq"], "1.000000000000000001");
    }
}

#[test]
fn individual_and_aggregate_native_ids_never_collide() {
    for (venue, symbol) in [(3, "BTCUSDT"), (12, "BTCUSD_PERP")] {
        let mut session = machine(venue);
        let individual = drive(
            session.as_mut(),
            &format!(
                r#"{{"e":"trade","E":2,"T":1,"s":"{symbol}","t":7,"p":"10","q":"2","m":false,"X":"MARKET"}}"#
            ),
        );
        let aggregate = drive(
            session.as_mut(),
            &format!(
                r#"{{"e":"aggTrade","E":2,"T":1,"s":"{symbol}","a":7,"p":"10","q":"2","m":false}}"#
            ),
        );
        let (MarketEvent::Trade(a), MarketEvent::Trade(b)) =
            (&trade(&individual).payload, &trade(&aggregate).payload)
        else {
            unreachable!()
        };
        assert_eq!(a.trade_id.as_ref().unwrap().0, "7");
        assert_eq!(b.trade_id.as_ref().unwrap().0, "aggTrade:7");
        assert_eq!(a.provenance.as_ref().unwrap().source_channel, "trade");
        assert_eq!(
            a.provenance.as_ref().unwrap().granularity,
            TradeGranularity::Unverified,
            "current official futures docs do not qualify the legacy individual channel"
        );
        assert_eq!(a.aggressor, AggressorSide::Buy);
    }
}

#[test]
fn exclusions_are_visible_and_unclassified_bad_financial_values_fail() {
    for (venue, symbol) in [(3, "BTCUSDT"), (12, "BTCUSD_PERP")] {
        let mut session = machine(venue);
        for (price, quantity, kind) in [
            ("0", "0", "NA"),
            ("10", "2", "ADL"),
            ("10", "2", "NEW_TYPE"),
        ] {
            let actions = drive(
                session.as_mut(),
                &format!(
                    r#"{{"e":"trade","E":2,"T":1,"s":"{symbol}","t":7,"p":"{price}","q":"{quantity}","m":false,"X":"{kind}"}}"#
                ),
            );
            assert!(
                !actions
                    .iter()
                    .any(|a| matches!(a, SessionAction::EmitBatch(_)))
            );
            let record = actions
                .iter()
                .find_map(|a| match a {
                    SessionAction::EmitSystem(SystemEvent::TradeRecordExcluded(record)) => {
                        Some(record)
                    }
                    _ => None,
                })
                .expect("classified record is never silently ignored");
            assert_eq!(record.native_id.0, "7");
            assert_eq!(record.native_attributes["p"], price);
            assert_eq!(record.reason, format!("native_type_not_admitted:{kind}"));
        }
        for invalid in [
            r#""p":"0","q":"0""#,
            r#""p":"-1","q":"2""#,
            r#""p":"1","q":"-2""#,
            r#""p":"1","q":"2","nq":"3""#,
            r#""p":"1","q":"2","E":9223372036854775807""#,
        ] {
            let actions = drive(
                session.as_mut(),
                &format!(r#"{{"e":"trade","T":1,"s":"{symbol}","t":7,"m":false,{invalid}}}"#),
            );
            assert!(
                !actions
                    .iter()
                    .any(|a| matches!(a, SessionAction::EmitBatch(_)))
            );
            assert!(
                actions.iter().any(|a| matches!(
                    a,
                    SessionAction::EmitSystem(SystemEvent::ParseError { .. })
                ))
            );
        }
    }
}

#[test]
fn malformed_ranges_product_scope_and_timestamp_overflow_are_rejected() {
    for (venue, symbol, wrong_st) in [(3, "BTCUSDT", 2), (12, "BTCUSD_PERP", 1)] {
        for extra in [
            r#""f":4"#.to_owned(),
            r#""f":4,"l":3"#.into(),
            format!(r#""st":{wrong_st}"#),
            r#""T":9223372036854775807"#.into(),
        ] {
            let raw = format!(
                r#"{{"e":"aggTrade","s":"{symbol}","a":9,"p":"1","q":"2","m":false,"T":1,{extra}}}"#
            );
            let decoded = if venue == 3 {
                decode_usdm_text(raw.as_bytes()).map(|_| ())
            } else {
                decode_coinm_text(raw.as_bytes()).map(|_| ())
            };
            assert!(decoded.is_err(), "{raw}");
        }
    }
}

#[test]
fn spot_side_granularity_and_time_are_explicit() {
    let mut session = machine(2);
    let actions = drive(
        session.as_mut(),
        r#"{"e":"trade","E":2,"T":1,"s":"BTCUSDT","t":9007199254740993,"p":"10.00","q":"2.00","m":true}"#,
    );
    let event = trade(&actions);
    assert_eq!(event.source_sequence, None);
    let MarketEvent::Trade(trade) = &event.payload else {
        unreachable!()
    };
    assert_eq!(trade.trade_id.as_ref().unwrap().0, "9007199254740993");
    assert_eq!(
        trade.provenance.as_ref().unwrap().granularity,
        TradeGranularity::IndividualExecution
    );
    assert_eq!(trade.aggressor, AggressorSide::Sell);
    assert!(decode_text(br#"{"e":"trade","T":9223372036854775807,"s":"BTCUSDT","t":1,"p":"1","q":"1","m":true}"#).is_err());
}
