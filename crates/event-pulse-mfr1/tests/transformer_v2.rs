//! Fail-closed tests for the routed Binance MFR1 -> MechanicsInputV2 boundary.

use marketfeed_adapter_api::{ConcreteSubscriptionSet, HttpResponse, SessionSpec};
use marketfeed_adapter_binance::{
    BinanceUsdmRouteV4, BinanceUsdmSession, BinanceUsdmSessionConfig,
};
use marketfeed_event_pulse::{
    MechanicsInputRefV2, ProspectiveCaptureAdmissionV2, SourceProvenanceV2, SourceStateMachineV2,
    wire::{
        InstrumentIdentityV1, OpenInterestEncodingV1, ReplayCatalogV1, ReplayEpochEntryV1,
        Rfc3339Time, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_event_pulse_mfr1::{
    BinanceMfr1RouteV2, Mfr1MetadataBindingV2, Mfr1SessionBindingV2, Mfr1TransformContextV2,
    Mfr1TransformErrorV2, Mfr1TransformerV2,
};
use marketfeed_model::{
    AssetCode, CatalogVersion, CatalogView, ConnectionId, Fixed, Instrument, InstrumentId,
    InstrumentKey, InstrumentKind, InstrumentStatus, OverflowPolicy, SessionId, TimestampNs,
    VenueCode, VenueId,
};
use marketfeed_recording::{
    CatalogInstrumentMetadata, Direction, FixedMetadata, FrameOpcode, MetadataRecord,
    RawSegmentWriter, SessionRecordingMetadata, encode_http_response,
};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};

const PUBLIC_WS: &str = "wss://fstream.binance.com/public/ws";
const MARKET_WS: &str = "wss://fstream.binance.com/market/ws";

fn admission() -> ProspectiveCaptureAdmissionV2 {
    let value = json!({
        "schema":"event-pulse-e2-prospective-admission/2.0",
        "topology_binding":{"repository_url":"https://github.com/s1korrrr/rsibot.git","merge_commit":"05994ccd514ddb69fdd5c21a8c78af8bbe75d506","merged_at":"2026-08-23T06:58:18Z","path":"docs/superpowers/specs/event-pulse-e2-producer-evidence-freeze-v2.json","byte_length":6955,"sha256":"7216d9c5bc4b5bcd463b644c53309594413608586288beb0d32623412a42f0d7"},
        "wire_contract_binding":{"repository_url":"https://github.com/s1korrrr/rsibot.git","merge_commit":"44f3e091cb47c1b081f673e8bb09e8723a2090c6","merged_at":"2026-08-23T08:10:48Z","path":"docs/superpowers/specs/event-pulse-e2-wire-admission-v2-contract.json","byte_length":10119,"sha256":"dc79576062caf952be44e4808359c4328c2976282291838973d4884fadafa50b"},
        "capture_starts_at":"2026-08-23T08:10:48.001000Z","evidence_claim":"PROSPECTIVE_CAUSAL_CAPTURE","source_qualification":"UNVERIFIED",
        "authority":{"allocation_allowed":false,"canary_allowed":false,"capture_allowed":false,"credentials_allowed":false,"evidence_authoring_allowed":false,"execution_allowed":false,"live_allowed":false,"orders_allowed":false,"paper_allowed":false,"private_endpoints_allowed":false,"promotion_allowed":false,"risk_allowed":false}
    });
    ProspectiveCaptureAdmissionV2::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn catalog_view() -> CatalogView {
    let version = CatalogVersion(1);
    CatalogView::with_instruments(
        VenueId(3),
        version,
        vec![Instrument {
            id: InstrumentId(7),
            key: InstrumentKey {
                venue: VenueCode("binance-usdm".into()),
                native_symbol: "BNBUSDT".into(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: Some(AssetCode("USDT".into())),
                expiry_ns: None,
            },
            base: AssetCode("BNB".into()),
            quote: AssetCode("USDT".into()),
            settlement: Some(AssetCode("USDT".into())),
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

fn config(connection: u64, session: u64, l2: bool) -> BinanceUsdmSessionConfig {
    BinanceUsdmSessionConfig {
        symbols: vec!["BNBUSDT".into()],
        instrument_ids: HashMap::from([("BNBUSDT".into(), InstrumentId(7))]),
        connection: ConnectionId(connection),
        session: SessionId(session),
        enable_l2: l2,
        price_scale: 2,
        qty_scale: 3,
        ..BinanceUsdmSessionConfig::default()
    }
}

fn machines() -> (BinanceUsdmSession, BinanceUsdmSession) {
    BinanceUsdmSession::try_new_routed_pair_v4(
        SessionSpec {
            endpoint_name: PUBLIC_WS.into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        SessionSpec {
            endpoint_name: MARKET_WS.into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        catalog_view(),
        config(11, 21, true),
        config(12, 22, false),
    )
    .unwrap()
}

fn build_metadata() -> marketfeed_recording::BuildMetadata {
    let MetadataRecord::Build(v) = MetadataRecord::current_build() else {
        unreachable!()
    };
    v
}

fn session_metadata(route: BinanceMfr1RouteV2) -> SessionRecordingMetadata {
    let (session_id, endpoint) = if route == BinanceMfr1RouteV2::Public {
        (21, PUBLIC_WS)
    } else {
        (22, MARKET_WS)
    };
    SessionRecordingMetadata {
        schema_version: 1,
        session_id,
        venue_id: 3,
        adapter: "binance-usdm".into(),
        environment: "public".into(),
        endpoint: endpoint.into(),
        catalog_version: 1,
        catalog: vec![CatalogInstrumentMetadata {
            instrument_id: 7,
            native_symbol: "BNBUSDT".into(),
            kind: "PerpetualLinear".into(),
            base: "BNB".into(),
            quote: "USDT".into(),
            settlement: Some("USDT".into()),
            price_scale: 2,
            quantity_scale: 3,
            price_increment: FixedMetadata {
                coefficient: "1".into(),
                scale: 2,
            },
            quantity_increment: FixedMetadata {
                coefficient: "1".into(),
                scale: 3,
            },
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: "Active".into(),
            inverse: false,
        }],
        initial_subscriptions: vec![],
    }
}

fn replay_catalog(route: BinanceMfr1RouteV2) -> ReplayCatalogV1 {
    let (source, connection, session) = if route == BinanceMfr1RouteV2::Public {
        ("binance_primary_public", 11, 21)
    } else {
        ("binance_primary_market", 12, 22)
    };
    ReplayCatalogV1::new(
        BTreeMap::from([(3, VenueCatalogEntryV1::new("BINANCE", source).unwrap())]),
        BTreeMap::from([(
            7,
            InstrumentIdentityV1::new("BNB", "USDT", "PERPETUAL", "BINANCE", "BNBUSDT").unwrap(),
        )]),
        vec![
            ReplayEpochEntryV1::new(
                connection,
                session,
                if connection == 11 {
                    "epoch_public"
                } else {
                    "epoch_market"
                },
                0,
            )
            .unwrap(),
        ],
        if route == BinanceMfr1RouteV2::Market {
            BTreeMap::from([(7, OpenInterestEncodingV1::contracts())])
        } else {
            BTreeMap::new()
        },
    )
    .unwrap()
}

fn context(route: BinanceMfr1RouteV2) -> (Mfr1TransformerV2, i64) {
    context_with_capacity(route, 64)
}

fn context_with_capacity(
    route: BinanceMfr1RouteV2,
    dispatch_capacity: usize,
) -> (Mfr1TransformerV2, i64) {
    let admission = admission();
    let start = admission.capture_starts_at().utc_micros() * 1_000;
    let (name, connection_id, session_id) = if route == BinanceMfr1RouteV2::Public {
        ("binance_primary_public_connection", 11, 21)
    } else {
        ("binance_primary_market_connection", 12, 22)
    };
    let connection = admission
        .mechanics_config()
        .connections()
        .iter()
        .find(|key| key.source_id() == name)
        .unwrap()
        .clone();
    let system = SystemSourceV1::new(
        admission.mechanics_config().system_sources()[0].clone(),
        "epoch_system_0",
        0,
    )
    .unwrap();
    let metadata = Mfr1MetadataBindingV2::new(build_metadata(), session_metadata(route)).unwrap();
    let context = Mfr1TransformContextV2::new(
        admission,
        replay_catalog(route),
        Mfr1SessionBindingV2::new(connection, connection_id, session_id, route),
        metadata,
        system,
        dispatch_capacity,
        OverflowPolicy::DropNewest,
    )
    .unwrap();
    (Mfr1TransformerV2::new(context), start)
}

fn mfr(route: BinanceMfr1RouteV2, start: i64, records: &[(u64, FrameOpcode, Vec<u8>)]) -> Vec<u8> {
    let session = if route == BinanceMfr1RouteV2::Public {
        21
    } else {
        22
    };
    let mut writer = RawSegmentWriter::create(Vec::new(), start).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start)
        .unwrap();
    writer
        .write_metadata(&MetadataRecord::Session(session_metadata(route)), start)
        .unwrap();
    for (ordinal, (frame, opcode, payload)) in records.iter().enumerate() {
        let at = start + (i64::try_from(ordinal).unwrap() + 1) * 1_000_000;
        writer
            .write_record(
                SessionId(session),
                *frame,
                at,
                u64::try_from(at).unwrap(),
                Direction::Inbound,
                *opcode,
                0,
                payload,
            )
            .unwrap();
    }
    writer.into_inner()
}
fn decision(start: i64, records: usize) -> Rfc3339Time {
    Rfc3339Time::from_unix_nanos(start + (i64::try_from(records).unwrap() + 2) * 1_000_000).unwrap()
}

#[test]
fn public_quote_preserves_full_u64_provenance_with_derived_raw_cursor() {
    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let source_ms = u64::try_from(start.div_euclid(1_000_000)).unwrap();
    let payload=format!(r#"{{"e":"bookTicker","E":{source_ms},"T":{source_ms},"u":{},"s":"BNBUSDT","b":"650.0","B":"1.2","a":"650.1","A":"0.8"}}"#,u64::MAX).into_bytes();
    let output = transformer
        .transform(
            public,
            &mfr(
                BinanceMfr1RouteV2::Public,
                start,
                &[(41, FrameOpcode::Text, payload)],
            ),
            TimestampNs(start),
            decision(start, 1),
        )
        .unwrap();
    assert_eq!(output.inputs().len(), 1);
    assert!(!output.evidence_authoring_allowed());
    assert_eq!(output.blocker(), "blocked:fixture-provenance");
    let MechanicsInputRefV2::Market {
        envelope,
        market_cursor,
        source_provenance,
        ..
    } = output.inputs()[0].view()
    else {
        panic!("market")
    };
    assert_eq!(envelope.frame_seq, 41);
    assert_eq!(market_cursor.derived_coordinate(), Some((41, 0, 0)));
    assert_eq!(
        source_provenance,
        &SourceProvenanceV2::BinanceBookTicker {
            update_id: u64::MAX,
            event_time_ms: source_ms,
            transaction_time_ms: source_ms
        }
    );
}

#[test]
fn market_trade_force_order_and_open_interest_are_strict_v2_inputs() {
    let (transformer, start) = context(BinanceMfr1RouteV2::Market);
    let (_, market) = machines();
    let source_ms = u64::try_from(start.div_euclid(1_000_000)).unwrap();
    let oi = encode_http_response(
        1,
        &HttpResponse {
            status: 200,
            headers: vec![],
            body: format!(
                r#"{{"symbol":"BNBUSDT","openInterest":"10659.509","time":{}}}"#,
                source_ms + 2
            )
            .into_bytes()
            .into(),
        },
    )
    .unwrap();
    let records=vec![(51,FrameOpcode::Text,format!(r#"{{"e":"aggTrade","E":{source_ms},"s":"BNBUSDT","a":42,"p":"650.1","q":"0.01","T":{source_ms},"m":false}}"#).into_bytes()),(52,FrameOpcode::Text,format!(r#"{{"e":"forceOrder","E":{},"o":{{"s":"BNBUSDT","S":"SELL","ap":"649","l":"0.5","T":{}}}}}"#,source_ms+1,source_ms+1).into_bytes()),(53,FrameOpcode::HttpResponse,oi)];
    let output = transformer
        .transform(
            market,
            &mfr(BinanceMfr1RouteV2::Market, start, &records),
            TimestampNs(start),
            decision(start, records.len()),
        )
        .unwrap();
    assert_eq!(output.inputs().len(), 3);
    assert!(output.canonical_jsonl().ends_with(b"\n"));
    let mut state = SourceStateMachineV2::new(admission().mechanics_config().clone());
    for input in output.inputs() {
        input.validate_static().unwrap();
        state.ingest(input).unwrap();
    }
}

#[test]
fn public_book_snapshot_consumes_buffered_and_live_delta_provenance() {
    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let source_ms = u64::try_from(start.div_euclid(1_000_000)).unwrap();
    let snapshot = encode_http_response(
        1,
        &HttpResponse {
            status: 200,
            headers: vec![],
            body: format!(r#"{{"lastUpdateId":100,"E":{},"T":{},"bids":[["650.0","1"]],"asks":[["651.0","2"]]}}"#,source_ms+2,source_ms+2).into_bytes().into(),
        },
    )
    .unwrap();
    let records = vec![
        (
            1,
            FrameOpcode::Text,
            format!(r#"{{"e":"bookTicker","E":{source_ms},"T":{source_ms},"u":99,"s":"BNBUSDT","b":"650.0","B":"1","a":"651.0","A":"2"}}"#).into_bytes(),
        ),
        (
            2,
            FrameOpcode::Text,
            format!(r#"{{"e":"depthUpdate","E":{},"T":{},"s":"BNBUSDT","U":99,"u":101,"pu":98,"b":[["650.0","1.5"]],"a":[["651.0","1.5"]]}}"#,source_ms+1,source_ms+1).into_bytes(),
        ),
        (3, FrameOpcode::HttpResponse, snapshot),
        (
            4,
            FrameOpcode::Text,
            format!(r#"{{"e":"depthUpdate","E":{},"T":{},"s":"BNBUSDT","U":102,"u":102,"pu":101,"b":[["650.0","2"]],"a":[]}}"#,source_ms+3,source_ms+3).into_bytes(),
        ),
    ];
    let output = transformer
        .transform(
            public,
            &mfr(BinanceMfr1RouteV2::Public, start, &records),
            TimestampNs(start),
            decision(start, records.len()),
        )
        .unwrap();
    assert_eq!(output.inputs().len(), 4);
    assert_eq!(
        output
            .frames()
            .iter()
            .map(|frame| frame.frame_seq())
            .collect::<Vec<_>>(),
        vec![1, 3, 4]
    );
    assert_eq!(output.frames()[1].inputs().len(), 2);
    let provenances = output
        .inputs()
        .iter()
        .map(|input| match input.view() {
            MechanicsInputRefV2::Market {
                source_provenance, ..
            } => source_provenance,
            MechanicsInputRefV2::NonMarket(_) => panic!("market"),
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        provenances[1],
        SourceProvenanceV2::BinanceBookSnapshot {
            last_update_id: 100,
            ..
        }
    ));
    assert!(matches!(
        provenances[2],
        SourceProvenanceV2::BinanceBookDelta {
            first_update_id: 99,
            final_update_id: 101,
            previous_final_update_id: 98,
            ..
        }
    ));
    assert!(matches!(
        provenances[3],
        SourceProvenanceV2::BinanceBookDelta {
            first_update_id: 102,
            final_update_id: 102,
            previous_final_update_id: 101,
            ..
        }
    ));
}

#[test]
fn real_market_dispatch_overflow_is_reserved_and_does_not_leave_ledger_entries() {
    let (transformer, start) = context_with_capacity(BinanceMfr1RouteV2::Public, 1);
    let (public, _) = machines();
    let source_ms = u64::try_from(start.div_euclid(1_000_000)).unwrap();
    let snapshot = encode_http_response(
        1,
        &HttpResponse {
            status: 200,
            headers: vec![],
            body: format!(r#"{{"lastUpdateId":100,"E":{},"T":{},"bids":[["650.0","1"]],"asks":[["651.0","2"]]}}"#,source_ms+1,source_ms+1).into_bytes().into(),
        },
    )
    .unwrap();
    let records = vec![
        (2, FrameOpcode::Text, format!(r#"{{"e":"depthUpdate","E":{source_ms},"T":{source_ms},"s":"BNBUSDT","U":99,"u":101,"pu":98,"b":[["650.0","1.5"]],"a":[["651.0","1.5"]]}}"#).into_bytes()),
        (3, FrameOpcode::HttpResponse, snapshot),
    ];
    let output = transformer
        .transform(
            public,
            &mfr(BinanceMfr1RouteV2::Public, start, &records),
            TimestampNs(start),
            decision(start, records.len()),
        )
        .unwrap();
    assert_eq!(output.dropped_market_dispatch(), 1);
    assert_eq!(output.inputs().len(), 2);
    assert!(matches!(
        output.inputs()[1].view(),
        MechanicsInputRefV2::NonMarket(_)
    ));
}

#[test]
fn wrong_role_fails_before_any_output() {
    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let bytes = mfr(
        BinanceMfr1RouteV2::Public,
        start,
        &[(
            1,
            FrameOpcode::Text,
            br#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":1,"p":"650","q":"1","T":2,"m":false}"#
                .to_vec(),
        )],
    );
    assert_eq!(
        transformer
            .transform(public, &bytes, TimestampNs(start), decision(start, 1))
            .unwrap_err(),
        Mfr1TransformErrorV2::Provenance
    );
}

#[test]
fn routed_payload_bounds_symbol_times_and_market_frame_order_fail_closed() {
    let cases = [
        (BinanceMfr1RouteV2::Market, 1, br#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":9223372036854775808,"p":"650","q":"1","T":2,"m":false}"#.as_slice()),
        (BinanceMfr1RouteV2::Market, 1, br#"{"e":"aggTrade","E":1,"s":"BNBUSDC","a":1,"p":"650","q":"1","T":2,"m":false}"#.as_slice()),
        (BinanceMfr1RouteV2::Market, 1, br#"{"e":"aggTrade","E":1,"s":"BNBUSDT","a":1,"p":"650","q":"1","m":false}"#.as_slice()),
        (BinanceMfr1RouteV2::Public, 1, br#"{"e":"depthUpdate","E":1,"T":2,"s":"BNBUSDT","U":1,"u":9223372036854775808,"pu":0,"b":[],"a":[]}"#.as_slice()),
        (BinanceMfr1RouteV2::Public, 0, br#"{"e":"bookTicker","E":1,"T":2,"u":1,"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}"#.as_slice()),
    ];
    for (route, frame, payload) in cases {
        let (transformer, start) = context(route);
        let (public, market) = machines();
        let machine = if route == BinanceMfr1RouteV2::Public {
            public
        } else {
            market
        };
        let bytes = mfr(
            route,
            start,
            &[(frame, FrameOpcode::Text, payload.to_vec())],
        );
        assert!(
            transformer
                .transform(machine, &bytes, TimestampNs(start), decision(start, 1))
                .is_err()
        );
    }

    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let source_ms = start.div_euclid(1_000_000);
    let quote = format!(r#"{{"e":"bookTicker","E":{source_ms},"T":{source_ms},"u":1,"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}}"#).into_bytes();
    let bytes = mfr(
        BinanceMfr1RouteV2::Public,
        start,
        &[
            (2, FrameOpcode::Text, quote.clone()),
            (1, FrameOpcode::Text, quote),
        ],
    );
    assert_eq!(
        transformer
            .transform(public, &bytes, TimestampNs(start), decision(start, 2))
            .unwrap_err(),
        Mfr1TransformErrorV2::Order
    );
}

#[test]
fn source_time_must_be_inside_admission_and_not_after_receive() {
    for offset in [-1_i64, 2] {
        let (transformer, start) = context(BinanceMfr1RouteV2::Public);
        let (public, _) = machines();
        let source_ms = start.div_euclid(1_000_000) + offset;
        let quote = format!(r#"{{"e":"bookTicker","E":{source_ms},"T":{source_ms},"u":1,"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}}"#).into_bytes();
        let bytes = mfr(
            BinanceMfr1RouteV2::Public,
            start,
            &[(1, FrameOpcode::Text, quote)],
        );
        assert_eq!(
            transformer
                .transform(public, &bytes, TimestampNs(start), decision(start, 1))
                .unwrap_err(),
            Mfr1TransformErrorV2::Provenance
        );
    }
}

#[test]
fn exact_subscription_ack_is_control_only_and_other_ids_fail() {
    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let ok = mfr(
        BinanceMfr1RouteV2::Public,
        start,
        &[(1, FrameOpcode::Text, br#"{"result":null,"id":1}"#.to_vec())],
    );
    let output = transformer
        .transform(public, &ok, TimestampNs(start), decision(start, 1))
        .unwrap();
    assert!(output.inputs().is_empty());
    assert_eq!(output.frames_applied(), 1);

    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let bad = mfr(
        BinanceMfr1RouteV2::Public,
        start,
        &[(1, FrameOpcode::Text, br#"{"result":null,"id":2}"#.to_vec())],
    );
    assert_eq!(
        transformer
            .transform(public, &bad, TimestampNs(start), decision(start, 1))
            .unwrap_err(),
        Mfr1TransformErrorV2::Provenance
    );
}

#[test]
fn duplicate_book_native_ledger_key_and_unknown_http_id_fail() {
    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let source_ms = start.div_euclid(1_000_000);
    let depth = format!(r#"{{"e":"depthUpdate","E":{source_ms},"T":{source_ms},"s":"BNBUSDT","U":99,"u":101,"pu":98,"b":[],"a":[]}}"#).into_bytes();
    let duplicate = mfr(
        BinanceMfr1RouteV2::Public,
        start,
        &[
            (1, FrameOpcode::Text, depth.clone()),
            (2, FrameOpcode::Text, depth),
        ],
    );
    assert_eq!(
        transformer
            .transform(public, &duplicate, TimestampNs(start), decision(start, 2))
            .unwrap_err(),
        Mfr1TransformErrorV2::ProvenanceLedger
    );

    let (transformer, start) = context(BinanceMfr1RouteV2::Market);
    let (_, market) = machines();
    let body = format!(
        r#"{{"symbol":"BNBUSDT","openInterest":"1","time":{}}}"#,
        start.div_euclid(1_000_000)
    )
    .into_bytes()
    .into();
    let response = encode_http_response(
        99,
        &HttpResponse {
            status: 200,
            headers: vec![],
            body,
        },
    )
    .unwrap();
    let bytes = mfr(
        BinanceMfr1RouteV2::Market,
        start,
        &[(1, FrameOpcode::HttpResponse, response)],
    );
    assert!(matches!(
        transformer.transform(market, &bytes, TimestampNs(start), decision(start, 1)),
        Err(Mfr1TransformErrorV2::Adapter(_))
    ));
}

#[test]
fn one_to_three_byte_tails_fail_closed() {
    for tail in 1..=3 {
        let (transformer, start) = context(BinanceMfr1RouteV2::Public);
        let (public, _) = machines();
        let mut bytes = mfr(BinanceMfr1RouteV2::Public, start, &[]);
        bytes.extend(std::iter::repeat_n(1, tail));
        assert!(
            transformer
                .transform(public, &bytes, TimestampNs(start), decision(start, 0))
                .is_err()
        );
    }
}

#[test]
fn v2_transformer_api_is_additive_and_false_authority() {
    let _ = std::mem::size_of::<Mfr1SessionBindingV2>();
    let _ = std::mem::size_of::<Mfr1MetadataBindingV2>();
    let _ = std::mem::size_of::<Mfr1TransformContextV2>();
    let _ = std::mem::size_of::<Mfr1TransformerV2>();
    assert_ne!(BinanceMfr1RouteV2::Public, BinanceMfr1RouteV2::Market);
    assert_ne!(BinanceUsdmRouteV4::Public, BinanceUsdmRouteV4::Market);
}
