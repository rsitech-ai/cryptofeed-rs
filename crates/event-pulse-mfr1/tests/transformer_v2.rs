//! Fail-closed tests for the routed Binance MFR1 -> MechanicsInputV2 boundary.

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, HttpResponse, SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{
    BinanceUsdmRouteV4, BinanceUsdmSession, BinanceUsdmSessionConfig,
};
use marketfeed_event_pulse::{
    MechanicsInputRefV2, MechanicsInputV2JsonlReader, ProspectiveCaptureAdmissionV2,
    SourceProvenanceV2, SourceStateMachineV2,
    wire::{
        FamilyV1, InstrumentIdentityV1, OpenInterestEncodingV1, ReplayCatalogV1,
        ReplayEpochEntryV1, Rfc3339Time, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_event_pulse_mfr1::{
    BinanceMfr1RouteV2, Mfr1MetadataBindingV2, Mfr1SessionBindingV2, Mfr1TransformContextV2,
    Mfr1TransformErrorV2, Mfr1TransformOutputV2, Mfr1TransformerV2,
};
use marketfeed_model::{
    AssetCode, CatalogVersion, CatalogView, ConnectionId, Fixed, Instrument, InstrumentId,
    InstrumentKey, InstrumentKind, InstrumentStatus, OverflowPolicy, SessionId, TimestampNs,
    VenueCode, VenueId,
};
use marketfeed_recording::{
    CatalogInstrumentMetadata, Direction, FixedMetadata, FrameOpcode, MetadataRecord,
    RawSegmentWriter, SessionRecordingMetadata, SubscriptionMetadata, encode_http_response,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::process::Command;

const PUBLIC_WS: &str = "wss://fstream.binance.com/public/ws";
const MARKET_WS: &str = "wss://fstream.binance.com/market/ws";
const ORACLE_PATH: &str = "crates/event-pulse-mfr1/tests/fixtures/routed_v2_expected.jsonl";

fn assert_git_lf_if_repository(path: &std::path::Path) {
    assert_git_lf_if_repository_with_command(path, "git");
}

fn assert_git_lf_if_repository_with_command(path: &std::path::Path, command: &str) {
    let probe = match Command::new(command)
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()
    {
        Ok(probe) => probe,
        Err(_) => return,
    };
    if !probe.status.success() {
        return;
    }

    let output = Command::new(command)
        .args(["check-attr", "text", "eol", "--", ORACLE_PATH])
        .current_dir(path)
        .output()
        .expect("git check-attr must execute");
    assert!(output.status.success());
    let attributes = String::from_utf8(output.stdout).expect("Git attributes must be UTF-8");
    assert!(
        attributes.contains(&format!("{ORACLE_PATH}: text: set")),
        "oracle must be classified as text: {attributes}"
    );
    assert!(
        attributes.contains(&format!("{ORACLE_PATH}: eol: lf")),
        "oracle must retain LF bytes on every checkout: {attributes}"
    );
}

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

#[test]
fn routed_v2_oracle_has_repository_enforced_lf_and_exact_bytes() {
    let bytes = include_bytes!("fixtures/routed_v2_expected.jsonl");
    assert_eq!(bytes.len(), 7_736);
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 7);
    assert!(!bytes.contains(&b'\r'));
    assert_eq!(
        format!("{:x}", Sha256::digest(bytes)),
        "a65c1f39f7dc0150748d0f0facb0ea6cc09ca0dcedeaaff07284513c90040237"
    );

    let repository_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    assert_git_lf_if_repository(repository_root);

    let archive_root = std::env::temp_dir().join(format!(
        "marketfeed-event-pulse-source-archive-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&archive_root).unwrap();
    assert_git_lf_if_repository(&archive_root);
    assert_git_lf_if_repository_with_command(
        &archive_root,
        "definitely-not-an-installed-git-executable",
    );
    std::fs::remove_dir(&archive_root).unwrap();
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
    context_with_policy(route, dispatch_capacity, OverflowPolicy::DropNewest)
}

fn context_with_policy(
    route: BinanceMfr1RouteV2,
    dispatch_capacity: usize,
    overflow: OverflowPolicy,
) -> (Mfr1TransformerV2, i64) {
    let (context, start) = context_result_with_policy(route, dispatch_capacity, overflow);
    (Mfr1TransformerV2::new(context.unwrap()), start)
}

fn context_result_with_policy(
    route: BinanceMfr1RouteV2,
    dispatch_capacity: usize,
    overflow: OverflowPolicy,
) -> (Result<Mfr1TransformContextV2, Mfr1TransformErrorV2>, i64) {
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
        overflow,
    );
    (context, start)
}

fn public_fixture_output() -> Mfr1TransformOutputV2 {
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
        (1, FrameOpcode::Text, format!(r#"{{"e":"bookTicker","E":{source_ms},"T":{source_ms},"u":99,"s":"BNBUSDT","b":"650.0","B":"1","a":"651.0","A":"2"}}"#).into_bytes()),
        (2, FrameOpcode::Text, format!(r#"{{"e":"depthUpdate","E":{},"T":{},"s":"BNBUSDT","U":99,"u":101,"pu":98,"b":[["650.0","1.5"]],"a":[["651.0","1.5"]]}}"#,source_ms+1,source_ms+1).into_bytes()),
        (3, FrameOpcode::HttpResponse, snapshot),
        (4, FrameOpcode::Text, format!(r#"{{"e":"depthUpdate","E":{},"T":{},"s":"BNBUSDT","U":102,"u":102,"pu":101,"b":[["650.0","2"]],"a":[]}}"#,source_ms+3,source_ms+3).into_bytes()),
    ];
    transformer
        .transform(
            public,
            &mfr(BinanceMfr1RouteV2::Public, start, &records),
            TimestampNs(start),
            decision(start, records.len()),
        )
        .unwrap()
}

fn market_fixture_output() -> Mfr1TransformOutputV2 {
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
    transformer
        .transform(
            market,
            &mfr(BinanceMfr1RouteV2::Market, start, &records),
            TimestampNs(start),
            decision(start, records.len()),
        )
        .unwrap()
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

fn mfr_pings(route: BinanceMfr1RouteV2, start: i64, count: usize) -> Vec<u8> {
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
    for ordinal in 0..count {
        let offset = i64::try_from(ordinal).unwrap() + 1;
        writer
            .write_record(
                SessionId(session),
                u64::try_from(ordinal).unwrap() + 1,
                start + offset,
                u64::try_from(start + offset).unwrap(),
                Direction::Inbound,
                FrameOpcode::Ping,
                0,
                &[],
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
    let output = public_fixture_output();
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
fn seven_record_oracle_is_independent_canonical_and_state_equivalent() {
    let public = public_fixture_output();
    let market = market_fixture_output();
    let mut actual_bytes = public.canonical_jsonl().to_vec();
    actual_bytes.extend_from_slice(market.canonical_jsonl());
    let expected_bytes = include_bytes!("fixtures/routed_v2_expected.jsonl");
    assert_eq!(actual_bytes, expected_bytes);

    let start = admission().capture_starts_at().utc_micros() * 1_000;
    let split = expected_bytes
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(3)
        .map(|(index, _)| index + 1)
        .unwrap();
    let not_after = Rfc3339Time::from_unix_nanos(start + 1_000_000_000).unwrap();
    let mut expected =
        MechanicsInputV2JsonlReader::new(&expected_bytes[..split], not_after.clone())
            .read_all()
            .unwrap();
    expected.extend(
        MechanicsInputV2JsonlReader::new(&expected_bytes[split..], not_after)
            .read_all()
            .unwrap(),
    );
    let actual = public
        .inputs()
        .iter()
        .chain(market.inputs())
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(
        actual
            .iter()
            .map(|input| input.payload_hash())
            .collect::<Vec<_>>(),
        [
            "c4bdb697dbc1ac40b4e3afb396d6edd18f22a9502faf1f0c6e333e3a9ba0a802",
            "4b606841f271568385eccc7e7f7d520ce15d5a0abbfbf02256a855cb5edf34c4",
            "7487328cc27525e711121693cb1621986b68db9660b8444dc9d86a4891b85626",
            "3bb2442736ef2fb1f18c6c2f7402d08ec85cbad3004f7d246b730e5292a6b107",
            "46aa3b7d2c37465169cdf16a8ccf51558c7ef3d56ad0630e9a8931bd520d354e",
            "46e249573e362c3865164ebbe54fe7f1843011a2ece8df7d62f37678d7ec728d",
            "31915db8a8d5c510085a575ebab5f463b53ee526dd652ec43f3b238cc10085be",
        ]
    );

    let config = admission().mechanics_config().clone();
    let mut actual_state = SourceStateMachineV2::new(config.clone());
    let mut expected_state = SourceStateMachineV2::new(config.clone());
    let actual_outcomes = actual
        .iter()
        .map(|input| actual_state.ingest(input))
        .collect::<Vec<_>>();
    let expected_outcomes = expected
        .iter()
        .map(|input| expected_state.ingest(input))
        .collect::<Vec<_>>();
    assert_eq!(actual_outcomes, expected_outcomes);
    assert!(actual_outcomes.iter().all(Result::is_ok));
    for contributor in config.contributors() {
        for family in contributor.allowed_families() {
            if matches!(
                family,
                FamilyV1::Trade
                    | FamilyV1::Quote
                    | FamilyV1::Book
                    | FamilyV1::OpenInterest
                    | FamilyV1::Liquidation
            ) {
                assert_eq!(
                    actual_state.market_state(contributor.key(), *family),
                    expected_state.market_state(contributor.key(), *family)
                );
                assert_eq!(
                    actual_state.market_cursor(contributor.key(), *family),
                    expected_state.market_cursor(contributor.key(), *family)
                );
            }
        }
    }
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
fn fail_engine_policy_rejects_the_same_real_dispatch_overflow_atomically() {
    let (transformer, start) =
        context_with_policy(BinanceMfr1RouteV2::Public, 1, OverflowPolicy::FailEngine);
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
        (2, FrameOpcode::Text, format!(r#"{{"e":"depthUpdate","E":{source_ms},"T":{source_ms},"s":"BNBUSDT","U":99,"u":101,"pu":98,"b":[],"a":[]}}"#).into_bytes()),
        (3, FrameOpcode::HttpResponse, snapshot),
    ];
    assert!(matches!(
        transformer.transform(
            public,
            &mfr(BinanceMfr1RouteV2::Public, start, &records),
            TimestampNs(start),
            decision(start, records.len()),
        ),
        Err(Mfr1TransformErrorV2::Adapter(_))
    ));
}

#[test]
fn derived_action_capacity_rejects_65_536_for_both_policies() {
    for policy in [OverflowPolicy::DropNewest, OverflowPolicy::FailEngine] {
        let (result, _) = context_result_with_policy(BinanceMfr1RouteV2::Public, 16_384, policy);
        assert!(matches!(
            result,
            Err(Mfr1TransformErrorV2::InvalidExecutionMetadata)
        ));
    }
}

#[test]
fn replay_record_cap_accepts_exact_boundary_and_rejects_one_over() {
    let exact = mfr_pings(
        BinanceMfr1RouteV2::Public,
        admission().capture_starts_at().utc_micros() * 1_000,
        65_534,
    );
    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    let output = transformer
        .transform(
            public,
            &exact,
            TimestampNs(start),
            Rfc3339Time::from_unix_nanos(start + 1_000_000_000).unwrap(),
        )
        .unwrap();
    assert!(output.inputs().is_empty());

    let one_over = mfr_pings(BinanceMfr1RouteV2::Public, start, 65_535);
    let (transformer, _) = context(BinanceMfr1RouteV2::Public);
    let (public, _) = machines();
    assert_eq!(
        transformer
            .transform(
                public,
                &one_over,
                TimestampNs(start),
                Rfc3339Time::from_unix_nanos(start + 1_000_000_000).unwrap(),
            )
            .unwrap_err(),
        Mfr1TransformErrorV2::Capacity
    );
}

#[test]
fn late_replay_failure_returns_no_partial_output() {
    let (transformer, start) = context(BinanceMfr1RouteV2::Market);
    let (_, market) = machines();
    let source_ms = start.div_euclid(1_000_000);
    let unknown = encode_http_response(
        99,
        &HttpResponse {
            status: 200,
            headers: vec![],
            body: format!(r#"{{"symbol":"BNBUSDT","openInterest":"1","time":{source_ms}}}"#)
                .into_bytes()
                .into(),
        },
    )
    .unwrap();
    let records = vec![
        (1, FrameOpcode::Text, format!(r#"{{"e":"aggTrade","E":{source_ms},"s":"BNBUSDT","a":42,"p":"650.1","q":"0.01","T":{source_ms},"m":false}}"#).into_bytes()),
        (2, FrameOpcode::HttpResponse, unknown),
    ];
    assert!(matches!(
        transformer.transform(
            market,
            &mfr(BinanceMfr1RouteV2::Market, start, &records),
            TimestampNs(start),
            decision(start, records.len()),
        ),
        Err(Mfr1TransformErrorV2::Adapter(_))
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
fn event_time_is_retained_but_only_transaction_time_is_causal() {
    for event_offset in [-100_i64, 100] {
        let (transformer, start) = context(BinanceMfr1RouteV2::Public);
        let (public, _) = machines();
        let start_ms = start.div_euclid(1_000_000);
        let event_ms = start_ms + event_offset;
        let quote = format!(r#"{{"e":"bookTicker","E":{event_ms},"T":{start_ms},"u":1,"s":"BNBUSDT","b":"650","B":"1","a":"651","A":"1"}}"#).into_bytes();
        let bytes = mfr(
            BinanceMfr1RouteV2::Public,
            start,
            &[(1, FrameOpcode::Text, quote)],
        );
        let output = transformer
            .transform(public, &bytes, TimestampNs(start), decision(start, 1))
            .unwrap();
        let MechanicsInputRefV2::Market {
            source_provenance:
                SourceProvenanceV2::BinanceBookTicker {
                    event_time_ms,
                    transaction_time_ms,
                    ..
                },
            ..
        } = output.inputs()[0].view()
        else {
            panic!("quote")
        };
        assert_eq!(*event_time_ms, u64::try_from(event_ms).unwrap());
        assert_eq!(*transaction_time_ms, u64::try_from(start_ms).unwrap());
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
fn transformer_rejects_wrong_legacy_advanced_and_mismatched_routed_machine_before_replay() {
    for (context_route, use_public, with_ack) in [
        (BinanceMfr1RouteV2::Public, false, false),
        (BinanceMfr1RouteV2::Market, true, false),
        (BinanceMfr1RouteV2::Public, false, true),
        (BinanceMfr1RouteV2::Market, true, true),
    ] {
        let (transformer, start) = context(context_route);
        let (public, market) = machines();
        let machine = if use_public { public } else { market };
        let records = if with_ack {
            vec![(1, FrameOpcode::Text, br#"{"result":null,"id":1}"#.to_vec())]
        } else {
            vec![]
        };
        let bytes = mfr(context_route, start, &records);
        assert_eq!(
            transformer
                .transform(
                    machine,
                    &bytes,
                    TimestampNs(start),
                    decision(start, records.len())
                )
                .unwrap_err(),
            Mfr1TransformErrorV2::MachineIdentity,
        );
    }

    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let legacy = BinanceUsdmSession::new(
        SessionSpec {
            endpoint_name: "legacy".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        catalog_view(),
        config(11, 21, true),
    );
    assert_eq!(
        transformer
            .transform(
                legacy,
                &mfr(BinanceMfr1RouteV2::Public, start, &[]),
                TimestampNs(start),
                decision(start, 0)
            )
            .unwrap_err(),
        Mfr1TransformErrorV2::MachineIdentity
    );

    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let (mut advanced, _) = machines();
    let mut ack = br#"{"result":null,"id":1}"#.to_vec();
    advanced
        .on_input(
            SessionInput::TextFrame {
                bytes: &mut ack,
                received: marketfeed_model::FrameStamp {
                    receive_ts: TimestampNs(start),
                    mono_ns: u64::try_from(start).unwrap(),
                },
            },
            &mut ActionBuffer::new(),
        )
        .unwrap();
    assert_eq!(
        transformer
            .transform(
                advanced,
                &mfr(BinanceMfr1RouteV2::Public, start, &[]),
                TimestampNs(start),
                decision(start, 0)
            )
            .unwrap_err(),
        Mfr1TransformErrorV2::MachineIdentity
    );

    let (transformer, start) = context(BinanceMfr1RouteV2::Public);
    let mut wrong_public = config(31, 41, true);
    wrong_public.instrument_ids = HashMap::from([("BNBUSDT".into(), InstrumentId(8))]);
    let mut wrong_market = config(32, 42, false);
    wrong_market.instrument_ids = wrong_public.instrument_ids.clone();
    let (wrong, _) = BinanceUsdmSession::try_new_routed_pair_v4(
        SessionSpec {
            endpoint_name: PUBLIC_WS.into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        SessionSpec {
            endpoint_name: MARKET_WS.into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        {
            let mut view = catalog_view();
            let mut instrument = view.instruments[0].clone();
            instrument.id = InstrumentId(8);
            instrument.catalog_version = view.version;
            view = CatalogView::with_instruments(VenueId(3), view.version, vec![instrument]);
            view
        },
        wrong_public,
        wrong_market,
    )
    .unwrap();
    assert_eq!(
        transformer
            .transform(
                wrong,
                &mfr(BinanceMfr1RouteV2::Public, start, &[]),
                TimestampNs(start),
                decision(start, 0)
            )
            .unwrap_err(),
        Mfr1TransformErrorV2::MachineIdentity
    );
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

#[test]
fn metadata_binding_requires_complete_build_and_empty_routed_subscriptions() {
    for field in 0..4 {
        let mut build = build_metadata();
        match field {
            0 => build.package_name.clear(),
            1 => build.package_version.clear(),
            2 => build.target_os.clear(),
            3 => build.target_arch.clear(),
            _ => unreachable!(),
        }
        assert_eq!(
            Mfr1MetadataBindingV2::new(build, session_metadata(BinanceMfr1RouteV2::Public))
                .unwrap_err(),
            Mfr1TransformErrorV2::InvalidExecutionMetadata,
        );
    }
    let mut session = session_metadata(BinanceMfr1RouteV2::Public);
    session.initial_subscriptions.push(SubscriptionMetadata {
        instrument_id: 7,
        channel: "depth".into(),
        emit_book_snapshots: true,
        emit_book_deltas: true,
        emit_bbo: false,
    });
    assert_eq!(
        Mfr1MetadataBindingV2::new(build_metadata(), session).unwrap_err(),
        Mfr1TransformErrorV2::InvalidExecutionMetadata,
    );
}
