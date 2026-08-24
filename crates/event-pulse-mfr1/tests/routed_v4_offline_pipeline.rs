//! Offline-only cross-crate regression for the synthetic routed Binance V4 path.

use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
};

use marketfeed_adapter_api::{ConcreteSubscriptionSet, HttpResponse, SessionSpec};
use marketfeed_adapter_binance::{BinanceUsdmSession, BinanceUsdmSessionConfig};
use marketfeed_event_pulse::{
    MechanicsInputV2, MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter,
    OfflineArtifactPreflightV4, ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2,
    ReplayInputError, SnapshotProcessorV2,
    wire::{
        InstrumentIdentityV1, OpenInterestEncodingV1, ReplayCatalogV1, ReplayEpochEntryV1,
        Rfc3339Time, SnapshotAuthoringV1, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_event_pulse_capture::{FixtureV4Assembler, FixtureV4Request};
use marketfeed_event_pulse_mfr1::{
    BinanceMfr1RouteV2, Mfr1MetadataBindingV2, Mfr1SessionBindingV2, Mfr1TransformContextV2,
    Mfr1TransformerV2,
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
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const PUBLIC_WS: &str = "wss://fstream.binance.com/public/ws";
const MARKET_WS: &str = "wss://fstream.binance.com/market/ws";
const FIXTURE_V4_CONTRACT: &[u8] = include_bytes!(
    "../../event-pulse-capture/contracts/fixture-v4/event-pulse-e2-fixture-v4-contract.json"
);
const FIXTURE_V4_SIDECARS: &[u8] = include_bytes!(
    "../../event-pulse-capture/tests/fixtures/event-pulse-e2-fixture-v4-rust-writer.jsonl"
);

fn admission() -> ProspectiveCaptureAdmissionV2 {
    let contract: Value = serde_json::from_slice(FIXTURE_V4_CONTRACT).unwrap();
    let descriptor = json!({
        "schema": "event-pulse-e2-prospective-admission/2.0",
        "topology_binding": contract["bindings"]["topology"],
        "wire_contract_binding": contract["bindings"]["wire"],
        "capture_starts_at": "2026-08-24T00:00:00Z",
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "authority": contract["authority"],
    });
    ProspectiveCaptureAdmissionV2::from_json(&serde_json::to_vec(&descriptor).unwrap()).unwrap()
}

fn catalog_view() -> CatalogView {
    CatalogView::with_instruments(
        VenueId(3),
        CatalogVersion(1),
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
            catalog_version: CatalogVersion(1),
        }],
    )
}

fn session_config(connection: u64, session: u64, enable_l2: bool) -> BinanceUsdmSessionConfig {
    BinanceUsdmSessionConfig {
        symbols: vec!["BNBUSDT".into()],
        instrument_ids: HashMap::from([("BNBUSDT".into(), InstrumentId(7))]),
        connection: ConnectionId(connection),
        session: SessionId(session),
        enable_l2,
        price_scale: 2,
        qty_scale: 3,
        ..BinanceUsdmSessionConfig::default()
    }
}

fn routed_machines() -> (BinanceUsdmSession, BinanceUsdmSession) {
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
        session_config(11, 21, true),
        session_config(12, 22, false),
    )
    .unwrap()
}

fn build_metadata() -> marketfeed_recording::BuildMetadata {
    let MetadataRecord::Build(metadata) = MetadataRecord::current_build() else {
        unreachable!()
    };
    metadata
}

fn session_metadata(route: BinanceMfr1RouteV2) -> SessionRecordingMetadata {
    let (session_id, endpoint) = match route {
        BinanceMfr1RouteV2::Public => (21, PUBLIC_WS),
        BinanceMfr1RouteV2::Market => (22, MARKET_WS),
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
    let (source, connection, session, epoch) = match route {
        BinanceMfr1RouteV2::Public => ("binance_primary_public", 11, 21, "epoch_public"),
        BinanceMfr1RouteV2::Market => ("binance_primary_market", 12, 22, "epoch_market"),
    };
    ReplayCatalogV1::new(
        BTreeMap::from([(3, VenueCatalogEntryV1::new("BINANCE", source).unwrap())]),
        BTreeMap::from([(
            7,
            InstrumentIdentityV1::new("BNB", "USDT", "PERPETUAL", "BINANCE", "BNBUSDT").unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(connection, session, epoch, 0).unwrap()],
        matches!(route, BinanceMfr1RouteV2::Market)
            .then(|| BTreeMap::from([(7, OpenInterestEncodingV1::contracts())]))
            .unwrap_or_default(),
    )
    .unwrap()
}

fn transformer(
    admission: ProspectiveCaptureAdmissionV2,
    route: BinanceMfr1RouteV2,
) -> Mfr1TransformerV2 {
    let (connection_name, connection_id, session_id) = match route {
        BinanceMfr1RouteV2::Public => ("binance_primary_public_connection", 11, 21),
        BinanceMfr1RouteV2::Market => ("binance_primary_market_connection", 12, 22),
    };
    let connection = admission
        .mechanics_config()
        .connections()
        .iter()
        .find(|key| key.source_id() == connection_name)
        .unwrap()
        .clone();
    let system_source = SystemSourceV1::new(
        admission.mechanics_config().system_sources()[0].clone(),
        "epoch_system_0",
        0,
    )
    .unwrap();
    let context = Mfr1TransformContextV2::new(
        admission,
        replay_catalog(route),
        Mfr1SessionBindingV2::new(connection, connection_id, session_id, route),
        Mfr1MetadataBindingV2::new(build_metadata(), session_metadata(route)).unwrap(),
        system_source,
        64,
        OverflowPolicy::DropNewest,
    )
    .unwrap();
    Mfr1TransformerV2::new(context)
}

fn mfr(route: BinanceMfr1RouteV2, start: i64, records: &[(u64, FrameOpcode, Vec<u8>)]) -> Vec<u8> {
    let session = match route {
        BinanceMfr1RouteV2::Public => 21,
        BinanceMfr1RouteV2::Market => 22,
    };
    let mut writer = RawSegmentWriter::create(Vec::new(), start).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start)
        .unwrap();
    writer
        .write_metadata(&MetadataRecord::Session(session_metadata(route)), start)
        .unwrap();
    for (ordinal, (frame_seq, opcode, payload)) in records.iter().enumerate() {
        let received_at = start + (i64::try_from(ordinal).unwrap() + 1) * 1_000_000;
        writer
            .write_record(
                SessionId(session),
                *frame_seq,
                received_at,
                u64::try_from(received_at).unwrap(),
                Direction::Inbound,
                *opcode,
                0,
                payload,
            )
            .unwrap();
    }
    writer.into_inner()
}

fn transformed_binance_inputs(
    admission: &ProspectiveCaptureAdmissionV2,
) -> (Vec<MechanicsInputV2>, Vec<MechanicsInputV2>) {
    let start = admission.capture_starts_at().utc_micros() * 1_000;
    let source_ms = u64::try_from(start.div_euclid(1_000_000)).unwrap();
    let snapshot = encode_http_response(
        1,
        &HttpResponse {
            status: 200,
            headers: vec![],
            body: format!(
                r#"{{"lastUpdateId":100,"E":{},"T":{},"bids":[["650.0","1"]],"asks":[["651.0","2"]]}}"#,
                source_ms + 2,
                source_ms + 2
            )
            .into_bytes()
            .into(),
        },
    )
    .unwrap();
    let public_records = vec![
        (1, FrameOpcode::Text, format!(r#"{{"e":"bookTicker","E":{source_ms},"T":{source_ms},"u":99,"s":"BNBUSDT","b":"650.0","B":"1","a":"651.0","A":"2"}}"#).into_bytes()),
        (2, FrameOpcode::Text, format!(r#"{{"e":"depthUpdate","E":{},"T":{},"s":"BNBUSDT","U":99,"u":101,"pu":98,"b":[["650.0","1.5"]],"a":[["651.0","1.5"]]}}"#, source_ms + 1, source_ms + 1).into_bytes()),
        (3, FrameOpcode::HttpResponse, snapshot),
        (4, FrameOpcode::Text, format!(r#"{{"e":"depthUpdate","E":{},"T":{},"s":"BNBUSDT","U":102,"u":102,"pu":101,"b":[["650.0","2"]],"a":[]}}"#, source_ms + 3, source_ms + 3).into_bytes()),
    ];
    let open_interest = encode_http_response(
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
    let market_records = vec![
        (51, FrameOpcode::Text, format!(r#"{{"e":"aggTrade","E":{source_ms},"s":"BNBUSDT","a":42,"p":"650.1","q":"0.01","T":{source_ms},"m":false}}"#).into_bytes()),
        (52, FrameOpcode::Text, format!(r#"{{"e":"forceOrder","E":{},"o":{{"s":"BNBUSDT","S":"SELL","ap":"649","l":"0.5","T":{}}}}}"#, source_ms + 1, source_ms + 1).into_bytes()),
        (53, FrameOpcode::HttpResponse, open_interest),
    ];
    let (public_machine, market_machine) = routed_machines();
    let decision = Rfc3339Time::from_unix_nanos(start + 17_000_000).unwrap();
    let public = transformer(admission.clone(), BinanceMfr1RouteV2::Public)
        .transform(
            public_machine,
            &mfr(BinanceMfr1RouteV2::Public, start, &public_records),
            TimestampNs(start),
            decision.clone(),
        )
        .unwrap();
    let market = transformer(admission.clone(), BinanceMfr1RouteV2::Market)
        .transform(
            market_machine,
            &mfr(BinanceMfr1RouteV2::Market, start, &market_records),
            TimestampNs(start),
            decision,
        )
        .unwrap();
    assert!(!public.evidence_authoring_allowed());
    assert!(!market.evidence_authoring_allowed());
    assert_eq!(public.blocker(), "blocked:fixture-provenance");
    assert_eq!(market.blocker(), "blocked:fixture-provenance");
    (public.inputs().to_vec(), market.inputs().to_vec())
}

fn oracle_sidecars() -> Vec<MechanicsInputV2> {
    FIXTURE_V4_SIDECARS
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .filter(|line| {
            let value: Value = serde_json::from_slice(line).unwrap();
            value["kind"] != "MARKET"
                || value["catalog"]["venue_sources"]["3"]["source_id"] == "hyperliquid_confirmation"
        })
        .map(|line| MechanicsInputV2::from_json_line(line).unwrap())
        .collect()
}

fn complete_jsonl(
    public: &[MechanicsInputV2],
    market: &[MechanicsInputV2],
    sidecars: &[MechanicsInputV2],
) -> Vec<u8> {
    assert_eq!(public.len(), 4);
    assert_eq!(market.len(), 3);
    assert_eq!(sidecars.len(), 10);
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    for input in [
        &market[0], &public[0], &market[1], &market[2], &public[1], &public[2], &public[3],
    ]
    .into_iter()
    .chain(sidecars.iter())
    {
        writer.write_input(input).unwrap();
    }
    writer.finish()
}

fn authoring(admission: &ProspectiveCaptureAdmissionV2) -> SnapshotAuthoringV1 {
    SnapshotAuthoringV1::new(
        "event_pulse_mechanics_synthetic_v4",
        "lineage_synthetic_v4",
        "event_cluster_synthetic_v4",
        admission.mechanics_config().contributors()[0]
            .key()
            .instrument()
            .clone(),
        1,
        None,
        15_000,
        "synthetic-routed-v4",
    )
    .unwrap()
}

fn rehash(mut value: Value) -> Vec<u8> {
    value.as_object_mut().unwrap().remove("payload_hash");
    value["payload_hash"] = json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).unwrap())
    ));
    serde_json::to_vec(&value).unwrap()
}

#[test]
fn synthetic_routed_v4_pipeline_is_strict_deterministic_and_non_authoritative() {
    let admission = admission();
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    let decision = Rfc3339Time::parse("2026-08-24T00:00:00.017000Z").unwrap();
    assert!(!admission.evidence_authoring_allowed());
    assert_eq!(admission.blocker(), "blocked:fixture-provenance");
    assert!(!policy.evidence_authoring_allowed());

    let (public, market) = transformed_binance_inputs(&admission);
    let sidecars = oracle_sidecars();
    let jsonl = complete_jsonl(&public, &market, &sidecars);
    let strict_readback = MechanicsInputV2JsonlReader::new(Cursor::new(&jsonl), decision.clone())
        .read_all()
        .unwrap();
    assert_eq!(strict_readback.len(), 17);
    assert_eq!(
        strict_readback
            .iter()
            .map(|input| input.payload_hash())
            .collect::<Vec<_>>(),
        MechanicsInputV2JsonlReader::new(Cursor::new(&jsonl), decision.clone())
            .read_all()
            .unwrap()
            .iter()
            .map(|input| input.payload_hash())
            .collect::<Vec<_>>()
    );

    let preflight =
        OfflineArtifactPreflightV4::build(&admission, &policy, decision.clone(), &jsonl).unwrap();
    assert!(!preflight.evidence_authoring_allowed());
    assert_eq!(preflight.blocker(), "blocked:fixture-provenance");
    let assembler = FixtureV4Assembler::new(admission.clone(), policy.clone()).unwrap();
    let first_package = assembler
        .assemble(FixtureV4Request {
            fixture_id: "synthetic-routed-v4",
            capture_ends_at: Rfc3339Time::parse("2026-08-24T00:00:00.016000Z").unwrap(),
            decision_time: decision.clone(),
            source_terms: "synthetic offline regression only",
            complete_jsonl: &jsonl,
        })
        .unwrap();
    let second_package = assembler
        .assemble(FixtureV4Request {
            fixture_id: "synthetic-routed-v4",
            capture_ends_at: Rfc3339Time::parse("2026-08-24T00:00:00.016000Z").unwrap(),
            decision_time: decision.clone(),
            source_terms: "synthetic offline regression only",
            complete_jsonl: &jsonl,
        })
        .unwrap();
    assert_eq!(first_package, second_package);
    assert_eq!(first_package.status(), "STRUCTURAL_V4_CANDIDATE");
    assert_eq!(first_package.blocker(), "blocked:fixture-provenance");
    assert!(!first_package.evidence_authoring_allowed());
    assert!(!first_package.capture_allowed());
    assert!(!first_package.execution_allowed());
    let manifest: Value =
        serde_json::from_slice(first_package.file("manifest.json").unwrap()).unwrap();
    assert_eq!(manifest["authority"]["source_qualification"], "UNVERIFIED");
    assert_eq!(
        manifest["retention"]["source_terms"],
        "synthetic offline regression only"
    );
    assert!(
        manifest["authority"]
            .as_object()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.as_str() != "source_qualification")
            .all(|(_, value)| value == &Value::Bool(false))
    );

    let mut first = SnapshotProcessorV2::new(&admission, &policy, authoring(&admission)).unwrap();
    let mut second = SnapshotProcessorV2::new(&admission, &policy, authoring(&admission)).unwrap();
    for input in &strict_readback {
        first.ingest(input).unwrap();
        second.ingest(input).unwrap();
    }
    let first_snapshot = first.snapshot(decision.clone()).unwrap();
    let second_snapshot = second.snapshot(decision.clone()).unwrap();
    let first_json = first_snapshot.canonical_json();
    let second_json = second_snapshot.canonical_json();
    let first_bytes = first_json.as_bytes();
    let second_bytes = second_json.as_bytes();
    assert_eq!(first_bytes, second_bytes);
    assert_eq!(Sha256::digest(first_bytes), Sha256::digest(second_bytes));
    assert_eq!(
        first_snapshot.content_hash(),
        second_snapshot.content_hash()
    );

    let mut wrong_source = serde_json::to_value(&public[0]).unwrap();
    wrong_source["catalog"]["venue_sources"]["3"]["source_id"] = json!("binance_primary_market");
    assert!(MechanicsInputV2::from_json_line(&rehash(wrong_source)).is_err());

    let mut wrong_family = serde_json::to_value(&public[0]).unwrap();
    let trade = serde_json::to_value(&market[0]).unwrap();
    wrong_family["envelope"]["payload"] = trade["envelope"]["payload"].clone();
    wrong_family["envelope"]["source_sequence"] = trade["envelope"]["source_sequence"].clone();
    wrong_family["market_cursor"] = trade["market_cursor"].clone();
    wrong_family["source_provenance"] = trade["source_provenance"].clone();
    assert!(MechanicsInputV2::from_json_line(&rehash(wrong_family)).is_err());

    let mut wrong_cursor = serde_json::to_value(&public[0]).unwrap();
    wrong_cursor["market_cursor"]["raw_frame_seq"] = json!(999_u64);
    assert!(MechanicsInputV2::from_json_line(&rehash(wrong_cursor)).is_err());

    let mut wrong_available_at = serde_json::to_value(&sidecars[1]).unwrap();
    wrong_available_at["available_at"] = json!("2026-08-24T00:00:00.018000Z");
    let mut wrong_available_at = rehash(wrong_available_at);
    wrong_available_at.push(b'\n');
    assert_eq!(
        MechanicsInputV2JsonlReader::new(Cursor::new(&wrong_available_at), decision).read_all(),
        Err(ReplayInputError::FutureInput)
    );
}
