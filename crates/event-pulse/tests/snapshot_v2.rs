use std::collections::BTreeMap;
use std::io::Cursor;

use marketfeed_event_pulse::{
    MarketCursorV2, MechanicsInputV2, MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter,
    ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2, SnapshotProcessorV2,
    SnapshotV2Error, SourceProvenanceV2,
    snapshot::SnapshotError,
    snapshot::{SNAPSHOT_V2_CONTRACT_SHA256, SNAPSHOT_V2_ROOT_MERGE, SNAPSHOT_V2_ROOT_TREE},
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceV1, ClockStateV1,
        ContributorV1, CoverageCursorV1, CoverageSourceV1, CursorV1, DropCategoryV1, FaultScopeV1,
        MechanicsInputV1, OpenInterestEncodingV1, ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time,
        SnapshotAuthoringV1, SystemFaultV1, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookChange, BookDelta, BookLevel, BookOperation, BookSide, BookSnapshot,
    ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, Liquidation, MarketEvent,
    OpenInterest, Price, PricePoint, Quantity, Quote, SequenceRange, SessionId, TimestampNs, Trade,
    VenueId,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn admission() -> ProspectiveCaptureAdmissionV2 {
    let value = json!({
        "schema": "event-pulse-e2-prospective-admission/2.0",
        "topology_binding": {
            "repository_url": "https://github.com/s1korrrr/rsibot.git",
            "merge_commit": "05994ccd514ddb69fdd5c21a8c78af8bbe75d506",
            "merged_at": "2026-08-23T06:58:18Z",
            "path": "docs/superpowers/specs/event-pulse-e2-producer-evidence-freeze-v2.json",
            "byte_length": 6955,
            "sha256": "7216d9c5bc4b5bcd463b644c53309594413608586288beb0d32623412a42f0d7"
        },
        "wire_contract_binding": {
            "repository_url": "https://github.com/s1korrrr/rsibot.git",
            "merge_commit": "44f3e091cb47c1b081f673e8bb09e8723a2090c6",
            "merged_at": "2026-08-23T08:10:48Z",
            "path": "docs/superpowers/specs/event-pulse-e2-wire-admission-v2-contract.json",
            "byte_length": 10119,
            "sha256": "dc79576062caf952be44e4808359c4328c2976282291838973d4884fadafa50b"
        },
        "capture_starts_at": "2026-08-23T08:10:48.001000Z",
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "authority": {
            "allocation_allowed": false, "canary_allowed": false, "capture_allowed": false,
            "credentials_allowed": false, "evidence_authoring_allowed": false,
            "execution_allowed": false, "live_allowed": false, "orders_allowed": false,
            "paper_allowed": false, "private_endpoints_allowed": false,
            "promotion_allowed": false, "risk_allowed": false
        }
    });
    ProspectiveCaptureAdmissionV2::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn authoring(admission: &ProspectiveCaptureAdmissionV2) -> SnapshotAuthoringV1 {
    SnapshotAuthoringV1::new(
        "event_pulse_mechanics_snapshot_v2_test",
        "lineage_snapshot_v2_test",
        "event_cluster_snapshot_v2_test",
        admission.mechanics_config().contributors()[0]
            .key()
            .instrument()
            .clone(),
        1,
        None,
        15_000,
        "snapshot-v2-test",
    )
    .unwrap()
}

fn processor(admission: &ProspectiveCaptureAdmissionV2) -> SnapshotProcessorV2 {
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(admission).unwrap();
    SnapshotProcessorV2::new(admission, &policy, authoring(admission)).unwrap()
}

fn complete_inputs(admission: &ProspectiveCaptureAdmissionV2) -> Vec<MechanicsInputV2> {
    let config = admission.mechanics_config();
    let public = config.contributors()[0].key().clone();
    let market = config.contributors()[1].key().clone();
    let confirmation = config.contributors()[2].key().clone();
    let catalog = ReplayCatalogV1::new(
        BTreeMap::from([
            (
                1,
                VenueCatalogEntryV1::new("BINANCE", public.source_id()).unwrap(),
            ),
            (
                2,
                VenueCatalogEntryV1::new("BINANCE", market.source_id()).unwrap(),
            ),
            (
                3,
                VenueCatalogEntryV1::new("HYPERLIQUID", confirmation.source_id()).unwrap(),
            ),
        ]),
        BTreeMap::from([
            (1, public.instrument().clone()),
            (2, market.instrument().clone()),
            (3, confirmation.instrument().clone()),
        ]),
        vec![
            ReplayEpochEntryV1::new(11, 21, "epoch_public", 0).unwrap(),
            ReplayEpochEntryV1::new(12, 22, "epoch_market", 0).unwrap(),
            ReplayEpochEntryV1::new(13, 23, "epoch_confirmation", 0).unwrap(),
        ],
        BTreeMap::from([(2, OpenInterestEncodingV1::contracts())]),
    )
    .unwrap();
    let start_ns = admission.capture_starts_at().utc_micros() * 1_000;
    let source_ms = u64::try_from(start_ns.div_euclid(1_000_000)).unwrap();
    let price = |value| Price(Fixed::new(value, 0));
    let quantity = |value| Quantity(Fixed::new(value, 0));
    let envelope =
        |venue, instrument, connection, session, frame, at_ms, sequence, payload| EventEnvelope {
            schema_version: 1,
            venue: VenueId(venue),
            instrument: Some(InstrumentId(instrument)),
            connection: ConnectionId(connection),
            session: SessionId(session),
            frame_seq: frame,
            event_index: 0,
            exchange_ts: Some(TimestampNs(i64::try_from(at_ms).unwrap() * 1_000_000)),
            receive_ts: TimestampNs(start_ns + i64::try_from(frame).unwrap() * 1_000_000),
            source_sequence: sequence,
            flags: EventFlags::empty(),
            payload,
        };
    let mut records = vec![
        MechanicsInputV2::market(
            envelope(
                2,
                2,
                12,
                22,
                1,
                source_ms,
                Some(SequenceRange {
                    first: 100,
                    last: 100,
                }),
                MarketEvent::Trade(Trade {
                    price: price(100),
                    quantity: quantity(2),
                    aggressor: AggressorSide::Buy,
                    trade_id: None,
                }),
            ),
            0,
            catalog.clone(),
            MarketCursorV2::Native {
                first_sequence: 100,
                last_sequence: 100,
            },
            SourceProvenanceV2::BinanceAggregateTrade {
                aggregate_trade_id: 100,
                event_time_ms: source_ms,
                trade_time_ms: source_ms,
            },
        )
        .unwrap(),
        MechanicsInputV2::market(
            envelope(
                1,
                1,
                11,
                21,
                2,
                source_ms + 1,
                None,
                MarketEvent::Quote(Quote {
                    bid_price: price(99),
                    bid_quantity: Some(quantity(1)),
                    ask_price: price(101),
                    ask_quantity: Some(quantity(1)),
                }),
            ),
            0,
            catalog.clone(),
            MarketCursorV2::Derived {
                raw_frame_seq: 2,
                action_index: 0,
                item_index: 0,
            },
            SourceProvenanceV2::BinanceBookTicker {
                update_id: 9_999,
                event_time_ms: source_ms + 1,
                transaction_time_ms: source_ms + 1,
            },
        )
        .unwrap(),
        MechanicsInputV2::market(
            envelope(
                1,
                1,
                11,
                21,
                3,
                source_ms + 2,
                Some(SequenceRange {
                    first: 200,
                    last: 200,
                }),
                MarketEvent::BookSnapshot(BookSnapshot {
                    bids: vec![BookLevel {
                        price: price(99),
                        quantity: quantity(3),
                    }],
                    asks: vec![BookLevel {
                        price: price(101),
                        quantity: quantity(4),
                    }],
                    depth: Some(1),
                    checksum: None,
                }),
            ),
            0,
            catalog.clone(),
            MarketCursorV2::Native {
                first_sequence: 200,
                last_sequence: 200,
            },
            SourceProvenanceV2::BinanceBookSnapshot {
                last_update_id: 200,
                event_time_ms: source_ms + 2,
                transaction_time_ms: source_ms + 2,
            },
        )
        .unwrap(),
        MechanicsInputV2::market(
            envelope(
                2,
                2,
                12,
                22,
                4,
                source_ms + 3,
                None,
                MarketEvent::OpenInterest(OpenInterest {
                    quantity: quantity(10),
                }),
            ),
            0,
            catalog.clone(),
            MarketCursorV2::Derived {
                raw_frame_seq: 4,
                action_index: 0,
                item_index: 0,
            },
            SourceProvenanceV2::BinanceOpenInterest {
                source_time_ms: source_ms + 3,
            },
        )
        .unwrap(),
        MechanicsInputV2::market(
            envelope(
                2,
                2,
                12,
                22,
                5,
                source_ms + 4,
                None,
                MarketEvent::Liquidation(Liquidation {
                    price: price(98),
                    quantity: quantity(1),
                    side: AggressorSide::Sell,
                }),
            ),
            0,
            catalog.clone(),
            MarketCursorV2::Derived {
                raw_frame_seq: 5,
                action_index: 0,
                item_index: 0,
            },
            SourceProvenanceV2::BinanceForceOrder {
                event_time_ms: source_ms + 4,
                order_trade_time_ms: source_ms + 4,
            },
        )
        .unwrap(),
        MechanicsInputV2::market(
            envelope(
                3,
                3,
                13,
                23,
                6,
                source_ms + 5,
                None,
                MarketEvent::MarkPrice(PricePoint { price: price(100) }),
            ),
            0,
            catalog,
            MarketCursorV2::Derived {
                raw_frame_seq: 6,
                action_index: 0,
                item_index: 0,
            },
            SourceProvenanceV2::None,
        )
        .unwrap(),
    ];
    for (index, key) in config.clock_sources().iter().enumerate() {
        let epoch = match key.subject().source_id() {
            "binance_primary_public" => "epoch_public",
            "binance_primary_market" => "epoch_market",
            _ => "epoch_confirmation",
        };
        let at_ns = start_ns + (7 + i64::try_from(index).unwrap()) * 1_000_000;
        let v1 = MechanicsInputV1::clock(
            ContributorV1::new(key.subject().clone(), epoch, 0).unwrap(),
            ClockSourceV1::new(key.clone(), &format!("epoch_clock_{index}"), 0).unwrap(),
            Rfc3339Time::from_unix_nanos(at_ns).unwrap(),
            Rfc3339Time::from_unix_nanos(at_ns).unwrap(),
            ClockCursorV1::native(1, 1).unwrap(),
            ClockStateV1::Synchronized,
            CanonicalDecimal::parse("0", 18, 8).unwrap(),
            2_000,
            ClockQualityV1::Validated,
            "SOURCE_CLOCK_WITHIN_TOLERANCE",
        )
        .unwrap();
        records.push(MechanicsInputV2::from_v1_non_market(v1).unwrap());
    }
    for (index, key) in config.coverage_sources().iter().enumerate() {
        let epoch = match key.subject().source_id() {
            "binance_primary_public" => "epoch_public",
            "binance_primary_market" => "epoch_market",
            _ => "epoch_confirmation",
        };
        let at_ns = start_ns + (10 + i64::try_from(index).unwrap()) * 1_000_000;
        let v1 = MechanicsInputV1::coverage(
            ContributorV1::new(key.subject().clone(), epoch, 0).unwrap(),
            CoverageSourceV1::new(key.clone(), &format!("epoch_coverage_{index}"), 0).unwrap(),
            key.family(),
            admission.capture_starts_at().clone(),
            Rfc3339Time::from_unix_nanos(at_ns).unwrap(),
            Rfc3339Time::from_unix_nanos(at_ns).unwrap(),
            CoverageCursorV1::native(1, 1).unwrap(),
        )
        .unwrap();
        records.push(MechanicsInputV2::from_v1_non_market(v1).unwrap());
    }
    records
}

fn decision(admission: &ProspectiveCaptureAdmissionV2) -> Rfc3339Time {
    Rfc3339Time::from_unix_nanos(admission.capture_starts_at().utc_micros() * 1_000 + 20_000_000)
        .unwrap()
}

fn source_cursors(snapshot: &marketfeed_event_pulse::snapshot::AuthoredSnapshot) -> &[Value] {
    snapshot.value()["source_cursors"].as_array().unwrap()
}

fn retimed_quote(
    input: &MechanicsInputV2,
    admission: &ProspectiveCaptureAdmissionV2,
    frame: u64,
) -> MechanicsInputV2 {
    let mut value = serde_json::to_value(input).unwrap();
    let at_ns = admission.capture_starts_at().utc_micros() * 1_000
        + i64::try_from(frame).unwrap() * 1_000_000;
    let at_ms = u64::try_from(at_ns.div_euclid(1_000_000)).unwrap();
    value["envelope"]["frame_seq"] = json!(frame);
    value["envelope"]["exchange_ts"] = json!(at_ns);
    value["envelope"]["receive_ts"] = json!(at_ns);
    value["market_cursor"]["raw_frame_seq"] = json!(frame);
    value["source_provenance"]["event_time_ms"] = json!(at_ms);
    value["source_provenance"]["transaction_time_ms"] = json!(at_ms);
    value.as_object_mut().unwrap().remove("payload_hash");
    value["payload_hash"] = json!(marketfeed_event_pulse::content_hash(&value).unwrap());
    MechanicsInputV2::from_json_line(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn rehash_value(mut value: Value) -> MechanicsInputV2 {
    value.as_object_mut().unwrap().remove("payload_hash");
    value["payload_hash"] = json!(marketfeed_event_pulse::content_hash(&value).unwrap());
    MechanicsInputV2::from_json_line(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn market_at(
    input: &MechanicsInputV2,
    admission: &ProspectiveCaptureAdmissionV2,
    frame: u64,
    at_offset_ms: i64,
    generation: u8,
) -> Value {
    let mut value = serde_json::to_value(input).unwrap();
    let at_ns = admission.capture_starts_at().utc_micros() * 1_000 + at_offset_ms * 1_000_000;
    let at_ms = u64::try_from(at_ns.div_euclid(1_000_000)).unwrap();
    value["envelope"]["frame_seq"] = json!(frame);
    value["envelope"]["exchange_ts"] = json!(at_ns);
    value["envelope"]["receive_ts"] = json!(at_ns);
    let connection = value["envelope"]["connection"].clone();
    let session = value["envelope"]["session"].clone();
    if generation > 0 {
        for entry in value["catalog"]["connection_epochs"]
            .as_array_mut()
            .unwrap()
        {
            if entry["connection_id"] == connection && entry["session_id"] == session {
                entry["connection_epoch"] = json!(format!("epoch_recovery_{generation}"));
                entry["epoch_generation"] = json!(generation);
            }
        }
    }
    if let Some(provenance) = value["source_provenance"].as_object_mut() {
        for field in [
            "event_time_ms",
            "trade_time_ms",
            "transaction_time_ms",
            "source_time_ms",
            "order_trade_time_ms",
        ] {
            if provenance.contains_key(field) {
                provenance.insert(field.to_owned(), json!(at_ms));
            }
        }
    }
    value
}

fn native_trade_at(
    input: &MechanicsInputV2,
    admission: &ProspectiveCaptureAdmissionV2,
    sequence: u64,
    frame: u64,
    at_offset_ms: i64,
    generation: u8,
) -> MechanicsInputV2 {
    let mut value = market_at(input, admission, frame, at_offset_ms, generation);
    value["envelope"]["source_sequence"]["first"] = json!(sequence);
    value["envelope"]["source_sequence"]["last"] = json!(sequence);
    value["market_cursor"]["first_sequence"] = json!(sequence);
    value["market_cursor"]["last_sequence"] = json!(sequence);
    value["source_provenance"]["aggregate_trade_id"] = json!(sequence);
    rehash_value(value)
}

fn quote_at(
    input: &MechanicsInputV2,
    admission: &ProspectiveCaptureAdmissionV2,
    frame: u64,
    at_offset_ms: i64,
    generation: u8,
) -> MechanicsInputV2 {
    let mut value = market_at(input, admission, frame, at_offset_ms, generation);
    value["market_cursor"]["raw_frame_seq"] = json!(frame);
    value["source_provenance"]["update_id"] = json!(10_000 + frame);
    rehash_value(value)
}

fn book_delta_at(
    snapshot: &MechanicsInputV2,
    admission: &ProspectiveCaptureAdmissionV2,
    frame: u64,
    at_offset_ms: i64,
    first_update_id: u64,
    final_update_id: u64,
    previous_final_update_id: u64,
) -> MechanicsInputV2 {
    let mut value = market_at(snapshot, admission, frame, at_offset_ms, 0);
    value["envelope"]["source_sequence"]["first"] = json!(first_update_id);
    value["envelope"]["source_sequence"]["last"] = json!(final_update_id);
    value["envelope"]["payload"] = serde_json::to_value(MarketEvent::BookDelta(BookDelta {
        changes: vec![BookChange {
            side: BookSide::Bid,
            operation: BookOperation::Upsert,
            price: Price(Fixed::new(99, 0)),
            quantity: Some(Quantity(Fixed::new(5, 0))),
        }],
        checksum: None,
    }))
    .unwrap();
    value["market_cursor"]["first_sequence"] = json!(first_update_id);
    value["market_cursor"]["last_sequence"] = json!(final_update_id);
    let event_time_ms = value["source_provenance"]["event_time_ms"].clone();
    let transaction_time_ms = value["source_provenance"]["transaction_time_ms"].clone();
    value["source_provenance"] = json!({
        "kind": "BINANCE_BOOK_DELTA",
        "first_update_id": first_update_id,
        "final_update_id": final_update_id,
        "previous_final_update_id": previous_final_update_id,
        "event_time_ms": event_time_ms,
        "transaction_time_ms": transaction_time_ms
    });
    rehash_value(value)
}

fn decision_offset(admission: &ProspectiveCaptureAdmissionV2, offset_ms: i64) -> Rfc3339Time {
    Rfc3339Time::from_unix_nanos(
        admission.capture_starts_at().utc_micros() * 1_000 + offset_ms * 1_000_000,
    )
    .unwrap()
}

fn live_inputs(admission: &ProspectiveCaptureAdmissionV2) -> Vec<MechanicsInputV2> {
    let initial = complete_inputs(admission);
    let mut live = Vec::with_capacity(initial.len());
    for (index, input) in initial.iter().take(6).enumerate() {
        let frame = 101 + u64::try_from(index).unwrap();
        let mut value = market_at(
            input,
            admission,
            frame,
            60_001 + i64::try_from(index).unwrap(),
            0,
        );
        match index {
            0 => {
                value["envelope"]["source_sequence"]["first"] = json!(101);
                value["envelope"]["source_sequence"]["last"] = json!(101);
                value["market_cursor"]["first_sequence"] = json!(101);
                value["market_cursor"]["last_sequence"] = json!(101);
                value["source_provenance"]["aggregate_trade_id"] = json!(101);
            }
            1 | 3 | 4 | 5 => value["market_cursor"]["raw_frame_seq"] = json!(frame),
            2 => {
                value["envelope"]["source_sequence"]["first"] = json!(201);
                value["envelope"]["source_sequence"]["last"] = json!(201);
                value["market_cursor"]["first_sequence"] = json!(201);
                value["market_cursor"]["last_sequence"] = json!(201);
                value["source_provenance"]["last_update_id"] = json!(201);
            }
            _ => unreachable!(),
        }
        value.as_object_mut().unwrap().remove("payload_hash");
        value["payload_hash"] = json!(marketfeed_event_pulse::content_hash(&value).unwrap());
        let bytes = serde_json::to_vec(&value).unwrap();
        live.push(
            MechanicsInputV2::from_json_line(&bytes).unwrap_or_else(|error| {
                panic!(
                    "live market {index} failed: {error:?}: {}",
                    String::from_utf8_lossy(&bytes)
                )
            }),
        );
    }
    for (index, input) in initial.iter().skip(6).enumerate() {
        let offset = 60_007 + i64::try_from(index).unwrap();
        let at = decision_offset(admission, offset).canonical().to_owned();
        let mut value = serde_json::to_value(input).unwrap();
        value["available_at"] = json!(at);
        if value.get("observed_at").is_some() {
            value["observed_at"] = json!(at);
            value["clock_cursor"]["start"] = json!(2);
            value["clock_cursor"]["end"] = json!(2);
        } else {
            value["covered_through"] = json!(at);
            value["coverage_cursor"]["start"] = json!(2);
            value["coverage_cursor"]["end"] = json!(2);
        }
        value.as_object_mut().unwrap().remove("payload_hash");
        value["payload_hash"] = json!(marketfeed_event_pulse::content_hash(&value).unwrap());
        let bytes = serde_json::to_vec(&value).unwrap();
        live.push(
            MechanicsInputV2::from_json_line(&bytes).unwrap_or_else(|error| {
                panic!(
                    "live sidecar {index} failed: {error:?}: {}",
                    String::from_utf8_lossy(&bytes)
                )
            }),
        );
    }
    live
}

#[test]
fn embedded_contract_and_independent_root_pins_are_exact() {
    let bytes = include_bytes!("../contracts/snapshot-v2/event-pulse-e2-snapshot-v2-contract.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(bytes)),
        SNAPSHOT_V2_CONTRACT_SHA256
    );
    assert_eq!(
        SNAPSHOT_V2_ROOT_MERGE,
        "4d3e0f0398d3e113a79df7ac901f38912eaa8edd"
    );
    assert_eq!(
        SNAPSHOT_V2_ROOT_TREE,
        "273163e3d06578065f7327a90a1b9fbfcded3a6d"
    );
}

#[test]
fn complete_prefix_authors_exact_fifteen_family_and_sidecar_cursors() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    let snapshot = processor.snapshot(decision(&admission)).unwrap();
    assert_eq!(source_cursors(&snapshot).len(), 15);
    let ids = source_cursors(&snapshot)
        .iter()
        .map(|cursor| cursor["source_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    for expected in [
        "binance_primary_public_quote",
        "binance_primary_public_book",
        "binance_primary_market_trade",
        "binance_primary_market_open_interest",
        "binance_primary_market_liquidation",
        "hyperliquid_confirmation_price",
        "clock_binance_public",
        "clock_binance_market",
        "clock_hyperliquid_confirmation",
    ] {
        assert!(ids.contains(&expected), "missing {expected}");
    }
    let clock = source_cursors(&snapshot)
        .iter()
        .find(|cursor| cursor["source_id"] == "clock_binance_public")
        .unwrap();
    assert_eq!(clock["connection_epoch"], "epoch_clock_0");
    assert_eq!(clock["source_payload_hash"], inputs[6].payload_hash());
    let coverage = source_cursors(&snapshot)
        .iter()
        .find(|cursor| cursor["source_id"] == "coverage_binance_public_quote")
        .unwrap();
    assert_eq!(coverage["connection_epoch"], "epoch_coverage_0");
    assert_eq!(coverage["source_payload_hash"], inputs[9].payload_hash());
}

#[test]
fn strict_jsonl_replay_matches_direct_snapshot_bytes_and_hash() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    for input in &inputs {
        writer.write_input(input).unwrap();
    }
    let replayed =
        MechanicsInputV2JsonlReader::new(Cursor::new(writer.finish()), decision(&admission))
            .read_all()
            .unwrap();
    let mut direct = processor(&admission);
    let mut replay = processor(&admission);
    for input in &inputs {
        direct.ingest(input).unwrap();
    }
    for input in &replayed {
        replay.ingest(input).unwrap();
    }
    let direct = direct.snapshot(decision(&admission)).unwrap();
    let replay = replay.snapshot(decision(&admission)).unwrap();
    assert_eq!(direct.canonical_json(), replay.canonical_json());
    assert_eq!(direct.content_hash(), replay.content_hash());
}

#[test]
fn repeated_family_projects_only_latest_cursor() {
    let admission = admission();
    let mut inputs = complete_inputs(&admission);
    inputs.push(retimed_quote(&inputs[1], &admission, 16));
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    let snapshot = processor.snapshot(decision(&admission)).unwrap();
    let quote = source_cursors(&snapshot)
        .iter()
        .find(|cursor| cursor["source_id"] == "binance_primary_public_quote")
        .unwrap();
    assert_eq!(quote["sequence_start"], json!(16u64 << 32));
    assert_eq!(source_cursors(&snapshot).len(), 15);
}

#[test]
fn failed_incomplete_snapshot_does_not_seal_and_same_time_repair_matches_fresh() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let mut repaired = processor(&admission);
    for input in &inputs[..14] {
        repaired.ingest(input).unwrap();
    }
    assert!(repaired.snapshot(decision(&admission)).is_err());
    assert_eq!(repaired.next_revision(), 1);
    repaired.ingest(&inputs[14]).unwrap();
    let repaired = repaired.snapshot(decision(&admission)).unwrap();
    let mut fresh = processor(&admission);
    for input in &inputs {
        fresh.ingest(input).unwrap();
    }
    let fresh = fresh.snapshot(decision(&admission)).unwrap();
    assert_eq!(repaired.canonical_json(), fresh.canonical_json());
}

#[test]
fn truthful_empty_system_rejects_before_mutation() {
    let admission = admission();
    let at = decision(&admission);
    let source = SystemSourceV1::new(
        admission.mechanics_config().system_sources()[0].clone(),
        "epoch_system",
        0,
    )
    .unwrap();
    let system = MechanicsInputV1::system(
        source,
        FaultScopeV1::processor(admission.mechanics_config().processor_id()).unwrap(),
        at.clone(),
        at,
        CursorV1::derived_drop(100, 1).unwrap(),
        SystemFaultV1::events_dropped(1, DropCategoryV1::MarketDispatch).unwrap(),
        None,
    )
    .unwrap();
    let system = MechanicsInputV2::from_v1_non_market(system).unwrap();
    let mut processor = processor(&admission);
    assert_eq!(processor.ingest(&system), Err(SnapshotV2Error::SystemInput));
    assert_eq!(processor.buffered_record_count(), 0);
    assert_eq!(processor.next_revision(), 1);
}

#[test]
fn unrepresentable_derived_cursor_does_not_seal_or_consume_revision() {
    let admission = admission();
    let mut inputs = complete_inputs(&admission);
    let mut value = serde_json::to_value(&inputs[1]).unwrap();
    value["envelope"]["frame_seq"] = json!(2_147_483_648u64);
    value["market_cursor"]["raw_frame_seq"] = json!(2_147_483_648u64);
    value.as_object_mut().unwrap().remove("payload_hash");
    value["payload_hash"] = json!(marketfeed_event_pulse::content_hash(&value).unwrap());
    inputs[1] = MechanicsInputV2::from_json_line(&serde_json::to_vec(&value).unwrap()).unwrap();
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    assert_eq!(
        processor.snapshot(decision(&admission)),
        Err(SnapshotV2Error::CursorNotE1Representable)
    );
    assert_eq!(processor.next_revision(), 1);
}

#[test]
fn rejected_native_gap_is_replayed_as_family_invalidity_until_greater_generation() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    let gap = native_trade_at(&inputs[0], &admission, 102, 16, 16, 0);
    assert!(processor.ingest(&gap).is_err());
    let invalid = processor.snapshot(decision_offset(&admission, 20)).unwrap();
    assert_eq!(invalid.value()["quality_state"], "INVALID");
    assert!(
        invalid.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );

    let same_generation = native_trade_at(&inputs[0], &admission, 101, 17, 17, 0);
    assert!(processor.ingest(&same_generation).is_err());
    let recovery = native_trade_at(&inputs[0], &admission, 1, 18, 60_100, 1);
    assert!(processor.ingest(&recovery).is_ok());
}

#[test]
fn mutated_duplicate_is_replayed_as_sequence_failure_without_new_evidence() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    let mut mutation = market_at(&inputs[0], &admission, 16, 16, 0);
    mutation["envelope"]["payload"] = serde_json::to_value(MarketEvent::Trade(Trade {
        price: Price(Fixed::new(100, 0)),
        quantity: Quantity(Fixed::new(999, 0)),
        aggressor: AggressorSide::Buy,
        trade_id: None,
    }))
    .unwrap();
    let mutation = rehash_value(mutation);
    let mutation_error = processor.ingest(&mutation).unwrap_err();
    assert!(
        mutation_error
            .to_string()
            .contains("cursor coordinate was reused with different payload"),
        "unexpected mutation error: {mutation_error}"
    );
    let invalid = processor.snapshot(decision_offset(&admission, 20)).unwrap();
    assert_eq!(invalid.value()["quality_state"], "INVALID");
    assert!(
        invalid.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
}

#[test]
fn v2_book_features_accept_snapshot_overlap_then_multiple_pu_contiguous_deltas() {
    let admission = admission();
    let mut inputs = complete_inputs(&admission);
    inputs.push(book_delta_at(&inputs[2], &admission, 16, 16, 190, 205, 0));
    inputs.push(book_delta_at(&inputs[2], &admission, 17, 17, 150, 210, 205));
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    let snapshot = processor.snapshot(decision_offset(&admission, 20)).unwrap();
    let book = source_cursors(&snapshot)
        .iter()
        .find(|cursor| cursor["source_id"] == "binance_primary_public_book")
        .unwrap();
    assert_eq!(book["sequence_start"], 150);
    assert_eq!(book["sequence_end"], 210);
}

#[test]
fn successful_cache_survives_future_ingest_until_later_snapshot_replaces_it() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    let at_t = processor.snapshot(decision_offset(&admission, 20)).unwrap();
    processor
        .ingest(&quote_at(&inputs[1], &admission, 21, 21, 0))
        .unwrap();
    let cached = processor.snapshot(decision_offset(&admission, 20)).unwrap();
    assert_eq!(cached.canonical_json(), at_t.canonical_json());
    assert_eq!(cached.revision(), at_t.revision());

    let later = processor.snapshot(decision_offset(&admission, 22)).unwrap();
    assert_eq!(later.revision(), at_t.revision() + 1);
    assert_ne!(later.content_hash(), at_t.content_hash());
    assert_eq!(
        processor
            .snapshot(decision_offset(&admission, 22))
            .unwrap()
            .canonical_json(),
        later.canonical_json()
    );
}

#[test]
fn feature_capacity_latches_queue_drop_on_exact_family_and_needs_greater_generation() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let mut processor = processor(&admission);
    for input in &inputs {
        processor.ingest(input).unwrap();
    }
    let mut frame = 16_u64;
    loop {
        let input = quote_at(&inputs[1], &admission, frame, 16, 0);
        match processor.ingest(&input) {
            Ok(_) => frame += 1,
            Err(SnapshotV2Error::Snapshot(SnapshotError::FeatureQueueDrop)) => break,
            Err(error) => panic!("unexpected capacity result: {error}"),
        }
        assert!(frame < 5_000, "feature queue never filled");
    }
    let dropped = processor.snapshot(decision_offset(&admission, 20)).unwrap();
    assert!(
        dropped.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "QUEUE_DROP")
    );
    assert!(
        processor
            .ingest(&quote_at(&inputs[1], &admission, frame + 1, 17, 0))
            .is_err()
    );
    assert!(
        processor
            .ingest(&quote_at(&inputs[1], &admission, frame + 2, 60_100, 1))
            .is_ok()
    );
}

#[test]
fn optional_oi_mutation_invalidates_only_its_family_feature() {
    let admission = admission();
    let inputs = complete_inputs(&admission);
    let live = live_inputs(&admission);
    let mut processor = processor(&admission);
    for input in inputs.iter().chain(&live) {
        processor.ingest(input).unwrap();
    }

    let mut healthy = Vec::with_capacity(6);
    for (index, input) in live.iter().take(6).enumerate() {
        let frame = 201 + u64::try_from(index).unwrap();
        let mut value = market_at(
            input,
            &admission,
            frame,
            60_017 + i64::try_from(index).unwrap(),
            0,
        );
        match index {
            0 => {
                value["envelope"]["source_sequence"]["first"] = json!(102);
                value["envelope"]["source_sequence"]["last"] = json!(102);
                value["market_cursor"]["first_sequence"] = json!(102);
                value["market_cursor"]["last_sequence"] = json!(102);
                value["source_provenance"]["aggregate_trade_id"] = json!(102);
            }
            1 | 3 | 4 | 5 => value["market_cursor"]["raw_frame_seq"] = json!(frame),
            2 => {
                value["envelope"]["source_sequence"]["first"] = json!(202);
                value["envelope"]["source_sequence"]["last"] = json!(202);
                value["market_cursor"]["first_sequence"] = json!(202);
                value["market_cursor"]["last_sequence"] = json!(202);
                value["source_provenance"]["last_update_id"] = json!(202);
            }
            _ => unreachable!(),
        }
        healthy.push(rehash_value(value));
    }
    for input in healthy.iter().take(4) {
        processor.ingest(input).unwrap();
    }

    let mutation = (11..1_000)
        .map(|quantity| {
            let mut value = serde_json::to_value(&healthy[3]).unwrap();
            value["envelope"]["payload"] =
                serde_json::to_value(MarketEvent::OpenInterest(OpenInterest {
                    quantity: Quantity(Fixed::new(quantity, 0)),
                }))
                .unwrap();
            rehash_value(value)
        })
        .find(|candidate| candidate.payload_hash() > healthy[3].payload_hash())
        .expect("bounded mutation with a later authenticated hash");
    let mutation_error = processor.ingest(&mutation).unwrap_err();
    assert!(
        mutation_error
            .to_string()
            .contains("cursor coordinate was reused with different payload"),
        "unexpected OI mutation error: {mutation_error}"
    );
    for input in healthy.iter().skip(4) {
        processor.ingest(input).unwrap();
    }

    for (index, input) in live.iter().skip(6).enumerate() {
        let at = decision_offset(&admission, 60_023 + i64::try_from(index).unwrap())
            .canonical()
            .to_owned();
        let mut value = serde_json::to_value(input).unwrap();
        value["available_at"] = json!(at);
        if value.get("observed_at").is_some() {
            value["observed_at"] = json!(at);
            value["clock_cursor"]["start"] = json!(3);
            value["clock_cursor"]["end"] = json!(3);
        } else {
            value["covered_through"] = json!(at);
            value["coverage_cursor"]["start"] = json!(3);
            value["coverage_cursor"]["end"] = json!(3);
        }
        processor.ingest(&rehash_value(value)).unwrap();
    }

    let snapshot = processor
        .snapshot(decision_offset(&admission, 60_031))
        .unwrap();
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "OI_STALE")
    );
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
    let oi = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "open_interest_change")
        .unwrap();
    assert_eq!(oi["reason_code"], "SOURCE_INVALIDATED");
    let trade = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "log_return")
        .unwrap();
    assert_ne!(trade["reason_code"], "SOURCE_INVALIDATED");
}
