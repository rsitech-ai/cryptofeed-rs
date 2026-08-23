use std::collections::BTreeMap;
use std::io::Cursor;

use marketfeed_event_pulse::{
    MarketCursorV2, MechanicsInputV2, MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter,
    ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2, SnapshotProcessorV2,
    SnapshotV2Error, SourceProvenanceV2,
    snapshot::{SNAPSHOT_V2_CONTRACT_SHA256, SNAPSHOT_V2_ROOT_MERGE, SNAPSHOT_V2_ROOT_TREE},
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceV1, ClockStateV1,
        ContributorV1, CoverageCursorV1, CoverageSourceV1, CursorV1, DropCategoryV1, FaultScopeV1,
        MechanicsInputV1, OpenInterestEncodingV1, ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time,
        SnapshotAuthoringV1, SystemFaultV1, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookLevel, BookSnapshot, ConnectionId, EventEnvelope, EventFlags, Fixed,
    InstrumentId, Liquidation, MarketEvent, OpenInterest, Price, PricePoint, Quantity, Quote,
    SequenceRange, SessionId, TimestampNs, Trade, VenueId,
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
