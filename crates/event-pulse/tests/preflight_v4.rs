use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    ArtifactRoleV1, CursorError, IngestOutcome, MarketCursorV2, MechanicsInputV2,
    MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter, OfflineArtifactErrorV4,
    OfflineArtifactPreflightV4, ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2,
    SourceProvenanceV2, SourceStateMachineV2,
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceV1, ClockStateV1,
        ContributorV1, CoverageCursorV1, CoverageSourceV1, CursorV1, DropCategoryV1, FaultScopeV1,
        MechanicsInputV1, OpenInterestEncodingV1, ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time,
        SystemFaultV1, SystemSourceV1, VenueCatalogEntryV1,
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

fn admission_value() -> Value {
    json!({
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
    })
}

fn admission() -> ProspectiveCaptureAdmissionV2 {
    ProspectiveCaptureAdmissionV2::from_json(&serde_json::to_vec(&admission_value()).unwrap())
        .unwrap()
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
    let source_ms = start_ns.div_euclid(1_000_000) as u64;
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

fn retime(input: &MechanicsInputV2, at_ns: i64, frame: u64) -> MechanicsInputV2 {
    let mut value = serde_json::to_value(input).unwrap();
    value.as_object_mut().unwrap().remove("payload_hash");
    value["envelope"]["receive_ts"] = json!(at_ns);
    value["envelope"]["exchange_ts"] = json!(at_ns);
    value["envelope"]["frame_seq"] = json!(frame);
    if value["market_cursor"]["kind"] == "DERIVED" {
        value["market_cursor"]["raw_frame_seq"] = json!(frame);
    }
    let at_ms = u64::try_from(at_ns.div_euclid(1_000_000)).unwrap();
    if let Some(provenance) = value["source_provenance"].as_object_mut() {
        for field in [
            "event_time_ms",
            "transaction_time_ms",
            "trade_time_ms",
            "source_time_ms",
            "order_trade_time_ms",
        ] {
            if provenance.contains_key(field) {
                provenance.insert(field.to_owned(), json!(at_ms));
            }
        }
    }
    value["payload_hash"] = json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).unwrap())
    ));
    MechanicsInputV2::from_json_line(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn rehash_value(mut value: Value) -> MechanicsInputV2 {
    value.as_object_mut().unwrap().remove("payload_hash");
    value["payload_hash"] = json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).unwrap())
    ));
    MechanicsInputV2::from_json_line(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn book_delta_from(
    snapshot: &MechanicsInputV2,
    frame: u64,
    first_update_id: u64,
    final_update_id: u64,
    previous_final_update_id: u64,
) -> MechanicsInputV2 {
    let mut value = serde_json::to_value(snapshot).unwrap();
    value["envelope"]["frame_seq"] = json!(frame);
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

fn generation(
    input: &MechanicsInputV2,
    at_ns: i64,
    frame: u64,
    epoch: &str,
    generation: u8,
) -> MechanicsInputV2 {
    let mut value = serde_json::to_value(retime(input, at_ns, frame)).unwrap();
    value["catalog"]["connection_epochs"][0]["connection_epoch"] = json!(epoch);
    value["catalog"]["connection_epochs"][0]["epoch_generation"] = json!(generation);
    rehash_value(value)
}

fn build_one(
    admission: &ProspectiveCaptureAdmissionV2,
    input: &MechanicsInputV2,
) -> Result<OfflineArtifactPreflightV4, OfflineArtifactErrorV4> {
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(admission).unwrap();
    let mut bytes = serde_json::to_vec(input).unwrap();
    bytes.push(b'\n');
    OfflineArtifactPreflightV4::build(
        admission,
        &policy,
        Rfc3339Time::from_unix_nanos(
            admission.capture_starts_at().utc_micros() * 1_000 + 1_000_000_000,
        )
        .unwrap(),
        &bytes,
    )
}

#[test]
fn fifteen_records_from_twelve_sources_build_nine_deterministic_artifacts() {
    let admission = admission();
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    let records = complete_inputs(&admission);
    assert_eq!(records.len(), 15);
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    for record in &records {
        writer.write_input(record).unwrap();
    }
    let bytes = writer.finish();
    let decision = Rfc3339Time::from_unix_nanos(
        admission.capture_starts_at().utc_micros() * 1_000 + 20_000_000,
    )
    .unwrap();
    let first =
        OfflineArtifactPreflightV4::build(&admission, &policy, decision.clone(), &bytes).unwrap();
    let second = OfflineArtifactPreflightV4::build(&admission, &policy, decision, &bytes).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.artifacts().len(), 9);
    for role in ArtifactRoleV1::ALL {
        let artifact = first
            .artifacts()
            .iter()
            .find(|item| item.role() == role)
            .unwrap();
        if role == ArtifactRoleV1::System {
            assert_eq!(artifact.record_count(), 0);
            assert!(artifact.bytes().is_empty());
        } else {
            assert!(artifact.record_count() > 0);
            let decoded = MechanicsInputV2JsonlReader::new(
                artifact.bytes(),
                artifact.last_available_at().unwrap().clone(),
            )
            .read_all()
            .unwrap();
            assert_eq!(decoded.len() as u64, artifact.record_count());
        }
    }
    assert!(!first.evidence_authoring_allowed());
    assert_eq!(first.blocker(), "blocked:fixture-provenance");
}

#[test]
fn preflight_accepts_multi_record_binance_book_bootstrap_and_pu_continuity() {
    let admission = admission();
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    let mut records = complete_inputs(&admission);
    let first = book_delta_from(&records[2], 4, 195, 205, 17);
    let next = book_delta_from(&records[2], 5, 206, 210, 205);
    records.insert(3, first);
    records.insert(4, next);
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    for record in &records {
        writer.write_input(record).unwrap();
    }
    let decision = Rfc3339Time::from_unix_nanos(
        admission.capture_starts_at().utc_micros() * 1_000 + 20_000_000,
    )
    .unwrap();
    let built =
        OfflineArtifactPreflightV4::build(&admission, &policy, decision, &writer.finish()).unwrap();
    let book = built
        .artifacts()
        .iter()
        .find(|artifact| artifact.role() == ArtifactRoleV1::Book)
        .unwrap();
    assert_eq!(book.record_count(), 3);
}

#[test]
fn preflight_rejects_binance_book_no_overlap_and_wrong_pu() {
    let admission = admission();
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    let decision = Rfc3339Time::from_unix_nanos(
        admission.capture_starts_at().utc_micros() * 1_000 + 20_000_000,
    )
    .unwrap();
    for deltas in [
        vec![book_delta_from(
            &complete_inputs(&admission)[2],
            4,
            201,
            205,
            200,
        )],
        vec![
            book_delta_from(&complete_inputs(&admission)[2], 4, 195, 205, 17),
            book_delta_from(&complete_inputs(&admission)[2], 5, 206, 210, 204),
        ],
    ] {
        let mut records = complete_inputs(&admission);
        for (offset, delta) in deltas.into_iter().enumerate() {
            records.insert(3 + offset, delta);
        }
        let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
        for record in &records {
            writer.write_input(record).unwrap();
        }
        assert_eq!(
            OfflineArtifactPreflightV4::build(
                &admission,
                &policy,
                decision.clone(),
                &writer.finish(),
            ),
            Err(OfflineArtifactErrorV4::Topology(CursorError::NativeGap))
        );
    }
}

#[test]
fn equal_time_market_replay_uses_raw_coordinates_for_every_cursor_mode() {
    let admission = admission();
    let records = complete_inputs(&admission);
    let at = admission.capture_starts_at().utc_micros() * 1_000 + 20_000_000;
    let quote = retime(&records[1], at, 10);
    let book = retime(&records[2], at, 20);
    let mut public = MechanicsInputV2JsonlWriter::new(Vec::new());
    public.write_input(&quote).unwrap();
    public.write_input(&book).unwrap();
    let mut public_reversed = MechanicsInputV2JsonlWriter::new(Vec::new());
    public_reversed.write_input(&book).unwrap();
    assert_eq!(
        public_reversed.write_input(&quote),
        Err(marketfeed_event_pulse::ReplayInputError::OrderViolation)
    );

    let trade = retime(&records[0], at, 30);
    let open_interest = retime(&records[3], at, 31);
    let liquidation = retime(&records[4], at, 32);
    let mut market = MechanicsInputV2JsonlWriter::new(Vec::new());
    market.write_input(&trade).unwrap();
    market.write_input(&open_interest).unwrap();
    market.write_input(&liquidation).unwrap();
    let regressing_open_interest = retime(&records[3], at, 20);
    let mut market_reversed = MechanicsInputV2JsonlWriter::new(Vec::new());
    market_reversed.write_input(&trade).unwrap();
    assert_eq!(
        market_reversed.write_input(&regressing_open_interest),
        Err(marketfeed_event_pulse::ReplayInputError::OrderViolation)
    );
}

#[test]
fn family_keyed_state_accepts_mixed_modes_in_all_orders_and_rejects_cross_family_time_regression() {
    let admission = admission();
    let records = complete_inputs(&admission);
    let config = admission.mechanics_config();
    let start = admission.capture_starts_at().utc_micros() * 1_000;
    for order in [[1_usize, 2_usize], [2, 1]] {
        let mut state = SourceStateMachineV2::new(config.clone());
        for (offset, index) in order.into_iter().enumerate() {
            let input = retime(
                &records[index],
                start + (offset as i64 + 1) * 1_000_000,
                100 + offset as u64,
            );
            assert!(matches!(
                state.ingest(&input),
                Ok(IngestOutcome::AcceptedWarming)
            ));
        }
    }
    for order in [
        [0_usize, 3_usize, 4_usize],
        [0, 4, 3],
        [3, 0, 4],
        [3, 4, 0],
        [4, 0, 3],
        [4, 3, 0],
    ] {
        let mut state = SourceStateMachineV2::new(config.clone());
        for (offset, index) in order.into_iter().enumerate() {
            let input = retime(
                &records[index],
                start + (offset as i64 + 1) * 1_000_000,
                200 + offset as u64,
            );
            assert!(matches!(
                state.ingest(&input),
                Ok(IngestOutcome::AcceptedWarming)
            ));
        }
    }
    let mut state = SourceStateMachineV2::new(config.clone());
    assert!(
        state
            .ingest(&retime(&records[1], start + 100_000_000, 301))
            .is_ok()
    );
    assert!(
        state
            .ingest(&retime(&records[2], start + 300_000_000, 302))
            .is_ok()
    );
    assert_eq!(
        state.ingest(&retime(&records[1], start + 200_000_000, 303)),
        Err(CursorError::AvailabilityRegression)
    );
    let public = config.contributors()[0].key();
    assert!(
        state
            .market_cursor(public, marketfeed_event_pulse::wire::FamilyV1::Quote)
            .is_none()
    );
    assert!(
        state
            .market_cursor(public, marketfeed_event_pulse::wire::FamilyV1::Book)
            .is_none()
    );

    let mut isolated = SourceStateMachineV2::new(config.clone());
    isolated
        .ingest(&retime(&records[1], start + 10_000_000, 401))
        .unwrap();
    isolated
        .ingest(&retime(&records[2], start + 20_000_000, 402))
        .unwrap();
    let gap = retime(
        &book_delta_from(&records[2], 403, 201, 202, 200),
        start + 30_000_000,
        403,
    );
    assert_eq!(isolated.ingest(&gap), Err(CursorError::NativeGap));
    assert!(
        isolated
            .market_cursor(public, marketfeed_event_pulse::wire::FamilyV1::Quote)
            .is_some()
    );
    assert_eq!(
        isolated.market_invalidity(public, marketfeed_event_pulse::wire::FamilyV1::Book),
        Some(marketfeed_event_pulse::Invalidity::Recoverable)
    );

    let mut recovery = SourceStateMachineV2::new(config.clone());
    recovery
        .ingest(&generation(
            &records[1],
            start + 1_000_000,
            501,
            "epoch_public",
            0,
        ))
        .unwrap();
    recovery
        .ingest(&generation(
            &records[2],
            start + 2_000_000,
            502,
            "epoch_public",
            0,
        ))
        .unwrap();
    recovery
        .ingest(&generation(
            &records[1],
            start + 3_000_000,
            503,
            "epoch_recovered",
            1,
        ))
        .unwrap();
    assert!(
        recovery
            .market_cursor(public, marketfeed_event_pulse::wire::FamilyV1::Book)
            .is_none()
    );
}

#[test]
fn preflight_rejects_pre_start_market_clock_and_coverage_causal_times() {
    let admission = admission();
    let records = complete_inputs(&admission);
    let before_ms =
        u64::try_from(admission.capture_starts_at().utc_micros().div_euclid(1_000) - 1).unwrap();
    let before_ns = i64::try_from(before_ms).unwrap() * 1_000_000;

    let mut market = serde_json::to_value(&records[0]).unwrap();
    market["envelope"]["exchange_ts"] = json!(before_ns);
    market["envelope"]["receive_ts"] = json!(before_ns);
    market["source_provenance"]["event_time_ms"] = json!(before_ms);
    market["source_provenance"]["trade_time_ms"] = json!(before_ms);
    assert_eq!(
        build_one(&admission, &rehash_value(market)),
        Err(OfflineArtifactErrorV4::InputBeforeCaptureStart(
            ArtifactRoleV1::Trade
        ))
    );

    let before = Rfc3339Time::from_unix_nanos(before_ns)
        .unwrap()
        .canonical()
        .to_owned();
    let mut clock = serde_json::to_value(&records[6]).unwrap();
    clock["observed_at"] = json!(before);
    clock["available_at"] = json!(before);
    assert_eq!(
        build_one(&admission, &rehash_value(clock)),
        Err(OfflineArtifactErrorV4::InputBeforeCaptureStart(
            ArtifactRoleV1::Clock
        ))
    );

    let mut coverage = serde_json::to_value(&records[9]).unwrap();
    coverage["covered_from"] = json!(before);
    coverage["covered_through"] = json!(before);
    coverage["available_at"] = json!(before);
    assert_eq!(
        build_one(&admission, &rehash_value(coverage)),
        Err(OfflineArtifactErrorV4::InputBeforeCaptureStart(
            ArtifactRoleV1::Coverage
        ))
    );
}

#[test]
fn preflight_rejects_missing_duplicate_and_any_system_record() {
    let admission = admission();
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    let records = complete_inputs(&admission);
    let decision = Rfc3339Time::from_unix_nanos(
        admission.capture_starts_at().utc_micros() * 1_000 + 30_000_000,
    )
    .unwrap();
    let write = |items: &[MechanicsInputV2]| {
        let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
        for item in items {
            writer.write_input(item).unwrap();
        }
        writer.finish()
    };
    assert_eq!(
        OfflineArtifactPreflightV4::build(
            &admission,
            &policy,
            decision.clone(),
            &write(&records[..14])
        ),
        Err(OfflineArtifactErrorV4::IncompleteTopology)
    );
    let mut duplicate = records.clone();
    duplicate.insert(1, records[0].clone());
    assert_eq!(
        OfflineArtifactPreflightV4::build(
            &admission,
            &policy,
            decision.clone(),
            &write(&duplicate)
        ),
        Err(OfflineArtifactErrorV4::DuplicateRecord)
    );

    let at = Rfc3339Time::from_unix_nanos(
        admission.capture_starts_at().utc_micros() * 1_000 + 20_000_000,
    )
    .unwrap();
    let system_key = admission.mechanics_config().system_sources()[0].clone();
    let system = MechanicsInputV1::system(
        SystemSourceV1::new(system_key, "epoch_system", 0).unwrap(),
        FaultScopeV1::processor(admission.mechanics_config().processor_id()).unwrap(),
        at.clone(),
        at,
        CursorV1::derived_drop(100, 1).unwrap(),
        SystemFaultV1::events_dropped(1, DropCategoryV1::MarketDispatch).unwrap(),
        None,
    )
    .unwrap();
    let mut with_system = records;
    with_system.push(MechanicsInputV2::from_v1_non_market(system).unwrap());
    assert_eq!(
        OfflineArtifactPreflightV4::build(&admission, &policy, decision, &write(&with_system)),
        Err(OfflineArtifactErrorV4::NonEmptyTruthfulEmptySystem)
    );
}

#[test]
fn preflight_rejects_aggregate_one_over_before_parsing() {
    let admission = admission();
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    let decision = Rfc3339Time::from_unix_nanos(
        admission.capture_starts_at().utc_micros() * 1_000 + 1_000_000,
    )
    .unwrap();
    assert_eq!(
        OfflineArtifactPreflightV4::build(
            &admission,
            &policy,
            decision,
            &vec![b' '; marketfeed_event_pulse::wire::MAX_INPUT_BYTES + 1],
        ),
        Err(OfflineArtifactErrorV4::AggregateTooLarge)
    );
}
