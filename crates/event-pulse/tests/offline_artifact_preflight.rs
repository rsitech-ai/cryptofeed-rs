use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    ArtifactRoleV1, EpinJson1Reader, EpinJson1Writer, OfflineArtifactError,
    OfflineArtifactPreflightV1, ProspectiveCaptureAdmissionV1, ReplayInputError,
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceV1, ClockStateV1,
        ContributorRoleV1, ContributorV1, CoverageCursorV1, CoverageSourceV1, CursorV1,
        DropCategoryV1, FaultScopeV1, MechanicsInputV1, OpenInterestEncodingV1, ReplayCatalogV1,
        ReplayEpochEntryV1, Rfc3339Time, SystemFaultV1, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookLevel, BookSnapshot, ConnectionId, EventEnvelope, EventFlags, Fixed,
    InstrumentId, Liquidation, MarketEvent, OpenInterest, Price, Quantity, Quote, SequenceRange,
    SessionId, TimestampNs, Trade, VenueId,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn sha(byte: char, len: usize) -> String {
    std::iter::repeat_n(byte, len).collect()
}

fn binding(source: &str, venue: &str, blob: char, roles: &[&str]) -> Value {
    json!({
        "source_id": source,
        "connection_id": format!("{source}_connection"),
        "format": "MFR1",
        "instrument": {
            "base_asset": "BTC", "quote_asset": "USDT", "market_type": "PERPETUAL",
            "venue": venue, "venue_symbol": if venue == "BINANCE" { "BTCUSDT" } else { "BTC" }
        },
        "roles": roles,
        "families": if venue == "BINANCE" {
            json!(["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"])
        } else {
            json!(["CONFIRMATION_PRICE"])
        },
        "public_read_only": true,
        "repository_url": "https://github.com/rsitech-ai/cryptofeed-rs",
        "producer_commit": sha('a', 40),
        "producer_path": format!("crates/event-pulse-capture/src/{source}.rs"),
        "producer_blob_sha256": sha(blob, 64)
    })
}

fn admission() -> ProspectiveCaptureAdmissionV1 {
    let clock = |source: &str, subject: &str, blob: char| {
        json!({
            "source_id": source, "subject_source_id": subject,
            "evidence_kind": "UTC_MONOTONIC_OBSERVATION", "derivation": "INDEPENDENT_SIDECAR",
            "producer_commit": sha('d', 40),
            "producer_path": format!("crates/event-pulse-capture/src/{source}.rs"),
            "producer_blob_sha256": sha(blob, 64)
        })
    };
    let coverage = |source: &str, subject: &str, family: &str, blob: char| {
        json!({
            "source_id": source, "subject_source_id": subject, "family": family,
            "evidence_kind": "EXPLICIT_HEARTBEAT_RANGE", "derivation": "INDEPENDENT_SIDECAR",
            "producer_commit": sha('8', 40),
            "producer_path": format!("crates/event-pulse-capture/src/{source}.rs"),
            "producer_blob_sha256": sha(blob, 64)
        })
    };
    let value = json!({
        "schema": "event-pulse-e2-prospective-admission/1.0",
        "root_amendment_commit": "24b51a58c670ab722538bec4a3e1def0278b1107",
        "root_default_reachable_at": "2026-08-22T07:35:52Z",
        "capture_starts_at": "2026-08-22T07:35:52.000001Z",
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE", "source_qualification": "UNVERIFIED",
        "required_roles": ["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION",
            "CONFIRMATION", "CLOCK", "COVERAGE", "SYSTEM"],
        "primary": binding("binance_primary", "BINANCE", 'b',
            &["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"]),
        "confirmation": binding("hyperliquid_confirmation", "HYPERLIQUID", 'c', &["CONFIRMATION"]),
        "clocks": [clock("primary_clock", "binance_primary", 'e'),
            clock("confirmation_clock", "hyperliquid_confirmation", 'f')],
        "coverage": [
            coverage("primary_trade_coverage", "binance_primary", "TRADE", '1'),
            coverage("primary_quote_coverage", "binance_primary", "QUOTE", '2'),
            coverage("primary_book_coverage", "binance_primary", "BOOK", '3'),
            coverage("primary_oi_coverage", "binance_primary", "OPEN_INTEREST", '4'),
            coverage("primary_liq_coverage", "binance_primary", "LIQUIDATION", '5'),
            coverage("confirmation_price_coverage", "hyperliquid_confirmation", "CONFIRMATION_PRICE", '6')
        ],
        "system": {
            "source_id": "capture_system", "processor_id": "event_pulse_e2_prospective",
            "target": "PROCESSOR", "fault_scope": "PROCESSOR", "cursor_mode": "DERIVED",
            "evidence_kind": "STABLE_SYSTEM_FAULT_MAPPING", "producer_commit": sha('7', 40),
            "producer_path": "crates/event-pulse-capture/src/system.rs",
            "producer_blob_sha256": sha('a', 64)
        },
        "authority": {"credentials_allowed": false, "private_endpoints_allowed": false,
            "orders_allowed": false, "execution_authority": false, "paper_authority": false,
            "promotion_authority": false}
    });
    ProspectiveCaptureAdmissionV1::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn time(ns: i64) -> Rfc3339Time {
    Rfc3339Time::from_unix_nanos(ns).unwrap()
}

fn complete_inputs(admission: &ProspectiveCaptureAdmissionV1) -> Vec<MechanicsInputV1> {
    let config = admission.mechanics_config();
    let primary = config
        .contributors()
        .iter()
        .find(|value| value.role() == ContributorRoleV1::Primary)
        .unwrap()
        .key()
        .clone();
    let confirmation = config
        .contributors()
        .iter()
        .find(|value| value.role() == ContributorRoleV1::Confirmation)
        .unwrap()
        .key()
        .clone();
    let catalog = ReplayCatalogV1::new(
        BTreeMap::from([
            (
                1,
                VenueCatalogEntryV1::new("BINANCE", primary.source_id()).unwrap(),
            ),
            (
                2,
                VenueCatalogEntryV1::new("HYPERLIQUID", confirmation.source_id()).unwrap(),
            ),
        ]),
        BTreeMap::from([
            (1, primary.instrument().clone()),
            (2, confirmation.instrument().clone()),
        ]),
        vec![
            ReplayEpochEntryV1::new(1, 1, "epoch_primary", 0).unwrap(),
            ReplayEpochEntryV1::new(2, 2, "epoch_confirmation", 0).unwrap(),
        ],
        BTreeMap::from([(1, OpenInterestEncodingV1::contracts())]),
    )
    .unwrap();
    let market = |venue, instrument, connection, session, sequence, ns, payload| {
        MechanicsInputV1::market(
            EventEnvelope {
                schema_version: 1,
                venue: VenueId(venue),
                instrument: Some(InstrumentId(instrument)),
                connection: ConnectionId(connection),
                session: SessionId(session),
                frame_seq: sequence,
                event_index: 0,
                exchange_ts: Some(TimestampNs(ns)),
                receive_ts: TimestampNs(ns),
                source_sequence: Some(SequenceRange {
                    first: sequence,
                    last: sequence,
                }),
                flags: EventFlags::empty(),
                payload,
            },
            0,
            catalog.clone(),
        )
        .unwrap()
    };
    let price = |value| Price(Fixed::new(value, 0));
    let quantity = |value| Quantity(Fixed::new(value, 0));
    let mut inputs = vec![
        market(
            1,
            1,
            1,
            1,
            1,
            1_000,
            MarketEvent::Trade(Trade {
                price: price(100),
                quantity: quantity(2),
                aggressor: AggressorSide::Buy,
                trade_id: None,
            }),
        ),
        market(
            1,
            1,
            1,
            1,
            2,
            2_000,
            MarketEvent::Quote(Quote {
                bid_price: price(99),
                bid_quantity: Some(quantity(1)),
                ask_price: price(101),
                ask_quantity: Some(quantity(1)),
            }),
        ),
        market(
            1,
            1,
            1,
            1,
            3,
            3_000,
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
        market(
            1,
            1,
            1,
            1,
            4,
            4_000,
            MarketEvent::OpenInterest(OpenInterest {
                quantity: quantity(10),
            }),
        ),
        market(
            1,
            1,
            1,
            1,
            5,
            5_000,
            MarketEvent::Liquidation(Liquidation {
                price: price(98),
                quantity: quantity(1),
                side: AggressorSide::Sell,
            }),
        ),
        market(
            2,
            2,
            2,
            2,
            1,
            6_000,
            MarketEvent::Trade(Trade {
                price: price(100),
                quantity: quantity(1),
                aggressor: AggressorSide::Unknown,
                trade_id: None,
            }),
        ),
    ];
    for (index, key) in config.clock_sources().iter().enumerate() {
        let contributor = ContributorV1::new(
            key.subject().clone(),
            if key.subject() == &primary {
                "epoch_primary"
            } else {
                "epoch_confirmation"
            },
            0,
        )
        .unwrap();
        let ns = 7_000 + i64::try_from(index).unwrap() * 1_000;
        inputs.push(
            MechanicsInputV1::clock(
                contributor,
                ClockSourceV1::new(key.clone(), "epoch_clock", 0).unwrap(),
                time(ns),
                time(ns),
                ClockCursorV1::native(1, 1).unwrap(),
                ClockStateV1::Synchronized,
                CanonicalDecimal::parse("0", 18, 8).unwrap(),
                2_000,
                ClockQualityV1::Validated,
                "SOURCE_CLOCK_WITHIN_TOLERANCE",
            )
            .unwrap(),
        );
    }
    for (index, key) in config.coverage_sources().iter().enumerate() {
        let contributor = ContributorV1::new(
            key.subject().clone(),
            if key.subject() == &primary {
                "epoch_primary"
            } else {
                "epoch_confirmation"
            },
            0,
        )
        .unwrap();
        let ns = 10_000 + i64::try_from(index).unwrap() * 1_000;
        inputs.push(
            MechanicsInputV1::coverage(
                contributor,
                CoverageSourceV1::new(key.clone(), "epoch_coverage", 0).unwrap(),
                key.family(),
                time(0),
                time(ns),
                time(ns),
                CoverageCursorV1::native(1, 1).unwrap(),
            )
            .unwrap(),
        );
    }
    let system_key = config.system_sources()[0].clone();
    inputs.push(
        MechanicsInputV1::system(
            SystemSourceV1::new(system_key, "epoch_system", 0).unwrap(),
            FaultScopeV1::processor(config.processor_id()).unwrap(),
            time(20_000),
            time(20_000),
            CursorV1::derived_drop(1, 0).unwrap(),
            SystemFaultV1::events_dropped(1, DropCategoryV1::ActionBuffer).unwrap(),
            None,
        )
        .unwrap(),
    );
    inputs
}

fn epin(inputs: &[MechanicsInputV1]) -> Vec<u8> {
    let mut writer = EpinJson1Writer::new(Vec::new());
    for input in inputs {
        writer.write_input(input).unwrap();
    }
    writer.finish()
}

#[test]
fn complete_canonical_epin_partitions_into_exactly_nine_deterministic_artifacts() {
    let admission = admission();
    let bytes = epin(&complete_inputs(&admission));
    let decision_time = time(20_000);
    let first =
        OfflineArtifactPreflightV1::build(&admission, decision_time.clone(), &bytes).unwrap();
    let second = OfflineArtifactPreflightV1::build(&admission, decision_time, &bytes).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.artifacts().len(), 9);
    assert_eq!(
        first
            .artifacts()
            .iter()
            .map(|artifact| artifact.role())
            .collect::<Vec<_>>(),
        ArtifactRoleV1::ALL
    );
    assert_eq!(
        first
            .artifacts()
            .iter()
            .map(|artifact| artifact.record_count())
            .collect::<Vec<_>>(),
        [1, 1, 1, 1, 1, 1, 2, 6, 1]
    );
    assert_eq!(
        first
            .artifacts()
            .iter()
            .map(|artifact| artifact.role().as_str())
            .collect::<Vec<_>>(),
        [
            "TRADE",
            "QUOTE",
            "BOOK",
            "OPEN_INTEREST",
            "LIQUIDATION",
            "CONFIRMATION",
            "CLOCK",
            "COVERAGE",
            "SYSTEM",
        ]
    );
    for artifact in first.artifacts() {
        assert!(artifact.record_count() > 0);
        assert!(artifact.bytes().ends_with(b"\n"));
        assert_eq!(
            artifact.byte_len(),
            u64::try_from(artifact.bytes().len()).unwrap()
        );
        assert_eq!(
            artifact.sha256(),
            format!("{:x}", Sha256::digest(artifact.bytes()))
        );
        assert!(artifact.first_available_at() <= artifact.last_available_at());
        EpinJson1Reader::new(artifact.bytes(), artifact.last_available_at().clone())
            .read_all()
            .unwrap();
    }
    assert!(!first.evidence_authoring_allowed());
    assert_eq!(first.blocker(), "blocked:fixture-provenance");
}

#[test]
fn missing_role_future_and_noncanonical_inputs_fail_closed() {
    let admission = admission();
    let mut inputs = complete_inputs(&admission);
    inputs.remove(4);
    assert_eq!(
        OfflineArtifactPreflightV1::build(&admission, time(20_000), &epin(&inputs)),
        Err(OfflineArtifactError::MissingRole(
            ArtifactRoleV1::Liquidation
        ))
    );

    let complete = epin(&complete_inputs(&admission));
    assert_eq!(
        OfflineArtifactPreflightV1::build(&admission, time(19_000), &complete),
        Err(OfflineArtifactError::Replay(ReplayInputError::FutureInput))
    );
    let first_line = complete.split(|byte| *byte == b'\n').next().unwrap();
    let pretty =
        serde_json::to_vec_pretty(&serde_json::from_slice::<Value>(first_line).unwrap()).unwrap();
    assert!(matches!(
        OfflineArtifactPreflightV1::build(&admission, time(20_000), &pretty),
        Err(OfflineArtifactError::Replay(
            ReplayInputError::MissingNewline
        )) | Err(OfflineArtifactError::Replay(
            ReplayInputError::InvalidInput(_)
        ))
    ));
}

#[test]
fn topology_mismatch_is_rejected_before_any_result_is_returned() {
    let admission = admission();
    let mut inputs = complete_inputs(&admission);
    let foreign = admission
        .mechanics_config()
        .contributors()
        .iter()
        .find(|value| value.role() == ContributorRoleV1::Confirmation)
        .unwrap()
        .key()
        .clone();
    let bad_clock = MechanicsInputV1::clock(
        ContributorV1::new(foreign.clone(), "epoch_confirmation", 0).unwrap(),
        ClockSourceV1::new(
            marketfeed_event_pulse::wire::ClockSourceKeyV1::new("foreign_clock", foreign).unwrap(),
            "epoch_clock",
            0,
        )
        .unwrap(),
        time(19_000),
        time(19_000),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        CanonicalDecimal::parse("0", 18, 8).unwrap(),
        2_000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_WITHIN_TOLERANCE",
    )
    .unwrap();
    inputs.insert(inputs.len() - 1, bad_clock);
    assert!(matches!(
        OfflineArtifactPreflightV1::build(&admission, time(20_000), &epin(&inputs)),
        Err(OfflineArtifactError::Topology(_))
    ));
}

#[test]
fn every_configured_source_must_be_represented_even_when_its_role_is_nonempty() {
    let admission = admission();
    let mut inputs = complete_inputs(&admission);
    inputs.remove(6);
    assert_eq!(
        OfflineArtifactPreflightV1::build(&admission, time(20_000), &epin(&inputs)),
        Err(OfflineArtifactError::IncompleteTopology)
    );
}
