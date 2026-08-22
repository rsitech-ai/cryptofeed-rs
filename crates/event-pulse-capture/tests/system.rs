use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    ArtifactRoleV1, EpinJson1Writer, OfflineArtifactError, ProspectiveAdmissionError,
    ProspectiveCaptureAdmissionV1,
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceV1, ClockStateV1,
        ContributorRoleV1, ContributorV1, CoverageCursorV1, CoverageSourceV1, MechanicsInputV1,
        OpenInterestEncodingV1, ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time,
        VenueCatalogEntryV1,
    },
};
use marketfeed_event_pulse_capture::{TruthfulEmptySystemAssemblerV1, TruthfulEmptySystemError};
use marketfeed_model::{
    AggressorSide, BookLevel, BookSnapshot, ConnectionId, EventEnvelope, EventFlags, Fixed,
    InstrumentId, Liquidation, MarketEvent, OpenInterest, Price, Quantity, Quote, SequenceRange,
    SessionId, TimestampNs, Trade, VenueId,
};
use serde_json::{Value, json};

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
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "required_roles": ["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION",
            "CONFIRMATION", "CLOCK", "COVERAGE", "SYSTEM"],
        "primary": binding("binance_primary", "BINANCE", 'b',
            &["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"]),
        "confirmation": binding("hyperliquid_confirmation", "HYPERLIQUID", 'c',
            &["CONFIRMATION"]),
        "clocks": [clock("primary_clock", "binance_primary", 'e'),
            clock("confirmation_clock", "hyperliquid_confirmation", 'f')],
        "coverage": [
            coverage("primary_trade_coverage", "binance_primary", "TRADE", '1'),
            coverage("primary_quote_coverage", "binance_primary", "QUOTE", '2'),
            coverage("primary_book_coverage", "binance_primary", "BOOK", '3'),
            coverage("primary_oi_coverage", "binance_primary", "OPEN_INTEREST", '4'),
            coverage("primary_liq_coverage", "binance_primary", "LIQUIDATION", '5'),
            coverage("confirmation_price_coverage", "hyperliquid_confirmation",
                "CONFIRMATION_PRICE", '6')
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

fn capture_start_ns() -> i64 {
    Rfc3339Time::parse("2026-08-22T07:35:52.000001Z")
        .unwrap()
        .utc_micros()
        .checked_mul(1_000)
        .unwrap()
}

fn complete_epin_without_system(admission: &ProspectiveCaptureAdmissionV1) -> Vec<u8> {
    let start_ns = capture_start_ns();
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
            start_ns,
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
            start_ns + 1_000,
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
            start_ns + 2_000,
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
            start_ns + 3_000,
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
            start_ns + 4_000,
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
            start_ns + 5_000,
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
        let ns = start_ns + 6_000 + i64::try_from(index).unwrap() * 1_000;
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
        let ns = start_ns + 9_000 + i64::try_from(index).unwrap() * 1_000;
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
    let mut writer = EpinJson1Writer::new(Vec::new());
    for input in inputs {
        writer.write_input(&input).unwrap();
    }
    writer.finish()
}

#[test]
fn assembler_is_bound_to_truthful_empty_policy_and_has_no_fault_authoring_authority() {
    let freeze = include_bytes!(
        "../../event-pulse/contracts/prospective/event-pulse-e2-producer-evidence-freeze.json"
    );
    assert!(!TruthfulEmptySystemAssemblerV1::evidence_authoring_allowed());
    assert_eq!(
        TruthfulEmptySystemAssemblerV1::blocker(),
        "blocked:fixture-provenance"
    );
    assert_eq!(
        TruthfulEmptySystemAssemblerV1::assemble(
            &admission(),
            freeze,
            Rfc3339Time::parse("2026-08-22T07:35:52.000001Z").unwrap(),
            b"",
        ),
        Err(TruthfulEmptySystemError::Preflight(
            OfflineArtifactError::IncompleteTopology
        ))
    );

    let mut mutated = freeze.to_vec();
    mutated[0] = b'[';
    assert_eq!(
        TruthfulEmptySystemAssemblerV1::assemble(
            &admission(),
            &mutated,
            Rfc3339Time::parse("2026-08-22T07:35:52.000001Z").unwrap(),
            b"",
        ),
        Err(TruthfulEmptySystemError::Admission(
            ProspectiveAdmissionError::SystemFreeze
        ))
    );
}

#[test]
fn assembler_returns_the_exact_nine_artifact_report_for_complete_eight_role_epin() {
    let admission = admission();
    let freeze = include_bytes!(
        "../../event-pulse/contracts/prospective/event-pulse-e2-producer-evidence-freeze.json"
    );
    let complete_epin = complete_epin_without_system(&admission);
    let result = TruthfulEmptySystemAssemblerV1::assemble(
        &admission,
        freeze,
        time(capture_start_ns() + 20_000),
        &complete_epin,
    )
    .unwrap();
    assert_eq!(result.artifacts().len(), 9);
    let system = result
        .artifacts()
        .iter()
        .find(|artifact| artifact.role() == ArtifactRoleV1::System)
        .unwrap();
    assert_eq!(system.bytes(), b"");
    assert_eq!(system.record_count(), 0);
    assert_eq!(system.byte_len(), 0);
    assert_eq!(
        system.sha256(),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(system.first_available_at(), None);
    assert_eq!(system.last_available_at(), None);
    assert!(system.record_identities().is_empty());
}
