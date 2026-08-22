use marketfeed_event_pulse::{
    ContractBundle, ContractError,
    wire::{CanonicalDecimal, ConnectionKeyV1, CursorV1, Rfc3339Time, SystemChainPreimage},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

fn golden(index: usize) -> Value {
    let suite: Value = serde_json::from_slice(include_bytes!(
        "../contracts/event-pulse/event_pulse_v1_golden.json"
    ))
    .unwrap();
    suite["vectors"][index]["payload"].clone()
}

fn q1_golden(index: usize) -> Value {
    let suite: Value = serde_json::from_slice(include_bytes!(
        "../contracts/quant-harness/quant_harness_v1_golden.json"
    ))
    .unwrap();
    suite["vectors"][index]["payload"].clone()
}

fn rehash(mut value: Value) -> Value {
    value.as_object_mut().unwrap().remove("content_hash");
    value["content_hash"] = json!(marketfeed_event_pulse::content_hash(&value).unwrap());
    value
}

fn rehash_epin(mut value: Value) -> Value {
    value.as_object_mut().unwrap().remove("payload_hash");
    let hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&value).unwrap()));
    value["payload_hash"] = json!(hash);
    value
}

#[test]
fn rfc3339_preserves_instant_but_emits_canonical_zero_or_six_fraction() {
    let utc = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let offset = Rfc3339Time::parse("2026-08-21T12:00:00.123456789+02:00").unwrap();
    let same = Rfc3339Time::parse("2026-08-21T10:00:00.123456Z").unwrap();
    assert_eq!(utc.canonical(), "2026-08-21T10:00:00Z");
    assert_eq!(offset.canonical(), "2026-08-21T12:00:00.123456+02:00");
    assert_eq!(offset.utc_micros(), same.utc_micros());
    assert_eq!(offset, same);
    assert_eq!(
        Rfc3339Time::parse("2026-08-21T10:00:00.000000+00:00")
            .unwrap()
            .as_str(),
        "2026-08-21T10:00:00Z"
    );
    assert!(Rfc3339Time::parse("Tue, 21 Aug 2026 10:00:00 GMT").is_err());
}

#[test]
fn rfc3339_unix_nanos_floors_negative_submicrosecond_instants() {
    assert_eq!(
        Rfc3339Time::from_unix_nanos(-1).unwrap().canonical(),
        "1969-12-31T23:59:59.999999Z"
    );
    assert_eq!(
        Rfc3339Time::from_unix_nanos(-1_001).unwrap().canonical(),
        "1969-12-31T23:59:59.999998Z"
    );
}

#[test]
fn cursor_display_sequence_is_shared_by_ordinary_and_reserved_coordinates() {
    assert_eq!(
        CursorV1::derived(0, 0, 0)
            .unwrap()
            .display_sequence()
            .unwrap(),
        0
    );
    assert!(CursorV1::derived(0, 65_535, 0).is_err());
    assert!(CursorV1::derived(0, 0, 65_535).is_ok());
    assert_eq!(
        CursorV1::derived_drop(0, 2)
            .unwrap()
            .display_sequence()
            .unwrap(),
        4_294_901_762
    );
}

#[test]
fn system_chain_preimages_are_raw_bytes_not_json_or_hex_text() {
    let payload = "aa".repeat(32);
    let first = SystemChainPreimage::first(&payload).unwrap();
    assert_eq!(&first[..29], b"event-pulse-system-chain-v1\0\0");
    let next = SystemChainPreimage::next(&"bb".repeat(32), &payload).unwrap();
    assert_eq!(next.len(), 29 + 32 + 32);
    assert_eq!(
        SystemChainPreimage::hash_first(&payload).unwrap(),
        "78a1d73ab3505b75dea10436cd79947b7a4a151acea6a5d37eeb9a660234c912"
    );
    assert_eq!(
        SystemChainPreimage::hash_next(&"bb".repeat(32), &payload).unwrap(),
        "a70cb387c5cbfe9e37bfdc9fbd288e7e0e8958d6ac202431119d7d8894a86347"
    );
}

#[test]
fn q1_risk_decision_rejects_rehashed_semantic_mutations() {
    let bundle = ContractBundle::load_embedded().unwrap();
    let template = q1_golden(0);
    let mut risk = json!({
        "schema_version":"quant-harness/1.0",
        "contract_id":"risk_decision_test",
        "contract_type":"risk_decision",
        "lineage_id":"lineage_test",
        "revision":1,
        "predecessor_content_hash":null,
        "causal_time":template["causal_time"],
        "proposal_request_content_hash":"aa".repeat(32),
        "issuer":"research_os_risk_governor",
        "outcome":"hold",
        "reason_codes":["INSUFFICIENT_EVIDENCE"],
        "evidence_content_hashes":["bb".repeat(32)]
    });
    risk = rehash(risk);
    bundle
        .validate_q1_json(&serde_json::to_vec(&risk).unwrap())
        .unwrap();
    for (field, value) in [
        ("outcome", json!("execute_live")),
        ("proposal_request_content_hash", json!("not-a-hash")),
        ("reason_codes", json!([])),
        ("reason_codes", json!(["DUPLICATE", "DUPLICATE"])),
        ("evidence_content_hashes", json!([])),
        (
            "evidence_content_hashes",
            json!(["bb".repeat(32), "bb".repeat(32)]),
        ),
    ] {
        let mut changed = risk.clone();
        changed[field] = value;
        changed = rehash(changed);
        assert!(
            matches!(
                bundle.validate_q1_json(&serde_json::to_vec(&changed).unwrap()),
                Err(ContractError::Semantic(_))
            ),
            "accepted mutated {field}"
        );
    }
}

#[test]
fn e1_rejects_rehashed_context_and_mechanics_vocabulary_drift() {
    let bundle = ContractBundle::load_embedded().unwrap();
    let context = golden(1);
    for (field, value) in [
        ("source_qualification", json!("QUALIFIED")),
        ("attribution_state", json!("PROBABLY")),
        ("quality_state", json!("EXCELLENT")),
    ] {
        let mut changed = context.clone();
        changed[field] = value;
        changed = rehash(changed);
        assert!(matches!(
            bundle.validate_e1_json(&serde_json::to_vec(&changed).unwrap()),
            Err(ContractError::Semantic(_))
        ));
    }
    let mechanics = golden(0);
    for mutation in [
        (
            "features",
            json!([{"name":"unknown","value":"0.1","unit":"RATIO","horizon_ms":1000,"quality_state":"VALIDATED","reason_code":"OBSERVATION_VALID"}]),
        ),
        ("quality_flags", json!(["NOT_A_FLAG"])),
        ("expected_half_life_ms", json!(0)),
    ] {
        let mut changed = mechanics.clone();
        changed[mutation.0] = mutation.1;
        changed = rehash(changed);
        assert!(matches!(
            bundle.validate_e1_json(&serde_json::to_vec(&changed).unwrap()),
            Err(ContractError::Semantic(_))
        ));
    }
}

#[test]
fn canonical_decimal_and_bounded_identity_fail_closed() {
    assert!(CanonicalDecimal::parse("-0", usize::MAX, usize::MAX).is_err());
    assert!(CanonicalDecimal::parse("-0.000", usize::MAX, usize::MAX).is_err());
    assert!(
        CanonicalDecimal::parse("123456789012345678901234567890.1", usize::MAX, usize::MAX).is_ok()
    );
    assert!(ConnectionKeyV1::new(&"a".repeat(129)).is_ok());
    assert!(ConnectionKeyV1::new("1not_lower_leading").is_err());
    assert!(ConnectionKeyV1::new("Uppercase").is_err());
}

#[test]
fn pinned_schema_lexical_patterns_are_exact_not_e2_caps() {
    let bundle = ContractBundle::load_embedded().unwrap();

    let mut q1 = q1_golden(0);
    q1["causal_time"]["clock_quality"]["source_id"] = json!("Clock source 1");
    q1["causal_time"]["clock_quality"]["observed_skew_ms"] =
        json!(format!("1.{}", "2".repeat(128)));
    q1 = rehash(q1);
    bundle
        .validate_q1_json(&serde_json::to_vec(&q1).unwrap())
        .unwrap();

    let mut mechanics = golden(0);
    mechanics["scope"]["instrument"]["venue_symbol"] = json!("A".repeat(129));
    mechanics = rehash(mechanics);
    bundle
        .validate_e1_json(&serde_json::to_vec(&mechanics).unwrap())
        .unwrap();

    for (path, value) in [("name", json!("1invalid")), ("unit", json!("1INVALID"))] {
        let mut invalid = golden(0);
        invalid["features"][0][path] = value;
        invalid = rehash(invalid);
        assert!(
            bundle
                .validate_e1_json(&serde_json::to_vec(&invalid).unwrap())
                .is_err()
        );
    }

    for symbol in ["_BNBUSDC", "BNB.USDC"] {
        let mut invalid = golden(0);
        invalid["scope"]["instrument"]["venue_symbol"] = json!(symbol);
        invalid = rehash(invalid);
        assert!(
            bundle
                .validate_e1_json(&serde_json::to_vec(&invalid).unwrap())
                .is_err()
        );
    }
}

#[test]
fn e2_snapshot_profile_requires_exact_nine_rows_and_horizons() {
    let bundle = ContractBundle::load_embedded().unwrap();
    let golden_one = bundle
        .validate_e1_json(&serde_json::to_vec(&golden(0)).unwrap())
        .unwrap();
    assert!(marketfeed_event_pulse::validate_e2_mechanics_profile(&golden_one).is_err());

    let rows = [
        ("book_depth_10bps", 250, "USDC", "1"),
        ("cross_venue_breadth", 1000, "RATIO", "0.5"),
        ("cvd_slope", 1000, "BASE_PER_SECOND", "1"),
        ("liquidation_notional", 5000, "USDC", "1"),
        ("log_return", 1000, "LOG_RETURN", "0.1"),
        ("open_interest_change", 5000, "CONTRACTS", "1"),
        ("reversal_from_extreme", 5000, "RATIO", "0.5"),
        ("spread_bps", 250, "BPS", "1"),
        ("taker_imbalance", 1000, "RATIO", "0.1"),
    ];
    let mut payload = golden(0);
    payload["features"] = Value::Array(
        rows.into_iter()
            .map(|(name, horizon, unit, value)| {
                json!({"name":name,"horizon_ms":horizon,"unit":unit,"value":value,"quality_state":"VALIDATED","reason_code":"OBSERVATION_VALID"})
            })
            .collect(),
    );
    payload = rehash(payload);
    let accepted = bundle
        .validate_e1_json(&serde_json::to_vec(&payload).unwrap())
        .unwrap();
    marketfeed_event_pulse::validate_e2_mechanics_profile(&accepted).unwrap();
}

#[test]
fn system_input_is_mode_scope_target_and_reserved_drop_safe() {
    use marketfeed_event_pulse::wire::{
        ConfiguredTargetKeyV1, CursorModeV1, DropCategoryV1, FaultScopeKindV1, FaultScopeRefV1,
        FaultScopeV1, MechanicsInputRefV1, MechanicsInputV1, OpenInterestEncodingRefV1,
        OpenInterestEncodingV1, SystemFaultRefV1, SystemFaultV1, SystemSourceKeyV1, SystemSourceV1,
    };
    let key = SystemSourceKeyV1::new(
        "system_drop_source",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::processor("processor_one").unwrap(),
        CursorModeV1::Derived,
    )
    .unwrap();
    let source = SystemSourceV1::new(key, "epoch_one", 0).unwrap();
    let at = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let input = MechanicsInputV1::system(
        source.clone(),
        FaultScopeV1::processor("processor_one").unwrap(),
        at.clone(),
        at.clone(),
        CursorV1::derived_drop(1, 0).unwrap(),
        SystemFaultV1::events_dropped(1, DropCategoryV1::ActionBuffer).unwrap(),
        None,
    )
    .unwrap();
    input.validate_static().unwrap();
    match input.view() {
        MechanicsInputRefV1::System { scope, fault, .. } => {
            assert!(matches!(
                scope.view(),
                FaultScopeRefV1::Processor {
                    processor_id: "processor_one"
                }
            ));
            assert!(matches!(
                fault.view(),
                SystemFaultRefV1::EventsDropped {
                    count: 1,
                    category: DropCategoryV1::ActionBuffer
                }
            ));
        }
        _ => panic!("expected system view"),
    }
    assert!(matches!(
        OpenInterestEncodingV1::base("1.5").unwrap().view(),
        OpenInterestEncodingRefV1::Base { contracts_per_base }
            if contracts_per_base.as_str() == "1.5"
    ));
    assert!(
        MechanicsInputV1::system(
            source,
            FaultScopeV1::processor("processor_two").unwrap(),
            at.clone(),
            at,
            CursorV1::derived_drop(1, 1).unwrap(),
            SystemFaultV1::events_dropped(1, DropCategoryV1::ActionBuffer).unwrap(),
            None,
        )
        .is_err()
    );
}

#[test]
fn epin_system_payload_is_strict_hash_bound_and_tamper_evident() {
    use marketfeed_event_pulse::wire::{
        ConfiguredTargetKeyV1, CursorModeV1, DropCategoryV1, FaultScopeKindV1, FaultScopeV1,
        MechanicsInputV1, SystemFaultV1, SystemSourceKeyV1, SystemSourceV1,
    };
    let key = SystemSourceKeyV1::new(
        "system_drop_source",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::processor("processor_one").unwrap(),
        CursorModeV1::Derived,
    )
    .unwrap();
    let source = SystemSourceV1::new(key, "epoch_one", 0).unwrap();
    let at = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let input = MechanicsInputV1::system(
        source,
        FaultScopeV1::processor("processor_one").unwrap(),
        at.clone(),
        at,
        CursorV1::derived_drop(1, 0).unwrap(),
        SystemFaultV1::events_dropped(1, DropCategoryV1::ActionBuffer).unwrap(),
        None,
    )
    .unwrap();
    let value = serde_json::to_value(&input).unwrap();
    assert_eq!(input.payload_hash(), input.expected_payload_hash().unwrap());
    MechanicsInputV1::from_epin_json(&serde_json::to_vec(&value).unwrap()).unwrap();

    let mut unknown_top = value.clone();
    unknown_top["unknown"] = json!(true);
    assert!(MechanicsInputV1::from_epin_json(&serde_json::to_vec(&unknown_top).unwrap()).is_err());

    let mut unknown_nested = value.clone();
    unknown_nested["system_source"]["key"]["unknown"] = json!(true);
    unknown_nested = rehash_epin(unknown_nested);
    assert!(
        MechanicsInputV1::from_epin_json(&serde_json::to_vec(&unknown_nested).unwrap()).is_err()
    );

    let mut tampered = value.clone();
    tampered["occurred_at"] = json!("2026-08-21T09:59:59Z");
    assert!(MechanicsInputV1::from_epin_json(&serde_json::to_vec(&tampered).unwrap()).is_err());

    let repaired = rehash_epin(tampered);
    MechanicsInputV1::from_epin_json(&serde_json::to_vec(&repaired).unwrap()).unwrap();

    let mut wrong_target = value;
    wrong_target["scope"]["processor_id"] = json!("processor_two");
    wrong_target = rehash_epin(wrong_target);
    assert!(MechanicsInputV1::from_epin_json(&serde_json::to_vec(&wrong_target).unwrap()).is_err());
}

#[test]
fn epin_contributor_scope_roundtrips_through_strict_tagged_serde() {
    use marketfeed_event_pulse::wire::{
        ConfiguredTargetKeyV1, ContributorKeyV1, ContributorV1, CursorModeV1, FaultScopeKindV1,
        FaultScopeV1, InstrumentIdentityV1, MechanicsInputV1, SystemFaultV1, SystemSourceKeyV1,
        SystemSourceV1,
    };
    let contributor_key = ContributorKeyV1::new(
        "primary_source",
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap(),
    )
    .unwrap();
    let contributor = ContributorV1::new(contributor_key.clone(), "epoch_one", 0).unwrap();
    let source_key = SystemSourceKeyV1::new(
        "system_contributor",
        FaultScopeKindV1::Contributor,
        ConfiguredTargetKeyV1::contributor(contributor_key),
        CursorModeV1::Derived,
    )
    .unwrap();
    let source = SystemSourceV1::new(source_key, "epoch_system", 0).unwrap();
    let at = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let input = MechanicsInputV1::system(
        source,
        FaultScopeV1::contributor(contributor),
        at.clone(),
        at,
        CursorV1::derived(1, 0, 0).unwrap(),
        SystemFaultV1::sequence_gap(1, 2),
        None,
    )
    .unwrap();
    let bytes = serde_json::to_vec(&input).unwrap();
    MechanicsInputV1::from_epin_json(&bytes).unwrap();
}

#[test]
fn mechanics_config_enforces_roles_family_ownership_caps_and_targets() {
    use marketfeed_event_pulse::wire::{
        ClockSourceKeyV1, ConfiguredTargetKeyV1, ContributorKeyV1, ContributorRoleV1,
        ContributorSpecV1, CoverageSourceKeyV1, CursorModeV1, FamilyV1, FaultScopeKindV1,
        InstrumentIdentityV1, MechanicsConfigV1, SystemSourceKeyV1,
    };

    let instrument = || {
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap()
    };
    let primary_key = || ContributorKeyV1::new("primary_source", instrument()).unwrap();
    assert_eq!(
        serde_json::to_value(ContributorRoleV1::Primary).unwrap(),
        json!("PRIMARY")
    );
    assert!(
        ContributorSpecV1::new(
            primary_key(),
            ContributorRoleV1::Primary,
            [FamilyV1::Trade, FamilyV1::Quote]
        )
        .is_ok()
    );
    assert!(
        ContributorSpecV1::new(
            primary_key(),
            ContributorRoleV1::Primary,
            [
                FamilyV1::Trade,
                FamilyV1::Quote,
                FamilyV1::Book,
                FamilyV1::ConfirmationPrice,
            ]
        )
        .is_err()
    );
    assert!(
        ContributorSpecV1::new(
            primary_key(),
            ContributorRoleV1::Confirmation,
            [FamilyV1::Trade]
        )
        .is_err()
    );

    let make = |confirmation_count: usize,
                mismatched_confirmation: bool,
                wrong_system_target: bool| {
        let connection = ConnectionKeyV1::new("market_connection").unwrap();
        let primary = ContributorSpecV1::new(
            primary_key(),
            ContributorRoleV1::Primary,
            [FamilyV1::Trade, FamilyV1::Quote, FamilyV1::Book],
        )
        .unwrap();
        let mut contributors = vec![primary];
        for index in 0..confirmation_count {
            let confirmation_instrument = InstrumentIdentityV1::new(
                if mismatched_confirmation {
                    "ETH"
                } else {
                    "BNB"
                },
                "USDC",
                "PERPETUAL",
                "BINANCE",
                "BNB-USDC",
            )
            .unwrap();
            let key =
                ContributorKeyV1::new(&format!("confirmation_{index}"), confirmation_instrument)
                    .unwrap();
            contributors.push(
                ContributorSpecV1::new(
                    key,
                    ContributorRoleV1::Confirmation,
                    [FamilyV1::ConfirmationPrice],
                )
                .unwrap(),
            );
        }
        let contributor_connections = contributors
            .iter()
            .map(|spec| (spec.key().clone(), connection.clone()))
            .collect::<BTreeMap<_, _>>();
        let clock_sources = contributors
            .iter()
            .enumerate()
            .map(|(index, spec)| {
                ClockSourceKeyV1::new(&format!("clock_{index}"), spec.key().clone()).unwrap()
            })
            .collect();
        let coverage_sources = contributors
            .iter()
            .enumerate()
            .flat_map(|(index, spec)| {
                spec.allowed_families()
                    .iter()
                    .enumerate()
                    .map(move |(family_index, family)| {
                        CoverageSourceKeyV1::new(
                            &format!("coverage_{index}_{family_index}"),
                            spec.key().clone(),
                            *family,
                        )
                        .unwrap()
                    })
            })
            .collect();
        let system_sources = if wrong_system_target {
            vec![
                SystemSourceKeyV1::new(
                    "system_processor",
                    FaultScopeKindV1::Processor,
                    ConfiguredTargetKeyV1::processor("another_processor").unwrap(),
                    CursorModeV1::Derived,
                )
                .unwrap(),
            ]
        } else {
            Vec::new()
        };
        MechanicsConfigV1::new(
            "processor_one",
            vec![connection],
            contributors,
            contributor_connections,
            clock_sources,
            coverage_sources,
            system_sources,
        )
    };

    let config = make(0, false, false).unwrap();
    assert_eq!(config.processor_id(), "processor_one");
    assert_eq!(config.connections().len(), 1);
    assert_eq!(config.contributor_connections().len(), 1);
    assert_eq!(config.clock_sources().len(), 1);
    assert_eq!(config.coverage_sources().len(), 3);
    assert!(config.system_sources().is_empty());
    make(1, false, false).unwrap();
    assert!(make(2, false, false).is_err());
    assert!(make(16, false, false).is_err());
    assert!(make(1, true, false).is_err());
    assert!(make(0, false, true).is_err());

    let partitioned = |family_sets: Vec<Vec<FamilyV1>>, mismatch_second: bool| {
        let connection = ConnectionKeyV1::new("partition_connection").unwrap();
        let contributors = family_sets
            .into_iter()
            .enumerate()
            .map(|(index, families)| {
                let identity = if mismatch_second && index == 1 {
                    InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "BINANCE", "BNB-USDC")
                        .unwrap()
                } else {
                    instrument()
                };
                ContributorSpecV1::new(
                    ContributorKeyV1::new(&format!("partition_{index}"), identity).unwrap(),
                    ContributorRoleV1::Primary,
                    families,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let contributor_connections = contributors
            .iter()
            .map(|spec| (spec.key().clone(), connection.clone()))
            .collect::<BTreeMap<_, _>>();
        let clocks = contributors
            .iter()
            .enumerate()
            .map(|(index, spec)| {
                ClockSourceKeyV1::new(&format!("partition_clock_{index}"), spec.key().clone())
                    .unwrap()
            })
            .collect();
        let coverage = contributors
            .iter()
            .enumerate()
            .flat_map(|(index, spec)| {
                spec.allowed_families()
                    .iter()
                    .enumerate()
                    .map(move |(family_index, family)| {
                        CoverageSourceKeyV1::new(
                            &format!("partition_coverage_{index}_{family_index}"),
                            spec.key().clone(),
                            *family,
                        )
                        .unwrap()
                    })
            })
            .collect();
        MechanicsConfigV1::new(
            "processor_one",
            vec![connection],
            contributors,
            contributor_connections,
            clocks,
            coverage,
            Vec::new(),
        )
    };
    partitioned(
        vec![
            vec![FamilyV1::Trade],
            vec![FamilyV1::Quote],
            vec![FamilyV1::Book],
        ],
        false,
    )
    .unwrap();
    assert!(
        partitioned(
            vec![
                vec![FamilyV1::Trade],
                vec![FamilyV1::Trade, FamilyV1::Quote],
                vec![FamilyV1::Book],
            ],
            false,
        )
        .is_err()
    );
    assert!(
        partitioned(
            vec![
                vec![FamilyV1::Trade, FamilyV1::OpenInterest],
                vec![FamilyV1::Quote, FamilyV1::OpenInterest],
                vec![FamilyV1::Book],
            ],
            false,
        )
        .is_err()
    );
    assert!(
        partitioned(
            vec![
                vec![FamilyV1::Trade],
                vec![FamilyV1::Quote],
                vec![FamilyV1::Book],
            ],
            true,
        )
        .is_err()
    );
}

#[test]
fn market_epin_requires_exact_action_and_catalog_mapping() {
    use marketfeed_event_pulse::wire::{
        InstrumentIdentityV1, MechanicsInputV1, ReplayCatalogV1, ReplayEpochEntryV1,
        VenueCatalogEntryV1,
    };
    use marketfeed_model::{
        AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
        OpenInterest, Price, Quantity, SequenceRange, SessionId, TimestampNs, Trade, VenueId,
    };

    let venues = BTreeMap::from([
        (
            1,
            VenueCatalogEntryV1::new("BINANCE", "binance_source").unwrap(),
        ),
        (
            2,
            VenueCatalogEntryV1::new("HYPERLIQUID", "hyperliquid_source").unwrap(),
        ),
    ]);
    let instruments = BTreeMap::from([(
        7,
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap(),
    )]);
    let catalog = ReplayCatalogV1::new(
        venues,
        instruments,
        vec![ReplayEpochEntryV1::new(3, 4, "epoch_one", 0).unwrap()],
        BTreeMap::new(),
    )
    .unwrap();
    let trade = MarketEvent::Trade(Trade {
        price: Price(Fixed::new(100, 0)),
        quantity: Quantity(Fixed::new(1, 0)),
        aggressor: AggressorSide::Buy,
        trade_id: None,
    });
    let envelope = EventEnvelope {
        schema_version: 1,
        venue: VenueId(1),
        instrument: Some(InstrumentId(7)),
        connection: ConnectionId(3),
        session: SessionId(4),
        frame_seq: 1,
        event_index: 0,
        exchange_ts: Some(TimestampNs(1)),
        receive_ts: TimestampNs(2),
        source_sequence: None,
        flags: EventFlags::empty(),
        payload: trade,
    };
    assert!(MechanicsInputV1::market(envelope.clone(), 0, catalog.clone()).is_err());

    let mut correct = envelope.clone();
    correct.venue = VenueId(2);
    MechanicsInputV1::market(correct.clone(), 0, catalog.clone()).unwrap();

    correct.exchange_ts = None;
    assert!(MechanicsInputV1::market(correct.clone(), 0, catalog.clone()).is_err());
    correct.exchange_ts = Some(TimestampNs(1));
    correct.source_sequence = Some(SequenceRange {
        first: 1,
        last: u64::MAX,
    });
    assert!(MechanicsInputV1::market(correct.clone(), 0, catalog.clone()).is_err());

    correct.source_sequence = None;
    correct.payload = MarketEvent::OpenInterest(OpenInterest {
        quantity: Quantity(Fixed::new(1, 0)),
    });
    assert!(MechanicsInputV1::market(correct.clone(), 0, catalog.clone()).is_err());

    let mut overflow = envelope;
    overflow.venue = VenueId(2);
    overflow.frame_seq = u64::MAX;
    overflow.source_sequence = None;
    assert!(MechanicsInputV1::market(overflow, 0, catalog.clone()).is_err());

    let mut valid = correct;
    valid.payload = MarketEvent::Trade(Trade {
        price: Price(Fixed::new(100, 0)),
        quantity: Quantity(Fixed::new(1, 0)),
        aggressor: AggressorSide::Buy,
        trade_id: None,
    });
    valid.source_sequence = None;
    let mut native = valid.clone();
    native.source_sequence = Some(SequenceRange { first: 1, last: 1 });
    let native_max = MechanicsInputV1::market(native.clone(), 65_534, catalog.clone()).unwrap();
    assert!(MechanicsInputV1::market(native.clone(), 65_535, catalog.clone()).is_err());
    assert!(MechanicsInputV1::market(native, u32::MAX, catalog.clone()).is_err());

    for invalid_action in [65_535, u32::MAX] {
        let mut rehashed_native = serde_json::to_value(native_max.clone()).unwrap();
        rehashed_native["action_index"] = json!(invalid_action);
        rehashed_native = rehash_epin(rehashed_native);
        assert!(
            MechanicsInputV1::from_epin_json(&serde_json::to_vec(&rehashed_native).unwrap())
                .is_err()
        );
    }

    let authored = MechanicsInputV1::market(valid, 0, catalog).unwrap();
    let canonical = serde_json::to_vec(&authored).unwrap();
    assert_eq!(
        MechanicsInputV1::from_epin_json(&canonical).unwrap(),
        authored,
        "an authored MARKET record must survive its strict canonical EPIN boundary"
    );

    let authored_value = serde_json::to_value(&authored).unwrap();
    for invalid_key in ["07", "+7", "4294967296"] {
        let mut invalid = authored_value.clone();
        let instrument = invalid["catalog"]["instruments"]
            .as_object_mut()
            .unwrap()
            .remove("7")
            .unwrap();
        invalid["catalog"]["instruments"]
            .as_object_mut()
            .unwrap()
            .insert(invalid_key.to_owned(), instrument);
        invalid = rehash_epin(invalid);
        assert!(MechanicsInputV1::from_epin_json(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }

    let mut rehashed_overflow = serde_json::to_value(authored).unwrap();
    rehashed_overflow["envelope"]["frame_seq"] = json!(u64::MAX);
    rehashed_overflow = rehash_epin(rehashed_overflow);
    assert!(
        MechanicsInputV1::from_epin_json(&serde_json::to_vec(&rehashed_overflow).unwrap()).is_err()
    );
}

#[test]
fn checked_identity_deserialization_and_replay_bounds_cannot_be_bypassed() {
    use marketfeed_event_pulse::wire::{
        ContributorV1, InstrumentIdentityV1, OpenInterestEncodingV1, ReplayCatalogV1,
        SnapshotAuthoringV1,
    };

    assert!(serde_json::from_value::<ConnectionKeyV1>(json!("1bad")).is_err());
    assert!(
        serde_json::from_value::<InstrumentIdentityV1>(json!({
            "base_asset":"DOGE",
            "quote_asset":"USDC",
            "market_type":"PERPETUAL",
            "venue":"HYPERLIQUID",
            "venue_symbol":"DOGE-USDC"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<ContributorV1>(json!({
            "key": {
                "source_id":"source_one",
                "instrument": {
                    "base_asset":"BNB",
                    "quote_asset":"USDC",
                    "market_type":"PERPETUAL",
                    "venue":"HYPERLIQUID",
                    "venue_symbol":"BNB-USDC"
                }
            },
            "connection_epoch":"epoch_",
            "epoch_generation":0
        }))
        .is_err()
    );
    assert!(OpenInterestEncodingV1::base("0.0").is_err());

    let instrument =
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap();
    assert!(
        SnapshotAuthoringV1::new(
            "event_pulse_mechanics_",
            "lineage_one",
            "event_cluster_one",
            instrument.clone(),
            1,
            None,
            15_000,
            "v1"
        )
        .is_err()
    );
    let snapshot = SnapshotAuthoringV1::new(
        "event_pulse_mechanics_one",
        "lineage_one",
        "event_cluster_one",
        instrument,
        1,
        None,
        15_000,
        "v1",
    )
    .unwrap();
    assert_eq!(snapshot.contract_id(), "event_pulse_mechanics_one");
    assert_eq!(snapshot.lineage_id(), "lineage_one");
    assert_eq!(snapshot.event_cluster_id(), "event_cluster_one");
    assert_eq!(snapshot.revision_start(), 1);
    assert_eq!(snapshot.expected_half_life_ms(), 15_000);
    assert_eq!(snapshot.producer_version(), "v1");
    assert!(snapshot.predecessor_content_hash().is_none());

    let too_many_venues = (0..33)
        .map(|id| {
            (
                id.to_string(),
                json!({"venue":"BINANCE","source_id":format!("source_{id}")}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let oversized = json!({
        "venue_sources": too_many_venues,
        "instruments": {"1": {
            "base_asset":"BNB",
            "quote_asset":"USDC",
            "market_type":"PERPETUAL",
            "venue":"BINANCE",
            "venue_symbol":"BNB-USDC"
        }},
        "connection_epochs":[{
            "connection_id":1,
            "session_id":1,
            "connection_epoch":"epoch_one",
            "epoch_generation":0
        }],
        "open_interest":{}
    });
    assert!(serde_json::from_value::<ReplayCatalogV1>(oversized).is_err());
}

#[test]
fn clock_and_coverage_cursors_are_native_only_on_direct_and_epin_deserialization() {
    use marketfeed_event_pulse::wire::{
        ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1, ClockStateV1,
        ContributorKeyV1, ContributorV1, CoverageCursorV1, CoverageSourceKeyV1, CoverageSourceV1,
        FamilyV1, InstrumentIdentityV1, MechanicsInputV1,
    };
    let derived = json!({
        "kind":"DERIVED_ACTION",
        "frame_ordinal":1,
        "action_index":0,
        "item_index":0
    });
    assert!(serde_json::from_value::<ClockCursorV1>(derived.clone()).is_err());
    assert!(serde_json::from_value::<CoverageCursorV1>(derived.clone()).is_err());

    let key = ContributorKeyV1::new(
        "primary_source",
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap(),
    )
    .unwrap();
    let contributor = ContributorV1::new(key.clone(), "epoch_one", 0).unwrap();
    let at = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let clock_source = ClockSourceV1::new(
        ClockSourceKeyV1::new("clock_source", key.clone()).unwrap(),
        "epoch_clock",
        0,
    )
    .unwrap();
    let clock = MechanicsInputV1::clock(
        contributor.clone(),
        clock_source,
        at.clone(),
        at.clone(),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        CanonicalDecimal::parse_unbounded("1.0").unwrap(),
        1_000,
        ClockQualityV1::Validated,
        "CLOCK_VALID",
    )
    .unwrap();
    let mut clock_wire = serde_json::to_value(clock).unwrap();
    clock_wire["clock_cursor"] = derived.clone();
    clock_wire = rehash_epin(clock_wire);
    assert!(MechanicsInputV1::from_epin_json(&serde_json::to_vec(&clock_wire).unwrap()).is_err());

    let coverage_source = CoverageSourceV1::new(
        CoverageSourceKeyV1::new("coverage_source", key, FamilyV1::Trade).unwrap(),
        "epoch_coverage",
        0,
    )
    .unwrap();
    let coverage = MechanicsInputV1::coverage(
        contributor,
        coverage_source,
        FamilyV1::Trade,
        at.clone(),
        at.clone(),
        at,
        CoverageCursorV1::native(1, 1).unwrap(),
    )
    .unwrap();
    let mut coverage_wire = serde_json::to_value(coverage).unwrap();
    coverage_wire["coverage_cursor"] = derived;
    coverage_wire = rehash_epin(coverage_wire);
    assert!(
        MechanicsInputV1::from_epin_json(&serde_json::to_vec(&coverage_wire).unwrap()).is_err()
    );
}

#[test]
fn epin_reader_requires_exact_canonical_bytes_and_rejects_duplicate_keys() {
    use marketfeed_event_pulse::wire::{
        ConfiguredTargetKeyV1, CursorModeV1, DropCategoryV1, FaultScopeKindV1, FaultScopeV1,
        MechanicsInputV1, SystemFaultV1, SystemSourceKeyV1, SystemSourceV1,
    };
    let source = SystemSourceV1::new(
        SystemSourceKeyV1::new(
            "system_drop_source",
            FaultScopeKindV1::Processor,
            ConfiguredTargetKeyV1::processor("processor_one").unwrap(),
            CursorModeV1::Derived,
        )
        .unwrap(),
        "epoch_one",
        0,
    )
    .unwrap();
    let at = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let input = MechanicsInputV1::system(
        source,
        FaultScopeV1::processor("processor_one").unwrap(),
        at.clone(),
        at,
        CursorV1::derived_drop(1, 0).unwrap(),
        SystemFaultV1::events_dropped(1, DropCategoryV1::ActionBuffer).unwrap(),
        None,
    )
    .unwrap();
    let canonical = serde_json::to_vec(&input).unwrap();
    assert!(canonical.starts_with(br#"{"available_at":"#));
    MechanicsInputV1::from_epin_json(&canonical).unwrap();

    let pretty = serde_json::to_vec_pretty(&input).unwrap();
    assert!(MechanicsInputV1::from_epin_json(&pretty).is_err());

    let value = serde_json::to_value(&input).unwrap();
    let reordered = format!(
        "{{{}}}",
        value
            .as_object()
            .unwrap()
            .iter()
            .rev()
            .map(|(key, value)| format!(
                "{}:{}",
                serde_json::to_string(key).unwrap(),
                serde_json::to_string(value).unwrap()
            ))
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(MechanicsInputV1::from_epin_json(reordered.as_bytes()).is_err());

    let duplicate = String::from_utf8(canonical.clone())
        .unwrap()
        .strip_suffix('}')
        .map(|prefix| format!("{prefix},\"payload_hash\":\"{}\"}}", input.payload_hash()))
        .unwrap();
    assert!(MechanicsInputV1::from_epin_json(duplicate.as_bytes()).is_err());

    let mut noncanonical_time = value;
    noncanonical_time["occurred_at"] = json!("2026-08-21T10:00:00.000000+00:00");
    assert!(
        MechanicsInputV1::from_epin_json(&serde_json::to_vec(&noncanonical_time).unwrap()).is_err()
    );
}

#[test]
fn authoring_rejects_final_epin_above_sixteen_mib() {
    use marketfeed_event_pulse::wire::{
        ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1, ClockStateV1,
        ContributorKeyV1, ContributorV1, InstrumentIdentityV1, MAX_INPUT_BYTES, MechanicsInputV1,
    };
    let key = ContributorKeyV1::new(
        "primary_source",
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap(),
    )
    .unwrap();
    let contributor = ContributorV1::new(key.clone(), "epoch_one", 0).unwrap();
    let source = ClockSourceV1::new(
        ClockSourceKeyV1::new("clock_source", key).unwrap(),
        "epoch_clock",
        0,
    )
    .unwrap();
    let at = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    assert!(
        MechanicsInputV1::clock(
            contributor,
            source,
            at.clone(),
            at,
            ClockCursorV1::native(1, 1).unwrap(),
            ClockStateV1::Synchronized,
            CanonicalDecimal::parse_unbounded("1").unwrap(),
            1_000,
            ClockQualityV1::Validated,
            &"A".repeat(MAX_INPUT_BYTES),
        )
        .is_err()
    );
}

#[test]
fn q1_and_e1_ids_require_alphanumeric_suffix_starts_after_prefix() {
    let bundle = ContractBundle::load_embedded().unwrap();
    for (mut payload, field, invalid) in [
        (q1_golden(0), "contract_id", "evidence_-bad"),
        (q1_golden(0), "lineage_id", "lineage__bad"),
        (golden(0), "contract_id", "event_pulse_mechanics_-bad"),
        (golden(0), "event_cluster_id", "event_cluster__bad"),
    ] {
        payload[field] = json!(invalid);
        payload = rehash(payload);
        assert!(
            bundle
                .validate_json(&serde_json::to_vec(&payload).unwrap())
                .is_err()
        );
    }
}

#[test]
fn binder_rejects_substituted_mechanics_and_accepts_exact_golden_pair() {
    let bundle = ContractBundle::load_embedded().unwrap();
    let mechanics = bundle
        .validate_e1_json(&serde_json::to_vec(&golden(0)).unwrap())
        .unwrap();
    let context = bundle
        .validate_e1_json(&serde_json::to_vec(&golden(1)).unwrap())
        .unwrap();
    let composite = bundle
        .validate_e1_json(&serde_json::to_vec(&golden(2)).unwrap())
        .unwrap();
    bundle
        .bind_composite(&mechanics, Some(&context), &composite)
        .unwrap();
    let mut altered = golden(2);
    altered["mechanics_content_hash"] = json!("00".repeat(32));
    altered["content_hash"] = json!(marketfeed_event_pulse::try_content_hash(&altered).unwrap());
    let altered = bundle
        .validate_e1_json(&serde_json::to_vec(&altered).unwrap())
        .unwrap();
    assert!(matches!(
        bundle.bind_composite(&mechanics, Some(&context), &altered),
        Err(ContractError::EventPulse(
            marketfeed_event_pulse::EventPulseErrorCode::HashBinding
        ))
    ));
}

#[test]
fn binder_accepts_explicit_null_context_triplet() {
    let bundle = ContractBundle::load_embedded().unwrap();
    let mechanics = bundle
        .validate_e1_json(&serde_json::to_vec(&golden(0)).unwrap())
        .unwrap();
    let mut composite = golden(2);
    composite["context_content_hash"] = Value::Null;
    composite["context_lineage_id"] = Value::Null;
    composite["catalyst_confidence"] = Value::Null;
    composite = rehash(composite);
    let composite = bundle
        .validate_e1_json(&serde_json::to_vec(&composite).unwrap())
        .unwrap();
    bundle.bind_composite(&mechanics, None, &composite).unwrap();
}

#[test]
fn nested_scope_and_enum_drift_reject_with_a_stable_semantic_error() {
    let bundle = ContractBundle::load_embedded().unwrap();
    let mut invalid = golden(0);
    invalid["scope"] = json!({"kind":"PAIR","asset":"BNB","venue":"HYPERLIQUID"});
    invalid["content_hash"] = json!(marketfeed_event_pulse::try_content_hash(&invalid).unwrap());
    assert!(matches!(
        bundle.validate_e1_json(&serde_json::to_vec(&invalid).unwrap()),
        Err(ContractError::Structure("required field missing"))
    ));
    let mut invalid = golden(0);
    invalid["direction"] = json!("SIDEWAYS");
    invalid["content_hash"] = json!(marketfeed_event_pulse::try_content_hash(&invalid).unwrap());
    assert!(matches!(
        bundle.validate_e1_json(&serde_json::to_vec(&invalid).unwrap()),
        Err(ContractError::Semantic("invalid mechanics enum"))
    ));
}
