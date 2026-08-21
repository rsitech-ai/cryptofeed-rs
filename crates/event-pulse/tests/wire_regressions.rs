use marketfeed_event_pulse::{
    ContractBundle, ContractError,
    wire::{CanonicalDecimal, ConnectionKeyV1, CursorV1, Rfc3339Time, SystemChainPreimage},
};
use serde_json::{Value, json};

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
    assert!(ConnectionKeyV1::new(&"a".repeat(129)).is_err());
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
        ConfiguredTargetKeyV1, CursorModeV1, DropCategoryV1, FaultScopeKindV1, FaultScopeV1,
        MechanicsInputV1, SystemFaultV1, SystemSourceKeyV1, SystemSourceV1,
    };
    let key = SystemSourceKeyV1::new(
        "system_drop_source",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::Processor("processor_one".into()),
        CursorModeV1::Derived,
    )
    .unwrap();
    let source = SystemSourceV1::new(key, "epoch_one", 0).unwrap();
    let at = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let input = MechanicsInputV1::System {
        system_source: source.clone(),
        scope: FaultScopeV1::Processor {
            processor_id: "processor_one".into(),
        },
        occurred_at: at.clone(),
        available_at: at.clone(),
        system_cursor: CursorV1::derived_drop(1, 0).unwrap(),
        fault: SystemFaultV1::EventsDropped {
            count: 1,
            category: DropCategoryV1::ActionBuffer,
        },
        predecessor_system_chain_hash: None,
        payload_hash: "aa".repeat(32),
    };
    input.validate_static().unwrap();
    let wrong_target = MechanicsInputV1::System {
        system_source: source,
        scope: FaultScopeV1::Processor {
            processor_id: "processor_two".into(),
        },
        occurred_at: at.clone(),
        available_at: at,
        system_cursor: CursorV1::derived_drop(1, 1).unwrap(),
        fault: SystemFaultV1::EventsDropped {
            count: 1,
            category: DropCategoryV1::ActionBuffer,
        },
        predecessor_system_chain_hash: None,
        payload_hash: "aa".repeat(32),
    };
    assert!(wrong_target.validate_static().is_err());
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
        Err(ContractError::HashBinding)
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
