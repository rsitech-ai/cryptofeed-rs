use marketfeed_event_pulse::{ProspectiveAdmissionError, ProspectiveCaptureAdmissionV1};
use serde_json::{Value, json};

fn sha(byte: char, len: usize) -> String {
    std::iter::repeat_n(byte, len).collect()
}

fn binding(source_id: &str, venue: &str, format: &str, blob: char, roles: &[&str]) -> Value {
    json!({
        "source_id": source_id,
        "venue": venue,
        "format": format,
        "instrument": {
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "market_type": "PERPETUAL"
        },
        "roles": roles,
        "families": if venue == "HYPERLIQUID" {
            json!(["CONFIRMATION_PRICE"])
        } else {
            json!(["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"])
        },
        "public_read_only": true,
        "repository_url": "https://github.com/rsitech-ai/cryptofeed-rs",
        "producer_commit": sha('a', 40),
        "producer_path": format!("crates/event-pulse-capture/src/{source_id}.rs"),
        "producer_blob_sha256": sha(blob, 64)
    })
}

fn valid_request() -> Value {
    json!({
        "schema": "event-pulse-e2-prospective-admission/1.0",
        "root_amendment_commit": "24b51a58c670ab722538bec4a3e1def0278b1107",
        "root_default_reachable_at": "2026-08-22T07:35:52Z",
        "capture_starts_at": "2026-08-22T07:35:52.000001Z",
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "required_roles": [
            "TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION",
            "CONFIRMATION", "CLOCK", "COVERAGE", "SYSTEM"
        ],
        "primary": binding(
            "binance_primary", "BINANCE", "MFR1", 'b',
            &["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"]
        ),
        "confirmation": binding(
            "hyperliquid_confirmation", "HYPERLIQUID", "MFR1", 'c',
            &["CONFIRMATION"]
        ),
        "clocks": [
            clock("primary_clock", "binance_primary", 'e'),
            clock("confirmation_clock", "hyperliquid_confirmation", 'f')
        ],
        "coverage": [
            coverage("primary_trade_coverage", "binance_primary", "TRADE", '1'),
            coverage("primary_quote_coverage", "binance_primary", "QUOTE", '2'),
            coverage("primary_book_coverage", "binance_primary", "BOOK", '3'),
            coverage("primary_oi_coverage", "binance_primary", "OPEN_INTEREST", '4'),
            coverage("primary_liq_coverage", "binance_primary", "LIQUIDATION", '5'),
            coverage(
                "confirmation_price_coverage",
                "hyperliquid_confirmation",
                "CONFIRMATION_PRICE",
                '6'
            )
        ],
        "system": {
            "source_id": "capture_system",
            "processor_id": "event_pulse_e2_prospective",
            "target": "PROCESSOR",
            "fault_scope": "PROCESSOR",
            "cursor_mode": "DERIVED",
            "evidence_kind": "STABLE_SYSTEM_FAULT_MAPPING",
            "producer_commit": sha('7', 40),
            "producer_path": "crates/event-pulse-capture/src/system.rs",
            "producer_blob_sha256": sha('a', 64)
        },
        "authority": {
            "credentials_allowed": false,
            "private_endpoints_allowed": false,
            "orders_allowed": false,
            "execution_authority": false,
            "paper_authority": false,
            "promotion_authority": false
        }
    })
}

fn clock(source_id: &str, subject_source_id: &str, blob: char) -> Value {
    json!({
        "source_id": source_id,
        "subject_source_id": subject_source_id,
        "evidence_kind": "UTC_MONOTONIC_OBSERVATION",
        "derivation": "INDEPENDENT_SIDECAR",
        "producer_commit": sha('d', 40),
        "producer_path": format!("crates/event-pulse-capture/src/{source_id}.rs"),
        "producer_blob_sha256": sha(blob, 64)
    })
}

fn coverage(source_id: &str, subject_source_id: &str, family: &str, blob: char) -> Value {
    json!({
        "source_id": source_id,
        "subject_source_id": subject_source_id,
        "family": family,
        "evidence_kind": "EXPLICIT_HEARTBEAT_RANGE",
        "derivation": "INDEPENDENT_SIDECAR",
        "producer_commit": sha('8', 40),
        "producer_path": format!("crates/event-pulse-capture/src/{source_id}.rs"),
        "producer_blob_sha256": sha(blob, 64)
    })
}

fn parse(value: &Value) -> Result<ProspectiveCaptureAdmissionV1, ProspectiveAdmissionError> {
    ProspectiveCaptureAdmissionV1::from_json(&serde_json::to_vec(value).unwrap())
}

#[test]
fn admits_only_the_exact_truthful_nine_role_source_topology() {
    let admission = parse(&valid_request()).unwrap();
    assert_eq!(admission.primary_venue(), "BINANCE");
    assert_eq!(admission.confirmation_venue(), "HYPERLIQUID");
    assert_eq!(admission.required_role_count(), 9);
    assert!(!admission.evidence_authoring_allowed());
    assert_eq!(admission.blocker(), "blocked:fixture-provenance");
}

#[test]
fn capture_must_begin_strictly_after_default_reachability() {
    for start in [
        "2026-08-22T07:35:51.999999Z",
        "2026-08-22T07:35:52Z",
        "2026-08-22T08:35:52.000001+01:00",
        "2026-08-22T07:35:52.0000001Z",
    ] {
        let mut value = valid_request();
        value["capture_starts_at"] = json!(start);
        assert_eq!(parse(&value), Err(ProspectiveAdmissionError::CaptureTiming));
    }
}

#[test]
fn rejects_false_market_and_confirmation_shortcuts() {
    for (pointer, replacement, expected) in [
        (
            "/primary/format",
            json!("MFPE_JSON1"),
            ProspectiveAdmissionError::PrimarySource,
        ),
        (
            "/confirmation/venue",
            json!("BINANCE"),
            ProspectiveAdmissionError::ConfirmationSource,
        ),
        (
            "/confirmation/venue",
            json!("OKX"),
            ProspectiveAdmissionError::ConfirmationSource,
        ),
        (
            "/confirmation/venue",
            json!("KRAKEN"),
            ProspectiveAdmissionError::ConfirmationSource,
        ),
        (
            "/confirmation/format",
            json!("MFNE_JSON1"),
            ProspectiveAdmissionError::ConfirmationSource,
        ),
    ] {
        let mut value = valid_request();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert_eq!(parse(&value), Err(expected));
    }
}

#[test]
fn rejects_inferred_clock_coverage_or_missing_system_mapping() {
    for (pointer, replacement, expected) in [
        (
            "/clocks/0/derivation",
            json!("MARKET_TIMESTAMPS"),
            ProspectiveAdmissionError::ClockEvidence,
        ),
        (
            "/coverage/0/derivation",
            json!("NO_GAP_OBSERVED"),
            ProspectiveAdmissionError::CoverageEvidence,
        ),
        (
            "/system/evidence_kind",
            json!("NONE"),
            ProspectiveAdmissionError::SystemEvidence,
        ),
    ] {
        let mut value = valid_request();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert_eq!(parse(&value), Err(expected));
    }
}

#[test]
fn rejects_mutable_or_unbound_sources_and_authority_escalation() {
    let mut mutable = valid_request();
    mutable["primary"]["producer_commit"] = json!("main");
    assert_eq!(
        parse(&mutable),
        Err(ProspectiveAdmissionError::SourceBinding)
    );

    let mut blank_hash = valid_request();
    blank_hash["coverage"][0]["producer_blob_sha256"] = json!("");
    assert_eq!(
        parse(&blank_hash),
        Err(ProspectiveAdmissionError::SourceBinding)
    );

    for key in [
        "credentials_allowed",
        "private_endpoints_allowed",
        "orders_allowed",
        "execution_authority",
        "paper_authority",
        "promotion_authority",
    ] {
        let mut value = valid_request();
        value["authority"][key] = json!(true);
        assert_eq!(parse(&value), Err(ProspectiveAdmissionError::Authority));
    }
}

#[test]
fn rejects_instrument_role_and_source_independence_drift() {
    let mut instrument = valid_request();
    instrument["confirmation"]["instrument"]["quote_asset"] = json!("USD");
    assert_eq!(
        parse(&instrument),
        Err(ProspectiveAdmissionError::ConfirmationSource)
    );

    let mut roles = valid_request();
    roles["primary"]["roles"] = json!(["TRADE", "QUOTE", "BOOK"]);
    assert_eq!(parse(&roles), Err(ProspectiveAdmissionError::PrimarySource));

    let mut confirmation_family = valid_request();
    confirmation_family["confirmation"]["families"] = json!(["TRADE"]);
    assert_eq!(
        parse(&confirmation_family),
        Err(ProspectiveAdmissionError::ConfirmationSource)
    );

    for (pointer, expected) in [
        (
            "/confirmation/source_id",
            ProspectiveAdmissionError::ClockEvidence,
        ),
        (
            "/clocks/0/source_id",
            ProspectiveAdmissionError::SourceBinding,
        ),
        (
            "/coverage/0/source_id",
            ProspectiveAdmissionError::SourceBinding,
        ),
        (
            "/system/source_id",
            ProspectiveAdmissionError::SourceBinding,
        ),
    ] {
        let mut value = valid_request();
        *value.pointer_mut(pointer).unwrap() = json!("binance_primary");
        assert_eq!(parse(&value), Err(expected));
    }

    let mut shared_blob = valid_request();
    shared_blob["clocks"][0]["producer_blob_sha256"] =
        shared_blob["primary"]["producer_blob_sha256"].clone();
    assert_eq!(
        parse(&shared_blob),
        Err(ProspectiveAdmissionError::SourceBinding)
    );

    let mut escaped_path = valid_request();
    escaped_path["system"]["producer_path"] = json!("../system.rs");
    assert_eq!(
        parse(&escaped_path),
        Err(ProspectiveAdmissionError::SourceBinding)
    );
}

#[test]
fn requires_replayable_clock_coverage_and_system_topology() {
    let mut missing_clock = valid_request();
    missing_clock["clocks"].as_array_mut().unwrap().pop();
    assert_eq!(
        parse(&missing_clock),
        Err(ProspectiveAdmissionError::ClockEvidence)
    );

    let mut missing_coverage = valid_request();
    missing_coverage["coverage"].as_array_mut().unwrap().pop();
    assert_eq!(
        parse(&missing_coverage),
        Err(ProspectiveAdmissionError::CoverageEvidence)
    );

    let mut wrong_family = valid_request();
    wrong_family["coverage"][5]["family"] = json!("TRADE");
    assert_eq!(
        parse(&wrong_family),
        Err(ProspectiveAdmissionError::CoverageEvidence)
    );

    for (field, value) in [
        ("processor_id", json!("another_processor")),
        ("fault_scope", json!("CONTRIBUTOR")),
        ("cursor_mode", json!("NATIVE")),
    ] {
        let mut system = valid_request();
        system["system"][field] = value;
        assert_eq!(
            parse(&system),
            Err(ProspectiveAdmissionError::SystemEvidence)
        );
    }
}

#[test]
fn rejects_schema_role_root_and_unknown_field_drift() {
    let mut roles = valid_request();
    roles["required_roles"].as_array_mut().unwrap().swap(0, 1);
    assert_eq!(parse(&roles), Err(ProspectiveAdmissionError::Roles));

    let mut root = valid_request();
    root["root_amendment_commit"] = json!(sha('9', 40));
    assert_eq!(parse(&root), Err(ProspectiveAdmissionError::RootBinding));

    let mut unknown = valid_request();
    unknown["unexpected"] = json!(false);
    assert_eq!(parse(&unknown), Err(ProspectiveAdmissionError::Shape));

    let mut nested_unknown = valid_request();
    nested_unknown["clocks"][0]["market_timestamp_fallback"] = json!(false);
    assert_eq!(
        parse(&nested_unknown),
        Err(ProspectiveAdmissionError::Shape)
    );

    let mut uppercase_hash = valid_request();
    uppercase_hash["system"]["producer_blob_sha256"] = json!(sha('A', 64));
    assert_eq!(
        parse(&uppercase_hash),
        Err(ProspectiveAdmissionError::SourceBinding)
    );
}
