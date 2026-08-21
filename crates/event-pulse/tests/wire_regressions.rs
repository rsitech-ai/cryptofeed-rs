use marketfeed_event_pulse::{
    ContractBundle, ContractError,
    wire::{CursorV1, Rfc3339Time, SystemChainPreimage},
};
use serde_json::{Value, json};

fn golden(index: usize) -> Value {
    let suite: Value = serde_json::from_slice(include_bytes!(
        "../contracts/event-pulse/event_pulse_v1_golden.json"
    ))
    .unwrap();
    suite["vectors"][index]["payload"].clone()
}

#[test]
fn rfc3339_preserves_instant_but_emits_canonical_zero_or_six_fraction() {
    let utc = Rfc3339Time::parse("2026-08-21T10:00:00Z").unwrap();
    let offset = Rfc3339Time::parse("2026-08-21T12:00:00.123456789+02:00").unwrap();
    let same = Rfc3339Time::parse("2026-08-21T10:00:00.123456Z").unwrap();
    assert_eq!(utc.canonical(), "2026-08-21T10:00:00Z");
    assert_eq!(offset.canonical(), "2026-08-21T12:00:00.123456+02:00");
    assert_eq!(offset.utc_micros(), same.utc_micros());
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
