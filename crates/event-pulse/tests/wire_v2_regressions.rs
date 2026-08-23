use marketfeed_event_pulse::{
    MarketCursorV2, MechanicsInputRefV2, MechanicsInputV2, SourceProvenanceV2,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const ROOT_HASH: &str = "3763341032b451fedc399d27b192ba2583dd0edb4d01e247a98d839db57cfa5e";

fn root_quote() -> Value {
    let contract: Value = serde_json::from_slice(include_bytes!(
        "../contracts/prospective/event-pulse-e2-wire-admission-v2-contract.json"
    ))
    .unwrap();
    contract["mechanics_input_v2"]["market_golden"].clone()
}

fn canonical(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn rehash(mut value: Value) -> Value {
    value.as_object_mut().unwrap().remove("payload_hash");
    value["payload_hash"] = json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).unwrap())
    ));
    value
}

#[test]
fn root_derived_quote_golden_parses_with_exact_v2_hash_and_provenance() {
    let bytes = canonical(&root_quote());
    assert_eq!(bytes.len(), 1_065);
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "d08849ba74b54ef02fa62308be8e16f3af7d300c7bd7c092d92ec3dfdfcfe846"
    );
    let input = MechanicsInputV2::from_json_line(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&input).unwrap(), bytes);
    assert_eq!(input.payload_hash(), ROOT_HASH);
    match input.view() {
        MechanicsInputRefV2::Market {
            market_cursor,
            source_provenance,
            ..
        } => {
            assert_eq!(
                market_cursor,
                &MarketCursorV2::Derived {
                    raw_frame_seq: 41,
                    action_index: 2,
                    item_index: 0,
                }
            );
            assert_eq!(
                source_provenance,
                &SourceProvenanceV2::BinanceBookTicker {
                    update_id: 99,
                    event_time_ms: 1_001,
                    transaction_time_ms: 1_000,
                }
            );
        }
        MechanicsInputRefV2::NonMarket(_) => panic!("root MARKET golden lowered to V1"),
    }
}

#[test]
fn v2_json_is_unique_exact_and_rehashed_nested_drift_still_rejects() {
    let bytes = canonical(&root_quote());
    let spaced = [b" ".as_slice(), bytes.as_slice()].concat();
    assert!(MechanicsInputV2::from_json_line(&spaced).is_err());
    let text = String::from_utf8(bytes).unwrap();
    let duplicate = text.replacen(
        "{\"action_index\":2,",
        "{\"action_index\":2,\"action_index\":2,",
        1,
    );
    assert!(MechanicsInputV2::from_json_line(duplicate.as_bytes()).is_err());

    for pointer in ["/envelope/frame_seq", "/catalog/instruments/7/quote_asset"] {
        let mut changed = root_quote();
        *changed.pointer_mut(pointer).unwrap() = if pointer.ends_with("frame_seq") {
            json!(42)
        } else {
            json!("USDC")
        };
        let changed = rehash(changed);
        assert!(MechanicsInputV2::from_json_line(&canonical(&changed)).is_err());
    }
}

#[test]
fn quote_provenance_accepts_full_u64_without_selecting_native_cursor() {
    let mut quote = root_quote();
    quote["source_provenance"]["update_id"] = json!(u64::MAX);
    let quote = rehash(quote);
    let parsed = MechanicsInputV2::from_json_line(&canonical(&quote)).unwrap();
    assert!(matches!(
        parsed.view(),
        MechanicsInputRefV2::Market {
            market_cursor: MarketCursorV2::Derived { .. },
            source_provenance: SourceProvenanceV2::BinanceBookTicker {
                update_id: u64::MAX,
                ..
            },
            ..
        }
    ));
}

#[test]
fn derived_market_cursor_accepts_full_raw_frame_domain_without_v1_packing() {
    for frame in [2_147_483_648_u64, u64::MAX] {
        let mut quote = root_quote();
        quote["envelope"]["frame_seq"] = json!(frame);
        quote["market_cursor"]["raw_frame_seq"] = json!(frame);
        let parsed = MechanicsInputV2::from_json_line(&canonical(&rehash(quote))).unwrap();
        assert!(matches!(
            parsed.view(),
            MechanicsInputRefV2::Market {
                market_cursor: MarketCursorV2::Derived { raw_frame_seq, action_index: 2, item_index: 0 },
                ..
            } if *raw_frame_seq == frame
        ));
    }
    for (action, item) in [(65_535_u64, 0_u64), (2, 65_536)] {
        let mut quote = root_quote();
        quote["action_index"] = json!(action);
        quote["market_cursor"]["action_index"] = json!(action);
        quote["market_cursor"]["item_index"] = json!(item);
        assert!(MechanicsInputV2::from_json_line(&canonical(&rehash(quote))).is_err());
    }
}

#[test]
fn timestamp_aliases_and_out_of_range_values_fail_closed() {
    for value in [
        json!(-1),
        json!(9_223_372_036_855_u64),
        json!(1.0),
        json!("1000"),
        json!(true),
        Value::Null,
    ] {
        let mut quote = root_quote();
        quote["source_provenance"]["transaction_time_ms"] = value;
        let quote = rehash(quote);
        assert!(MechanicsInputV2::from_json_line(&canonical(&quote)).is_err());
    }
}

#[test]
fn native_i64_plus_one_and_rehashed_family_source_provenance_drift_fail_exactly() {
    let too_large = i64::MAX as u64 + 1;
    let mut native = root_quote();
    native["envelope"]["source_sequence"] = json!({"first": too_large, "last": too_large});
    native["market_cursor"] =
        json!({"kind":"NATIVE","first_sequence":too_large,"last_sequence":too_large});
    assert!(MechanicsInputV2::from_json_line(&canonical(&rehash(native))).is_err());

    let mut wrong_family = root_quote();
    wrong_family["envelope"]["payload"] = json!({"Trade": {
        "price":{"coefficient":6500,"scale":1},
        "quantity":{"coefficient":1,"scale":0},
        "aggressor":"Buy",
        "trade_id":null
    }});
    assert!(MechanicsInputV2::from_json_line(&canonical(&rehash(wrong_family))).is_err());

    let mut wrong_source = root_quote();
    wrong_source["catalog"]["venue_sources"]["3"]["source_id"] = json!("binance_primary_market");
    assert!(MechanicsInputV2::from_json_line(&canonical(&rehash(wrong_source))).is_err());

    let mut wrong_provenance = root_quote();
    wrong_provenance["source_provenance"] =
        json!({"kind":"BINANCE_OPEN_INTEREST","source_time_ms":1000});
    assert!(MechanicsInputV2::from_json_line(&canonical(&rehash(wrong_provenance))).is_err());
}
