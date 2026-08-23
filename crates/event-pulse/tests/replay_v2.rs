use marketfeed_event_pulse::{
    MechanicsInputV2, MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter, ReplayInputError,
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1,
        ClockStateV1, ContributorKeyV1, ContributorV1, InstrumentIdentityV1, MechanicsInputV1,
        Rfc3339Time,
    },
};
use serde_json::Value;

fn root_quote() -> MechanicsInputV2 {
    let contract: Value = serde_json::from_slice(include_bytes!(
        "../contracts/prospective/event-pulse-e2-wire-admission-v2-contract.json"
    ))
    .unwrap();
    MechanicsInputV2::from_json_line(
        &serde_json::to_vec(&contract["mechanics_input_v2"]["market_golden"]).unwrap(),
    )
    .unwrap()
}

#[test]
fn mechanics_v2_jsonl_roundtrips_canonically_without_epin_relabeling() {
    let input = root_quote();
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    writer.write_input(&input).unwrap();
    let bytes = writer.finish();
    assert_eq!(bytes.last(), Some(&b'\n'));

    let decision = Rfc3339Time::from_unix_nanos(2_000_000_000).unwrap();
    let decoded = MechanicsInputV2JsonlReader::new(bytes.as_slice(), decision)
        .read_all()
        .unwrap();
    assert_eq!(decoded, vec![input]);
}

#[test]
fn mechanics_v2_jsonl_rejects_missing_newline_and_future_input() {
    let input = root_quote();
    let bytes = serde_json::to_vec(&input).unwrap();
    let decision = Rfc3339Time::from_unix_nanos(2_000_000_000).unwrap();
    assert_eq!(
        MechanicsInputV2JsonlReader::new(bytes.as_slice(), decision.clone()).read_all(),
        Err(ReplayInputError::MissingNewline)
    );

    let mut newline = bytes;
    newline.push(b'\n');
    let before = Rfc3339Time::from_unix_nanos(999_999_999).unwrap();
    assert_eq!(
        MechanicsInputV2JsonlReader::new(newline.as_slice(), before).read_all(),
        Err(ReplayInputError::FutureInput)
    );
}

#[test]
fn writer_strict_readback_preserves_exact_v1_nonmarket_bytes() {
    let contributor = ContributorKeyV1::new(
        "source",
        InstrumentIdentityV1::new("BNB", "USDT", "PERPETUAL", "BINANCE", "BNBUSDT").unwrap(),
    )
    .unwrap();
    let at = Rfc3339Time::from_unix_nanos(2_000_000_000).unwrap();
    let v1 = MechanicsInputV1::clock(
        ContributorV1::new(contributor.clone(), "epoch_market", 0).unwrap(),
        ClockSourceV1::new(
            ClockSourceKeyV1::new("clock_source", contributor).unwrap(),
            "epoch_clock",
            0,
        )
        .unwrap(),
        at.clone(),
        at.clone(),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        CanonicalDecimal::parse("0", 18, 8).unwrap(),
        2_000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_WITHIN_TOLERANCE",
    )
    .unwrap();
    let expected = serde_json::to_vec(&v1).unwrap();
    let v2 = MechanicsInputV2::from_v1_non_market(v1).unwrap();
    assert_eq!(serde_json::to_vec(&v2).unwrap(), expected);
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    writer.write_input(&v2).unwrap();
    let mut expected_line = expected;
    expected_line.push(b'\n');
    assert_eq!(writer.finish(), expected_line);
}
