use marketfeed_event_pulse::{
    MechanicsInputV2, MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter, ReplayInputError,
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1,
        ClockStateV1, ContributorKeyV1, ContributorV1, InstrumentIdentityV1, MAX_INPUT_BYTES,
        MechanicsInputV1, Rfc3339Time,
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

fn quote_at_frame(frame: u64) -> MechanicsInputV2 {
    use sha2::{Digest, Sha256};
    let mut value = serde_json::to_value(root_quote()).unwrap();
    value.as_object_mut().unwrap().remove("payload_hash");
    value["envelope"]["frame_seq"] = serde_json::json!(frame);
    value["market_cursor"]["raw_frame_seq"] = serde_json::json!(frame);
    value["payload_hash"] = serde_json::json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).unwrap())
    ));
    MechanicsInputV2::from_json_line(&serde_json::to_vec(&value).unwrap()).unwrap()
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
fn replay_orders_full_width_derived_frames_without_v1_display_packing() {
    let first = quote_at_frame(2_147_483_648);
    let last = quote_at_frame(u64::MAX);
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    writer.write_input(&first).unwrap();
    writer.write_input(&last).unwrap();
    let bytes = writer.finish();
    let decision = Rfc3339Time::from_unix_nanos(2_000_000_000).unwrap();
    assert_eq!(
        MechanicsInputV2JsonlReader::new(bytes.as_slice(), decision)
            .read_all()
            .unwrap(),
        vec![first, last]
    );
}

#[test]
fn replay_rejects_full_width_order_regression_and_line_overflow() {
    let first = quote_at_frame(u64::MAX);
    let regressing = quote_at_frame(2_147_483_648);
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    writer.write_input(&first).unwrap();
    assert_eq!(
        writer.write_input(&regressing),
        Err(ReplayInputError::OrderViolation)
    );

    let mut oversized = vec![b' '; MAX_INPUT_BYTES + 1];
    oversized.push(b'\n');
    let decision = Rfc3339Time::from_unix_nanos(2_000_000_000).unwrap();
    assert_eq!(
        MechanicsInputV2JsonlReader::new(oversized.as_slice(), decision).read_all(),
        Err(ReplayInputError::LineTooLarge)
    );
}

#[test]
fn replay_record_capacity_is_exact_at_65536_and_one_over() {
    let input = root_quote();
    let mut line = serde_json::to_vec(&input).unwrap();
    line.push(b'\n');
    let decision = Rfc3339Time::from_unix_nanos(2_000_000_000).unwrap();
    let exact = line.repeat(65_536);
    assert_eq!(
        MechanicsInputV2JsonlReader::new(exact.as_slice(), decision.clone())
            .read_all()
            .unwrap()
            .len(),
        65_536
    );
    let one_over = line.repeat(65_537);
    assert_eq!(
        MechanicsInputV2JsonlReader::new(one_over.as_slice(), decision).read_all(),
        Err(ReplayInputError::RecordCapacity)
    );
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
