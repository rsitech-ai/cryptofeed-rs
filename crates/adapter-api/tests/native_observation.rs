use marketfeed_adapter_api::*;
use marketfeed_model::Fixed;

// Independent wire golden: this is a codec fixture, not admitted exchange data.
fn fixture() -> NativeObservationAttachment {
    serde_json::from_str(include_str!("fixtures/native_observations_v1.json")).unwrap()
}
fn limits() -> NativeObservationLimits {
    NativeObservationLimits {
        rows: 6,
        associations: 5,
        book_levels: 3,
        mids: 2,
        users: 2,
        string_bytes: 4096,
    }
}

#[test]
fn native_golden_preserves_financial_extrema_presence_counts_and_original_end() {
    let data = fixture();
    assert_eq!(data.validate_layout(5, limits()).unwrap().rows, 6);
    let NativeObservation::HyperliquidTrade(t) = &data.rows[0].observation else {
        panic!()
    };
    assert_eq!(t.price, Fixed::new(i128::MAX, 20));
    assert_eq!(t.tid, u64::MAX);
    assert_eq!(t.users, ["public-A", "public-B"]);
    assert_eq!(t.unique_id(), "@1:1000:18446744073709551615");
    let NativeObservation::HyperliquidBbo(b) = &data.rows[1].observation else {
        panic!()
    };
    assert!(b.sides[0].is_none());
    assert_eq!(b.sides[1].as_ref().unwrap().orders, 0);
    assert!(data.rows[1].canonical_positions.is_empty());
    let NativeObservation::HyperliquidBook(b) = &data.rows[2].observation else {
        panic!()
    };
    assert_eq!(b.sides[0][0].orders, u64::MAX);
    let NativeObservation::HyperliquidCandle(c) = &data.rows[3].observation else {
        panic!()
    };
    assert_eq!((c.open_ms, c.end_ms, c.trades), (1000, 1001, u64::MAX));
    let NativeObservation::HyperliquidContext(c) = &data.rows[4].observation else {
        panic!()
    };
    assert_eq!(c.mid_price, None);
    assert_eq!(c.circulating_supply, Some(Fixed::new(0, 7)));
    assert_eq!(c.funding, Some(Fixed::new(i128::MIN, 255)));
    assert_eq!(c.impact_prices.unwrap()[1], Fixed::new(i128::MAX, 255));
    let bytes = serde_json::to_vec(&data).unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("-170141183460469231731687303715884105728"));
    let restored: NativeObservationAttachment = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored, data);
}

#[test]
fn native_layout_refuses_orphan_duplicate_missing_and_false_native_only_rows() {
    let mut data = fixture();
    data.rows[0].canonical_positions[0] = 5;
    assert!(data.validate_layout(5, limits()).is_err());
    let mut data = fixture();
    data.rows[2].canonical_positions[0] = 0;
    assert!(data.validate_layout(5, limits()).is_err());
    let mut data = fixture();
    data.rows[4].canonical_positions.pop();
    assert!(data.validate_layout(5, limits()).is_err());
    let mut data = fixture();
    data.rows[0].canonical_positions.clear();
    assert!(data.validate_layout(5, limits()).is_err());
    let mut data = fixture();
    let NativeObservation::HyperliquidBbo(b) = &mut data.rows[1].observation else {
        panic!()
    };
    b.sides[0] = b.sides[1].clone();
    assert!(data.validate_layout(5, limits()).is_err());
    let mut data = fixture();
    let NativeObservation::HyperliquidAllMids(m) = &mut data.rows[5].observation else {
        panic!()
    };
    m.entries[1].coin = m.entries[0].coin.clone();
    assert!(data.validate_layout(5, limits()).is_err());
    for index in [1, 5] {
        let mut data = fixture();
        data.rows[index].canonical_positions.push(5);
        let cap = NativeObservationLimits {
            associations: 6,
            ..limits()
        };
        assert!(data.validate_layout(6, cap).is_err());
    }
}

#[test]
fn native_layout_charges_exact_counters_and_refuses_unsupported_schema() {
    let data = fixture();
    let usage = data.validate_layout(5, limits()).unwrap();
    assert_eq!(
        (
            usage.associations,
            usage.book_levels,
            usage.mids,
            usage.users
        ),
        (5, 3, 2, 2)
    );
    let exact = NativeObservationLimits {
        string_bytes: usage.string_bytes,
        ..limits()
    };
    assert_eq!(data.validate_layout(5, exact).unwrap(), usage);
    for index in 0..6 {
        let mut cap = exact;
        let field = match index {
            0 => &mut cap.rows,
            1 => &mut cap.associations,
            2 => &mut cap.book_levels,
            3 => &mut cap.mids,
            4 => &mut cap.users,
            _ => &mut cap.string_bytes,
        };
        *field -= 1;
        assert!(data.validate_layout(5, cap).is_err(), "counter {index}");
    }
    let mut data = data;
    data.schema_version = 2;
    assert!(data.validate_layout(5, limits()).is_err());
}

#[test]
fn observed_unit_enforces_encoded_boundary_and_validates_deserialization() {
    let native = NativeObservationAttachment {
        schema_version: 1,
        rows: vec![fixture().rows[1].clone()],
    };
    let canonical = EventBatch {
        session: marketfeed_model::SessionId(1),
        frame_seq: 1,
        events: vec![],
    };
    let source = ObservedBatchSource {
        venue: marketfeed_model::VenueId(1),
        connection: marketfeed_model::ConnectionId(1),
        received: marketfeed_model::FrameStamp {
            receive_ts: marketfeed_model::TimestampNs(123),
            mono_ns: 456,
        },
    };
    let mut exact = ObservedBatchLimits::HARD_MAX;
    let unit = loop {
        let candidate =
            ObservedEventBatch::try_new(canonical.clone(), native.clone(), source, exact).unwrap();
        let len = candidate.encoded_len(exact.encoded_bytes).unwrap();
        if len == exact.encoded_bytes {
            break candidate;
        }
        exact.encoded_bytes = len;
    };
    let bytes = serde_json::to_vec(&unit).unwrap();
    assert_eq!(bytes.len(), exact.encoded_bytes);
    assert!(
        ObservedEventBatch::try_new(
            canonical,
            native,
            source,
            ObservedBatchLimits {
                encoded_bytes: bytes.len() - 1,
                ..exact
            }
        )
        .is_err()
    );
    assert_eq!(
        serde_json::from_slice::<ObservedEventBatch>(&bytes).unwrap(),
        unit
    );
    let mut stamped = MarketBatch::Observed(unit.clone());
    assert!(
        stamped
            .stamp_runtime(
                marketfeed_model::VenueId(1),
                marketfeed_model::SessionId(u64::MAX),
                marketfeed_model::ConnectionId(u64::MAX),
                u64::MAX,
                Some(marketfeed_model::FrameStamp {
                    receive_ts: marketfeed_model::TimestampNs(i64::MIN),
                    mono_ns: u64::MAX
                }),
                0
            )
            .is_err()
    );
    assert_eq!(stamped, MarketBatch::Observed(unit));
    let bad_version = String::from_utf8(bytes.clone())
        .unwrap()
        .replace("\"schema_version\":1", "\"schema_version\":2");
    assert!(serde_json::from_str::<ObservedEventBatch>(&bad_version).is_err());
    let orphan = String::from_utf8(bytes.clone())
        .unwrap()
        .replace("\"canonical_positions\":[]", "\"canonical_positions\":[0]");
    assert!(serde_json::from_str::<ObservedEventBatch>(&orphan).is_err());
    let unknown = String::from_utf8(bytes)
        .unwrap()
        .replacen('{', "{\"unknown\":true,", 1);
    assert!(serde_json::from_str::<ObservedEventBatch>(&unknown).is_err());
}

#[test]
fn native_only_units_keep_outer_scope_clocks_and_reject_missing_input_stamp() {
    use marketfeed_model::*;
    let make = || {
        MarketBatch::Observed(
            ObservedEventBatch::try_new(
                EventBatch {
                    session: SessionId(0),
                    frame_seq: 0,
                    events: vec![],
                },
                NativeObservationAttachment {
                    schema_version: 1,
                    rows: vec![fixture().rows[1].clone()],
                },
                ObservedBatchSource {
                    venue: VenueId(0),
                    connection: ConnectionId(0),
                    received: FrameStamp {
                        receive_ts: TimestampNs(0),
                        mono_ns: 0,
                    },
                },
                ObservedBatchLimits::HARD_MAX,
            )
            .unwrap(),
        )
    };
    let mut no_stamp = make();
    assert!(
        no_stamp
            .stamp_runtime(VenueId(1), SessionId(2), ConnectionId(3), 4, None, 0)
            .is_err()
    );
    assert_eq!(no_stamp, make());
    let receipt = FrameStamp {
        receive_ts: TimestampNs(i64::MIN),
        mono_ns: u64::MAX,
    };
    let mut a = make();
    let mut b = make();
    a.stamp_runtime(
        VenueId(21),
        SessionId(2),
        ConnectionId(3),
        4,
        Some(receipt),
        0,
    )
    .unwrap();
    b.stamp_runtime(
        VenueId(22),
        SessionId(2),
        ConnectionId(5),
        4,
        Some(receipt),
        0,
    )
    .unwrap();
    assert_ne!(a, b);
    let MarketBatch::Observed(a) = a else {
        panic!()
    };
    assert_eq!(
        a.source(),
        ObservedBatchSource {
            venue: VenueId(21),
            connection: ConnectionId(3),
            received: receipt
        }
    );
    assert_eq!(
        serde_json::from_slice::<ObservedEventBatch>(&serde_json::to_vec(&a).unwrap()).unwrap(),
        a
    );
}
