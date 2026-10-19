use marketfeed_datasets::parquet::{self, Exact, TradeRow};
use marketfeed_model::Fixed;
#[test]
fn full_i128_and_u8_scale_domain_roundtrips_without_decimal128_or_float() {
    let rows: Vec<_> = [i128::MIN, i128::MAX, -9007199254740993, 9007199254740993, 0]
        .into_iter()
        .flat_map(|coefficient| {
            [0, 18, 38, 255].into_iter().map(move |scale| TradeRow {
                price: Exact::from(Fixed::new(coefficient, scale)),
                native_quantity: Exact::from(Fixed::new(coefficient, scale)),
                fields: serde_json::json!({"test":"full-domain", "event_time_ns":"-1", "receipt_time_ns":"9223372036854775807", "native_symbol":"BTC", "native_trade_id":"9007199254740993"}),
            })
        })
        .collect();
    let bytes = parquet::write(&rows).unwrap();
    assert_eq!(parquet::read(bytes).unwrap(), rows);
}
#[test]
fn noncanonical_coefficients_and_wrong_parquet_schemas_are_rejected() {
    assert!(
        Exact {
            coefficient: "01".into(),
            scale: 0
        }
        .fixed()
        .is_err()
    );
    assert!(parquet::read(b"not a parquet file".to_vec()).is_err());
}
