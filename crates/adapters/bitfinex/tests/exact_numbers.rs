use marketfeed_adapter_bitfinex as messages;
use marketfeed_adapter_bitfinex::{ChanBinding, ChanKind, Decoded};
use marketfeed_model::{AggressorSide, Fixed};
use std::collections::HashMap;
fn channels() -> HashMap<u32, ChanBinding> {
    HashMap::from([(
        7,
        ChanBinding {
            kind: ChanKind::Trades,
            symbol: "tBTCUSD".into(),
            candle_interval: None,
        },
    )])
}
#[test]
fn original_numeric_price_and_signed_amount_are_exact() {
    for (token, expected) in [
        (
            "12345678901234567890.123456789012345678",
            Fixed::new(12345678901234567890123456789012345678, 18),
        ),
        ("1.2500e-8", Fixed::new(12500, 12)),
        (
            "170141183460469231731687303715884105727",
            Fixed::new(i128::MAX, 0),
        ),
    ] {
        for token in [token.to_owned(), format!("\"{token}\"")] {
            let raw = format!(
                r#"[7,"te",[9007199254740993,1792411200123,-0.000000000000000001,{token}]]"#
            );
            let rows = messages::decode_text(raw.as_bytes(), &channels()).unwrap();
            let Decoded::Trade(trade) = &rows[0] else {
                panic!("trade")
            };
            assert_eq!(trade.price.0, expected);
            assert_eq!(trade.quantity.0, Fixed::new(1, 18));
            assert_eq!(trade.aggressor, AggressorSide::Sell);
            assert_eq!(trade.trade_id, "9007199254740993");
        }
    }
}
#[test]
fn signed_abs_overflow_zero_price_and_wrapping_timestamps_are_rejected() {
    for (amount, price, ts) in [
        (
            "-170141183460469231731687303715884105728",
            "100",
            "1792411200123",
        ),
        ("0", "100", "1792411200123"),
        ("1", "0", "1792411200123"),
        ("1", "-1", "1792411200123"),
        ("1", "100", "18446744073709551615"),
        ("1", "100", "9223372036854775807"),
    ] {
        let raw = format!(r#"[7,"te",[1,{ts},{amount},{price}]]"#);
        assert!(
            messages::decode_text(raw.as_bytes(), &channels()).is_err(),
            "{raw}"
        );
    }
}
