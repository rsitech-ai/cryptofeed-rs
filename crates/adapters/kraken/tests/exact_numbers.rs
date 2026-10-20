use marketfeed_adapter_kraken as messages;
use marketfeed_adapter_kraken::DecodedEvent;
use marketfeed_model::Fixed;
fn frame(price: &str, qty: &str) -> String {
    format!(
        r#"{{"channel":"trade","type":"update","data":[{{"symbol":"BTC/USD","trade_id":9007199254740993,"price":{price},"qty":{qty},"side":"buy","timestamp":"2026-10-19T12:00:00.123456789Z"}}]}}"#
    )
}
#[test]
fn numeric_tokens_and_strings_keep_original_decimal_precision_for_every_backend() {
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
            let raw = frame(&token, "0.000000000000000001");
            for decode in [messages::decode_text, messages::decode_text_serde] {
                let DecodedEvent::Trades(rows) = decode(raw.as_bytes()).unwrap() else {
                    panic!("trades")
                };
                assert_eq!(rows[0].price.0, expected);
                assert_eq!(rows[0].quantity.0, Fixed::new(1, 18));
                assert_eq!(rows[0].trade_id, 9007199254740993);
                assert_eq!(rows[0].exchange_ts_ns, Some(1792411200123456789));
            }
            #[cfg(feature = "simd-json")]
            assert_eq!(
                messages::decode_text_simd(raw.as_bytes()).unwrap(),
                messages::decode_text_serde(raw.as_bytes()).unwrap()
            );
        }
    }
}
#[test]
fn overflow_zero_and_negative_financial_values_fail_closed() {
    for price in [
        "170141183460469231731687303715884105728",
        "1e99999999999999999999",
        "0",
        "-1",
    ] {
        assert!(
            messages::decode_text(frame(price, "1").as_bytes()).is_err(),
            "{price}"
        );
    }
    for qty in ["0", "-1"] {
        assert!(messages::decode_text(frame("100", qty).as_bytes()).is_err());
    }
}

#[test]
fn futures_rejects_oversized_exponents_instead_of_expanding_or_changing_them() {
    for token in ["1e2147483646", "1e2147483647", "0.1e-255", "0", "-1"] {
        let raw = format!(
            r#"{{"feed":"trade","product_id":"PI_XBTUSD","uid":"unit","price":{token},"qty":1,"side":"buy","type":"fill","seq":1,"time":1792411200123}}"#
        );
        assert!(
            messages::decode_futures_text(raw.as_bytes()).is_err(),
            "{token}"
        );
    }
}
#[test]
fn futures_metadata_decimal_tokens_do_not_round() {
    let raw=br#"{"result":"success","instruments":[{"symbol":"PI_XBTUSD","type":"futures_inverse","tickSize":0.1234567890123456789012345678,"contractSize":1.2500e-8,"tradeable":true}]}"#;
    let response = marketfeed_adapter_api::HttpResponse {
        status: 200,
        headers: vec![],
        body: bytes::Bytes::from_static(raw),
    };
    let definitions = messages::parse_futures_instruments(&[response]).unwrap();
    assert_eq!(
        definitions[0].price_increment,
        Fixed::new(1234567890123456789012345678, 28)
    );
    assert_eq!(definitions[0].contract_size, Some(Fixed::new(12500, 12)));
}
