use marketfeed_adapter_deribit as messages;
use marketfeed_adapter_deribit::DecodedEvent;
use marketfeed_model::Fixed;
fn frame(price: &str, amount: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"subscription","params":{{"channel":"trades.BTC-PERPETUAL.raw","data":[{{"instrument_name":"BTC-PERPETUAL","trade_id":9007199254740993,"trade_seq":9007199254740994,"timestamp":1792411200123,"price":{price},"amount":{amount},"direction":"buy"}}]}}}}"#
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
                assert_eq!(rows[0].trade_id, "9007199254740993");
                assert_eq!(rows[0].trade_seq, Some(9007199254740994));
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
    for amount in ["0", "-1"] {
        assert!(messages::decode_text(frame("100", amount).as_bytes()).is_err());
    }
}

#[test]
fn metadata_decimal_tokens_do_not_round_and_large_trade_timestamps_reject() {
    let raw=br#"{"result":[{"instrument_name":"BTC-PERPETUAL","kind":"future","is_active":true,"base_currency":"BTC","quote_currency":"USD","settlement_period":"perpetual","tick_size":0.1234567890123456789012345678,"min_trade_amount":1.2500e-8,"contract_size":9007199254740993}]}"#;
    let response = marketfeed_adapter_api::HttpResponse {
        status: 200,
        headers: vec![],
        body: bytes::Bytes::from_static(raw),
    };
    let definitions = messages::parse_instruments(&[response]).unwrap();
    assert_eq!(
        definitions[0].price_increment,
        Fixed::new(1234567890123456789012345678, 28)
    );
    assert_eq!(definitions[0].quantity_increment, Fixed::new(12500, 12));
    assert_eq!(
        definitions[0].contract_size,
        Some(Fixed::new(9007199254740993, 0))
    );
    let raw = frame("100", "1").replace("1792411200123", &i64::MAX.to_string());
    assert!(messages::decode_text(raw.as_bytes()).is_err());
}
