//! Pagination is response completeness, not an atomic exchange-universe snapshot.
use bytes::Bytes;
use marketfeed_adapter_api::{Environment, HttpResponse, VenueFactory};
use marketfeed_adapter_bybit::{BybitCategory, BybitFactory, parse_instruments_info};
use serde_json::{Value, json};

fn row(symbol: &str) -> Value {
    json!({"symbol":symbol,"status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","contractType":"LinearPerpetual","priceFilter":{"tickSize":"0.10"},"lotSizeFilter":{"qtyStep":"0.001"}})
}
fn page(category: &str, cursor: Option<Value>, symbol: &str) -> HttpResponse {
    let mut value = json!({"retCode":0,"result":{"category":category,"list":[row(symbol)]}});
    match category {
        "spot" => {
            value["result"]["list"][0]
                .as_object_mut()
                .unwrap()
                .remove("contractType");
            let lot = value["result"]["list"][0]["lotSizeFilter"]
                .as_object_mut()
                .unwrap();
            lot.remove("qtyStep");
            lot.insert("basePrecision".into(), json!("0.00001"));
        }
        "inverse" => value["result"]["list"][0]["contractType"] = json!("InversePerpetual"),
        _ => {}
    }
    if let Some(cursor) = cursor {
        value["result"]["nextPageCursor"] = cursor;
    }
    response(value)
}
fn response(value: Value) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: Bytes::from(serde_json::to_vec(&value).unwrap()),
    }
}
fn change(response: &mut HttpResponse, f: impl FnOnce(&mut Value)) {
    let mut value = serde_json::from_slice(&response.body).unwrap();
    f(&mut value);
    response.body = Bytes::from(serde_json::to_vec(&value).unwrap());
}
fn factory() -> BybitFactory {
    BybitFactory::default()
}
fn refuses(responses: &[HttpResponse]) {
    let mut output = Vec::new();
    assert!(factory().parse_instruments(responses, &mut output).is_err());
    assert!(output.is_empty(), "no partially published definitions");
}
#[test]
fn complete_ordered_pages_have_unique_request_ids_and_exact_opaque_cursor_encoding() {
    let first = page("linear", Some(json!("A&B=+/%雪")), "BTCUSDT");
    let second = page("linear", Some(json!("last")), "ETHUSDT");
    let third = page("linear", Some(json!("")), "SOLUSDT");
    let requests = factory()
        .instrument_continuation_requests(std::slice::from_ref(&first))
        .unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].id, 2);
    assert_eq!(
        requests[0].url,
        "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000&cursor=A%26B%3D%2B%2F%25%E9%9B%AA"
    );
    assert_eq!(
        factory()
            .instrument_continuation_requests(&[first.clone(), second.clone()])
            .unwrap()[0]
            .id,
        3
    );
    refuses(std::slice::from_ref(&first));
    let responses = [first, second, third];
    assert!(
        factory()
            .instrument_continuation_requests(&responses)
            .unwrap()
            .is_empty()
    );
    let defs = parse_instruments_info(&responses, BybitCategory::Linear).unwrap();
    assert_eq!(
        defs.iter()
            .map(|d| d.key.native_symbol.as_str())
            .collect::<Vec<_>>(),
        ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    );
    assert_eq!(defs[0].price_increment.coefficient, 10);
    assert_eq!(defs[0].price_increment.scale, 2);
}
#[test]
fn spot_never_sends_pagination_parameters_and_derivatives_require_terminal_cursor() {
    let spot = BybitFactory {
        category: BybitCategory::Spot,
        enable_l2: false,
    };
    assert_eq!(
        spot.instrument_requests(Environment::Production).unwrap()[0].url,
        "https://api.bybit.com/v5/market/instruments-info?category=spot"
    );
    assert!(
        factory()
            .instrument_requests(Environment::Production)
            .unwrap()[0]
            .url
            .ends_with("category=linear&limit=1000")
    );
    let data = page("spot", None, "BTCUSDT");
    assert!(
        spot.instrument_continuation_requests(&[data])
            .unwrap()
            .is_empty()
    );
    assert!(
        spot.instrument_continuation_requests(&[page("spot", Some(json!("A")), "BTCUSDT")])
            .is_err()
    );
    for cursor in [None, Some(Value::Null), Some(json!(7)), Some(json!(false))] {
        let data = page("linear", cursor, "BTCUSDT");
        assert!(
            factory()
                .instrument_continuation_requests(std::slice::from_ref(&data))
                .is_err()
        );
        refuses(&[data]);
    }
}
#[test]
fn cursor_cycles_and_responses_after_terminal_and_duplicate_symbols_are_refused() {
    for responses in [
        vec![
            page("linear", Some(json!("A")), "ONE"),
            page("linear", Some(json!("A")), "TWO"),
        ],
        vec![
            page("linear", Some(json!("A")), "ONE"),
            page("linear", Some(json!("B")), "TWO"),
            page("linear", Some(json!("A")), "THREE"),
        ],
        vec![
            page("linear", Some(json!("")), "ONE"),
            page("linear", Some(json!("")), "TWO"),
        ],
        vec![
            page("linear", Some(json!("A")), "ONE"),
            page("linear", Some(json!("")), "ONE"),
        ],
    ] {
        assert!(
            factory()
                .instrument_continuation_requests(&responses)
                .is_err()
        );
        refuses(&responses);
    }
    let mut duplicate = page("linear", Some(json!("")), "ONE");
    change(&mut duplicate, |v| {
        v["result"]["list"].as_array_mut().unwrap().push(row("ONE"))
    });
    refuses(&[duplicate]);
}
#[test]
fn scope_status_shape_duplicate_keys_and_numeric_cursor_cannot_be_silently_accepted() {
    for field in [
        "category", "retCode", "symbol", "base", "http", "cursor", "list",
    ] {
        let mut data = page("linear", Some(json!("")), "BTCUSDT");
        if field == "http" {
            data.status = 500;
        } else {
            change(&mut data, |v| match field {
                "category" => v["result"]["category"] = json!("inverse"),
                "retCode" => v["retCode"] = json!(10001),
                "symbol" => v["result"]["list"][0]["symbol"] = json!(""),
                "base" => v["result"]["list"][0]["baseCoin"] = json!(""),
                "cursor" => v["result"]["nextPageCursor"] = json!("a".repeat(2049)),
                _ => v["result"]["list"] = Value::Null,
            });
        }
        refuses(&[data]);
    }
    for body in [
        br#"{"retCode":0,"retCode":0,"result":{"category":"linear","nextPageCursor":"","list":[]}}"#.as_slice(),
        br#"{"retCode":0,"result":{"category":"linear","nextPageCursor":18446744073709551616,"list":[]}}"#,
    ] {
        let data=HttpResponse{status:200,headers:vec![],body:Bytes::copy_from_slice(body)};
        assert!(factory().instrument_continuation_requests(std::slice::from_ref(&data)).is_err());
        refuses(&[data]);
    }
}
#[test]
fn page_byte_row_and_round_limits_refuse_incomplete_catalogs() {
    let mut responses: Vec<_> = (0..16)
        .map(|i| {
            page(
                "linear",
                Some(json!(format!("next{i}"))),
                &format!("COIN{i}"),
            )
        })
        .collect();
    assert!(
        factory()
            .instrument_continuation_requests(&responses)
            .is_err()
    );
    change(responses.last_mut().unwrap(), |v| {
        v["result"]["nextPageCursor"] = json!("")
    });
    assert_eq!(
        parse_instruments_info(&responses, BybitCategory::Linear)
            .unwrap()
            .len(),
        16
    );
    responses.push(page("linear", Some(json!("")), "EXTRA"));
    refuses(&responses);
    let huge = HttpResponse {
        status: 200,
        headers: vec![],
        body: Bytes::from(vec![b' '; 8 * 1024 * 1024 + 1]),
    };
    refuses(&[huge]);
    let mut crowded = page("linear", Some(json!("")), "BTCUSDT");
    change(&mut crowded, |v| {
        v["result"]["list"] = Value::Array((0..1001).map(|i| row(&format!("COIN{i}"))).collect())
    });
    refuses(&[crowded]);
}
#[test]
fn invalid_late_financial_rows_never_partially_publish_earlier_pages() {
    for (field, token) in [
        ("tickSize", "0"),
        ("tickSize", "-1"),
        ("tickSize", "1e1000"),
        ("qtyStep", "0"),
        ("qtyStep", "bad"),
    ] {
        let first = page("linear", Some(json!("NEXT")), "ONE");
        let mut last = page("linear", Some(json!("")), "TWO");
        change(&mut last, |v| {
            v["result"]["list"][0][if field == "tickSize" {
                "priceFilter"
            } else {
                "lotSizeFilter"
            }][field] = json!(token)
        });
        refuses(&[first, last]);
    }
}

#[test]
fn exact_native_product_kind_is_required_and_inverse_pagination_is_supported() {
    use marketfeed_model::InstrumentKind;
    for (category, kind, contract) in [
        (
            BybitCategory::Linear,
            InstrumentKind::FutureLinear,
            "LinearFutures",
        ),
        (
            BybitCategory::Inverse,
            InstrumentKind::FutureInverse,
            "InverseFutures",
        ),
    ] {
        let first = page(category.as_str(), Some(json!("NEXT")), "FIRST");
        let mut last = page(category.as_str(), Some(json!("")), "LAST");
        change(&mut last, |v| {
            v["result"]["list"][0]["contractType"] = json!(contract)
        });
        assert_eq!(
            parse_instruments_info(&[first, last], category).unwrap()[1]
                .key
                .kind,
            kind
        );
        for native in [
            None,
            Some(""),
            Some("NewUnknown"),
            Some(if matches!(category, BybitCategory::Linear) {
                "InversePerpetual"
            } else {
                "LinearPerpetual"
            }),
        ] {
            let mut data = page(category.as_str(), Some(json!("")), "BTC");
            change(&mut data, |v| match native {
                Some(value) => v["result"]["list"][0]["contractType"] = json!(value),
                None => {
                    v["result"]["list"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("contractType");
                }
            });
            assert!(parse_instruments_info(&[data], category).is_err());
        }
    }
}

#[test]
fn quantity_increment_uses_the_native_category_field_without_cross_schema_fallback() {
    let mut spot = page("spot", None, "BTCUSDT");
    // A foreign qtyStep cannot override spot basePrecision.
    change(&mut spot, |v| {
        v["result"]["list"][0]["lotSizeFilter"]["qtyStep"] = json!("100")
    });
    let defs = parse_instruments_info(&[spot.clone()], BybitCategory::Spot).unwrap();
    assert_eq!(
        defs[0].quantity_increment,
        marketfeed_model::Fixed::new(1, 5)
    );
    change(&mut spot, |v| {
        v["result"]["list"][0]["lotSizeFilter"]
            .as_object_mut()
            .unwrap()
            .remove("basePrecision");
    });
    assert!(parse_instruments_info(&[spot], BybitCategory::Spot).is_err());
    for category in [BybitCategory::Linear, BybitCategory::Inverse] {
        let mut data = page(category.as_str(), Some(json!("")), "BTC");
        change(&mut data, |v| {
            let lot = v["result"]["list"][0]["lotSizeFilter"]
                .as_object_mut()
                .unwrap();
            lot.remove("qtyStep");
            lot.insert("basePrecision".into(), json!("0.001"));
        });
        assert!(parse_instruments_info(&[data], category).is_err());
    }
}
