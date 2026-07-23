//! Current Gemini public WebSocket streams plus REST decoders (exact `Fixed`).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{AggressorSide, Fixed, Price, Quantity, SourceId, TimestampNs};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    /// Current differential depth stream. With `snapshot=-1`, the first frame
    /// has `first_update_id == last_update_id` and contains absolute levels.
    DepthUpdate {
        symbol: String,
        first_update_id: u64,
        last_update_id: u64,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        exchange_ts_ns: i64,
    },
    Trade {
        symbol: String,
        trade_id: String,
        price: Price,
        quantity: Quantity,
        aggressor: AggressorSide,
        exchange_ts_ns: i64,
    },
    Quote {
        symbol: String,
        update_id: u64,
        bid_price: Price,
        bid_qty: Quantity,
        ask_price: Price,
        ask_qty: Quantity,
        exchange_ts_ns: i64,
    },
    Candle {
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
    },
    /// REST ticker stats — fields filled from v2 ticker and/or v1 pubticker.
    Statistics24h {
        open: Option<Price>,
        high: Option<Price>,
        low: Option<Price>,
        close: Option<Price>,
        volume: Option<Quantity>,
        quote_volume: Option<Quantity>,
    },
    SubscribeAck,
    Error {
        code: Option<i64>,
        detail: String,
    },
    /// Forward-compatible stream event explicitly ignored by Gemini guidance.
    Ignored,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevel {
    pub price: Price,
    pub quantity: Quantity,
}

pub fn decode_text(bytes: &[u8]) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let obj = v
        .as_object()
        .ok_or_else(|| "gemini frame not object".to_string())?;

    let is_subscription_response = matches!(
        obj.get("id"),
        Some(Value::String(id)) if id == "1"
    ) || obj.get("id").and_then(Value::as_u64) == Some(1);
    if is_subscription_response && obj.get("status").and_then(Value::as_u64) == Some(200) {
        return Ok(Decoded::SubscribeAck);
    }
    if let Some(error) = obj.get("error").and_then(Value::as_object) {
        let detail = error
            .get("msg")
            .and_then(Value::as_str)
            .unwrap_or("Gemini WebSocket error")
            .to_string();
        return Ok(Decoded::Error {
            code: error.get("code").and_then(Value::as_i64),
            detail,
        });
    }
    if let Some(event_type) = obj.get("e").and_then(Value::as_str) {
        return if event_type == "depthUpdate" {
            decode_depth_update(obj)
        } else {
            Ok(Decoded::Ignored)
        };
    }
    if obj.get("t").is_some()
        && obj.get("p").is_some()
        && obj.get("q").is_some()
        && obj.get("m").is_some()
    {
        return decode_trade(obj);
    }
    if obj.get("u").is_some()
        && obj.get("b").is_some()
        && obj.get("B").is_some()
        && obj.get("a").is_some()
        && obj.get("A").is_some()
    {
        return decode_quote(obj);
    }
    Ok(Decoded::Unknown)
}

pub fn candle_time_frame(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "1m",
        CandleInterval::M5 => "5m",
        CandleInterval::M15 => "15m",
        CandleInterval::H1 => "1h",
        CandleInterval::D1 => "1d",
    }
}

pub fn candle_interval_ns(interval: CandleInterval) -> i64 {
    match interval {
        CandleInterval::M1 => 60_000_000_000,
        CandleInterval::M5 => 300_000_000_000,
        CandleInterval::M15 => 900_000_000_000,
        CandleInterval::H1 => 3_600_000_000_000,
        CandleInterval::D1 => 86_400_000_000_000,
    }
}

pub fn decode_candles_rest(bytes: &[u8], interval: CandleInterval) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let rows = v
        .as_array()
        .ok_or_else(|| "gemini candles not array".to_string())?;
    let Some(row) = rows.first().and_then(|r| r.as_array()) else {
        return Err("gemini candles empty".into());
    };
    if row.len() < 6 {
        return Err("gemini candle row short".into());
    }
    let start_ms = match &row[0] {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|u| u as i64))
            .ok_or_else(|| "candle time not i64".to_string())?,
        Value::String(s) => s.parse().map_err(|e| format!("candle time: {e}"))?,
        _ => return Err("candle time not number/string".into()),
    };
    Ok(Decoded::Candle {
        open: Price(fixed_from_json(&row[1])?),
        high: Price(fixed_from_json(&row[2])?),
        low: Price(fixed_from_json(&row[3])?),
        close: Price(fixed_from_json(&row[4])?),
        volume: Quantity(fixed_from_json(&row[5])?),
        interval_ns: candle_interval_ns(interval),
        start_ts: TimestampNs(start_ms.saturating_mul(1_000_000)),
    })
}

/// `GET /v2/ticker/{symbol}` — open/high/low/close over last 24h; no volume field.
pub fn decode_ticker_rest(bytes: &[u8]) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let obj = v
        .as_object()
        .ok_or_else(|| "gemini ticker not object".to_string())?;
    let open = obj.get("open").map(fixed_from_json).transpose()?.map(Price);
    let high = obj.get("high").map(fixed_from_json).transpose()?.map(Price);
    let low = obj.get("low").map(fixed_from_json).transpose()?.map(Price);
    let close = obj
        .get("close")
        .map(fixed_from_json)
        .transpose()?
        .map(Price);
    if open.is_none() && high.is_none() && low.is_none() && close.is_none() {
        return Err("gemini ticker empty stats".into());
    }
    Ok(Decoded::Statistics24h {
        open,
        high,
        low,
        close,
        volume: None,
        quote_volume: None,
    })
}

/// `GET /v1/pubticker/{symbol}` — last + 24h base/quote volume.
pub fn decode_pubticker_rest(bytes: &[u8], symbol: &str) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let obj = v
        .as_object()
        .ok_or_else(|| "gemini pubticker not object".to_string())?;
    let close = obj.get("last").map(fixed_from_json).transpose()?.map(Price);
    let (volume, quote_volume) = pubticker_volumes(obj, symbol)?;
    if close.is_none() && volume.is_none() && quote_volume.is_none() {
        return Err("gemini pubticker empty stats".into());
    }
    Ok(Decoded::Statistics24h {
        open: None,
        high: None,
        low: None,
        close,
        volume,
        quote_volume,
    })
}

fn pubticker_volumes(
    obj: &serde_json::Map<String, Value>,
    symbol: &str,
) -> Result<(Option<Quantity>, Option<Quantity>), String> {
    let Some(vol) = obj.get("volume").and_then(|v| v.as_object()) else {
        return Ok((None, None));
    };
    let sym = symbol.to_ascii_uppercase();
    let mut base = None;
    let mut quote = None;
    for (k, v) in vol {
        if k.eq_ignore_ascii_case("timestamp") {
            continue;
        }
        let qty = Quantity(fixed_from_json(v)?);
        let key = k.to_ascii_uppercase();
        if sym.ends_with(&key) && key.len() < sym.len() {
            quote = Some(qty);
        } else {
            base = Some(qty);
        }
    }
    Ok((base, quote))
}

fn decode_depth_update(obj: &serde_json::Map<String, Value>) -> Result<Decoded, String> {
    let first_update_id = required_u64(obj, "U")?;
    let last_update_id = required_u64(obj, "u")?;
    if first_update_id > last_update_id {
        return Err(format!(
            "Gemini depth range is reversed: U={first_update_id} u={last_update_id}"
        ));
    }
    Ok(Decoded::DepthUpdate {
        symbol: required_symbol(obj)?,
        first_update_id,
        last_update_id,
        bids: decode_levels(obj, "b")?,
        asks: decode_levels(obj, "a")?,
        exchange_ts_ns: required_i64(obj, "E")?,
    })
}

fn decode_trade(obj: &serde_json::Map<String, Value>) -> Result<Decoded, String> {
    let trade_id = required_u64(obj, "t")?.to_string();
    let aggressor = match obj.get("m").and_then(Value::as_bool) {
        Some(true) => AggressorSide::Sell,
        Some(false) => AggressorSide::Buy,
        None => AggressorSide::Unknown,
    };
    Ok(Decoded::Trade {
        symbol: required_symbol(obj)?,
        trade_id,
        price: Price(fixed_from_json(
            obj.get("p")
                .ok_or_else(|| "Gemini trade missing p".to_string())?,
        )?),
        quantity: Quantity(fixed_from_json(
            obj.get("q")
                .ok_or_else(|| "Gemini trade missing q".to_string())?,
        )?),
        aggressor,
        exchange_ts_ns: required_i64(obj, "E")?,
    })
}

fn decode_quote(obj: &serde_json::Map<String, Value>) -> Result<Decoded, String> {
    Ok(Decoded::Quote {
        symbol: required_symbol(obj)?,
        update_id: required_u64(obj, "u")?,
        bid_price: Price(fixed_from_json(
            obj.get("b")
                .ok_or_else(|| "Gemini book ticker missing b".to_string())?,
        )?),
        bid_qty: Quantity(fixed_from_json(
            obj.get("B")
                .ok_or_else(|| "Gemini book ticker missing B".to_string())?,
        )?),
        ask_price: Price(fixed_from_json(
            obj.get("a")
                .ok_or_else(|| "Gemini book ticker missing a".to_string())?,
        )?),
        ask_qty: Quantity(fixed_from_json(
            obj.get("A")
                .ok_or_else(|| "Gemini book ticker missing A".to_string())?,
        )?),
        exchange_ts_ns: required_i64(obj, "E")?,
    })
}

fn decode_levels(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Vec<BookLevel>, String> {
    let rows = obj
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Gemini depth update missing {key}"))?;
    rows.iter()
        .map(|row| {
            let values = row
                .as_array()
                .ok_or_else(|| format!("Gemini {key} level is not an array"))?;
            if values.len() < 2 {
                return Err(format!("Gemini {key} level is short"));
            }
            Ok(BookLevel {
                price: Price(fixed_from_json(&values[0])?),
                quantity: Quantity(fixed_from_json(&values[1])?),
            })
        })
        .collect()
}

fn required_symbol(obj: &serde_json::Map<String, Value>) -> Result<String, String> {
    obj.get("s")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .ok_or_else(|| "Gemini frame missing s".to_string())
}

fn required_u64(obj: &serde_json::Map<String, Value>, key: &str) -> Result<u64, String> {
    obj.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("Gemini frame missing or invalid {key}"))
}

fn required_i64(obj: &serde_json::Map<String, Value>, key: &str) -> Result<i64, String> {
    let value = required_u64(obj, key)?;
    i64::try_from(value).map_err(|_| format!("Gemini frame {key} exceeds i64"))
}

fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("fixed value not string/number".into()),
    }
}

pub fn trade_id_source(id: &str) -> SourceId {
    SourceId(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_current_depth_update_exact_fixed() {
        let raw = br#"{"e":"depthUpdate","E":1751508260659505382,"s":"btcusd","U":12345677,"u":12345678,"b":[["45000.50","1.25000000"],["45000.25","0.00000000"]],"a":[["45001.00","0.75000000"]]}"#;
        let Decoded::DepthUpdate {
            symbol,
            first_update_id,
            last_update_id,
            bids,
            asks,
            exchange_ts_ns,
        } = decode_text(raw).unwrap()
        else {
            panic!("expected depth update");
        };
        assert_eq!(symbol, "BTCUSD");
        assert_eq!(first_update_id, 12_345_677);
        assert_eq!(last_update_id, 12_345_678);
        assert_eq!(exchange_ts_ns, 1_751_508_260_659_505_382);
        assert_eq!(bids[0].price.0, Fixed::parse_str("45000.50").unwrap());
        assert_eq!(bids[1].quantity.0.coefficient, 0);
        assert_eq!(asks[0].price.0, Fixed::parse_str("45001.00").unwrap());
    }

    #[test]
    fn decode_current_trade_and_book_ticker() {
        let trade = br#"{"E":1759873803503023900,"s":"btcusd","t":2840140956529623,"p":"120649.97000","q":"0.0046190900","m":true}"#;
        let Decoded::Trade {
            symbol,
            trade_id,
            price,
            quantity,
            aggressor,
            exchange_ts_ns,
        } = decode_text(trade).unwrap()
        else {
            panic!("trade");
        };
        assert_eq!(symbol, "BTCUSD");
        assert_eq!(trade_id, "2840140956529623");
        assert_eq!(price.0, Fixed::parse_str("120649.97000").unwrap());
        assert_eq!(quantity.0, Fixed::parse_str("0.0046190900").unwrap());
        assert_eq!(aggressor, AggressorSide::Sell);
        assert_eq!(exchange_ts_ns, 1_759_873_803_503_023_900);

        let ticker = br#"{"u":1751505576085,"E":1751508438600117161,"s":"btcusd","b":"45000.50","B":"1.25000000","a":"45001.00","A":"0.75000000"}"#;
        let Decoded::Quote {
            symbol,
            update_id,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
            exchange_ts_ns,
        } = decode_text(ticker).unwrap()
        else {
            panic!("book ticker");
        };
        assert_eq!(symbol, "BTCUSD");
        assert_eq!(update_id, 1_751_505_576_085);
        assert_eq!(bid_price.0, Fixed::parse_str("45000.50").unwrap());
        assert_eq!(bid_qty.0, Fixed::parse_str("1.25000000").unwrap());
        assert_eq!(ask_price.0, Fixed::parse_str("45001.00").unwrap());
        assert_eq!(ask_qty.0, Fixed::parse_str("0.75000000").unwrap());
        assert_eq!(exchange_ts_ns, 1_751_508_438_600_117_161);
    }

    #[test]
    fn decode_current_subscription_response() {
        assert!(matches!(
            decode_text(br#"{"id":"1","status":200,"result":null}"#).unwrap(),
            Decoded::SubscribeAck
        ));
        assert!(matches!(
            decode_text(br#"{"id":"ping","status":200,"result":null}"#).unwrap(),
            Decoded::Unknown
        ));
    }

    #[test]
    fn candle_paths_use_current_openapi_interval_tokens() {
        assert_eq!(candle_time_frame(CandleInterval::H1), "1h");
        assert_eq!(candle_time_frame(CandleInterval::D1), "1d");
    }

    #[test]
    fn unknown_event_is_ignored_and_reversed_depth_range_is_rejected() {
        assert!(matches!(
            decode_text(br#"{"e":"futureEvent","E":1}"#).unwrap(),
            Decoded::Ignored
        ));
        let error =
            decode_text(br#"{"e":"depthUpdate","E":1,"s":"btcusd","U":12,"u":11,"b":[],"a":[]}"#)
                .expect_err("reversed range");
        assert!(error.contains("reversed"), "{error}");
    }

    #[test]
    fn websocket_error_preserves_code_and_message() {
        let Decoded::Error { code, detail } = decode_text(
            br#"{"id":"1","status":429,"error":{"code":-1003,"msg":"Rate limit exceeded"}}"#,
        )
        .unwrap() else {
            panic!("error response");
        };
        assert_eq!(code, Some(-1003));
        assert_eq!(detail, "Rate limit exceeded");
    }

    #[test]
    fn decode_candles_rest_exact_fixed() {
        let raw = br#"[[1609459200000,"0.0010","0.0025","0.0015","0.0020","1000"]]"#;
        let Decoded::Candle {
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
        } = decode_candles_rest(raw, CandleInterval::M1).unwrap()
        else {
            panic!("candle")
        };
        assert_eq!(open.0, Fixed::parse_str("0.0010").unwrap());
        assert_eq!(high.0, Fixed::parse_str("0.0025").unwrap());
        assert_eq!(low.0, Fixed::parse_str("0.0015").unwrap());
        assert_eq!(close.0, Fixed::parse_str("0.0020").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("1000").unwrap());
        assert_eq!(interval_ns, 60_000_000_000);
        assert_eq!(start_ts, TimestampNs(1_609_459_200_000_000_000));
    }

    #[test]
    fn decode_ticker_rest_exact_fixed() {
        let raw = br#"{"symbol":"BTCUSD","open":"64000.00","high":"66000.50","low":"63000.25","close":"65000.12","changes":[],"bid":"65000.00","ask":"65000.10"}"#;
        let Decoded::Statistics24h {
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
        } = decode_ticker_rest(raw).unwrap()
        else {
            panic!("stats");
        };
        assert_eq!(open.unwrap().0, Fixed::parse_str("64000.00").unwrap());
        assert_eq!(high.unwrap().0, Fixed::parse_str("66000.50").unwrap());
        assert_eq!(low.unwrap().0, Fixed::parse_str("63000.25").unwrap());
        assert_eq!(close.unwrap().0, Fixed::parse_str("65000.12").unwrap());
        assert!(volume.is_none());
        assert!(quote_volume.is_none());
    }

    #[test]
    fn decode_pubticker_rest_exact_fixed() {
        let raw = br#"{"bid":"65000.00","ask":"65000.10","last":"65000.12","volume":{"BTC":"12.5","USD":"812500.00","timestamp":1609459200000}}"#;
        let Decoded::Statistics24h {
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
        } = decode_pubticker_rest(raw, "BTCUSD").unwrap()
        else {
            panic!("pubticker");
        };
        assert!(open.is_none() && high.is_none() && low.is_none());
        assert_eq!(close.unwrap().0, Fixed::parse_str("65000.12").unwrap());
        assert_eq!(volume.unwrap().0, Fixed::parse_str("12.5").unwrap());
        assert_eq!(
            quote_volume.unwrap().0,
            Fixed::parse_str("812500.00").unwrap()
        );
    }
}
