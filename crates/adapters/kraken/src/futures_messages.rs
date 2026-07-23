//! Kraken Futures WS v1 public decode + REST charts candles (exact Fixed; no f64 arithmetic).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{AggressorSide, Fixed, Price, Quantity, Rate, SourceId, TimestampNs};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
// Keeping decoded events inline avoids a heap allocation on every hot-path ticker frame.
#[allow(clippy::large_enum_variant)]
pub enum FuturesDecoded {
    Trades(Vec<FuturesTrade>),
    /// `ticker` feed: BBO when present, plus optional mark/index/funding/OI + 24h stats.
    Ticker {
        product_id: String,
        bid_price: Option<Price>,
        bid_qty: Option<Quantity>,
        ask_price: Option<Price>,
        ask_qty: Option<Quantity>,
        mark: Option<Price>,
        index: Option<Price>,
        funding_rate: Option<Rate>,
        next_funding_ts: Option<TimestampNs>,
        open_interest: Option<Quantity>,
        open: Option<Price>,
        high: Option<Price>,
        low: Option<Price>,
        close: Option<Price>,
        volume: Option<Quantity>,
        quote_volume: Option<Quantity>,
        exchange_ts_ms: i64,
    },
    BookSnapshot {
        product_id: String,
        seq: u64,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_ts_ms: i64,
    },
    /// Incremental level update; `qty == 0` deletes the price level.
    BookDelta {
        product_id: String,
        seq: u64,
        side: BookSideWire,
        price: Price,
        quantity: Quantity,
        exchange_ts_ms: i64,
    },
    /// REST charts `GET /api/charts/v1/trade/{symbol}/{resolution}` latest bar.
    Candle {
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
    },
    SubscriptionState {
        state: String,
        success: bool,
    },
    Heartbeat,
    Info,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSideWire {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuturesTrade {
    pub product_id: String,
    pub uid: String,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub seq: Option<u64>,
    pub exchange_ts_ms: i64,
    /// Venue `type=liquidation` (no dedicated public liq channel).
    pub liquidation: bool,
}

pub fn decode_futures_text(bytes: &[u8]) -> Result<FuturesDecoded, String> {
    let v: Value = crate::json::value_from_slice(bytes)?;
    let obj = v
        .as_object()
        .ok_or_else(|| "futures frame not object".to_string())?;

    if let Some(event) = obj.get("event").and_then(|e| e.as_str()) {
        return match event {
            "subscribed" | "unsubscribed" => Ok(FuturesDecoded::SubscriptionState {
                state: event.to_string(),
                success: true,
            }),
            "subscribed_failed" | "unsubscribed_failed" => {
                let message = obj
                    .get("message")
                    .and_then(|message| message.as_str())
                    .unwrap_or("unspecified venue error");
                Ok(FuturesDecoded::SubscriptionState {
                    state: format!("{event}: {message}"),
                    success: false,
                })
            }
            "info" | "alert" => Ok(FuturesDecoded::Info),
            "heartbeat" => Ok(FuturesDecoded::Heartbeat),
            "pong" => Ok(FuturesDecoded::Heartbeat),
            _ => Ok(FuturesDecoded::Unknown),
        };
    }

    let feed = obj.get("feed").and_then(|f| f.as_str()).unwrap_or("");
    match feed {
        "trade" => decode_trade(obj).map(|t| FuturesDecoded::Trades(vec![t])),
        "trade_snapshot" => decode_trade_snapshot(obj),
        "ticker" => decode_ticker(obj),
        "book_snapshot" => decode_book_snapshot(obj),
        "book" => decode_book_delta(obj),
        "heartbeat" => Ok(FuturesDecoded::Heartbeat),
        _ => Ok(FuturesDecoded::Unknown),
    }
}

fn decode_trade(obj: &serde_json::Map<String, Value>) -> Result<FuturesTrade, String> {
    let product_id = obj
        .get("product_id")
        .and_then(|p| p.as_str())
        .ok_or_else(|| "trade missing product_id".to_string())?
        .to_string();
    let uid = match obj.get("uid") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return Err("trade missing uid".into()),
    };
    let side = obj.get("side").and_then(|s| s.as_str()).unwrap_or("");
    let aggressor = match side {
        "buy" => AggressorSide::Buy,
        "sell" => AggressorSide::Sell,
        _ => AggressorSide::Unknown,
    };
    // Docs: type is "fill" | "liquidation".
    let liquidation = obj
        .get("type")
        .and_then(|t| t.as_str())
        .is_some_and(|t| t.eq_ignore_ascii_case("liquidation"));
    Ok(FuturesTrade {
        product_id,
        uid,
        price: Price(fixed_from_json(
            obj.get("price")
                .ok_or_else(|| "trade missing price".to_string())?,
        )?),
        quantity: Quantity(fixed_from_json(
            obj.get("qty")
                .ok_or_else(|| "trade missing qty".to_string())?,
        )?),
        aggressor,
        seq: obj.get("seq").and_then(|s| s.as_u64()),
        exchange_ts_ms: obj
            .get("time")
            .and_then(|t| t.as_i64())
            .ok_or_else(|| "trade missing time".to_string())?,
        liquidation,
    })
}

fn decode_trade_snapshot(obj: &serde_json::Map<String, Value>) -> Result<FuturesDecoded, String> {
    let trades = obj
        .get("trades")
        .and_then(|t| t.as_array())
        .ok_or_else(|| "trade_snapshot missing trades".to_string())?;
    let mut out = Vec::with_capacity(trades.len());
    for row in trades {
        let o = row
            .as_object()
            .ok_or_else(|| "trade_snapshot row not object".to_string())?;
        out.push(decode_trade(o)?);
    }
    Ok(FuturesDecoded::Trades(out))
}

fn decode_ticker(obj: &serde_json::Map<String, Value>) -> Result<FuturesDecoded, String> {
    let product_id = obj
        .get("product_id")
        .and_then(|p| p.as_str())
        .ok_or_else(|| "ticker missing product_id".to_string())?
        .to_string();

    let (bid_price, ask_price, bid_qty, ask_qty) = match (obj.get("bid"), obj.get("ask")) {
        (Some(bid), Some(ask)) if !bid.is_null() && !ask.is_null() => (
            Some(Price(fixed_from_json(bid)?)),
            Some(Price(fixed_from_json(ask)?)),
            Some(Quantity(match obj.get("bid_size") {
                Some(v) if !v.is_null() => fixed_from_json(v)?,
                _ => Fixed::new(0, 0),
            })),
            Some(Quantity(match obj.get("ask_size") {
                Some(v) if !v.is_null() => fixed_from_json(v)?,
                _ => Fixed::new(0, 0),
            })),
        ),
        _ => (None, None, None, None),
    };

    let mark = optional_price(obj, "markPrice")?;
    let index = optional_price(obj, "index")?;
    let open_interest = optional_qty(obj, "openInterest")?;
    // Docs: funding_rate omitted when zero (perps only).
    let funding_rate = optional_rate(obj, "funding_rate")?;
    let next_funding_ts = obj
        .get("next_funding_rate_time")
        .and_then(|t| t.as_i64())
        .map(ms_to_ts);
    let open = optional_price(obj, "open")?;
    let high = optional_price(obj, "high")?;
    let low = optional_price(obj, "low")?;
    let close = optional_price(obj, "last")?;
    let volume = optional_qty(obj, "volume")?;
    let quote_volume = optional_qty(obj, "volumeQuote")?;

    if bid_price.is_none()
        && mark.is_none()
        && index.is_none()
        && funding_rate.is_none()
        && open_interest.is_none()
        && open.is_none()
        && high.is_none()
        && low.is_none()
        && close.is_none()
        && volume.is_none()
        && quote_volume.is_none()
    {
        return Ok(FuturesDecoded::Unknown);
    }

    Ok(FuturesDecoded::Ticker {
        product_id,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        mark,
        index,
        funding_rate,
        next_funding_ts,
        open_interest,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        exchange_ts_ms: obj.get("time").and_then(|t| t.as_i64()).unwrap_or(0),
    })
}

fn optional_price(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Price>, String> {
    match obj.get(key) {
        Some(v) if !v.is_null() => Ok(Some(Price(fixed_from_json(v)?))),
        _ => Ok(None),
    }
}

fn optional_qty(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Quantity>, String> {
    match obj.get(key) {
        Some(v) if !v.is_null() => Ok(Some(Quantity(fixed_from_json(v)?))),
        _ => Ok(None),
    }
}

fn optional_rate(obj: &serde_json::Map<String, Value>, key: &str) -> Result<Option<Rate>, String> {
    match obj.get(key) {
        Some(v) if !v.is_null() => Ok(Some(Rate(fixed_from_json(v)?))),
        _ => Ok(None),
    }
}

fn decode_book_snapshot(obj: &serde_json::Map<String, Value>) -> Result<FuturesDecoded, String> {
    let product_id = obj
        .get("product_id")
        .and_then(|p| p.as_str())
        .ok_or_else(|| "book_snapshot missing product_id".to_string())?
        .to_string();
    let seq = obj
        .get("seq")
        .and_then(|s| s.as_u64())
        .ok_or_else(|| "book_snapshot missing seq".to_string())?;
    Ok(FuturesDecoded::BookSnapshot {
        product_id,
        seq,
        bids: decode_levels(obj.get("bids").unwrap_or(&Value::Null))?,
        asks: decode_levels(obj.get("asks").unwrap_or(&Value::Null))?,
        exchange_ts_ms: obj.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0),
    })
}

fn decode_book_delta(obj: &serde_json::Map<String, Value>) -> Result<FuturesDecoded, String> {
    let product_id = obj
        .get("product_id")
        .and_then(|p| p.as_str())
        .ok_or_else(|| "book delta missing product_id".to_string())?
        .to_string();
    let seq = obj
        .get("seq")
        .and_then(|s| s.as_u64())
        .ok_or_else(|| "book delta missing seq".to_string())?;
    let side = match obj.get("side").and_then(|s| s.as_str()) {
        Some("buy") => BookSideWire::Bid,
        Some("sell") => BookSideWire::Ask,
        other => return Err(format!("book delta bad side: {other:?}")),
    };
    Ok(FuturesDecoded::BookDelta {
        product_id,
        seq,
        side,
        price: Price(fixed_from_json(
            obj.get("price")
                .ok_or_else(|| "book delta missing price".to_string())?,
        )?),
        quantity: Quantity(fixed_from_json(
            obj.get("qty")
                .ok_or_else(|| "book delta missing qty".to_string())?,
        )?),
        exchange_ts_ms: obj.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0),
    })
}

fn decode_levels(v: &Value) -> Result<Vec<(Price, Quantity)>, String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "book levels not array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let o = row
            .as_object()
            .ok_or_else(|| "book level not object".to_string())?;
        out.push((
            Price(fixed_from_json(
                o.get("price")
                    .ok_or_else(|| "level missing price".to_string())?,
            )?),
            Quantity(fixed_from_json(
                o.get("qty")
                    .ok_or_else(|| "level missing qty".to_string())?,
            )?),
        ));
    }
    Ok(out)
}

pub fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => parse_fixed_loose(s),
        Value::Number(n) => parse_fixed_loose(&n.to_string()),
        _ => Err("fixed value not string/number".into()),
    }
}

/// `Fixed::parse_str` rejects sci-notation; Kraken ticker numbers often stringify as `1e-11`.
///
/// ponytail: expand mantissa×10^exp to a decimal string (no f64). Ceiling = absurd exponents;
/// upgrade = keep raw JSON lexical form if serde ever exposes it.
fn parse_fixed_loose(s: &str) -> Result<Fixed, String> {
    match Fixed::parse_str(s) {
        Ok(f) => Ok(f),
        Err(_) => {
            let expanded = expand_scientific(s)?;
            Fixed::parse_str(&expanded).map_err(|e| e.to_string())
        }
    }
}

fn expand_scientific(s: &str) -> Result<String, String> {
    let s = s.trim();
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (mantissa, exp_str) = body
        .split_once(['e', 'E'])
        .ok_or_else(|| format!("not scientific: {s}"))?;
    let exp: i32 = exp_str
        .parse()
        .map_err(|_| format!("bad scientific exponent: {s}"))?;
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty() || !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("bad scientific mantissa: {s}"));
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("bad scientific fraction: {s}"));
    }
    let digits = format!("{int_part}{frac_part}");
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let point_from_left = int_part.len() as i32 + exp;
    let out = if point_from_left <= 0 {
        let zeros = usize::try_from((-point_from_left) as u32).unwrap_or(0);
        format!("0.{}{digits}", "0".repeat(zeros))
    } else {
        let split = point_from_left as usize;
        if split >= digits.len() {
            format!("{digits}{}", "0".repeat(split - digits.len()))
        } else {
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    };
    Ok(if neg && out != "0" && out != "0.0" {
        format!("-{out}")
    } else {
        out
    })
}

pub fn trade_id_source(uid: &str) -> SourceId {
    SourceId(uid.to_string())
}

pub fn ms_to_ts(ms: i64) -> marketfeed_model::TimestampNs {
    marketfeed_model::TimestampNs(ms.saturating_mul(1_000_000))
}

/// Charts path resolution (`1m` / `5m` / `15m` / `1h` / `1d`).
pub fn candle_resolution(interval: CandleInterval) -> &'static str {
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

/// Decode public charts REST body; emits the **latest** candle (last row).
///
/// Wire: `{"candles":[{"time":ms,"open":"...","high":"...","low":"...","close":"...","volume":"..."}],...}`
/// `# ponytail`: poll re-emits latest bar each tick (no close-only filter); ceiling = partial bar.
pub fn decode_charts_rest(
    bytes: &[u8],
    interval: CandleInterval,
) -> Result<FuturesDecoded, String> {
    let v: Value = crate::json::value_from_slice(bytes)?;
    let candles = v
        .get("candles")
        .and_then(|c| c.as_array())
        .ok_or_else(|| "kf charts candles missing".to_string())?;
    let row = candles
        .last()
        .and_then(|r| r.as_object())
        .ok_or_else(|| "kf charts candles empty".to_string())?;
    let start_ms = match row.get("time") {
        Some(Value::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|u| u as i64))
            .ok_or_else(|| "candle time not i64".to_string())?,
        Some(Value::String(s)) => s.parse::<i64>().map_err(|e| format!("candle time: {e}"))?,
        _ => return Err("candle missing time".into()),
    };
    Ok(FuturesDecoded::Candle {
        open: Price(fixed_from_json(
            row.get("open").ok_or("candle missing open")?,
        )?),
        high: Price(fixed_from_json(
            row.get("high").ok_or("candle missing high")?,
        )?),
        low: Price(fixed_from_json(
            row.get("low").ok_or("candle missing low")?,
        )?),
        close: Price(fixed_from_json(
            row.get("close").ok_or("candle missing close")?,
        )?),
        volume: Quantity(fixed_from_json(
            row.get("volume").ok_or("candle missing volume")?,
        )?),
        interval_ns: candle_interval_ns(interval),
        start_ts: ms_to_ts(start_ms),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_trade_exact_fixed() {
        let raw = br#"{"feed":"trade","product_id":"PF_XBTUSD","uid":"caa9c653-420b-4c24-a9f1-462a054d86f1","side":"sell","type":"fill","seq":655508,"time":1612269657781,"qty":440,"price":34893}"#;
        let FuturesDecoded::Trades(t) = decode_futures_text(raw).unwrap() else {
            panic!("expected trade");
        };
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].price.0, Fixed::parse_str("34893").unwrap());
        assert_eq!(t[0].quantity.0, Fixed::parse_str("440").unwrap());
        assert_eq!(t[0].aggressor, AggressorSide::Sell);
        assert!(!t[0].liquidation);
    }

    #[test]
    fn decode_liquidation_trade_flag() {
        let raw = br#"{"feed":"trade","product_id":"PF_XBTUSD","uid":"liq-1","side":"buy","type":"liquidation","seq":1,"time":1612269657781,"qty":100,"price":35000}"#;
        let FuturesDecoded::Trades(t) = decode_futures_text(raw).unwrap() else {
            panic!("expected trade");
        };
        assert!(t[0].liquidation);
        assert_eq!(t[0].price.0, Fixed::parse_str("35000").unwrap());
    }

    #[test]
    fn decode_ticker_quote_and_derivatives() {
        let raw = br#"{"time":1676393235406,"product_id":"PF_XBTUSD","funding_rate":0.0001,"next_funding_rate_time":1676394000000,"feed":"ticker","bid":21978.5,"ask":21987.0,"bid_size":2536.0,"ask_size":13948.0,"index":21984.54,"openInterest":30072580.0,"markPrice":21979.5}"#;
        let FuturesDecoded::Ticker {
            bid_price,
            ask_price,
            mark,
            index,
            funding_rate,
            next_funding_ts,
            open_interest,
            ..
        } = decode_futures_text(raw).unwrap()
        else {
            panic!("expected ticker");
        };
        assert_eq!(bid_price.unwrap().0, Fixed::parse_str("21978.5").unwrap());
        assert_eq!(ask_price.unwrap().0, Fixed::parse_str("21987.0").unwrap());
        assert_eq!(mark.unwrap().0, Fixed::parse_str("21979.5").unwrap());
        assert_eq!(index.unwrap().0, Fixed::parse_str("21984.54").unwrap());
        assert_eq!(funding_rate.unwrap().0, Fixed::parse_str("0.0001").unwrap());
        assert_eq!(
            next_funding_ts,
            Some(TimestampNs(1_676_394_000_000_000_000))
        );
        assert_eq!(
            open_interest.unwrap().0,
            Fixed::parse_str("30072580.0").unwrap()
        );
    }

    #[test]
    fn expand_scientific_funding_rate() {
        assert_eq!(
            parse_fixed_loose("-6.2604214e-11").unwrap(),
            Fixed::parse_str("-0.000000000062604214").unwrap()
        );
        assert_eq!(
            parse_fixed_loose("1.5e3").unwrap(),
            Fixed::parse_str("1500").unwrap()
        );
    }

    #[test]
    fn decode_ticker_quote() {
        let raw = br#"{"feed":"ticker","product_id":"PF_XBTUSD","bid":21978.5,"ask":21987.0,"bid_size":2536.0,"ask_size":13948.0,"time":1676393235406}"#;
        let FuturesDecoded::Ticker {
            bid_price,
            ask_price,
            ..
        } = decode_futures_text(raw).unwrap()
        else {
            panic!("expected ticker");
        };
        assert_eq!(bid_price.unwrap().0, Fixed::parse_str("21978.5").unwrap());
        assert_eq!(ask_price.unwrap().0, Fixed::parse_str("21987.0").unwrap());
    }

    #[test]
    fn decode_charts_rest_exact_fixed() {
        let raw = br#"{"candles":[{"time":1609459200000,"open":"28050.0","high":"28150","low":"27983.0","close":"28126.0","volume":"1089794.00000000"}],"more_candles":false}"#;
        let FuturesDecoded::Candle {
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
        } = decode_charts_rest(raw, CandleInterval::M1).unwrap()
        else {
            panic!("candle");
        };
        assert_eq!(open.0, Fixed::parse_str("28050.0").unwrap());
        assert_eq!(high.0, Fixed::parse_str("28150").unwrap());
        assert_eq!(low.0, Fixed::parse_str("27983.0").unwrap());
        assert_eq!(close.0, Fixed::parse_str("28126.0").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("1089794.00000000").unwrap());
        assert_eq!(interval_ns, 60_000_000_000);
        assert_eq!(start_ts, TimestampNs(1_609_459_200_000_000_000));
    }

    #[test]
    fn decode_charts_rest_picks_latest_row() {
        let raw = br#"{"candles":[
            {"time":1609459140000,"open":"1","high":"1","low":"1","close":"1","volume":"1"},
            {"time":1609459200000,"open":"2","high":"2","low":"2","close":"2","volume":"2"}
        ]}"#;
        let FuturesDecoded::Candle {
            close, start_ts, ..
        } = decode_charts_rest(raw, CandleInterval::M1).unwrap()
        else {
            panic!("candle");
        };
        assert_eq!(close.0, Fixed::parse_str("2").unwrap());
        assert_eq!(start_ts, TimestampNs(1_609_459_200_000_000_000));
    }

    #[test]
    fn decode_book_snapshot_and_delta() {
        let snap = br#"{"feed":"book_snapshot","product_id":"PF_XBTUSD","timestamp":1612269825817,"seq":10,"bids":[{"price":34892.5,"qty":6385}],"asks":[{"price":34911.5,"qty":20598}]}"#;
        let FuturesDecoded::BookSnapshot {
            bids, asks, seq, ..
        } = decode_futures_text(snap).unwrap()
        else {
            panic!("snapshot");
        };
        assert_eq!(seq, 10);
        assert_eq!(bids.len(), 1);
        assert_eq!(asks.len(), 1);

        let delta = br#"{"feed":"book","product_id":"PF_XBTUSD","side":"sell","seq":11,"price":34981,"qty":0,"timestamp":1612269953629}"#;
        let FuturesDecoded::BookDelta {
            side,
            quantity,
            seq,
            ..
        } = decode_futures_text(delta).unwrap()
        else {
            panic!("delta");
        };
        assert_eq!(seq, 11);
        assert_eq!(side, BookSideWire::Ask);
        assert_eq!(quantity.0, Fixed::parse_str("0").unwrap());
    }
}
