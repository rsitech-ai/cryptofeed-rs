//! Binance Spot JSON message decoding (exact Fixed; no f64).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{
    AggressorSide, BookLevel, BookOperation, BookSide, Fixed, Price, Quantity, SourceId,
    TimestampNs,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedEvent {
    Trade {
        symbol: String,
        trade_id: u64,
        price: Price,
        quantity: Quantity,
        aggressor: AggressorSide,
        exchange_ts_ms: i64,
    },
    Quote {
        symbol: String,
        update_id: u64,
        bid_price: Price,
        bid_qty: Quantity,
        ask_price: Price,
        ask_qty: Quantity,
    },
    /// `@ticker` / `24hrTicker` — BBO plus rolling 24h stats.
    Ticker24h {
        symbol: String,
        bid_price: Price,
        bid_qty: Quantity,
        ask_price: Price,
        ask_qty: Quantity,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        quote_volume: Quantity,
        exchange_ts_ms: i64,
    },
    Candle {
        symbol: String,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
        exchange_ts_ms: i64,
        /// Venue `k.x`: bar closed.
        is_closed: bool,
    },
    DepthUpdate {
        symbol: String,
        first_update_id: u64,
        final_update_id: u64,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_ts_ms: i64,
    },
    DepthSnapshot {
        last_update_id: u64,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
    },
    SubscribeAck {
        id: Option<u64>,
    },
    Unknown,
}

#[derive(Debug, Deserialize)]
struct TradeMsg {
    #[allow(dead_code)]
    e: Option<String>,
    s: String,
    t: u64,
    p: String,
    q: String,
    #[serde(rename = "T")]
    trade_time: i64,
    /// Buyer is maker → taker/aggressor is seller.
    m: bool,
}

#[derive(Debug, Deserialize)]
struct BookTickerMsg {
    u: u64,
    s: String,
    b: String,
    #[serde(rename = "B")]
    bid_qty: String,
    a: String,
    #[serde(rename = "A")]
    ask_qty: String,
}

#[derive(Debug, Deserialize)]
struct Ticker24hMsg {
    #[allow(dead_code)]
    e: Option<String>,
    #[serde(rename = "E")]
    event_time: i64,
    s: String,
    o: String,
    h: String,
    l: String,
    c: String,
    v: String,
    q: String,
    b: String,
    #[serde(rename = "B")]
    bid_qty: String,
    a: String,
    #[serde(rename = "A")]
    ask_qty: String,
}

#[derive(Debug, Deserialize)]
struct DepthUpdateMsg {
    #[allow(dead_code)]
    e: Option<String>,
    #[serde(rename = "E")]
    event_time: i64,
    s: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    b: Vec<[String; 2]>,
    a: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct DepthSnapshotMsg {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct ResultMsg {
    #[allow(dead_code)]
    result: Option<Value>,
    id: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct KlineMsg {
    #[serde(rename = "E")]
    event_time: i64,
    s: String,
    k: KlineInner,
}

#[derive(Debug, Deserialize)]
struct KlineInner {
    #[serde(rename = "t")]
    start_ms: i64,
    i: String,
    o: String,
    h: String,
    l: String,
    c: String,
    v: String,
    x: bool,
}

/// Binance `@kline_*` stream suffix for a canonical interval.
pub fn kline_stream_interval(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "1m",
        CandleInterval::M5 => "5m",
        CandleInterval::M15 => "15m",
        CandleInterval::H1 => "1h",
        CandleInterval::D1 => "1d",
    }
}

/// Canonical interval length in nanoseconds.
pub fn candle_interval_ns(interval: CandleInterval) -> i64 {
    match interval {
        CandleInterval::M1 => 60_000_000_000,
        CandleInterval::M5 => 300_000_000_000,
        CandleInterval::M15 => 900_000_000_000,
        CandleInterval::H1 => 3_600_000_000_000,
        CandleInterval::D1 => 86_400_000_000_000,
    }
}

pub(crate) fn interval_ns_from_binance(i: &str) -> Result<i64, String> {
    let interval = match i {
        "1m" => CandleInterval::M1,
        "5m" => CandleInterval::M5,
        "15m" => CandleInterval::M15,
        "1h" => CandleInterval::H1,
        "1d" => CandleInterval::D1,
        other => return Err(format!("unsupported kline interval {other}")),
    };
    Ok(candle_interval_ns(interval))
}

/// Decode Spot WS/REST JSON into a canonical [`DecodedEvent`].
///
/// Default: `serde_json`. With `--features simd-json`, the initial slice→`Value`
/// parse uses the shared `json` helper — same `decode_value` path so
/// events stay identical (parity-tested).
pub fn decode_text(bytes: &[u8]) -> Result<DecodedEvent, String> {
    let v: Value = crate::json::value_from_slice(bytes)?;
    decode_value(&v)
}

/// Reference decode that always uses `serde_json` (parity oracle).
pub fn decode_text_serde(bytes: &[u8]) -> Result<DecodedEvent, String> {
    let v: Value = crate::json::value_from_slice_serde(bytes)?;
    decode_value(&v)
}

/// Feature-gated simd-json decode (parity probe; same canonical events as serde).
#[cfg(feature = "simd-json")]
pub fn decode_text_simd(bytes: &[u8]) -> Result<DecodedEvent, String> {
    let v: Value = crate::json::value_from_slice_simd(bytes)?;
    decode_value(&v)
}

fn decode_value(v: &Value) -> Result<DecodedEvent, String> {
    // Combined stream wrapper: {"stream":"...","data":{...}}
    if let Some(obj) = v.as_object() {
        if obj.contains_key("stream") {
            if let Some(data) = obj.get("data") {
                return decode_value(data);
            }
        }
        if obj.contains_key("result") && (obj.contains_key("id") || obj.len() <= 2) {
            let ack: ResultMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
            return Ok(DecodedEvent::SubscribeAck { id: ack.id });
        }
        match obj.get("e").and_then(|x| x.as_str()) {
            Some("trade") => return decode_trade(v),
            Some("kline") => return decode_kline(v),
            Some("depthUpdate") => return decode_depth_update(v),
            Some("24hrTicker") => return decode_24hr_ticker(v),
            _ => {}
        }
        if obj.contains_key("lastUpdateId") && obj.contains_key("bids") {
            return decode_snapshot(v);
        }
        // bookTicker has no "e" field.
        if obj.contains_key("b")
            && obj.contains_key("B")
            && obj.contains_key("a")
            && obj.contains_key("A")
            && obj.contains_key("u")
            && obj.contains_key("s")
            && !obj.contains_key("e")
        {
            return decode_book_ticker(v);
        }
    }
    Ok(DecodedEvent::Unknown)
}

fn decode_kline(v: &Value) -> Result<DecodedEvent, String> {
    let m: KlineMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(DecodedEvent::Candle {
        symbol: m.s,
        open: Price(parse_fixed(&m.k.o)?),
        high: Price(parse_fixed(&m.k.h)?),
        low: Price(parse_fixed(&m.k.l)?),
        close: Price(parse_fixed(&m.k.c)?),
        volume: Quantity(parse_fixed(&m.k.v)?),
        interval_ns: interval_ns_from_binance(&m.k.i)?,
        start_ts: TimestampNs(m.k.start_ms.saturating_mul(1_000_000)),
        exchange_ts_ms: m.event_time,
        is_closed: m.k.x,
    })
}

fn decode_trade(v: &Value) -> Result<DecodedEvent, String> {
    let m: TradeMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    let aggressor = if m.m {
        AggressorSide::Sell
    } else {
        AggressorSide::Buy
    };
    Ok(DecodedEvent::Trade {
        symbol: m.s,
        trade_id: m.t,
        price: Price(parse_fixed(&m.p)?),
        quantity: Quantity(parse_fixed(&m.q)?),
        aggressor,
        exchange_ts_ms: m.trade_time,
    })
}

fn decode_book_ticker(v: &Value) -> Result<DecodedEvent, String> {
    let m: BookTickerMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(DecodedEvent::Quote {
        symbol: m.s,
        update_id: m.u,
        bid_price: Price(parse_fixed(&m.b)?),
        bid_qty: Quantity(parse_fixed(&m.bid_qty)?),
        ask_price: Price(parse_fixed(&m.a)?),
        ask_qty: Quantity(parse_fixed(&m.ask_qty)?),
    })
}

fn decode_24hr_ticker(v: &Value) -> Result<DecodedEvent, String> {
    let m: Ticker24hMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(DecodedEvent::Ticker24h {
        symbol: m.s,
        bid_price: Price(parse_fixed(&m.b)?),
        bid_qty: Quantity(parse_fixed(&m.bid_qty)?),
        ask_price: Price(parse_fixed(&m.a)?),
        ask_qty: Quantity(parse_fixed(&m.ask_qty)?),
        open: Price(parse_fixed(&m.o)?),
        high: Price(parse_fixed(&m.h)?),
        low: Price(parse_fixed(&m.l)?),
        close: Price(parse_fixed(&m.c)?),
        volume: Quantity(parse_fixed(&m.v)?),
        quote_volume: Quantity(parse_fixed(&m.q)?),
        exchange_ts_ms: m.event_time,
    })
}

fn decode_depth_update(v: &Value) -> Result<DecodedEvent, String> {
    let m: DepthUpdateMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(DecodedEvent::DepthUpdate {
        symbol: m.s,
        first_update_id: m.first_update_id,
        final_update_id: m.final_update_id,
        bids: parse_levels(&m.b)?,
        asks: parse_levels(&m.a)?,
        exchange_ts_ms: m.event_time,
    })
}

fn decode_snapshot(v: &Value) -> Result<DecodedEvent, String> {
    let m: DepthSnapshotMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(DecodedEvent::DepthSnapshot {
        last_update_id: m.last_update_id,
        bids: parse_levels(&m.bids)?,
        asks: parse_levels(&m.asks)?,
    })
}

fn parse_levels(rows: &[[String; 2]]) -> Result<Vec<(Price, Quantity)>, String> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push((
            Price(parse_fixed(&row[0])?),
            Quantity(parse_fixed(&row[1])?),
        ));
    }
    Ok(out)
}

fn parse_fixed(s: &str) -> Result<Fixed, String> {
    Fixed::parse_str(s).map_err(|e| e.to_string())
}

pub fn level_op(qty: Quantity) -> (BookOperation, Option<Quantity>) {
    if qty.0.coefficient == 0 {
        (BookOperation::Delete, None)
    } else {
        (BookOperation::Upsert, Some(qty))
    }
}

pub fn to_book_levels(side: BookSide, levels: &[(Price, Quantity)]) -> Vec<BookLevel> {
    let _ = side;
    levels
        .iter()
        .map(|(p, q)| BookLevel {
            price: *p,
            quantity: *q,
        })
        .collect()
}

pub fn trade_id_source(id: u64) -> SourceId {
    SourceId(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_trade_aggressor_from_maker_flag() {
        let raw = br#"{"e":"trade","E":1,"s":"BTCUSDT","t":10,"p":"100.50","q":"0.010","T":2,"m":true,"M":true}"#;
        let DecodedEvent::Trade {
            aggressor, price, ..
        } = decode_text(raw).unwrap()
        else {
            panic!("expected trade");
        };
        assert_eq!(aggressor, AggressorSide::Sell);
        assert_eq!(price.0, Fixed::new(10050, 2));

        let raw = br#"{"e":"trade","E":1,"s":"BTCUSDT","t":11,"p":"100.50","q":"0.010","T":2,"m":false,"M":true}"#;
        let DecodedEvent::Trade { aggressor, .. } = decode_text(raw).unwrap() else {
            panic!("expected trade");
        };
        assert_eq!(aggressor, AggressorSide::Buy);
    }

    #[test]
    fn decode_24hr_ticker_exact_fixed() {
        let raw = br#"{"e":"24hrTicker","E":1000,"s":"BTCUSDT","p":"100","P":"1.00","w":"100","x":"99","c":"65000.12","Q":"0.1","b":"65000.00","B":"1.2","a":"65000.10","A":"0.8","o":"64000.00","h":"66000.50","l":"63000.25","v":"12.5","q":"812500.00","O":0,"C":86400000,"F":0,"L":1,"n":1}"#;
        let DecodedEvent::Ticker24h {
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            bid_price,
            ask_price,
            exchange_ts_ms,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("ticker24h");
        };
        assert_eq!(open.0, Fixed::new(6400000, 2));
        assert_eq!(high.0, Fixed::new(6600050, 2));
        assert_eq!(low.0, Fixed::new(6300025, 2));
        assert_eq!(close.0, Fixed::new(6500012, 2));
        assert_eq!(volume.0, Fixed::new(125, 1));
        assert_eq!(quote_volume.0, Fixed::new(81250000, 2));
        assert_eq!(bid_price.0, Fixed::new(6500000, 2));
        assert_eq!(ask_price.0, Fixed::new(6500010, 2));
        assert_eq!(exchange_ts_ms, 1000);
    }

    #[test]
    fn decode_book_ticker_and_combined() {
        let raw = br#"{"u":1,"s":"BTCUSDT","b":"100.00","B":"1.5","a":"100.01","A":"2.0"}"#;
        let DecodedEvent::Quote {
            bid_price,
            ask_price,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("quote");
        };
        assert_eq!(bid_price.0.coefficient, 10000);
        assert_eq!(ask_price.0.coefficient, 10001);

        let wrapped = br#"{"stream":"btcusdt@bookTicker","data":{"u":2,"s":"BTCUSDT","b":"1.00","B":"1","a":"1.01","A":"1"}}"#;
        assert!(matches!(
            decode_text(wrapped).unwrap(),
            DecodedEvent::Quote { .. }
        ));
    }

    #[test]
    fn decode_kline_exact_fixed() {
        let raw = br#"{"e":"kline","E":123456789,"s":"BTCUSDT","k":{"t":123400000,"T":123460000,"s":"BTCUSDT","i":"1m","f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":false,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}"#;
        let DecodedEvent::Candle {
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
            is_closed,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("kline");
        };
        assert_eq!(open.0, Fixed::new(10, 4));
        assert_eq!(high.0, Fixed::new(25, 4));
        assert_eq!(low.0, Fixed::new(15, 4));
        assert_eq!(close.0, Fixed::new(20, 4));
        assert_eq!(volume.0, Fixed::new(1000, 0));
        assert_eq!(interval_ns, candle_interval_ns(CandleInterval::M1));
        assert_eq!(start_ts, TimestampNs(123_400_000_000_000));
        assert!(!is_closed);
    }

    #[test]
    fn decode_depth_update_zero_qty_is_delete_candidate() {
        let raw = br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":100,"u":102,"b":[["100.00","0"],["99.00","1.5"]],"a":[["101.00","2"]]}"#;
        let DecodedEvent::DepthUpdate { bids, .. } = decode_text(raw).unwrap() else {
            panic!("depth");
        };
        let (op, q) = level_op(bids[0].1);
        assert_eq!(op, BookOperation::Delete);
        assert!(q.is_none());
    }

    /// Active backend must match the serde oracle (default = both serde).
    #[test]
    fn decode_text_matches_serde_oracle() {
        for raw in spot_parity_fixtures() {
            assert_eq!(
                decode_text(raw).unwrap(),
                decode_text_serde(raw).unwrap(),
                "active vs serde oracle diverged on {}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[cfg(feature = "simd-json")]
    #[test]
    fn decode_text_serde_simd_canonical_parity() {
        for raw in spot_parity_fixtures() {
            let serde_ev = decode_text_serde(raw).unwrap();
            let simd_ev = decode_text_simd(raw).unwrap();
            assert_eq!(
                serde_ev,
                simd_ev,
                "serde vs simd diverged on {}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    fn spot_parity_fixtures() -> &'static [&'static [u8]] {
        &[
            br#"{"e":"trade","E":1,"s":"BTCUSDT","t":10,"p":"100.50","q":"0.010","T":2,"m":true,"M":true}"#,
            br#"{"e":"trade","E":1,"s":"BTCUSDT","t":11,"p":"100.50","q":"0.010","T":2,"m":false,"M":true}"#,
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT","t":12,"p":"99.00","q":"1.0","T":3,"m":false}}"#,
            br#"{"u":1,"s":"BTCUSDT","b":"100.00","B":"1.5","a":"100.01","A":"2.0"}"#,
            br#"{"e":"kline","E":123456789,"s":"BTCUSDT","k":{"t":123400000,"T":123460000,"s":"BTCUSDT","i":"1m","f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":false,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}"#,
            br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":100,"u":102,"b":[["100.00","0"],["99.00","1.5"]],"a":[["101.00","2"]]}"#,
            br#"{"result":null,"id":1}"#,
        ]
    }
}
