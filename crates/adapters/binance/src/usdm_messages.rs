//! Binance USD-M futures JSON decode (aggTrade / bookTicker / ticker / mark / indexPrice / depth / forceOrder / OI / kline).

use marketfeed_model::{
    AggressorSide, BookLevel, BookOperation, BookSide, Fixed, Price, Quantity, Rate, SourceId,
    TimestampNs,
};
use serde::Deserialize;
use serde_json::Value;

use crate::messages::interval_ns_from_binance;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsdmDecoded {
    AggTrade {
        symbol: String,
        agg_id: u64,
        price: Price,
        quantity: Quantity,
        aggressor: AggressorSide,
        /// Outer message event time (`E`) retained separately from trade time.
        event_time_ms: Option<i64>,
        /// Venue aggregate-trade transaction time (`T`).
        exchange_ts_ms: i64,
    },
    Quote {
        symbol: String,
        update_id: u64,
        /// Venue message output time (`E`) when the source shape provides it.
        event_time_ms: Option<i64>,
        /// Venue transaction time (`T`) when the source shape provides it.
        transaction_time_ms: Option<i64>,
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
    MarkPrice {
        symbol: String,
        mark: Price,
        index: Price,
        funding_rate: Rate,
        next_funding_ts: TimestampNs,
        exchange_ts_ms: i64,
    },
    /// Dedicated `<symbol>@indexPrice@1s` (peer OKX `index-tickers`; mark stream also carries `i`).
    IndexPrice {
        symbol: String,
        price: Price,
        exchange_ts_ms: i64,
    },
    DepthUpdate {
        symbol: String,
        first_update_id: u64,
        final_update_id: u64,
        /// Previous event's `u` — futures continuity field.
        prev_final_update_id: u64,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        /// Venue message output time (`E`).
        event_time_ms: i64,
        /// Venue transaction time (`T`) on the current USD-M shape.
        transaction_time_ms: Option<i64>,
    },
    DepthSnapshot {
        last_update_id: u64,
        /// Venue message output time (`E`) on the current USD-M REST shape.
        event_time_ms: Option<i64>,
        /// Venue transaction time (`T`) retained separately from `E`.
        transaction_time_ms: Option<i64>,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
    },
    /// REST `/fapi/v1/openInterest` body (USD-M has no public OI WS stream).
    OpenInterest {
        symbol: String,
        quantity: Quantity,
        exchange_ts_ms: i64,
    },
    ForceOrder {
        symbol: String,
        price: Price,
        quantity: Quantity,
        side: AggressorSide,
        /// Outer force-order event time (`E`).
        outer_event_time_ms: i64,
        /// Inner order transaction time (`o.T`).
        inner_transaction_time_ms: Option<i64>,
    },
    SubscribeAck {
        id: Option<u64>,
    },
    /// Recognized exchange administrative record that is intentionally not a
    /// canonical market event.
    Ignored,
    Unknown,
}

#[derive(Debug, Deserialize)]
struct AggTradeMsg {
    #[serde(rename = "E")]
    event_time: Option<i64>,
    s: String,
    a: u64,
    p: String,
    q: String,
    #[serde(rename = "T")]
    trade_time: i64,
    m: bool,
}

/// Individual trade (`e=trade` / `@trade`) — preferred over silent `@aggTrade`.
#[derive(Debug, Deserialize)]
struct TradeMsg {
    #[serde(rename = "E")]
    event_time: Option<i64>,
    s: String,
    t: u64,
    p: String,
    q: String,
    #[serde(rename = "T")]
    trade_time: i64,
    m: bool,
}

#[derive(Debug, Deserialize)]
struct BookTickerMsg {
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "T")]
    transaction_time: Option<i64>,
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
struct MarkPriceMsg {
    s: String,
    p: String,
    i: String,
    r: String,
    #[serde(rename = "T")]
    next_funding_ms: i64,
    #[serde(rename = "E")]
    event_time: i64,
}

#[derive(Debug, Deserialize)]
struct DepthUpdateMsg {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "T")]
    transaction_time: Option<i64>,
    s: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    pu: u64,
    b: Vec<[String; 2]>,
    a: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct DepthSnapshotMsg {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "T")]
    transaction_time: Option<i64>,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct OpenInterestMsg {
    symbol: String,
    #[serde(rename = "openInterest")]
    open_interest: String,
    time: i64,
}

#[derive(Debug, Deserialize)]
struct ForceOrderMsg {
    #[serde(rename = "E")]
    event_time: i64,
    o: ForceOrderInner,
}

#[derive(Debug, Deserialize)]
struct ForceOrderInner {
    s: String,
    #[serde(rename = "S")]
    side: String,
    /// Average fill price.
    ap: String,
    /// Last filled quantity.
    l: String,
    #[serde(rename = "T")]
    transaction_time: Option<i64>,
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

#[derive(Debug, Deserialize)]
struct ResultMsg {
    id: Option<u64>,
}

/// Decode USD-M WS/REST JSON into a canonical [`UsdmDecoded`].
///
/// Default: `serde_json`. With `--features simd-json`, the initial slice→`Value`
/// parse uses the shared `json` helper — same `decode_value` path so
/// events stay identical (parity-tested).
pub fn decode_text(bytes: &[u8]) -> Result<UsdmDecoded, String> {
    let v: Value = crate::json::value_from_slice(bytes)?;
    decode_value(&v)
}

/// Reference decode that always uses `serde_json` (parity oracle).
pub fn decode_text_serde(bytes: &[u8]) -> Result<UsdmDecoded, String> {
    let v: Value = crate::json::value_from_slice_serde(bytes)?;
    decode_value(&v)
}

/// Feature-gated simd-json decode (parity probe; same canonical events as serde).
#[cfg(feature = "simd-json")]
pub fn decode_text_simd(bytes: &[u8]) -> Result<UsdmDecoded, String> {
    let v: Value = crate::json::value_from_slice_simd(bytes)?;
    decode_value(&v)
}

fn decode_value(v: &Value) -> Result<UsdmDecoded, String> {
    if let Some(obj) = v.as_object() {
        if obj.contains_key("stream") {
            if let Some(data) = obj.get("data") {
                return decode_value(data);
            }
        }
        if obj.contains_key("result") && (obj.contains_key("id") || obj.len() <= 2) {
            let ack: ResultMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
            return Ok(UsdmDecoded::SubscribeAck { id: ack.id });
        }
        match obj.get("e").and_then(|x| x.as_str()) {
            Some("aggTrade") => return decode_agg_trade(v),
            Some("trade") => return decode_trade(v),
            Some("kline") => return decode_kline(v),
            Some("24hrTicker") => return decode_24hr_ticker(v),
            Some("bookTicker") => return decode_book_ticker(v),
            Some("markPriceUpdate") => return decode_mark_price(v),
            Some("indexPriceUpdate") => return decode_index_price(v),
            Some("depthUpdate") => return decode_depth_update(v),
            Some("forceOrder") => return decode_force_order(v),
            _ => {}
        }
        if obj.contains_key("lastUpdateId") && obj.contains_key("bids") {
            return decode_snapshot(v);
        }
        if obj.contains_key("openInterest") && obj.contains_key("symbol") {
            return decode_open_interest(v);
        }
        // bookTicker: no e field.
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
    Ok(UsdmDecoded::Unknown)
}

fn decode_kline(v: &Value) -> Result<UsdmDecoded, String> {
    let m: KlineMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::Candle {
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

fn decode_agg_trade(v: &Value) -> Result<UsdmDecoded, String> {
    let m: AggTradeMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    let price = Price(parse_fixed(&m.p)?);
    let quantity = Quantity(parse_fixed(&m.q)?);
    if price.0.coefficient <= 0 || quantity.0.coefficient <= 0 {
        return Ok(UsdmDecoded::Ignored);
    }
    let aggressor = if m.m {
        AggressorSide::Sell
    } else {
        AggressorSide::Buy
    };
    Ok(UsdmDecoded::AggTrade {
        symbol: m.s,
        agg_id: m.a,
        price,
        quantity,
        aggressor,
        event_time_ms: m.event_time,
        exchange_ts_ms: m.trade_time,
    })
}

fn decode_trade(v: &Value) -> Result<UsdmDecoded, String> {
    let m: TradeMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    let price = Price(parse_fixed(&m.p)?);
    let quantity = Quantity(parse_fixed(&m.q)?);
    // USD-M occasionally publishes administrative `trade` records with
    // p="0", q="0", X="NA". They carry an id but are not executable tape
    // prints and must not contaminate profiles or order-flow analytics.
    if price.0.coefficient <= 0 || quantity.0.coefficient <= 0 {
        return Ok(UsdmDecoded::Ignored);
    }
    let aggressor = if m.m {
        AggressorSide::Sell
    } else {
        AggressorSide::Buy
    };
    // Reuse AggTrade variant — `agg_id` carries the venue trade id (`t`).
    Ok(UsdmDecoded::AggTrade {
        symbol: m.s,
        agg_id: m.t,
        price,
        quantity,
        aggressor,
        event_time_ms: m.event_time,
        exchange_ts_ms: m.trade_time,
    })
}

fn decode_book_ticker(v: &Value) -> Result<UsdmDecoded, String> {
    let m: BookTickerMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::Quote {
        symbol: m.s,
        update_id: m.u,
        event_time_ms: m.event_time,
        transaction_time_ms: m.transaction_time,
        bid_price: Price(parse_fixed(&m.b)?),
        bid_qty: Quantity(parse_fixed(&m.bid_qty)?),
        ask_price: Price(parse_fixed(&m.a)?),
        ask_qty: Quantity(parse_fixed(&m.ask_qty)?),
    })
}

fn decode_24hr_ticker(v: &Value) -> Result<UsdmDecoded, String> {
    let m: Ticker24hMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::Ticker24h {
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

fn decode_mark_price(v: &Value) -> Result<UsdmDecoded, String> {
    let m: MarkPriceMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::MarkPrice {
        symbol: m.s,
        mark: Price(parse_fixed(&m.p)?),
        index: Price(parse_fixed(&m.i)?),
        funding_rate: Rate(parse_fixed(&m.r)?),
        next_funding_ts: TimestampNs(m.next_funding_ms.saturating_mul(1_000_000)),
        exchange_ts_ms: m.event_time,
    })
}

#[derive(Debug, Deserialize)]
struct IndexPriceMsg {
    #[serde(rename = "E")]
    event_time: i64,
    /// Pair / symbol (USD-M: `BTCUSDT`).
    i: String,
    p: String,
}

fn decode_index_price(v: &Value) -> Result<UsdmDecoded, String> {
    let m: IndexPriceMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::IndexPrice {
        symbol: m.i,
        price: Price(parse_fixed(&m.p)?),
        exchange_ts_ms: m.event_time,
    })
}

fn decode_depth_update(v: &Value) -> Result<UsdmDecoded, String> {
    let m: DepthUpdateMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::DepthUpdate {
        symbol: m.s,
        first_update_id: m.first_update_id,
        final_update_id: m.final_update_id,
        prev_final_update_id: m.pu,
        bids: parse_levels(&m.b)?,
        asks: parse_levels(&m.a)?,
        event_time_ms: m.event_time,
        transaction_time_ms: m.transaction_time,
    })
}

fn decode_snapshot(v: &Value) -> Result<UsdmDecoded, String> {
    let m: DepthSnapshotMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::DepthSnapshot {
        last_update_id: m.last_update_id,
        event_time_ms: m.event_time,
        transaction_time_ms: m.transaction_time,
        bids: parse_levels(&m.bids)?,
        asks: parse_levels(&m.asks)?,
    })
}

fn decode_open_interest(v: &Value) -> Result<UsdmDecoded, String> {
    let m: OpenInterestMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    Ok(UsdmDecoded::OpenInterest {
        symbol: m.symbol,
        quantity: Quantity(parse_fixed(&m.open_interest)?),
        exchange_ts_ms: m.time,
    })
}

fn decode_force_order(v: &Value) -> Result<UsdmDecoded, String> {
    let m: ForceOrderMsg = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
    let inner_transaction_time_ms = m.o.transaction_time;
    let side = match m.o.side.as_str() {
        "BUY" => AggressorSide::Buy,
        "SELL" => AggressorSide::Sell,
        _ => AggressorSide::Unknown,
    };
    Ok(UsdmDecoded::ForceOrder {
        symbol: m.o.s,
        price: Price(parse_fixed(&m.o.ap)?),
        quantity: Quantity(parse_fixed(&m.o.l)?),
        side,
        outer_event_time_ms: m.event_time,
        inner_transaction_time_ms,
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

pub fn agg_id_source(id: u64) -> SourceId {
    SourceId(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_kline_exact_fixed() {
        let raw = br#"{"e":"kline","E":123456789,"s":"BTCUSDT","k":{"t":123400000,"T":123460000,"s":"BTCUSDT","i":"1m","f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":true,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}"#;
        let UsdmDecoded::Candle {
            open,
            close,
            interval_ns,
            is_closed,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("kline");
        };
        assert!(is_closed);
        assert_eq!(interval_ns, 60_000_000_000);
        assert_eq!(open.0, Fixed::new(10, 4));
        assert_eq!(close.0, Fixed::new(20, 4));
    }

    #[test]
    fn decode_agg_trade_and_mark() {
        let raw = br#"{"e":"aggTrade","E":1,"s":"BTCUSDT","a":9,"p":"65000.1","q":"0.01","f":1,"l":2,"T":3,"m":true}"#;
        let UsdmDecoded::AggTrade {
            aggressor, price, ..
        } = decode_text(raw).unwrap()
        else {
            panic!("aggTrade");
        };
        assert_eq!(aggressor, AggressorSide::Sell);
        assert_eq!(price.0, Fixed::new(650001, 1));

        let trade = br#"{"e":"trade","E":1,"s":"BTCUSDT","t":42,"p":"65000.30","q":"0.010","T":3,"m":false,"X":"MARKET"}"#;
        let UsdmDecoded::AggTrade {
            agg_id,
            aggressor,
            price,
            ..
        } = decode_text(trade).unwrap()
        else {
            panic!("trade");
        };
        assert_eq!(agg_id, 42);
        assert_eq!(aggressor, AggressorSide::Buy);
        assert_eq!(price.0, Fixed::new(6500030, 2));

        let mark = br#"{"e":"markPriceUpdate","E":10,"s":"BTCUSDT","p":"65000.00","i":"64990.00","P":"65001.00","r":"0.00010000","T":20}"#;
        let UsdmDecoded::MarkPrice {
            funding_rate,
            next_funding_ts,
            ..
        } = decode_text(mark).unwrap()
        else {
            panic!("mark");
        };
        assert_eq!(funding_rate.0, Fixed::new(10000, 8));
        assert_eq!(next_funding_ts, TimestampNs(20_000_000));

        let idx = br#"{"e":"indexPriceUpdate","E":11,"i":"BTCUSDT","p":"64991.00"}"#;
        let UsdmDecoded::IndexPrice { symbol, price, .. } = decode_text(idx).unwrap() else {
            panic!("indexPriceUpdate");
        };
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(price.0, Fixed::new(6499100, 2));
    }

    #[test]
    fn non_positive_trade_records_are_not_canonical_tape_prints() {
        let raw = br#"{"e":"trade","E":1786371578176,"T":1786371578176,"s":"BTCUSDT","t":7963476844,"p":"0","q":"0","X":"NA","m":true,"st":1}"#;
        assert_eq!(decode_text(raw).unwrap(), UsdmDecoded::Ignored);
    }

    #[test]
    fn decode_depth_pu_force_order_and_oi() {
        let depth = br#"{"e":"depthUpdate","E":1,"T":2,"s":"BTCUSDT","U":10,"u":12,"pu":9,"b":[["100.0","0"]],"a":[["101.0","1.5"]]}"#;
        let UsdmDecoded::DepthUpdate {
            prev_final_update_id,
            first_update_id,
            ..
        } = decode_text(depth).unwrap()
        else {
            panic!("depth");
        };
        assert_eq!(prev_final_update_id, 9);
        assert_eq!(first_update_id, 10);
        let (op, _) = level_op(Quantity(Fixed::new(0, 1)));
        assert_eq!(op, BookOperation::Delete);

        let liq = br#"{"e":"forceOrder","E":5,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.01","p":"9900","ap":"9910","X":"FILLED","l":"0.01","z":"0.01","T":5}}"#;
        let UsdmDecoded::ForceOrder { side, price, .. } = decode_text(liq).unwrap() else {
            panic!("forceOrder");
        };
        assert_eq!(side, AggressorSide::Sell);
        assert_eq!(price.0, Fixed::new(9910, 0));

        let oi = br#"{"symbol":"BTCUSDT","openInterest":"10659.509","time":1589437530011}"#;
        let UsdmDecoded::OpenInterest { quantity, .. } = decode_text(oi).unwrap() else {
            panic!("oi");
        };
        assert_eq!(quantity.0, Fixed::new(10659509, 3));
    }

    /// Active backend must match the serde oracle (default = both serde).
    #[test]
    fn decode_text_matches_serde_oracle() {
        for raw in usdm_parity_fixtures() {
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
        for raw in usdm_parity_fixtures() {
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

    fn usdm_parity_fixtures() -> &'static [&'static [u8]] {
        &[
            br#"{"e":"aggTrade","E":1,"s":"BTCUSDT","a":9,"p":"65000.1","q":"0.01","f":1,"l":2,"T":3,"m":true}"#,
            br#"{"e":"trade","E":1,"s":"BTCUSDT","t":42,"p":"65000.30","q":"0.010","T":3,"m":false}"#,
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT","t":10,"p":"65000.0","q":"0.02","T":4,"m":false}}"#,
            br#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":10,"p":"65000.0","q":"0.02","f":3,"l":4,"T":4,"m":false}}"#,
            br#"{"u":1,"s":"BTCUSDT","b":"100.00","B":"1.5","a":"100.01","A":"2.0"}"#,
            br#"{"e":"markPriceUpdate","E":10,"s":"BTCUSDT","p":"65000.00","i":"64990.00","P":"65001.00","r":"0.00010000","T":20}"#,
            br#"{"e":"indexPriceUpdate","E":11,"i":"BTCUSDT","p":"64991.00"}"#,
            br#"{"e":"depthUpdate","E":1,"T":2,"s":"BTCUSDT","U":10,"u":12,"pu":9,"b":[["100.0","0"]],"a":[["101.0","1.5"]]}"#,
            br#"{"e":"forceOrder","E":5,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.01","p":"9900","ap":"9910","X":"FILLED","l":"0.01","z":"0.01","T":5}}"#,
            br#"{"symbol":"BTCUSDT","openInterest":"10659.509","time":1589437530011}"#,
            br#"{"e":"kline","E":123456789,"s":"BTCUSDT","k":{"t":123400000,"T":123460000,"s":"BTCUSDT","i":"1m","f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":true,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}"#,
            br#"{"result":null,"id":1}"#,
        ]
    }
}
