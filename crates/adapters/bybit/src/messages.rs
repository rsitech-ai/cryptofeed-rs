//! Bybit V5 JSON message decoding (exact Fixed; no f64).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{
    AggressorSide, BookLevel, BookOperation, BookSide, Fixed, Price, Quantity, Rate, SourceId,
    TimestampNs,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
// Keeping decoded events inline avoids a heap allocation on every hot-path ticker frame.
#[allow(clippy::large_enum_variant)]
pub enum DecodedEvent {
    /// One or more trades from a `publicTrade.*` frame.
    Trades(Vec<TradeDecoded>),
    Orderbook {
        symbol: String,
        depth: u32,
        kind: OrderbookKind,
        update_id: u64,
        seq: Option<u64>,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
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
        /// Venue `confirm`: bar closed.
        is_closed: bool,
    },
    /// `tickers.{symbol}` — fields optional on delta updates (spot + linear/inverse).
    Tickers {
        symbol: String,
        mark: Option<Price>,
        index: Option<Price>,
        funding_rate: Option<Rate>,
        next_funding_ts: Option<TimestampNs>,
        open_interest: Option<Quantity>,
        /// W6-P0a 24h stats (spot always; linear/inverse when present).
        open: Option<Price>,
        high: Option<Price>,
        low: Option<Price>,
        close: Option<Price>,
        volume: Option<Quantity>,
        quote_volume: Option<Quantity>,
        exchange_ts_ms: i64,
    },
    /// Linear/inverse `allLiquidation.{symbol}` — one frame may carry many rows.
    Liquidations(Vec<LiquidationDecoded>),
    SubscribeAck {
        success: bool,
    },
    Pong,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiquidationDecoded {
    pub symbol: String,
    pub price: Price,
    pub quantity: Quantity,
    /// Aggressor of the forced order (not venue position side).
    pub side: AggressorSide,
    pub exchange_ts_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderbookKind {
    Snapshot,
    Delta,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeDecoded {
    pub symbol: String,
    pub trade_id: String,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub exchange_ts_ms: i64,
    pub seq: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TradeRow {
    #[serde(rename = "T")]
    trade_time: i64,
    s: String,
    #[serde(rename = "S")]
    side: String,
    v: String,
    p: String,
    i: String,
    seq: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OrderbookData {
    s: String,
    b: Vec<[String; 2]>,
    a: Vec<[String; 2]>,
    u: u64,
    seq: Option<u64>,
}

pub fn decode_text(bytes: &[u8]) -> Result<DecodedEvent, String> {
    decode_text_with(bytes, crate::json::value_from_slice)
}

/// Reference decode that always uses `serde_json` (parity oracle).
pub fn decode_text_serde(bytes: &[u8]) -> Result<DecodedEvent, String> {
    decode_text_with(bytes, crate::json::value_from_slice_serde)
}

/// Feature-gated simd-json decode (parity probe; same canonical events as serde).
#[cfg(feature = "simd-json")]
pub fn decode_text_simd(bytes: &[u8]) -> Result<DecodedEvent, String> {
    decode_text_with(bytes, crate::json::value_from_slice_simd)
}

fn decode_text_with(
    bytes: &[u8],
    parse: fn(&[u8]) -> Result<Value, String>,
) -> Result<DecodedEvent, String> {
    let v = parse(bytes)?;
    decode_value(&v)
}

fn decode_value(v: &Value) -> Result<DecodedEvent, String> {
    let Some(obj) = v.as_object() else {
        return Ok(DecodedEvent::Unknown);
    };

    if obj.get("op").and_then(|x| x.as_str()) == Some("pong") {
        return Ok(DecodedEvent::Pong);
    }

    if obj.get("op").and_then(|x| x.as_str()) == Some("subscribe")
        || (obj.contains_key("success") && obj.contains_key("ret_msg"))
    {
        let success = obj
            .get("success")
            .and_then(|x| x.as_bool())
            .unwrap_or(false);
        return Ok(DecodedEvent::SubscribeAck { success });
    }

    let topic = obj.get("topic").and_then(|x| x.as_str()).unwrap_or("");
    let msg_type = obj.get("type").and_then(|x| x.as_str()).unwrap_or("");
    let ts = obj.get("ts").and_then(|x| x.as_i64()).unwrap_or(0);

    if topic.starts_with("publicTrade.") {
        return decode_trades(obj.get("data").unwrap_or(&Value::Null));
    }

    if topic.starts_with("kline.") {
        return decode_kline(topic, obj.get("data").unwrap_or(&Value::Null), ts);
    }

    if topic.starts_with("tickers.") {
        return decode_tickers(topic, obj.get("data").unwrap_or(&Value::Null), ts);
    }

    if topic.starts_with("allLiquidation.") {
        return decode_all_liquidation(obj.get("data").unwrap_or(&Value::Null), ts);
    }

    if let Some(depth) = parse_orderbook_depth(topic) {
        let kind = match msg_type {
            "snapshot" => OrderbookKind::Snapshot,
            "delta" => OrderbookKind::Delta,
            _ => return Ok(DecodedEvent::Unknown),
        };
        let data = obj
            .get("data")
            .ok_or_else(|| "orderbook missing data".to_string())?;
        if data.get("s").is_none() {
            return Ok(DecodedEvent::Unknown);
        }
        let book: OrderbookData =
            serde_json::from_value(data.clone()).map_err(|e| e.to_string())?;
        return Ok(DecodedEvent::Orderbook {
            symbol: book.s,
            depth,
            kind,
            update_id: book.u,
            seq: book.seq,
            bids: parse_levels(&book.b)?,
            asks: parse_levels(&book.a)?,
            exchange_ts_ms: ts,
        });
    }

    Ok(DecodedEvent::Unknown)
}

/// Bybit V5 kline topic interval segment for a canonical interval.
pub fn kline_topic_interval(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "1",
        CandleInterval::M5 => "5",
        CandleInterval::M15 => "15",
        CandleInterval::H1 => "60",
        CandleInterval::D1 => "D",
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

fn interval_ns_from_bybit(i: &str) -> Result<i64, String> {
    let interval = match i {
        "1" => CandleInterval::M1,
        "5" => CandleInterval::M5,
        "15" => CandleInterval::M15,
        "60" => CandleInterval::H1,
        "D" | "1D" => CandleInterval::D1,
        other => return Err(format!("unsupported kline interval {other}")),
    };
    Ok(candle_interval_ns(interval))
}

#[derive(Debug, Deserialize)]
struct KlineRow {
    start: i64,
    interval: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    confirm: bool,
    timestamp: Option<i64>,
}

fn decode_kline(topic: &str, data: &Value, ts: i64) -> Result<DecodedEvent, String> {
    // topic: kline.{interval}.{symbol}
    let mut parts = topic.splitn(3, '.');
    let _ = parts.next();
    let _interval_seg = parts.next();
    let symbol = parts
        .next()
        .ok_or_else(|| format!("bad kline topic {topic}"))?
        .to_string();
    let rows: Vec<KlineRow> = serde_json::from_value(data.clone()).map_err(|e| e.to_string())?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| "kline data empty".to_string())?;
    Ok(DecodedEvent::Candle {
        symbol,
        open: Price(parse_fixed(&row.open)?),
        high: Price(parse_fixed(&row.high)?),
        low: Price(parse_fixed(&row.low)?),
        close: Price(parse_fixed(&row.close)?),
        volume: Quantity(parse_fixed(&row.volume)?),
        interval_ns: interval_ns_from_bybit(&row.interval)?,
        start_ts: TimestampNs(row.start.saturating_mul(1_000_000)),
        exchange_ts_ms: row.timestamp.unwrap_or(ts),
        is_closed: row.confirm,
    })
}

fn optional_fixed_price(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Price>, String> {
    match obj.get(key).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => Ok(Some(Price(parse_fixed(s)?))),
        _ => Ok(None),
    }
}

fn optional_fixed_qty(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Quantity>, String> {
    match obj.get(key).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => Ok(Some(Quantity(parse_fixed(s)?))),
        _ => Ok(None),
    }
}

fn optional_fixed_rate(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Rate>, String> {
    match obj.get(key).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => Ok(Some(Rate(parse_fixed(s)?))),
        _ => Ok(None),
    }
}

fn decode_tickers(topic: &str, data: &Value, ts: i64) -> Result<DecodedEvent, String> {
    // topic: tickers.{symbol}
    let symbol = topic
        .strip_prefix("tickers.")
        .ok_or_else(|| format!("bad tickers topic {topic}"))?
        .to_string();
    let obj = data
        .as_object()
        .ok_or_else(|| "tickers data not object".to_string())?;
    let symbol = obj
        .get("symbol")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or(symbol);
    let next_funding_ts = match obj.get("nextFundingTime").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => {
            let ms: i64 = s.parse().map_err(|_| format!("bad nextFundingTime {s}"))?;
            Some(TimestampNs(ms.saturating_mul(1_000_000)))
        }
        _ => None,
    };
    Ok(DecodedEvent::Tickers {
        symbol,
        mark: optional_fixed_price(obj, "markPrice")?,
        index: optional_fixed_price(obj, "indexPrice")?,
        funding_rate: optional_fixed_rate(obj, "fundingRate")?,
        next_funding_ts,
        open_interest: optional_fixed_qty(obj, "openInterest")?,
        open: optional_fixed_price(obj, "prevPrice24h")?,
        high: optional_fixed_price(obj, "highPrice24h")?,
        low: optional_fixed_price(obj, "lowPrice24h")?,
        close: optional_fixed_price(obj, "lastPrice")?,
        volume: optional_fixed_qty(obj, "volume24h")?,
        quote_volume: optional_fixed_qty(obj, "turnover24h")?,
        exchange_ts_ms: ts,
    })
}

fn decode_trades(data: &Value) -> Result<DecodedEvent, String> {
    let rows: Vec<TradeRow> = serde_json::from_value(data.clone()).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for m in rows {
        let aggressor = match m.side.as_str() {
            "Buy" => AggressorSide::Buy,
            "Sell" => AggressorSide::Sell,
            _ => AggressorSide::Unknown,
        };
        out.push(TradeDecoded {
            symbol: m.s,
            trade_id: m.i,
            price: Price(parse_fixed(&m.p)?),
            quantity: Quantity(parse_fixed(&m.v)?),
            aggressor,
            exchange_ts_ms: m.trade_time,
            seq: m.seq,
        });
    }
    Ok(DecodedEvent::Trades(out))
}

#[derive(Debug, Deserialize)]
struct AllLiquidationRow {
    #[serde(rename = "T")]
    trade_time: i64,
    s: String,
    /// Position side: `Buy` = long liquidated, `Sell` = short liquidated.
    #[serde(rename = "S")]
    side: String,
    v: String,
    p: String,
}

fn decode_all_liquidation(data: &Value, frame_ts: i64) -> Result<DecodedEvent, String> {
    let rows: Vec<AllLiquidationRow> =
        serde_json::from_value(data.clone()).map_err(|e| e.to_string())?;
    if rows.is_empty() {
        return Ok(DecodedEvent::Unknown);
    }
    let mut out = Vec::with_capacity(rows.len());
    for m in rows {
        // Venue `S` is position side; Liquidation.side is aggressor of the forced order.
        // Long liquidated (`Buy`) → forced sell; short liquidated (`Sell`) → forced buy.
        let side = match m.side.as_str() {
            "Buy" => AggressorSide::Sell,
            "Sell" => AggressorSide::Buy,
            _ => AggressorSide::Unknown,
        };
        let exchange_ts_ms = if m.trade_time != 0 {
            m.trade_time
        } else {
            frame_ts
        };
        out.push(LiquidationDecoded {
            symbol: m.s,
            price: Price(parse_fixed(&m.p)?),
            quantity: Quantity(parse_fixed(&m.v)?),
            side,
            exchange_ts_ms,
        });
    }
    Ok(DecodedEvent::Liquidations(out))
}

fn parse_orderbook_depth(topic: &str) -> Option<u32> {
    let rest = topic.strip_prefix("orderbook.")?;
    let depth_str = rest.split('.').next()?;
    depth_str.parse().ok()
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

pub fn trade_id_source(id: &str) -> SourceId {
    SourceId(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_public_trade_taker_side() {
        let raw = br#"{
          "topic":"publicTrade.BTCUSDT",
          "type":"snapshot",
          "ts":1000,
          "data":[{
            "T":1001,"s":"BTCUSDT","S":"Sell","v":"0.01","p":"65000.5",
            "L":"MinusTick","i":"abc-1","seq":9
          }]
        }"#;
        let DecodedEvent::Trades(trades) = decode_text(raw).unwrap() else {
            panic!("trades");
        };
        assert_eq!(trades[0].aggressor, AggressorSide::Sell);
        assert_eq!(trades[0].price.0, Fixed::new(650005, 1));
    }

    #[test]
    fn decode_orderbook_snapshot_and_delta() {
        let snap = br#"{
          "topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,
          "data":{"s":"BTCUSDT","b":[["100.00","1"]],"a":[["101.00","2"]],"u":10,"seq":100}
        }"#;
        let DecodedEvent::Orderbook {
            kind,
            update_id,
            depth,
            ..
        } = decode_text(snap).unwrap()
        else {
            panic!("ob");
        };
        assert_eq!(kind, OrderbookKind::Snapshot);
        assert_eq!(update_id, 10);
        assert_eq!(depth, 50);

        let delta = br#"{
          "topic":"orderbook.1.BTCUSDT","type":"delta","ts":2,
          "data":{"s":"BTCUSDT","b":[["100.00","0"]],"a":[["101.00","1.5"]],"u":11,"seq":101}
        }"#;
        let DecodedEvent::Orderbook {
            kind, bids, depth, ..
        } = decode_text(delta).unwrap()
        else {
            panic!("ob1");
        };
        assert_eq!(kind, OrderbookKind::Delta);
        assert_eq!(depth, 1);
        let (op, _) = level_op(bids[0].1);
        assert_eq!(op, BookOperation::Delete);
    }

    #[test]
    fn rest_orderbook_body_is_not_decoded_as_a_websocket_snapshot() {
        let rest = br#"{
          "retCode":0,
          "result":{
            "s":"BTCUSDT",
            "b":[["100.00","1"]],
            "a":[["101.00","2"]],
            "u":10,
            "seq":100,
            "ts":1
          }
        }"#;

        assert!(matches!(decode_text(rest).unwrap(), DecodedEvent::Unknown));
    }

    #[test]
    fn decode_subscribe_ack_and_pong() {
        assert!(matches!(
            decode_text(br#"{"success":true,"ret_msg":"","op":"subscribe"}"#).unwrap(),
            DecodedEvent::SubscribeAck { success: true }
        ));
        assert!(matches!(
            decode_text(br#"{"op":"pong"}"#).unwrap(),
            DecodedEvent::Pong
        ));
    }

    #[test]
    fn decode_kline_exact_fixed() {
        let raw = br#"{
          "topic":"kline.1.BTCUSDT","type":"snapshot","ts":1672324988887,
          "data":[{
            "start":1672324800000,"end":1672324859999,"interval":"1",
            "open":"16649.5","close":"16695","high":"16699","low":"16642",
            "volume":"2.081","turnover":"34666.4005","confirm":true,
            "timestamp":1672324859999
          }]
        }"#;
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
        assert!(is_closed);
        assert_eq!(interval_ns, candle_interval_ns(CandleInterval::M1));
        assert_eq!(start_ts, TimestampNs(1_672_324_800_000_000_000));
        assert_eq!(open.0, Fixed::new(166495, 1));
        assert_eq!(high.0, Fixed::new(16699, 0));
        assert_eq!(low.0, Fixed::new(16642, 0));
        assert_eq!(close.0, Fixed::new(16695, 0));
        assert_eq!(volume.0, Fixed::new(2081, 3));
    }

    /// Active backend must match the serde oracle (default = both serde).
    #[test]
    fn decode_text_matches_serde_oracle() {
        for raw in bybit_parity_fixtures() {
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
        for raw in bybit_parity_fixtures() {
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

    fn bybit_parity_fixtures() -> &'static [&'static [u8]] {
        &[
            br#"{"success":true,"ret_msg":"","op":"subscribe"}"#,
            br#"{"op":"pong"}"#,
            br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1000,"data":[{"T":1001,"s":"BTCUSDT","S":"Sell","v":"0.01","p":"65000.5","L":"MinusTick","i":"abc-1","seq":9}]}"#,
            br#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1"]],"a":[["101.00","2"]],"u":10,"seq":100}}"#,
            br#"{"topic":"orderbook.1.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","0"]],"a":[["101.00","1.5"]],"u":11,"seq":101}}"#,
            br#"{"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1672324988887,"data":[{"start":1672324800000,"end":1672324859999,"interval":"1","open":"16649.5","close":"16695","high":"16699","low":"16642","volume":"2.081","turnover":"34666.4005","confirm":true,"timestamp":1672324859999}]}"#,
            br#"{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1672376495650,"data":{"symbol":"BTCUSDT","markPrice":"16595.00","indexPrice":"16596.54","fundingRate":"0.0001","nextFundingTime":"1672387200000","openInterest":"458153.0"}}"#,
            br#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"0.01","p":"65000.5"}]}"#,
        ]
    }

    #[test]
    fn decode_all_liquidation_exact_fixed() {
        let raw = br#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"0.01","p":"65000.5"},{"T":1739502302930,"s":"BTCUSDT","S":"Buy","v":"1.5","p":"64999.0"}]}"#;
        let DecodedEvent::Liquidations(rows) = decode_text(raw).unwrap() else {
            panic!("allLiquidation");
        };
        assert_eq!(rows.len(), 2);
        // Sell position liquidated → forced buy aggressor.
        assert_eq!(rows[0].side, AggressorSide::Buy);
        assert_eq!(rows[0].price.0, Fixed::new(650005, 1));
        assert_eq!(rows[0].quantity.0, Fixed::new(1, 2));
        assert_eq!(rows[0].exchange_ts_ms, 1_739_502_302_929);
        // Buy position liquidated → forced sell aggressor.
        assert_eq!(rows[1].side, AggressorSide::Sell);
        assert_eq!(rows[1].price.0, Fixed::new(649990, 1));
        assert_eq!(rows[1].quantity.0, Fixed::new(15, 1));
    }

    #[test]
    fn decode_tickers_mark_funding_oi_exact_fixed() {
        let raw = br#"{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1672376495650,"data":{"symbol":"BTCUSDT","markPrice":"16595.00","indexPrice":"16596.54","fundingRate":"0.0001","nextFundingTime":"1672387200000","openInterest":"458153.0"}}"#;
        let DecodedEvent::Tickers {
            mark,
            index,
            funding_rate,
            next_funding_ts,
            open_interest,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("tickers");
        };
        assert_eq!(mark.unwrap().0, Fixed::new(1659500, 2));
        assert_eq!(index.unwrap().0, Fixed::new(1659654, 2));
        assert_eq!(funding_rate.unwrap().0, Fixed::new(1, 4));
        assert_eq!(
            next_funding_ts,
            Some(TimestampNs(1_672_387_200_000_000_000))
        );
        assert_eq!(open_interest.unwrap().0, Fixed::new(4581530, 1));
    }
}
