//! Bybit V5 JSON message decoding (exact Fixed; no f64).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{
    AggressorSide, BookLevel, BookOperation, BookProvenance, BookSide, CandleProvenance, Fixed,
    Price, Quantity, Rate, SourceId, TimestampNs, TradeGranularity, TradeProvenance,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
// Keeping decoded events inline avoids a heap allocation on every hot-path ticker frame.
#[allow(clippy::large_enum_variant)]
pub enum DecodedEvent {
    /// One or more trades from a `publicTrade.*` frame.
    Trades(Vec<TradeDecoded>),
    Orderbook(BookDecoded),
    Candles(Vec<CandleDecoded>),
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
pub struct BookDecoded {
    pub symbol: String,
    pub depth: u32,
    pub kind: OrderbookKind,
    pub update_id: u64,
    pub seq: Option<u64>,
    pub bids: Vec<(Price, Quantity)>,
    pub asks: Vec<(Price, Quantity)>,
    pub exchange_ts_ms: i64,
    pub provenance: Box<BookProvenance>,
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
pub struct CandleDecoded {
    pub symbol: String,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
    pub turnover: Option<Quantity>,
    pub interval_ns: i64,
    pub start_ts: TimestampNs,
    pub exchange_ts_ms: i64,
    pub is_closed: bool,
    pub provenance: Box<CandleProvenance>,
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
    pub provenance: Box<TradeProvenance>,
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
    #[serde(rename = "BT")]
    block: Option<bool>,
    #[serde(rename = "RPI")]
    rpi: Option<bool>,
    #[serde(rename = "L")]
    tick_direction: Option<String>,
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
    let ts = match obj.get("ts") {
        Some(value) => {
            let ms = value.as_i64().ok_or("invalid native frame ts")?;
            checked_ms(ms)?;
            ms
        }
        None => 0,
    };

    if topic.starts_with("publicTrade.") {
        if !obj.contains_key("ts") {
            return Err("trade frame ts required".into());
        }
        return decode_trades(
            topic,
            obj.get("data").unwrap_or(&Value::Null),
            obj.get("ts"),
        );
    }

    if topic.starts_with("kline.") {
        if !obj.contains_key("ts") {
            return Err("kline frame ts required".into());
        }
        return decode_kline(topic, obj.get("data").unwrap_or(&Value::Null), ts);
    }

    if topic.starts_with("tickers.") {
        return decode_tickers(topic, obj.get("data").unwrap_or(&Value::Null), ts);
    }

    if topic.starts_with("allLiquidation.") {
        return decode_all_liquidation(obj.get("data").unwrap_or(&Value::Null), ts);
    }

    if topic.starts_with("orderbook.") {
        let mut parts = topic.split('.');
        let _ = parts.next();
        let depth = parts
            .next()
            .and_then(|v| v.parse::<u32>().ok())
            .ok_or("invalid book depth")?;
        let symbol = parts
            .next()
            .filter(|v| !v.is_empty())
            .ok_or("missing book symbol")?;
        if parts.next().is_some() || !matches!(depth, 1 | 50 | 200 | 1000) {
            return Err("unsupported book topic".into());
        }
        if !obj.contains_key("ts") {
            return Err("native book ts required".into());
        }
        let kind = match msg_type {
            "snapshot" => OrderbookKind::Snapshot,
            "delta" => OrderbookKind::Delta,
            _ => return Err("unsupported book message type".into()),
        };
        let data = obj.get("data").ok_or("book missing data")?;
        for side in ["b", "a"] {
            if data
                .get(side)
                .and_then(Value::as_array)
                .is_none_or(|rows| rows.len() > 4096)
            {
                return Err("bounded native book side array required".into());
            }
        }
        let book: OrderbookData =
            serde_json::from_value(data.clone()).map_err(|e| e.to_string())?;
        if book.s != symbol || book.u == 0 {
            return Err("book topic/row symbol mismatch or zero update ID".into());
        }
        let mut native_attributes = attributes(data, &["s", "u", "seq"])?;
        native_attributes.insert("type".into(), msg_type.into());
        native_attributes.insert("ts".into(), ts.to_string());
        if let Some(cts) = obj.get("cts") {
            let ms = cts.as_i64().ok_or("invalid native cts")?;
            checked_ms(ms)?;
            native_attributes.insert("cts".into(), ms.to_string());
        }
        let bids = parse_book_levels(&book.b)?;
        let asks = parse_book_levels(&book.a)?;
        let effective_snapshot = kind == OrderbookKind::Snapshot || book.u == 1;
        if effective_snapshot {
            if bids.len() > depth as usize || asks.len() > depth as usize {
                return Err("snapshot exceeds native depth".into());
            }
            for (levels, bid) in [(&bids, true), (&asks, false)] {
                if levels.iter().any(|(_, q)| q.0.coefficient <= 0)
                    || levels.windows(2).any(|pair| {
                        if bid {
                            !pair[0].0.0.numeric_cmp(pair[1].0.0).is_gt()
                        } else {
                            !pair[0].0.0.numeric_cmp(pair[1].0.0).is_lt()
                        }
                    })
                {
                    return Err("invalid snapshot level order or size".into());
                }
            }
        }
        return Ok(DecodedEvent::Orderbook(BookDecoded {
            symbol: book.s,
            depth,
            kind,
            update_id: book.u,
            seq: book.seq,
            bids,
            asks,
            exchange_ts_ms: ts,
            provenance: Box::new(BookProvenance {
                source_channel: topic.into(),
                native_attributes,
            }),
        }));
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
    end: Option<i64>,
    interval: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    turnover: Option<String>,
    confirm: bool,
    timestamp: Option<i64>,
}

const MAX_TRADE_ROWS: usize = 1024;
const MAX_CANDLE_ROWS: usize = 4096;
fn checked_ms(ms: i64) -> Result<TimestampNs, String> {
    if ms < 0 {
        return Err("negative native timestamp".into());
    }
    ms.checked_mul(1_000_000)
        .map(TimestampNs)
        .ok_or_else(|| "native timestamp overflow".into())
}
fn bounded_rows(data: &Value, cap: usize) -> Result<&[Value], String> {
    let rows = data.as_array().ok_or("native data must be an array")?;
    if rows.is_empty() || rows.len() > cap {
        return Err("native row count outside capacity".into());
    }
    Ok(rows)
}
fn positive(text: &str) -> Result<Fixed, String> {
    let value = parse_fixed(text)?;
    if value.coefficient <= 0 {
        return Err("positive financial value required".into());
    }
    Ok(value)
}
fn nonnegative(text: &str) -> Result<Fixed, String> {
    let value = parse_fixed(text)?;
    if value.coefficient < 0 {
        return Err("nonnegative financial value required".into());
    }
    Ok(value)
}
fn attributes(
    value: &Value,
    keys: &[&str],
) -> Result<std::collections::BTreeMap<String, String>, String> {
    keys.iter()
        .filter_map(|key| value.get(*key).map(|v| (*key, v)))
        .map(|(key, value)| {
            let text = match value {
                Value::String(v) => v.clone(),
                Value::Number(v) if v.is_i64() || v.is_u64() => v.to_string(),
                Value::Bool(v) => v.to_string(),
                _ => return Err(format!("invalid native attribute {key}")),
            };
            Ok((key.into(), text))
        })
        .collect()
}
fn decode_kline(topic: &str, data: &Value, ts: i64) -> Result<DecodedEvent, String> {
    let mut parts = topic.splitn(3, '.');
    let _ = parts.next();
    let interval = parts.next().ok_or("missing kline interval")?;
    let symbol = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or("missing kline symbol")?;
    let interval_ns = interval_ns_from_bybit(interval)?;
    let rows = bounded_rows(data, MAX_CANDLE_ROWS)?;
    let mut out = Vec::with_capacity(rows.len());
    for value in rows {
        let row: KlineRow = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        if row.interval != interval {
            return Err("kline topic/row interval mismatch".into());
        }
        let start_ts = checked_ms(row.start)?;
        if let Some(end) = row.end {
            checked_ms(end)?;
            if end < row.start {
                return Err("kline end before start".into());
            }
        }
        if let Some(last_match) = row.timestamp {
            checked_ms(last_match)?;
        }
        let open = Price(positive(&row.open)?);
        let high = Price(positive(&row.high)?);
        let low = Price(positive(&row.low)?);
        let close = Price(positive(&row.close)?);
        if low.0.numeric_cmp(high.0).is_gt()
            || open.0.numeric_cmp(low.0).is_lt()
            || open.0.numeric_cmp(high.0).is_gt()
            || close.0.numeric_cmp(low.0).is_lt()
            || close.0.numeric_cmp(high.0).is_gt()
        {
            return Err("inconsistent kline OHLC".into());
        }
        let mut native_attributes = attributes(
            value,
            &[
                "start",
                "end",
                "interval",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "confirm",
                "timestamp",
            ],
        )?;
        native_attributes.insert("ts".into(), ts.to_string());
        out.push(CandleDecoded {
            symbol: symbol.into(),
            open,
            high,
            low,
            close,
            volume: Quantity(nonnegative(&row.volume)?),
            turnover: row
                .turnover
                .as_deref()
                .map(nonnegative)
                .transpose()?
                .map(Quantity),
            interval_ns,
            start_ts,
            exchange_ts_ms: ts,
            is_closed: row.confirm,
            provenance: Box::new(CandleProvenance {
                source_channel: topic.into(),
                native_attributes,
            }),
        });
    }
    Ok(DecodedEvent::Candles(out))
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
            Some(checked_ms(ms)?)
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

fn decode_trades(
    topic: &str,
    data: &Value,
    frame_ts: Option<&Value>,
) -> Result<DecodedEvent, String> {
    let symbol = topic
        .strip_prefix("publicTrade.")
        .filter(|s| !s.is_empty())
        .ok_or("missing trade topic symbol")?;
    let rows = bounded_rows(data, MAX_TRADE_ROWS)?;
    let mut out = Vec::with_capacity(rows.len());
    for value in rows {
        let m: TradeRow = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        if m.s != symbol || m.i.is_empty() {
            return Err("trade symbol mismatch or empty ID".into());
        }
        checked_ms(m.trade_time)?;
        let aggressor = match m.side.as_str() {
            "Buy" => AggressorSide::Buy,
            "Sell" => AggressorSide::Sell,
            _ => AggressorSide::Unknown,
        };
        // Deserialize optional typed fields as well as retaining their presence.
        let _ = (&m.block, &m.rpi, &m.tick_direction);
        let mut native_attributes = attributes(
            value,
            &["T", "s", "S", "v", "p", "i", "seq", "L", "BT", "RPI"],
        )?;
        if let Some(ts) = frame_ts {
            native_attributes.insert("ts".into(), ts.to_string());
        }
        out.push(TradeDecoded {
            symbol: m.s,
            trade_id: m.i,
            price: Price(positive(&m.p)?),
            quantity: Quantity(positive(&m.v)?),
            aggressor,
            exchange_ts_ms: m.trade_time,
            seq: m.seq,
            provenance: Box::new(TradeProvenance {
                source_channel: topic.into(),
                granularity: TradeGranularity::IndividualExecution,
                constituent_first_id: None,
                constituent_last_id: None,
                native_count: None,
                native_attributes,
            }),
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

fn parse_book_levels(rows: &[[String; 2]]) -> Result<Vec<(Price, Quantity)>, String> {
    if rows.len() > 4096 {
        return Err("book side exceeds local row capacity".into());
    }
    let mut out: Vec<(Price, Quantity)> = Vec::with_capacity(rows.len());
    let mut seen = std::collections::BTreeSet::new();
    for row in rows {
        let price = Price(positive(&row[0])?);
        let quantity = Quantity(nonnegative(&row[1])?);
        // Normalize only the lookup key; retain original coefficient/scale.
        let mut key = (price.0.coefficient, price.0.scale);
        while key.1 > 0 && key.0 % 10 == 0 {
            key.0 /= 10;
            key.1 -= 1;
        }
        if !seen.insert(key) {
            return Err("duplicate native level price".into());
        }
        out.push((price, quantity));
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
        let DecodedEvent::Orderbook(BookDecoded {
            kind,
            update_id,
            depth,
            ..
        }) = decode_text(snap).unwrap()
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
        let DecodedEvent::Orderbook(BookDecoded {
            kind, bids, depth, ..
        }) = decode_text(delta).unwrap()
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
        let DecodedEvent::Candles(rows) = decode_text(raw).unwrap() else {
            panic!("klines")
        };
        let CandleDecoded {
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
            is_closed,
            ..
        } = rows.into_iter().next().unwrap();
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
