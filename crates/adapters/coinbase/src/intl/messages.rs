//! Coinbase International MD decode (MATCH / LEVEL1 / LEVEL2).

use marketfeed_model::{AggressorSide, Fixed, Price, Quantity, SourceId, TimestampNs};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedEvent {
    Trade(TradeRow),
    Quote {
        sequence: Option<u64>,
        product_id: String,
        bid_price: Option<Price>,
        bid_qty: Option<Quantity>,
        ask_price: Option<Price>,
        ask_qty: Option<Quantity>,
        exchange_ts_ns: Option<i64>,
    },
    BookSnapshot {
        sequence: Option<u64>,
        product_id: String,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
    },
    BookDelta {
        sequence: Option<u64>,
        product_id: String,
        changes: Vec<BookLevelChange>,
        exchange_ts_ns: Option<i64>,
    },
    SubscribeAck,
    Error(String),
    Unknown,
}

impl DecodedEvent {
    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Trade(row) => row.sequence,
            Self::Quote { sequence, .. }
            | Self::BookSnapshot { sequence, .. }
            | Self::BookDelta { sequence, .. } => *sequence,
            Self::SubscribeAck | Self::Error(_) | Self::Unknown => None,
        }
    }

    pub fn requires_sequence(&self) -> bool {
        matches!(
            self,
            Self::Trade(_)
                | Self::Quote { .. }
                | Self::BookSnapshot { .. }
                | Self::BookDelta { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevelChange {
    pub side: BookSideWire,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSideWire {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeRow {
    pub product_id: String,
    pub trade_id: String,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub sequence: Option<u64>,
    pub exchange_ts_ns: Option<i64>,
}

pub fn decode_text(bytes: &[u8]) -> Result<Vec<DecodedEvent>, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    decode_value(&v)
}
pub fn trade_id_source(id: &str) -> SourceId {
    SourceId(id.to_string())
}
pub fn ns_to_ts(ns: i64) -> TimestampNs {
    TimestampNs(ns)
}

fn decode_value(v: &Value) -> Result<Vec<DecodedEvent>, String> {
    let Some(obj) = v.as_object() else {
        return Ok(vec![DecodedEvent::Unknown]);
    };
    let channel = obj.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    let msg_type = obj.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if msg_type.eq_ignore_ascii_case("error") || channel.eq_ignore_ascii_case("error") {
        let msg = obj
            .get("message")
            .or_else(|| obj.get("msg"))
            .and_then(|m| m.as_str())
            .unwrap_or("coinbase-intl error")
            .to_string();
        return Ok(vec![DecodedEvent::Error(msg)]);
    }
    if channel.eq_ignore_ascii_case("subscriptions")
        || msg_type.eq_ignore_ascii_case("subscriptions")
        || msg_type.eq_ignore_ascii_case("subscribed")
    {
        return Ok(vec![DecodedEvent::SubscribeAck]);
    }
    match channel {
        "MATCH" | "RFQ_MATCH" => decode_match(obj),
        "LEVEL1" => decode_level1(obj),
        "LEVEL2" => decode_level2(obj, msg_type),
        "" if msg_type.is_empty() => Ok(vec![DecodedEvent::Unknown]),
        _ => Ok(vec![DecodedEvent::Unknown]),
    }
}

fn decode_match(obj: &serde_json::Map<String, Value>) -> Result<Vec<DecodedEvent>, String> {
    let product_id = require_str(obj, "product_id")?;
    let trade_id = obj
        .get("match_id")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let price = Price(fixed_field(obj, "trade_price")?);
    let quantity = Quantity(fixed_field(obj, "trade_qty")?);
    let aggressor = match obj
        .get("aggressor_side")
        .and_then(|x| x.as_str())
        .unwrap_or("")
    {
        "BUY" | "OPENING_FILL" => AggressorSide::Buy,
        "SELL" => AggressorSide::Sell,
        _ => AggressorSide::Unknown,
    };
    let sequence = obj.get("sequence").and_then(|s| s.as_u64());
    let exchange_ts_ns = obj
        .get("time")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_to_ns);
    Ok(vec![DecodedEvent::Trade(TradeRow {
        product_id,
        trade_id,
        price,
        quantity,
        aggressor,
        sequence,
        exchange_ts_ns,
    })])
}

fn decode_level1(obj: &serde_json::Map<String, Value>) -> Result<Vec<DecodedEvent>, String> {
    let sequence = obj.get("sequence").and_then(|value| value.as_u64());
    let product_id = require_str(obj, "product_id")?;
    let bid_price = optional_price(obj, "bid_price")?;
    let ask_price = optional_price(obj, "ask_price")?;
    let bid_qty = optional_qty(obj, "bid_qty")?;
    let ask_qty = optional_qty(obj, "ask_qty")?;
    let exchange_ts_ns = obj
        .get("time")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_to_ns);
    Ok(vec![DecodedEvent::Quote {
        sequence,
        product_id,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        exchange_ts_ns,
    }])
}

fn decode_level2(
    obj: &serde_json::Map<String, Value>,
    msg_type: &str,
) -> Result<Vec<DecodedEvent>, String> {
    let sequence = obj.get("sequence").and_then(|value| value.as_u64());
    let product_id = require_str(obj, "product_id")?;
    let exchange_ts_ns = obj
        .get("time")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_to_ns);
    if msg_type.eq_ignore_ascii_case("SNAPSHOT") {
        return Ok(vec![DecodedEvent::BookSnapshot {
            sequence,
            product_id,
            bids: levels_array(obj, "bids")?,
            asks: levels_array(obj, "asks")?,
        }]);
    }
    let changes = obj
        .get("changes")
        .and_then(|c| c.as_array())
        .ok_or_else(|| "LEVEL2 update missing changes".to_string())?;
    let mut out = Vec::with_capacity(changes.len());
    for row in changes {
        let arr = row
            .as_array()
            .ok_or_else(|| "LEVEL2 change not array".to_string())?;
        if arr.len() < 3 {
            continue;
        }
        let side = match arr[0].as_str().unwrap_or("") {
            "BUY" => BookSideWire::Bid,
            "SELL" => BookSideWire::Ask,
            _ => continue,
        };
        out.push(BookLevelChange {
            side,
            price: Price(fixed_value(&arr[1])?),
            quantity: Quantity(fixed_value(&arr[2])?),
        });
    }
    Ok(vec![DecodedEvent::BookDelta {
        sequence,
        product_id,
        changes: out,
        exchange_ts_ns,
    }])
}

fn levels_array(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Vec<(Price, Quantity)>, String> {
    let arr = obj
        .get(key)
        .and_then(|a| a.as_array())
        .ok_or_else(|| format!("LEVEL2 snapshot missing {key}"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let pair = row
            .as_array()
            .ok_or_else(|| format!("LEVEL2 {key} row not array"))?;
        if pair.len() < 2 {
            continue;
        }
        out.push((
            Price(fixed_value(&pair[0])?),
            Quantity(fixed_value(&pair[1])?),
        ));
    }
    Ok(out)
}

fn require_str(obj: &serde_json::Map<String, Value>, key: &str) -> Result<String, String> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| format!("missing {key}"))
}
fn optional_price(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Price>, String> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(Price(fixed_value(v)?))),
    }
}
fn optional_qty(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<Quantity>, String> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(Quantity(fixed_value(v)?))),
    }
}
fn fixed_field(obj: &serde_json::Map<String, Value>, key: &str) -> Result<Fixed, String> {
    fixed_value(obj.get(key).ok_or_else(|| format!("missing {key}"))?)
}
fn fixed_value(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("price/qty not string/number".into()),
    }
}
fn rfc3339_to_ns(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-');
    let y: i64 = d.next()?.parse().ok()?;
    let mo: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    let (hms, frac) = match time.split_once('.') {
        Some((hms, f)) => (hms, Some(f)),
        None => (time, None),
    };
    let mut t = hms.split(':');
    let h: i64 = t.next()?.parse().ok()?;
    let mi: i64 = t.next()?.parse().ok()?;
    let sec: i64 = t.next()?.parse().ok()?;
    let days = days_from_civil(y, mo, day);
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec;
    let nanos = match frac {
        Some(f) => {
            let mut digits = f.as_bytes().to_vec();
            digits.truncate(9);
            while digits.len() < 9 {
                digits.push(b'0');
            }
            std::str::from_utf8(&digits).ok()?.parse::<i64>().ok()?
        }
        None => 0,
    };
    Some(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
}
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decode_match_trade() {
        let raw = br#"{"sequence":0,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.002Z","match_id":"177101110052388865","trade_qty":"0.006","aggressor_side":"BUY","trade_price":"28833.1","channel":"MATCH","type":"UPDATE"}"#;
        let ev = decode_text(raw).unwrap();
        assert!(matches!(&ev[0], DecodedEvent::Trade(t) if t.product_id == "BTC-PERP"));
    }
}
