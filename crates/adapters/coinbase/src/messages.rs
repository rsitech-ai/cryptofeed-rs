//! Coinbase Exchange WS + REST candle decoding (exact Fixed; no f64 arithmetic).
//!
//! Public WS: `matches` (`match`/`last_match`), `ticker`, `level2`
//! (`snapshot` + `l2update`), `status` (product trading status).
//! Candles via REST `GET /products/{id}/candles`.
//! Ticker also carries 24h OHLC/volume → `Statistics24h` (W6-P0a).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{
    AggressorSide, Fixed, InstrumentStatus, Price, Quantity, SourceId, TimestampNs,
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedEvent {
    Trade(TradeRow),
    Quote {
        product_id: String,
        bid_price: Price,
        bid_qty: Option<Quantity>,
        ask_price: Price,
        ask_qty: Option<Quantity>,
        exchange_ts_ns: Option<i64>,
    },
    BookSnapshot {
        product_id: String,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
    },
    /// Incremental level updates; `quantity == 0` deletes the price level.
    BookDelta {
        product_id: String,
        changes: Vec<BookLevelChange>,
        exchange_ts_ns: Option<i64>,
    },
    /// Latest candle from REST poll (`[time, low, high, open, close, volume]`).
    Candle {
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
    },
    /// 24h stats from `ticker` (emitted alongside Quote).
    Statistics24h {
        product_id: String,
        open: Option<Price>,
        high: Option<Price>,
        low: Option<Price>,
        close: Option<Price>,
        volume: Option<Quantity>,
        exchange_ts_ns: Option<i64>,
    },
    /// Product trading status from `status` channel.
    ProductStatus {
        product_id: String,
        status: InstrumentStatus,
        exchange_ts_ns: Option<i64>,
    },
    SubscribeAck(Vec<SubscriptionChannel>),
    Heartbeat(Heartbeat),
    Error(String),
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionChannel {
    pub name: String,
    pub product_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    pub product_id: String,
    pub sequence: u64,
    pub last_trade_id: u64,
    pub exchange_ts_ns: Option<i64>,
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
    pub trade_id: u64,
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

pub fn candle_granularity_secs(interval: CandleInterval) -> u64 {
    match interval {
        CandleInterval::M1 => 60,
        CandleInterval::M5 => 300,
        CandleInterval::M15 => 900,
        CandleInterval::H1 => 3600,
        CandleInterval::D1 => 86_400,
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

pub fn decode_candles_rest(bytes: &[u8], interval: CandleInterval) -> Result<DecodedEvent, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let rows = v
        .as_array()
        .ok_or_else(|| "coinbase candles not array".to_string())?;
    let Some(row) = rows.first().and_then(|r| r.as_array()) else {
        return Err("coinbase candles empty".into());
    };
    if row.len() < 6 {
        return Err("coinbase candle row short".into());
    }
    let start_sec = match &row[0] {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|u| u as i64))
            .ok_or_else(|| "candle time not i64".to_string())?,
        Value::String(s) => s.parse().map_err(|e| format!("candle time: {e}"))?,
        _ => return Err("candle time not number/string".into()),
    };
    Ok(DecodedEvent::Candle {
        low: Price(fixed_from_json(&row[1])?),
        high: Price(fixed_from_json(&row[2])?),
        open: Price(fixed_from_json(&row[3])?),
        close: Price(fixed_from_json(&row[4])?),
        volume: Quantity(fixed_from_json(&row[5])?),
        interval_ns: candle_interval_ns(interval),
        start_ts: TimestampNs(start_sec.saturating_mul(1_000_000_000)),
    })
}

fn decode_value(v: &Value) -> Result<Vec<DecodedEvent>, String> {
    let Some(obj) = v.as_object() else {
        return Ok(vec![DecodedEvent::Unknown]);
    };
    let typ = obj.get("type").and_then(|t| t.as_str()).unwrap_or("");
    match typ {
        "match" | "last_match" => Ok(vec![DecodedEvent::Trade(decode_match(obj)?)]),
        "ticker" => decode_ticker(obj),
        "status" => decode_status(obj),
        "snapshot" => Ok(vec![decode_snapshot(obj)?]),
        "l2update" => Ok(vec![decode_l2update(obj)?]),
        "subscriptions" => Ok(vec![DecodedEvent::SubscribeAck(
            decode_subscription_channels(obj)?,
        )]),
        "heartbeat" => Ok(vec![DecodedEvent::Heartbeat(decode_heartbeat(obj)?)]),
        "error" => {
            let msg = obj
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("coinbase error")
                .to_string();
            Ok(vec![DecodedEvent::Error(msg)])
        }
        _ => Ok(vec![DecodedEvent::Unknown]),
    }
}

fn decode_subscription_channels(
    obj: &serde_json::Map<String, Value>,
) -> Result<Vec<SubscriptionChannel>, String> {
    let rows = obj
        .get("channels")
        .and_then(Value::as_array)
        .ok_or_else(|| "subscriptions missing channels".to_string())?;
    rows.iter()
        .map(|row| {
            let row = row
                .as_object()
                .ok_or_else(|| "subscription channel not object".to_string())?;
            let name = required_str(row, "name")?.to_string();
            let product_ids =
                row.get("product_ids")
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("subscription channel {name} missing product_ids"))?
                    .iter()
                    .map(|product| {
                        product.as_str().map(str::to_string).ok_or_else(|| {
                            format!("subscription channel {name} product is not string")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            Ok(SubscriptionChannel { name, product_ids })
        })
        .collect()
}

fn decode_heartbeat(obj: &serde_json::Map<String, Value>) -> Result<Heartbeat, String> {
    Ok(Heartbeat {
        product_id: required_str(obj, "product_id")?.to_string(),
        sequence: required_u64(obj, "sequence")?,
        last_trade_id: required_u64(obj, "last_trade_id")?,
        exchange_ts_ns: obj
            .get("time")
            .and_then(Value::as_str)
            .and_then(rfc3339_to_ns),
    })
}

fn decode_match(obj: &serde_json::Map<String, Value>) -> Result<TradeRow, String> {
    let product_id = required_str(obj, "product_id")?.to_string();
    let trade_id = obj
        .get("trade_id")
        .and_then(|t| t.as_u64())
        .ok_or_else(|| "match missing trade_id".to_string())?;
    // Coinbase `side` is the **maker** side; aggressor (taker) is the opposite.
    let maker = obj.get("side").and_then(|s| s.as_str()).unwrap_or("");
    let aggressor = match maker {
        "buy" => AggressorSide::Sell,
        "sell" => AggressorSide::Buy,
        _ => AggressorSide::Unknown,
    };
    Ok(TradeRow {
        product_id,
        trade_id,
        price: Price(fixed_from_json(
            obj.get("price")
                .ok_or_else(|| "match missing price".to_string())?,
        )?),
        quantity: Quantity(fixed_from_json(
            obj.get("size")
                .ok_or_else(|| "match missing size".to_string())?,
        )?),
        aggressor,
        sequence: obj.get("sequence").and_then(|s| s.as_u64()),
        exchange_ts_ns: obj
            .get("time")
            .and_then(|t| t.as_str())
            .and_then(rfc3339_to_ns),
    })
}

fn decode_ticker(obj: &serde_json::Map<String, Value>) -> Result<Vec<DecodedEvent>, String> {
    let product_id = required_str(obj, "product_id")?.to_string();
    let Some(bid) = obj.get("best_bid") else {
        return Ok(vec![DecodedEvent::Unknown]);
    };
    let Some(ask) = obj.get("best_ask") else {
        return Ok(vec![DecodedEvent::Unknown]);
    };
    let bid_s = bid
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "ticker best_bid empty".to_string())?;
    let ask_s = ask
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "ticker best_ask empty".to_string())?;
    let exchange_ts_ns = obj
        .get("time")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_to_ns);
    let mut out = vec![DecodedEvent::Quote {
        product_id: product_id.clone(),
        bid_price: Price(Fixed::parse_str(bid_s).map_err(|e| e.to_string())?),
        bid_qty: optional_qty(obj.get("best_bid_size"))?,
        ask_price: Price(Fixed::parse_str(ask_s).map_err(|e| e.to_string())?),
        ask_qty: optional_qty(obj.get("best_ask_size"))?,
        exchange_ts_ns,
    }];
    // W6-P0a: 24h fields already on ticker wire.
    if obj.get("open_24h").is_some()
        || obj.get("high_24h").is_some()
        || obj.get("low_24h").is_some()
        || obj.get("volume_24h").is_some()
        || obj.get("price").is_some()
    {
        out.push(DecodedEvent::Statistics24h {
            product_id,
            open: optional_price(obj.get("open_24h"))?,
            high: optional_price(obj.get("high_24h"))?,
            low: optional_price(obj.get("low_24h"))?,
            close: optional_price(obj.get("price"))?,
            volume: optional_qty(obj.get("volume_24h"))?,
            exchange_ts_ns,
        });
    }
    Ok(out)
}

fn optional_price(v: Option<&Value>) -> Result<Option<Price>, String> {
    match v {
        Some(Value::String(s)) if !s.is_empty() => {
            Ok(Some(Price(Fixed::parse_str(s).map_err(|e| e.to_string())?)))
        }
        _ => Ok(None),
    }
}

fn map_product_status(s: &str) -> InstrumentStatus {
    match s {
        "online" => InstrumentStatus::Active,
        "offline" | "delisted" => InstrumentStatus::Delisted,
        "internal" | "auction" | "cancel_only" | "post_only" | "limit_only" => {
            InstrumentStatus::Suspended
        }
        _ => InstrumentStatus::Unknown,
    }
}

fn decode_status(obj: &serde_json::Map<String, Value>) -> Result<Vec<DecodedEvent>, String> {
    let products = obj
        .get("products")
        .and_then(|p| p.as_array())
        .ok_or_else(|| "status missing products".to_string())?;
    let mut out = Vec::new();
    for p in products {
        let Some(po) = p.as_object() else { continue };
        let Some(id) = po.get("id").and_then(|x| x.as_str()) else {
            continue;
        };
        let st = po.get("status").and_then(|x| x.as_str()).unwrap_or("");
        out.push(DecodedEvent::ProductStatus {
            product_id: id.to_string(),
            status: map_product_status(st),
            exchange_ts_ns: None,
        });
    }
    if out.is_empty() {
        Ok(vec![DecodedEvent::Unknown])
    } else {
        Ok(out)
    }
}

fn decode_snapshot(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let product_id = required_str(obj, "product_id")?.to_string();
    let bids = decode_levels(obj.get("bids").ok_or("snapshot missing bids")?)?;
    let asks = decode_levels(obj.get("asks").ok_or("snapshot missing asks")?)?;
    Ok(DecodedEvent::BookSnapshot {
        product_id,
        bids,
        asks,
    })
}

fn decode_l2update(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let product_id = required_str(obj, "product_id")?.to_string();
    let changes_v = obj
        .get("changes")
        .and_then(|c| c.as_array())
        .ok_or_else(|| "l2update missing changes".to_string())?;
    let mut changes = Vec::with_capacity(changes_v.len());
    for row in changes_v {
        let arr = row
            .as_array()
            .ok_or_else(|| "l2update change not array".to_string())?;
        if arr.len() < 3 {
            return Err("l2update change needs [side, price, size]".into());
        }
        let side = match arr[0].as_str().unwrap_or("") {
            "buy" => BookSideWire::Bid,
            "sell" => BookSideWire::Ask,
            other => return Err(format!("l2update unknown side {other}")),
        };
        let price = Price(fixed_from_json(&arr[1])?);
        let quantity = Quantity(fixed_from_json(&arr[2])?);
        changes.push(BookLevelChange {
            side,
            price,
            quantity,
        });
    }
    Ok(DecodedEvent::BookDelta {
        product_id,
        changes,
        exchange_ts_ns: obj
            .get("time")
            .and_then(|t| t.as_str())
            .and_then(rfc3339_to_ns),
    })
}

fn decode_levels(v: &Value) -> Result<Vec<(Price, Quantity)>, String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "book levels not array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let pair = row
            .as_array()
            .ok_or_else(|| "book level not [price, size]".to_string())?;
        if pair.len() < 2 {
            return Err("book level needs price and size".into());
        }
        out.push((
            Price(fixed_from_json(&pair[0])?),
            Quantity(fixed_from_json(&pair[1])?),
        ));
    }
    Ok(out)
}

fn optional_qty(v: Option<&Value>) -> Result<Option<Quantity>, String> {
    match v {
        None => Ok(None),
        Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(v) => Ok(Some(Quantity(fixed_from_json(v)?))),
    }
}

fn required_str<'a>(obj: &'a serde_json::Map<String, Value>, key: &str) -> Result<&'a str, String> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("missing {key}"))
}

fn required_u64(obj: &serde_json::Map<String, Value>, key: &str) -> Result<u64, String> {
    obj.get(key)
        .and_then(|value| match value {
            Value::Number(number) => number.as_u64(),
            Value::String(text) => text.parse().ok(),
            _ => None,
        })
        .ok_or_else(|| format!("missing or invalid {key}"))
}

fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("expected number or string".into()),
    }
}

pub fn trade_id_source(id: u64) -> SourceId {
    SourceId(id.to_string())
}

pub fn ns_to_ts(ns: i64) -> marketfeed_model::TimestampNs {
    marketfeed_model::TimestampNs(ns)
}

/// Minimal RFC3339 → Unix ns for `YYYY-MM-DDTHH:MM:SS[.frac]Z`.
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

/// Howard Hinnant civil → days since Unix epoch.
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
    fn decode_match_maker_side_inverts_aggressor() {
        let raw = br#"{"type":"match","trade_id":10,"sequence":50,"time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","size":"5.23512","price":"400.23","side":"sell"}"#;
        let DecodedEvent::Trade(t) = &decode_text(raw).unwrap()[0] else {
            panic!("trade");
        };
        assert_eq!(t.aggressor, AggressorSide::Buy);
        assert_eq!(t.price.0, Fixed::parse_str("400.23").unwrap());
        assert_eq!(t.quantity.0, Fixed::parse_str("5.23512").unwrap());
        assert_eq!(t.trade_id, 10);
    }

    #[test]
    fn decode_heartbeat_retains_recovery_metadata() {
        let raw = br#"{"type":"heartbeat","sequence":90,"last_trade_id":20,"product_id":"BTC-USD","time":"2014-11-07T08:19:28.000000Z"}"#;
        let DecodedEvent::Heartbeat(heartbeat) = &decode_text(raw).unwrap()[0] else {
            panic!("heartbeat")
        };

        assert_eq!(heartbeat.product_id, "BTC-USD");
        assert_eq!(heartbeat.sequence, 90);
        assert_eq!(heartbeat.last_trade_id, 20);
        assert!(heartbeat.exchange_ts_ns.is_some());
    }

    #[test]
    fn decode_ticker_exact_fixed_with_stats24h() {
        let raw = br#"{"type":"ticker","sequence":1,"product_id":"BTC-USD","price":"10.01","open_24h":"9.50","volume_24h":"100.5","low_24h":"9.00","high_24h":"11.00","best_bid":"9.99","best_ask":"10.01","best_bid_size":"1.5","best_ask_size":"2.25","time":"2023-09-25T07:49:37.708706Z"}"#;
        let decoded = decode_text(raw).unwrap();
        assert_eq!(decoded.len(), 2);
        let DecodedEvent::Quote {
            bid_price,
            ask_price,
            bid_qty,
            ask_qty,
            ..
        } = &decoded[0]
        else {
            panic!("quote");
        };
        assert_eq!(bid_price.0, Fixed::parse_str("9.99").unwrap());
        assert_eq!(ask_price.0, Fixed::parse_str("10.01").unwrap());
        assert_eq!(bid_qty.unwrap().0, Fixed::parse_str("1.5").unwrap());
        assert_eq!(ask_qty.unwrap().0, Fixed::parse_str("2.25").unwrap());
        let DecodedEvent::Statistics24h {
            open,
            high,
            low,
            close,
            volume,
            ..
        } = &decoded[1]
        else {
            panic!("stats");
        };
        assert_eq!(open.unwrap().0, Fixed::parse_str("9.50").unwrap());
        assert_eq!(high.unwrap().0, Fixed::parse_str("11.00").unwrap());
        assert_eq!(low.unwrap().0, Fixed::parse_str("9.00").unwrap());
        assert_eq!(close.unwrap().0, Fixed::parse_str("10.01").unwrap());
        assert_eq!(volume.unwrap().0, Fixed::parse_str("100.5").unwrap());
    }

    #[test]
    fn decode_status_maps_product_status() {
        let raw = br#"{"type":"status","products":[{"id":"BTC-USD","status":"online"},{"id":"ETH-USD","status":"offline"}]}"#;
        let decoded = decode_text(raw).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(matches!(
            &decoded[0],
            DecodedEvent::ProductStatus {
                product_id,
                status: InstrumentStatus::Active,
                ..
            } if product_id == "BTC-USD"
        ));
        assert!(matches!(
            &decoded[1],
            DecodedEvent::ProductStatus {
                product_id,
                status: InstrumentStatus::Delisted,
                ..
            } if product_id == "ETH-USD"
        ));
    }

    #[test]
    fn decode_snapshot_and_l2update() {
        let snap = br#"{"type":"snapshot","product_id":"BTC-USD","bids":[["101.10","1.5"],["101.00","2"]],"asks":[["101.20","3"],["101.30","0.5"]]}"#;
        let DecodedEvent::BookSnapshot { bids, asks, .. } = &decode_text(snap).unwrap()[0] else {
            panic!("snapshot");
        };
        assert_eq!(bids[0].0.0, Fixed::parse_str("101.10").unwrap());
        assert_eq!(asks[1].1.0, Fixed::parse_str("0.5").unwrap());

        let upd = br#"{"type":"l2update","product_id":"BTC-USD","time":"2014-11-07T08:19:27.028459Z","changes":[["buy","101.10","0"],["sell","101.25","1.25"]]}"#;
        let DecodedEvent::BookDelta { changes, .. } = &decode_text(upd).unwrap()[0] else {
            panic!("delta");
        };
        assert_eq!(changes[0].side, BookSideWire::Bid);
        assert_eq!(changes[0].quantity.0.coefficient, 0);
        assert_eq!(changes[1].price.0, Fixed::parse_str("101.25").unwrap());
    }

    #[test]
    fn decode_candles_rest_exact_fixed() {
        let raw = br#"[[1609459200,"0.0015","0.0025","0.0010","0.0020","1000"]]"#;
        let DecodedEvent::Candle {
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
}
