//! Coinbase Advanced Trade public decode (exact Fixed; no f64 arithmetic).
//!
//! Protocol split from Exchange Classic (`crate::messages`):
//! - WS frames use `channel` + `events[]` (not Exchange `type` field).
//! - Public channels (no JWT): `market_trades`, `ticker` / `ticker_batch`,
//!   `level2` (wire `l2_data`), `status`, `heartbeats`, optional WS `candles`.
//! - Candles primary path: public REST
//!   `GET /api/v3/brokerage/market/products/{id}/candles`
//!   with required `start` / `end` / `granularity` (string enum).
//!   WS `candles` is 5m-only (decode supported; SessionMachine does not subscribe).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{
    AggressorSide, Fixed, InstrumentStatus, Price, Quantity, SourceId, TimestampNs,
};
use serde_json::Value;

/// Advanced Trade public WS candles channel bucket (docs: five minutes).
pub const WS_CANDLE_INTERVAL_NS: i64 = 300_000_000_000;

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
    /// Candle from REST poll or public WS `candles` (5m).
    Candle {
        /// Empty when REST body has no product (session fills from pending map).
        product_id: String,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
    },
    /// Public `status` channel product row → `InstrumentUpdate`.
    InstrumentStatus {
        product_id: String,
        status: InstrumentStatus,
    },
    SubscribeAck,
    Heartbeat,
    Error(String),
    Unknown,
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

pub fn candle_granularity(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "ONE_MINUTE",
        CandleInterval::M5 => "FIVE_MINUTE",
        CandleInterval::M15 => "FIFTEEN_MINUTE",
        CandleInterval::H1 => "ONE_HOUR",
        CandleInterval::D1 => "ONE_DAY",
    }
}

pub fn candle_granularity_secs(interval: CandleInterval) -> i64 {
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

/// Build public candles URL. `now` drives required `start`/`end` window.
pub fn candles_url(
    rest_base: &str,
    product: &str,
    interval: CandleInterval,
    now: TimestampNs,
) -> String {
    let end_sec = now.0.div_euclid(1_000_000_000);
    let gran = candle_granularity_secs(interval);
    // ponytail: 3× granularity window is enough to always include the open bar.
    // Ceiling = sparse products may return empty; upgrade = widen or retry.
    let start_sec = end_sec.saturating_sub(gran.saturating_mul(3).max(gran));
    let g = candle_granularity(interval);
    format!(
        "{rest_base}/products/{product}/candles?start={start_sec}&end={end_sec}&granularity={g}&limit=3"
    )
}

pub fn decode_candles_rest(bytes: &[u8], interval: CandleInterval) -> Result<DecodedEvent, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let candles = v
        .get("candles")
        .and_then(|c| c.as_array())
        .ok_or_else(|| "coinbase-adv candles missing array".to_string())?;
    // Public API returns newest-first (live probe); take first non-empty row.
    let row = candles
        .first()
        .and_then(|c| c.as_object())
        .ok_or_else(|| "coinbase-adv candles empty".to_string())?;
    let start_sec = match row.get("start") {
        Some(Value::String(s)) => s.parse::<i64>().map_err(|e| format!("candle start: {e}"))?,
        Some(Value::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|u| u as i64))
            .ok_or_else(|| "candle start not i64".to_string())?,
        _ => return Err("candle start missing".into()),
    };
    Ok(DecodedEvent::Candle {
        product_id: String::new(),
        open: Price(fixed_field(row, "open")?),
        high: Price(fixed_field(row, "high")?),
        low: Price(fixed_field(row, "low")?),
        close: Price(fixed_field(row, "close")?),
        volume: Quantity(fixed_field(row, "volume")?),
        interval_ns: candle_interval_ns(interval),
        start_ts: TimestampNs(start_sec.saturating_mul(1_000_000_000)),
    })
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
    let channel = obj
        .get("channel")
        .and_then(|c| c.as_str())
        .or_else(|| obj.get("type").and_then(|t| t.as_str()))
        .unwrap_or("");
    let sequence = obj.get("sequence_num").and_then(|s| s.as_u64());
    let frame_ts = obj
        .get("timestamp")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_to_ns);
    match channel {
        "subscriptions" => Ok(vec![DecodedEvent::SubscribeAck]),
        "heartbeats" => Ok(vec![DecodedEvent::Heartbeat]),
        "error" => {
            let msg = obj
                .get("message")
                .and_then(|m| m.as_str())
                .or_else(|| {
                    obj.get("events")
                        .and_then(|e| e.as_array())
                        .and_then(|a| a.first())
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str())
                })
                .unwrap_or("coinbase-adv error")
                .to_string();
            Ok(vec![DecodedEvent::Error(msg)])
        }
        "market_trades" => decode_market_trades(obj, sequence),
        "ticker" | "ticker_batch" => decode_ticker(obj, frame_ts),
        // Subscribe channel is `level2`; wire payloads use `l2_data`.
        "l2_data" | "level2" => decode_l2(obj),
        "status" => decode_status(obj),
        "candles" => decode_ws_candles(obj),
        _ => Ok(vec![DecodedEvent::Unknown]),
    }
}

fn decode_status(obj: &serde_json::Map<String, Value>) -> Result<Vec<DecodedEvent>, String> {
    let events = obj
        .get("events")
        .and_then(|e| e.as_array())
        .ok_or_else(|| "status missing events".to_string())?;
    let mut out = Vec::new();
    for ev in events {
        let Some(ev) = ev.as_object() else {
            continue;
        };
        let products = ev
            .get("products")
            .and_then(|p| p.as_array())
            .ok_or_else(|| "status event missing products".to_string())?;
        for p in products {
            let Some(row) = p.as_object() else {
                continue;
            };
            let product_id = row
                .get("id")
                .and_then(|v| v.as_str())
                .or_else(|| row.get("product_id").and_then(|v| v.as_str()))
                .ok_or_else(|| "status product missing id".to_string())?
                .to_string();
            let status_raw = row.get("status").and_then(|s| s.as_str()).unwrap_or("");
            out.push(DecodedEvent::InstrumentStatus {
                product_id,
                status: map_product_status(status_raw),
            });
        }
    }
    if out.is_empty() {
        Ok(vec![DecodedEvent::Unknown])
    } else {
        Ok(out)
    }
}

fn map_product_status(raw: &str) -> InstrumentStatus {
    match raw.to_ascii_lowercase().as_str() {
        "online" => InstrumentStatus::Active,
        "offline" | "internal" => InstrumentStatus::Suspended,
        "delisted" => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    }
}

fn decode_ws_candles(obj: &serde_json::Map<String, Value>) -> Result<Vec<DecodedEvent>, String> {
    let events = obj
        .get("events")
        .and_then(|e| e.as_array())
        .ok_or_else(|| "candles missing events".to_string())?;
    let mut out = Vec::new();
    for ev in events {
        let Some(ev) = ev.as_object() else {
            continue;
        };
        let candles = ev
            .get("candles")
            .and_then(|c| c.as_array())
            .ok_or_else(|| "candles event missing candles".to_string())?;
        for c in candles {
            let Some(row) = c.as_object() else {
                continue;
            };
            let start_sec = match row.get("start") {
                Some(Value::String(s)) => {
                    s.parse::<i64>().map_err(|e| format!("candle start: {e}"))?
                }
                Some(Value::Number(n)) => n
                    .as_i64()
                    .or_else(|| n.as_u64().map(|u| u as i64))
                    .ok_or_else(|| "candle start not i64".to_string())?,
                _ => return Err("candle start missing".into()),
            };
            out.push(DecodedEvent::Candle {
                product_id: required_str(row, "product_id")?.to_string(),
                open: Price(fixed_field(row, "open")?),
                high: Price(fixed_field(row, "high")?),
                low: Price(fixed_field(row, "low")?),
                close: Price(fixed_field(row, "close")?),
                volume: Quantity(fixed_field(row, "volume")?),
                interval_ns: WS_CANDLE_INTERVAL_NS,
                start_ts: TimestampNs(start_sec.saturating_mul(1_000_000_000)),
            });
        }
    }
    if out.is_empty() {
        Ok(vec![DecodedEvent::Unknown])
    } else {
        Ok(out)
    }
}

fn decode_market_trades(
    obj: &serde_json::Map<String, Value>,
    sequence: Option<u64>,
) -> Result<Vec<DecodedEvent>, String> {
    let events = obj
        .get("events")
        .and_then(|e| e.as_array())
        .ok_or_else(|| "market_trades missing events".to_string())?;
    let mut out = Vec::new();
    for ev in events {
        let Some(ev) = ev.as_object() else {
            continue;
        };
        let trades = ev
            .get("trades")
            .and_then(|t| t.as_array())
            .ok_or_else(|| "market_trades event missing trades".to_string())?;
        for t in trades {
            let Some(row) = t.as_object() else {
                continue;
            };
            out.push(DecodedEvent::Trade(decode_trade_row(row, sequence)?));
        }
    }
    if out.is_empty() {
        Ok(vec![DecodedEvent::Unknown])
    } else {
        Ok(out)
    }
}

fn decode_trade_row(
    row: &serde_json::Map<String, Value>,
    sequence: Option<u64>,
) -> Result<TradeRow, String> {
    let product_id = required_str(row, "product_id")?.to_string();
    let trade_id = match row.get("trade_id") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return Err("market_trades missing trade_id".into()),
    };
    // Advanced Trade `side` is the **maker** side (BUY/SELL); aggressor is opposite.
    let maker = row.get("side").and_then(|s| s.as_str()).unwrap_or("");
    let aggressor = match maker {
        "BUY" | "buy" => AggressorSide::Sell,
        "SELL" | "sell" => AggressorSide::Buy,
        _ => AggressorSide::Unknown,
    };
    Ok(TradeRow {
        product_id,
        trade_id,
        price: Price(fixed_field(row, "price")?),
        quantity: Quantity(fixed_field(row, "size")?),
        aggressor,
        sequence,
        exchange_ts_ns: row
            .get("time")
            .and_then(|t| t.as_str())
            .and_then(rfc3339_to_ns),
    })
}

fn decode_ticker(
    obj: &serde_json::Map<String, Value>,
    frame_ts: Option<i64>,
) -> Result<Vec<DecodedEvent>, String> {
    let events = obj
        .get("events")
        .and_then(|e| e.as_array())
        .ok_or_else(|| "ticker missing events".to_string())?;
    let mut out = Vec::new();
    for ev in events {
        let Some(ev) = ev.as_object() else {
            continue;
        };
        let tickers = ev
            .get("tickers")
            .and_then(|t| t.as_array())
            .ok_or_else(|| "ticker event missing tickers".to_string())?;
        for t in tickers {
            let Some(row) = t.as_object() else {
                continue;
            };
            let Some(bid) = row
                .get("best_bid")
                .and_then(|b| b.as_str())
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let Some(ask) = row
                .get("best_ask")
                .and_then(|a| a.as_str())
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let product_id = required_str(row, "product_id")?.to_string();
            out.push(DecodedEvent::Quote {
                product_id: product_id.clone(),
                bid_price: Price(Fixed::parse_str(bid).map_err(|e| e.to_string())?),
                bid_qty: optional_qty(row.get("best_bid_quantity"))?,
                ask_price: Price(Fixed::parse_str(ask).map_err(|e| e.to_string())?),
                ask_qty: optional_qty(row.get("best_ask_quantity"))?,
                exchange_ts_ns: frame_ts,
            });
            // W6-P0a: 24h fields already on Advanced Trade ticker wire.
            if row.get("open_24_h").is_some()
                || row.get("high_24_h").is_some()
                || row.get("low_24_h").is_some()
                || row.get("volume_24_h").is_some()
                || row.get("price").is_some()
            {
                out.push(DecodedEvent::Statistics24h {
                    product_id,
                    open: optional_price(row.get("open_24_h"))?,
                    high: optional_price(row.get("high_24_h"))?,
                    low: optional_price(row.get("low_24_h"))?,
                    close: optional_price(row.get("price"))?,
                    volume: optional_qty(row.get("volume_24_h"))?,
                    exchange_ts_ns: frame_ts,
                });
            }
        }
    }
    if out.is_empty() {
        Ok(vec![DecodedEvent::Unknown])
    } else {
        Ok(out)
    }
}

fn decode_l2(obj: &serde_json::Map<String, Value>) -> Result<Vec<DecodedEvent>, String> {
    let events = obj
        .get("events")
        .and_then(|e| e.as_array())
        .ok_or_else(|| "l2 missing events".to_string())?;
    let mut out = Vec::new();
    for ev in events {
        let Some(ev) = ev.as_object() else {
            continue;
        };
        let product_id = required_str(ev, "product_id")?.to_string();
        let typ = ev.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let updates = ev
            .get("updates")
            .and_then(|u| u.as_array())
            .ok_or_else(|| "l2 event missing updates".to_string())?;
        match typ {
            "snapshot" => {
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                for u in updates {
                    let Some(row) = u.as_object() else {
                        continue;
                    };
                    let price = Price(fixed_field(row, "price_level")?);
                    let qty = Quantity(fixed_field(row, "new_quantity")?);
                    match row.get("side").and_then(|s| s.as_str()).unwrap_or("") {
                        "bid" | "BID" | "buy" | "BUY" => bids.push((price, qty)),
                        "ask" | "ASK" | "offer" | "sell" | "SELL" => asks.push((price, qty)),
                        other => return Err(format!("l2 snapshot unknown side {other}")),
                    }
                }
                out.push(DecodedEvent::BookSnapshot {
                    product_id,
                    bids,
                    asks,
                });
            }
            "update" => {
                let mut changes = Vec::with_capacity(updates.len());
                let mut exchange_ts_ns = None;
                for u in updates {
                    let Some(row) = u.as_object() else {
                        continue;
                    };
                    let side = match row.get("side").and_then(|s| s.as_str()).unwrap_or("") {
                        "bid" | "BID" | "buy" | "BUY" => BookSideWire::Bid,
                        "ask" | "ASK" | "offer" | "sell" | "SELL" => BookSideWire::Ask,
                        other => return Err(format!("l2 update unknown side {other}")),
                    };
                    changes.push(BookLevelChange {
                        side,
                        price: Price(fixed_field(row, "price_level")?),
                        quantity: Quantity(fixed_field(row, "new_quantity")?),
                    });
                    if exchange_ts_ns.is_none() {
                        exchange_ts_ns = row
                            .get("event_time")
                            .and_then(|t| t.as_str())
                            .and_then(rfc3339_to_ns);
                    }
                }
                out.push(DecodedEvent::BookDelta {
                    product_id,
                    changes,
                    exchange_ts_ns,
                });
            }
            _ => {}
        }
    }
    if out.is_empty() {
        Ok(vec![DecodedEvent::Unknown])
    } else {
        Ok(out)
    }
}

fn fixed_field(obj: &serde_json::Map<String, Value>, key: &str) -> Result<Fixed, String> {
    let v = obj.get(key).ok_or_else(|| format!("missing {key}"))?;
    fixed_from_json(v)
}

fn optional_price(v: Option<&Value>) -> Result<Option<Price>, String> {
    match v {
        Some(Value::String(s)) if !s.is_empty() => {
            Ok(Some(Price(Fixed::parse_str(s).map_err(|e| e.to_string())?)))
        }
        _ => Ok(None),
    }
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

fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("expected number or string".into()),
    }
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
    fn decode_candles_rest_exact_fixed() {
        let raw = br#"{"candles":[{"start":"1609459980","low":"28800","high":"28902.46","open":"28901.57","close":"28800.01","volume":"49.3149836"}]}"#;
        let DecodedEvent::Candle {
            product_id,
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
        } = decode_candles_rest(raw, CandleInterval::M1).unwrap()
        else {
            panic!("candle");
        };
        assert!(product_id.is_empty());
        assert_eq!(open.0, Fixed::parse_str("28901.57").unwrap());
        assert_eq!(high.0, Fixed::parse_str("28902.46").unwrap());
        assert_eq!(low.0, Fixed::parse_str("28800").unwrap());
        assert_eq!(close.0, Fixed::parse_str("28800.01").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("49.3149836").unwrap());
        assert_eq!(interval_ns, 60_000_000_000);
        assert_eq!(start_ts, TimestampNs(1_609_459_980_000_000_000));
    }

    #[test]
    fn candles_url_uses_public_market_path_and_window() {
        let url = candles_url(
            "https://api.coinbase.com/api/v3/brokerage/market",
            "BTC-USD",
            CandleInterval::M1,
            TimestampNs(1_609_460_000_000_000_000),
        );
        assert!(url.contains("/market/products/BTC-USD/candles"), "{url}");
        assert!(url.contains("granularity=ONE_MINUTE"), "{url}");
        assert!(url.contains("start=1609459820"), "{url}");
        assert!(url.contains("end=1609460000"), "{url}");
    }

    #[test]
    fn decode_subscriptions_and_heartbeats() {
        let sub = br#"{"channel":"subscriptions","events":[{"subscriptions":{"heartbeats":["heartbeats"]}}]}"#;
        assert_eq!(decode_text(sub).unwrap(), vec![DecodedEvent::SubscribeAck]);
        let hb = br#"{"channel":"heartbeats","events":[{"current_time":"2023-06-23 17:17:22 +0000 UTC","heartbeat_counter":"1"}]}"#;
        assert_eq!(decode_text(hb).unwrap(), vec![DecodedEvent::Heartbeat]);
    }

    #[test]
    fn decode_market_trades_maker_side_inverts_aggressor() {
        let raw = br#"{"channel":"market_trades","timestamp":"2023-02-09T20:19:35.39625135Z","sequence_num":0,"events":[{"type":"update","trades":[{"trade_id":"10","product_id":"BTC-USD","price":"400.23","size":"5.23512","side":"SELL","time":"2014-11-07T08:19:27.028459Z"}]}]}"#;
        let events = decode_text(raw).unwrap();
        let DecodedEvent::Trade(t) = &events[0] else {
            panic!("trade");
        };
        assert_eq!(t.aggressor, AggressorSide::Buy);
        assert_eq!(t.price.0, Fixed::parse_str("400.23").unwrap());
        assert_eq!(t.quantity.0, Fixed::parse_str("5.23512").unwrap());
        assert_eq!(t.trade_id, "10");
    }

    #[test]
    fn decode_ticker_exact_fixed() {
        let raw = br#"{"channel":"ticker","timestamp":"2023-02-09T20:30:37.167359596Z","sequence_num":0,"events":[{"type":"snapshot","tickers":[{"type":"ticker","product_id":"BTC-USD","price":"10.01","open_24_h":"9.50","volume_24_h":"100.5","low_24_h":"9.00","high_24_h":"11.00","best_bid":"9.99","best_ask":"10.01","best_bid_quantity":"1.5","best_ask_quantity":"2.25"}]}]}"#;
        let events = decode_text(raw).unwrap();
        assert_eq!(events.len(), 2);
        let DecodedEvent::Quote {
            bid_price,
            ask_price,
            bid_qty,
            ask_qty,
            ..
        } = &events[0]
        else {
            panic!("quote");
        };
        assert_eq!(bid_price.0, Fixed::parse_str("9.99").unwrap());
        assert_eq!(ask_price.0, Fixed::parse_str("10.01").unwrap());
        assert_eq!(
            bid_qty.as_ref().unwrap().0,
            Fixed::parse_str("1.5").unwrap()
        );
        assert_eq!(
            ask_qty.as_ref().unwrap().0,
            Fixed::parse_str("2.25").unwrap()
        );
        let DecodedEvent::Statistics24h {
            open,
            high,
            low,
            close,
            volume,
            ..
        } = &events[1]
        else {
            panic!("stats");
        };
        assert_eq!(open.as_ref().unwrap().0, Fixed::parse_str("9.50").unwrap());
        assert_eq!(high.as_ref().unwrap().0, Fixed::parse_str("11.00").unwrap());
        assert_eq!(low.as_ref().unwrap().0, Fixed::parse_str("9.00").unwrap());
        assert_eq!(
            close.as_ref().unwrap().0,
            Fixed::parse_str("10.01").unwrap()
        );
        assert_eq!(
            volume.as_ref().unwrap().0,
            Fixed::parse_str("100.5").unwrap()
        );
    }

    #[test]
    fn decode_l2_snapshot_and_update() {
        let snap = br#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:50.714964855Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"101.10","new_quantity":"1.5"},{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"101.00","new_quantity":"2"},{"side":"ask","event_time":"1970-01-01T00:00:00Z","price_level":"101.20","new_quantity":"3"},{"side":"ask","event_time":"1970-01-01T00:00:00Z","price_level":"101.30","new_quantity":"0.5"}]}]}"#;
        let events = decode_text(snap).unwrap();
        let DecodedEvent::BookSnapshot { bids, asks, .. } = &events[0] else {
            panic!("snapshot");
        };
        assert_eq!(bids[0].0.0, Fixed::parse_str("101.10").unwrap());
        assert_eq!(asks[1].1.0, Fixed::parse_str("0.5").unwrap());

        let upd = br#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:51Z","sequence_num":1,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2014-11-07T08:19:27.028459Z","price_level":"101.10","new_quantity":"0"},{"side":"ask","event_time":"2014-11-07T08:19:27.028459Z","price_level":"101.25","new_quantity":"1.25"}]}]}"#;
        let events = decode_text(upd).unwrap();
        let DecodedEvent::BookDelta { changes, .. } = &events[0] else {
            panic!("delta");
        };
        assert_eq!(changes[0].side, BookSideWire::Bid);
        assert_eq!(changes[0].quantity.0.coefficient, 0);
        assert_eq!(changes[1].price.0, Fixed::parse_str("101.25").unwrap());
    }

    #[test]
    fn decode_status_maps_product_status() {
        let raw = br#"{"channel":"status","timestamp":"2023-02-09T20:29:49.753424311Z","sequence_num":0,"events":[{"type":"snapshot","products":[{"product_type":"SPOT","id":"BTC-USD","status":"online"},{"id":"DEAD-USD","status":"delisted"},{"id":"OFF-USD","status":"offline"}]}]}"#;
        let events = decode_text(raw).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0],
            DecodedEvent::InstrumentStatus {
                product_id: "BTC-USD".into(),
                status: InstrumentStatus::Active,
            }
        );
        assert_eq!(
            events[1],
            DecodedEvent::InstrumentStatus {
                product_id: "DEAD-USD".into(),
                status: InstrumentStatus::Delisted,
            }
        );
        assert_eq!(
            events[2],
            DecodedEvent::InstrumentStatus {
                product_id: "OFF-USD".into(),
                status: InstrumentStatus::Suspended,
            }
        );
    }

    #[test]
    fn decode_ws_candles_five_minute_bucket() {
        let raw = br#"{"channel":"candles","timestamp":"2023-06-09T20:19:35.39625135Z","sequence_num":0,"events":[{"type":"snapshot","candles":[{"start":"1688998200","high":"1867.72","low":"1865.63","open":"1867.38","close":"1866.81","volume":"0.20269406","product_id":"ETH-USD"}]}]}"#;
        let events = decode_text(raw).unwrap();
        let DecodedEvent::Candle {
            product_id,
            open,
            interval_ns,
            start_ts,
            ..
        } = &events[0]
        else {
            panic!("candle");
        };
        assert_eq!(product_id, "ETH-USD");
        assert_eq!(*interval_ns, WS_CANDLE_INTERVAL_NS);
        assert_eq!(open.0, Fixed::parse_str("1867.38").unwrap());
        assert_eq!(*start_ts, TimestampNs(1_688_998_200_000_000_000));
    }
}
