//! Kraken Spot WS v2 JSON decoding (exact Fixed; no f64 arithmetic).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{AggressorSide, Fixed, Price, Quantity, SourceId, TimestampNs};
use serde::Deserialize;
use serde_json::Value;
use serde_json::value::RawValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedEvent {
    /// All trades from one `trade` frame (a single frame may batch several rows).
    Trades(Vec<TradeDecoded>),
    Quote {
        symbol: String,
        bid_price: Price,
        bid_qty: Quantity,
        ask_price: Price,
        ask_qty: Quantity,
        /// W6-P0a: ticker high/low/volume/last → Statistics24h in session.
        high: Option<Price>,
        low: Option<Price>,
        volume: Option<Quantity>,
        last: Option<Price>,
    },
    BookSnapshot {
        symbol: String,
        bids: Vec<RawLevel>,
        asks: Vec<RawLevel>,
        checksum: u32,
        exchange_ts_ns: Option<i64>,
    },
    BookUpdate {
        symbol: String,
        bids: Vec<RawLevel>,
        asks: Vec<RawLevel>,
        checksum: u32,
        exchange_ts_ns: Option<i64>,
    },
    /// One `ohlc` row (snapshot frames may carry several history bars).
    Candle {
        symbol: String,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
        exchange_ts_ns: Option<i64>,
    },
    VenueStatus {
        status: String,
    },
    SubscribeAck,
    Heartbeat,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeDecoded {
    pub symbol: String,
    pub trade_id: u64,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub exchange_ts_ns: Option<i64>,
}

/// One book price level, keeping both the parsed exact value (for the book/model
/// layer) and the literal wire text (for the CRC32 checksum, which is sensitive to
/// the exact digit string — see `checksum.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLevel {
    pub price: Price,
    pub quantity: Quantity,
    pub price_str: String,
    pub qty_str: String,
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
    if v.get("channel").and_then(Value::as_str) == Some("book") {
        let msg_type = v.get("type").and_then(Value::as_str).unwrap_or("");
        return decode_book(bytes, msg_type);
    }
    decode_value(&v)
}

fn decode_value(v: &Value) -> Result<DecodedEvent, String> {
    let Some(obj) = v.as_object() else {
        return Ok(DecodedEvent::Unknown);
    };

    if obj.get("channel").and_then(|c| c.as_str()) == Some("heartbeat") {
        return Ok(DecodedEvent::Heartbeat);
    }
    if obj.get("channel").and_then(|c| c.as_str()) == Some("status") {
        let status = obj
            .get("data")
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(Value::as_object)
            .and_then(|row| row.get("system"))
            .and_then(Value::as_str)
            .ok_or_else(|| "status missing system".to_string())?;
        return Ok(DecodedEvent::VenueStatus {
            status: status.to_string(),
        });
    }

    if obj.get("method").and_then(|m| m.as_str()) == Some("subscribe") {
        if obj.get("success").and_then(|s| s.as_bool()) == Some(true) {
            return Ok(DecodedEvent::SubscribeAck);
        }
        if let Some(err) = obj.get("error").and_then(|e| e.as_str()) {
            return Err(format!("subscribe failed: {err}"));
        }
        return Ok(DecodedEvent::SubscribeAck);
    }

    let channel = obj.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    let data = obj.get("data");

    match channel {
        "trade" => decode_trades(data.unwrap_or(&Value::Null)),
        "ticker" => decode_ticker(data.unwrap_or(&Value::Null)),
        "ohlc" => decode_ohlc(data.unwrap_or(&Value::Null)),
        _ => Ok(DecodedEvent::Unknown),
    }
}

/// Kraken `ohlc` subscribe `interval` (minutes) for a canonical interval.
pub fn ohlc_interval_minutes(interval: CandleInterval) -> u32 {
    match interval {
        CandleInterval::M1 => 1,
        CandleInterval::M5 => 5,
        CandleInterval::M15 => 15,
        CandleInterval::H1 => 60,
        CandleInterval::D1 => 1440,
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

fn interval_ns_from_minutes(mins: u64) -> Result<i64, String> {
    let interval = match mins {
        1 => CandleInterval::M1,
        5 => CandleInterval::M5,
        15 => CandleInterval::M15,
        60 => CandleInterval::H1,
        1440 => CandleInterval::D1,
        other => return Err(format!("unsupported ohlc interval minutes {other}")),
    };
    Ok(candle_interval_ns(interval))
}

/// Decode `ohlc` data array; returns the **last** row (live update or newest history bar).
///
/// # ponytail
/// Kraken has no `is_closed` flag — every trade may push a partial bar. Ceiling =
/// consumers see in-progress OHLC; upgrade = buffer until `interval_begin` advances.
fn decode_ohlc(data: &Value) -> Result<DecodedEvent, String> {
    let arr = data
        .as_array()
        .ok_or_else(|| "ohlc data not array".to_string())?;
    let row = arr
        .last()
        .ok_or_else(|| "ohlc data empty".to_string())?
        .as_object()
        .ok_or_else(|| "ohlc row not object".to_string())?;
    let symbol = row
        .get("symbol")
        .and_then(|s| s.as_str())
        .ok_or_else(|| "ohlc missing symbol".to_string())?
        .to_string();
    let interval_mins = row
        .get("interval")
        .and_then(|i| i.as_u64())
        .ok_or_else(|| "ohlc missing interval".to_string())?;
    let start_ts = row
        .get("interval_begin")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_to_ns)
        .ok_or_else(|| "ohlc missing/bad interval_begin".to_string())?;
    let exchange_ts_ns = row
        .get("timestamp")
        .and_then(|t| t.as_str())
        .and_then(rfc3339_to_ns);
    Ok(DecodedEvent::Candle {
        symbol,
        open: Price(fixed_from_json(
            row.get("open")
                .ok_or_else(|| "ohlc missing open".to_string())?,
        )?),
        high: Price(fixed_from_json(
            row.get("high")
                .ok_or_else(|| "ohlc missing high".to_string())?,
        )?),
        low: Price(fixed_from_json(
            row.get("low")
                .ok_or_else(|| "ohlc missing low".to_string())?,
        )?),
        close: Price(fixed_from_json(
            row.get("close")
                .ok_or_else(|| "ohlc missing close".to_string())?,
        )?),
        volume: Quantity(fixed_from_json(
            row.get("volume")
                .ok_or_else(|| "ohlc missing volume".to_string())?,
        )?),
        interval_ns: interval_ns_from_minutes(interval_mins)?,
        start_ts: TimestampNs(start_ts),
        exchange_ts_ns,
    })
}

fn decode_trades(data: &Value) -> Result<DecodedEvent, String> {
    let arr = data
        .as_array()
        .ok_or_else(|| "trade data not array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let o = row
            .as_object()
            .ok_or_else(|| "trade row not object".to_string())?;
        let symbol = o
            .get("symbol")
            .and_then(|s| s.as_str())
            .ok_or_else(|| "trade missing symbol".to_string())?
            .to_string();
        let trade_id = o
            .get("trade_id")
            .and_then(|t| t.as_u64())
            .ok_or_else(|| "trade missing trade_id".to_string())?;
        let side = o.get("side").and_then(|s| s.as_str()).unwrap_or("");
        let aggressor = match side {
            "buy" => AggressorSide::Buy,
            "sell" => AggressorSide::Sell,
            _ => AggressorSide::Unknown,
        };
        out.push(TradeDecoded {
            symbol,
            trade_id,
            price: Price(fixed_from_json(
                o.get("price")
                    .ok_or_else(|| "trade missing price".to_string())?,
            )?),
            quantity: Quantity(fixed_from_json(
                o.get("qty")
                    .ok_or_else(|| "trade missing qty".to_string())?,
            )?),
            aggressor,
            exchange_ts_ns: o
                .get("timestamp")
                .and_then(|t| t.as_str())
                .and_then(rfc3339_to_ns),
        });
    }
    Ok(DecodedEvent::Trades(out))
}

fn decode_ticker(data: &Value) -> Result<DecodedEvent, String> {
    let row = match data {
        Value::Array(a) => a.first().ok_or_else(|| "empty ticker data".to_string())?,
        Value::Object(_) => data,
        _ => return Err("ticker data unexpected".into()),
    };
    let o = row
        .as_object()
        .ok_or_else(|| "ticker row not object".to_string())?;
    let symbol = o
        .get("symbol")
        .and_then(|s| s.as_str())
        .ok_or_else(|| "ticker missing symbol".to_string())?
        .to_string();
    let bid = o
        .get("bid")
        .ok_or_else(|| "ticker missing bid".to_string())?;
    let ask = o
        .get("ask")
        .ok_or_else(|| "ticker missing ask".to_string())?;
    let bid_qty = o
        .get("bid_qty")
        .ok_or_else(|| "ticker missing bid_qty".to_string())?;
    let ask_qty = o
        .get("ask_qty")
        .ok_or_else(|| "ticker missing ask_qty".to_string())?;
    Ok(DecodedEvent::Quote {
        symbol,
        bid_price: Price(fixed_from_json(bid)?),
        bid_qty: Quantity(fixed_from_json(bid_qty)?),
        ask_price: Price(fixed_from_json(ask)?),
        ask_qty: Quantity(fixed_from_json(ask_qty)?),
        high: optional_price(o, "high")?,
        low: optional_price(o, "low")?,
        volume: optional_qty(o, "volume")?,
        last: optional_price(o, "last")?,
    })
}

fn optional_price(o: &serde_json::Map<String, Value>, key: &str) -> Result<Option<Price>, String> {
    match o.get(key) {
        Some(v) => Ok(Some(Price(fixed_from_json(v)?))),
        None => Ok(None),
    }
}

fn optional_qty(o: &serde_json::Map<String, Value>, key: &str) -> Result<Option<Quantity>, String> {
    match o.get(key) {
        Some(v) => Ok(Some(Quantity(fixed_from_json(v)?))),
        None => Ok(None),
    }
}

#[derive(Deserialize)]
struct BookFrameWire {
    data: Vec<BookRowWire>,
}

#[derive(Deserialize)]
struct BookRowWire {
    symbol: String,
    bids: Vec<BookLevelWire>,
    asks: Vec<BookLevelWire>,
    checksum: u64,
    timestamp: Option<String>,
}

#[derive(Deserialize)]
struct BookLevelWire {
    price: Box<RawValue>,
    qty: Box<RawValue>,
}

fn decode_book(bytes: &[u8], msg_type: &str) -> Result<DecodedEvent, String> {
    let frame: BookFrameWire = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let row = frame
        .data
        .into_iter()
        .next()
        .ok_or_else(|| "empty book data".to_string())?;
    let bids = decode_raw_levels(row.bids)?;
    let asks = decode_raw_levels(row.asks)?;
    let checksum =
        u32::try_from(row.checksum).map_err(|_| "book checksum overflows u32".to_string())?;
    let exchange_ts_ns = row.timestamp.as_deref().and_then(rfc3339_to_ns);
    match msg_type {
        "snapshot" => Ok(DecodedEvent::BookSnapshot {
            symbol: row.symbol,
            bids,
            asks,
            checksum,
            exchange_ts_ns,
        }),
        "update" => Ok(DecodedEvent::BookUpdate {
            symbol: row.symbol,
            bids,
            asks,
            checksum,
            exchange_ts_ns,
        }),
        _ => Ok(DecodedEvent::Unknown),
    }
}

fn decode_raw_levels(rows: Vec<BookLevelWire>) -> Result<Vec<RawLevel>, String> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let price_str = raw_decimal_lexeme(&row.price)?;
        let qty_str = raw_decimal_lexeme(&row.qty)?;
        out.push(RawLevel {
            price: Price(Fixed::parse_str(&price_str).map_err(|error| error.to_string())?),
            quantity: Quantity(Fixed::parse_str(&qty_str).map_err(|error| error.to_string())?),
            price_str,
            qty_str,
        });
    }
    Ok(out)
}

fn raw_decimal_lexeme(raw: &RawValue) -> Result<String, String> {
    let token = raw.get();
    if token.starts_with('"') {
        serde_json::from_str::<String>(token).map_err(|error| error.to_string())
    } else {
        Fixed::parse_str(token).map_err(|error| error.to_string())?;
        Ok(token.to_string())
    }
}

/// ponytail: JSON numbers via `Number::to_string` (serde_json wire form); ceiling = f64
/// round-trip for non-integral values; upgrade = raw-token decimal scan.
pub fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("expected number or string".into()),
    }
}

pub fn trade_id_source(id: u64) -> SourceId {
    SourceId(id.to_string())
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
    fn decode_trade_update() {
        let raw = br#"{
          "channel":"trade","type":"update",
          "data":[{"symbol":"BTC/USD","side":"sell","price":65000.12,"qty":0.01,
                   "ord_type":"market","trade_id":42,
                   "timestamp":"2023-09-25T07:49:37.708706Z"}]
        }"#;
        let DecodedEvent::Trades(trades) = decode_text(raw).unwrap() else {
            panic!("trades");
        };
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].aggressor, AggressorSide::Sell);
        assert_eq!(trades[0].trade_id, 42);
        assert_eq!(trades[0].price.0, Fixed::parse_str("65000.12").unwrap());
    }

    #[test]
    fn decode_multiple_trades_in_one_frame() {
        let raw = br#"{
          "channel":"trade","type":"update",
          "data":[
            {"symbol":"BTC/USD","side":"buy","price":1,"qty":1,"ord_type":"limit","trade_id":1,"timestamp":"2023-09-25T07:49:37.000000Z"},
            {"symbol":"BTC/USD","side":"sell","price":2,"qty":2,"ord_type":"limit","trade_id":2,"timestamp":"2023-09-25T07:49:38.000000Z"},
            {"symbol":"BTC/USD","side":"buy","price":3,"qty":3,"ord_type":"limit","trade_id":3,"timestamp":"2023-09-25T07:49:39.000000Z"}
          ]
        }"#;
        let DecodedEvent::Trades(trades) = decode_text(raw).unwrap() else {
            panic!("trades");
        };
        assert_eq!(trades.len(), 3);
        assert_eq!(
            trades.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn decode_ticker_quote() {
        let raw = br#"{
          "channel":"ticker","type":"update",
          "data":[{"symbol":"BTC/USD","bid":65000.0,"bid_qty":1.2,"ask":65000.1,"ask_qty":0.8,
                   "last":65000.05,"volume":1.5,"vwap":1,"low":64000.0,"high":66000.0,"change":0,"change_pct":0}]
        }"#;
        let DecodedEvent::Quote {
            bid_price,
            ask_price,
            high,
            low,
            volume,
            last,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("quote");
        };
        assert_eq!(bid_price.0, Fixed::parse_str("65000.0").unwrap());
        assert_eq!(ask_price.0, Fixed::parse_str("65000.1").unwrap());
        assert_eq!(high.unwrap().0, Fixed::parse_str("66000.0").unwrap());
        assert_eq!(low.unwrap().0, Fixed::parse_str("64000.0").unwrap());
        assert_eq!(volume.unwrap().0, Fixed::parse_str("1.5").unwrap());
        assert_eq!(last.unwrap().0, Fixed::parse_str("65000.05").unwrap());
    }

    #[test]
    fn decode_heartbeat_and_ack() {
        assert!(matches!(
            decode_text(br#"{"channel":"heartbeat"}"#).unwrap(),
            DecodedEvent::Heartbeat
        ));
        assert!(matches!(
            decode_text(br#"{"method":"subscribe","success":true,"result":{"channel":"trade"}}"#)
                .unwrap(),
            DecodedEvent::SubscribeAck
        ));
    }

    #[test]
    fn decode_book_snapshot_and_update() {
        let snap = br#"{
          "channel":"book","type":"snapshot",
          "data":[{"symbol":"BTC/USD",
            "bids":[{"price":"45283.5","qty":"0.10000000"}],
            "asks":[{"price":"45285.2","qty":"0.00100000"}],
            "checksum":3310070434,"timestamp":"2023-10-06T17:35:55.440295Z"}]
        }"#;
        let DecodedEvent::BookSnapshot {
            symbol,
            bids,
            asks,
            checksum,
            exchange_ts_ns,
        } = decode_text(snap).unwrap()
        else {
            panic!("snapshot");
        };
        assert_eq!(symbol, "BTC/USD");
        assert_eq!(checksum, 3_310_070_434);
        assert_eq!(bids[0].price_str, "45283.5");
        assert_eq!(bids[0].quantity.0, Fixed::parse_str("0.10000000").unwrap());
        assert_eq!(asks[0].qty_str, "0.00100000");
        assert!(exchange_ts_ns.is_some());

        let upd = br#"{
          "channel":"book","type":"update",
          "data":[{"symbol":"BTC/USD",
            "bids":[{"price":"45283.5","qty":"0"}],
            "asks":[],
            "checksum":123,"timestamp":"2023-10-06T17:35:56.000000Z"}]
        }"#;
        let DecodedEvent::BookUpdate { bids, checksum, .. } = decode_text(upd).unwrap() else {
            panic!("update");
        };
        assert_eq!(checksum, 123);
        assert_eq!(bids[0].quantity.0.coefficient, 0);
    }

    #[test]
    fn decode_book_handles_bare_number_levels() {
        // Some Kraken responses render price/qty as bare JSON numbers rather than strings.
        let raw = br#"{
          "channel":"book","type":"snapshot",
          "data":[{"symbol":"MATIC/USD",
            "bids":[{"price":0.566,"qty":18097.1547}],
            "asks":[{"price":0.5668,"qty":4410.79769741}],
            "checksum":2439117997}]
        }"#;
        let DecodedEvent::BookSnapshot { bids, asks, .. } = decode_text(raw).unwrap() else {
            panic!("snapshot");
        };
        assert_eq!(bids[0].price_str, "0.566");
        assert_eq!(asks[0].qty_str, "4410.79769741");
    }

    #[test]
    fn bare_book_number_preserves_checksum_lexeme() {
        let raw = br#"{
          "channel":"book","type":"snapshot",
          "data":[{"symbol":"BTC/USD",
            "bids":[{"price":1.2300,"qty":2}],
            "asks":[],
            "checksum":0}]
        }"#;
        let DecodedEvent::BookSnapshot { bids, .. } = decode_text(raw).unwrap() else {
            panic!("snapshot");
        };

        assert_eq!(bids[0].price_str, "1.2300");
    }

    #[test]
    fn decode_ohlc_candle_exact_fixed() {
        let raw = br#"{
          "channel":"ohlc","type":"update",
          "data":[{"symbol":"BTC/USD","open":65000.1,"high":65020.5,"low":64990.0,"close":65015.2,
                   "trades":12,"volume":1.234,"vwap":65010.0,
                   "interval_begin":"2023-10-04T15:30:00.000000000Z","interval":1,
                   "timestamp":"2023-10-04T15:31:00.000000Z"}]
        }"#;
        let DecodedEvent::Candle {
            symbol,
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("candle");
        };
        assert_eq!(symbol, "BTC/USD");
        assert_eq!(open.0, Fixed::parse_str("65000.1").unwrap());
        assert_eq!(high.0, Fixed::parse_str("65020.5").unwrap());
        assert_eq!(low.0, Fixed::parse_str("64990.0").unwrap());
        assert_eq!(close.0, Fixed::parse_str("65015.2").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("1.234").unwrap());
        assert_eq!(interval_ns, candle_interval_ns(CandleInterval::M1));
        assert_eq!(ohlc_interval_minutes(CandleInterval::M1), 1);
        assert!(start_ts.0 > 0);
    }

    /// Active backend must match the serde oracle (default = both serde).
    #[test]
    fn decode_text_matches_serde_oracle() {
        for raw in kraken_parity_fixtures() {
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
        for raw in kraken_parity_fixtures() {
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

    fn kraken_parity_fixtures() -> &'static [&'static [u8]] {
        &[
            br#"{"channel":"heartbeat"}"#,
            br#"{"method":"subscribe","success":true,"result":{"channel":"trade"}}"#,
            br#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"sell","price":65000.12,"qty":0.01,"ord_type":"market","trade_id":42,"timestamp":"2023-09-25T07:49:37.708706Z"}]}"#,
            br#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":65000.0,"bid_qty":1.2,"ask":65000.1,"ask_qty":0.8,"last":65000.05,"volume":1,"vwap":1,"low":1,"high":1,"change":0,"change_pct":0}]}"#,
            br#"{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":"45283.5","qty":"0.10000000"}],"asks":[{"price":"45285.2","qty":"0.00100000"}],"checksum":3310070434,"timestamp":"2023-10-06T17:35:55.440295Z"}]}"#,
            br#"{"channel":"ohlc","type":"update","data":[{"symbol":"BTC/USD","open":65000.1,"high":65020.5,"low":64990.0,"close":65015.2,"trades":12,"volume":1.234,"vwap":65010.0,"interval_begin":"2023-10-04T15:30:00.000000000Z","interval":1,"timestamp":"2023-10-04T15:31:00.000000Z"}]}"#,
        ]
    }
}
