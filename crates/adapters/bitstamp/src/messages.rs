//! Bitstamp public WS + REST OHLC decode (exact Fixed; no f64 arithmetic).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{AggressorSide, Fixed, Price, Quantity, SourceId, TimestampNs};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    Trade(TradeRow),
    /// Full book snapshot (`order_book_*`); also drives BBO quotes.
    BookSnapshot {
        pair: String,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_ts_us: i64,
    },
    /// Incremental levels (`diff_order_book_*`); qty=0 deletes.
    BookDelta {
        pair: String,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_ts_us: i64,
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
    Statistics24h {
        open: Option<Price>,
        high: Option<Price>,
        low: Option<Price>,
        close: Option<Price>,
        volume: Option<Quantity>,
        quote_volume: Option<Quantity>,
        exchange_ts: Option<TimestampNs>,
    },
    SubscribeAck,
    Heartbeat,
    Reconnect,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeRow {
    pub pair: String,
    pub trade_id: String,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub exchange_ts_us: i64,
}

pub fn decode_text(bytes: &[u8]) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let obj = v
        .as_object()
        .ok_or_else(|| "bitstamp frame not object".to_string())?;

    let event = obj.get("event").and_then(|e| e.as_str()).unwrap_or("");
    match event {
        "bts:subscription_succeeded" | "bts:unsubscription_succeeded" => Ok(Decoded::SubscribeAck),
        "bts:heartbeat" => Ok(Decoded::Heartbeat),
        "bts:request_reconnect" => Ok(Decoded::Reconnect),
        "trade" => decode_trade(obj),
        "data" => decode_book_data(obj),
        _ => Ok(Decoded::Unknown),
    }
}

pub fn candle_step_secs(interval: CandleInterval) -> u64 {
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

pub fn decode_ohlc_rest(bytes: &[u8], interval: CandleInterval) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let ohlc = v
        .pointer("/data/ohlc")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "bitstamp ohlc missing".to_string())?;
    let row = ohlc
        .first()
        .and_then(|r| r.as_object())
        .ok_or_else(|| "bitstamp ohlc empty".to_string())?;
    let start_sec = match row.get("timestamp") {
        Some(Value::String(s)) => s.parse::<i64>().map_err(|e| format!("timestamp: {e}"))?,
        Some(Value::Number(n)) => n.as_i64().ok_or_else(|| "timestamp not i64".to_string())?,
        _ => return Err("ohlc missing timestamp".into()),
    };
    Ok(Decoded::Candle {
        open: Price(fixed_from_json(
            row.get("open").ok_or("ohlc missing open")?,
        )?),
        high: Price(fixed_from_json(
            row.get("high").ok_or("ohlc missing high")?,
        )?),
        low: Price(fixed_from_json(row.get("low").ok_or("ohlc missing low")?)?),
        close: Price(fixed_from_json(
            row.get("close").ok_or("ohlc missing close")?,
        )?),
        volume: Quantity(fixed_from_json(
            row.get("volume").ok_or("ohlc missing volume")?,
        )?),
        interval_ns: candle_interval_ns(interval),
        start_ts: TimestampNs(start_sec.saturating_mul(1_000_000_000)),
    })
}

/// `GET /ticker/{pair}/` — rolling 24h high/low/volume/last; `open_24` preferred over day `open`.
pub fn decode_ticker_rest(bytes: &[u8]) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let obj = v
        .as_object()
        .ok_or_else(|| "bitstamp ticker not object".to_string())?;
    let open = obj
        .get("open_24")
        .or_else(|| obj.get("open"))
        .map(fixed_from_json)
        .transpose()?
        .map(Price);
    let high = obj.get("high").map(fixed_from_json).transpose()?.map(Price);
    let low = obj.get("low").map(fixed_from_json).transpose()?.map(Price);
    let close = obj.get("last").map(fixed_from_json).transpose()?.map(Price);
    let volume = obj
        .get("volume")
        .map(fixed_from_json)
        .transpose()?
        .map(Quantity);
    let exchange_ts = match obj.get("timestamp") {
        Some(Value::String(s)) => s
            .parse::<i64>()
            .ok()
            .map(|sec| TimestampNs(sec.saturating_mul(1_000_000_000))),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(|sec| TimestampNs(sec.saturating_mul(1_000_000_000))),
        _ => None,
    };
    if open.is_none() && high.is_none() && low.is_none() && close.is_none() && volume.is_none() {
        return Err("bitstamp ticker empty stats".into());
    }
    Ok(Decoded::Statistics24h {
        open,
        high,
        low,
        close,
        volume,
        quote_volume: None,
        exchange_ts,
    })
}

fn channel_pair(channel: &str) -> Option<&str> {
    channel
        .strip_prefix("live_trades_")
        .or_else(|| channel.strip_prefix("diff_order_book_"))
        .or_else(|| channel.strip_prefix("order_book_"))
}

fn decode_trade(obj: &serde_json::Map<String, Value>) -> Result<Decoded, String> {
    let channel = obj
        .get("channel")
        .and_then(|c| c.as_str())
        .ok_or_else(|| "trade missing channel".to_string())?;
    let pair = channel_pair(channel)
        .ok_or_else(|| format!("trade bad channel {channel}"))?
        .to_string();
    let data = obj
        .get("data")
        .and_then(|d| d.as_object())
        .ok_or_else(|| "trade missing data".to_string())?;
    // Prefer exact decimal wire strings when present.
    let price = fixed_prefer_str(data, "price_str", "price")?;
    let quantity = fixed_prefer_str(data, "amount_str", "amount")?;
    let trade_id = match data.get("id") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return Err("trade missing id".into()),
    };
    // type: 0 = buy (buyer aggressor), 1 = sell.
    let aggressor = match data.get("type").and_then(|t| t.as_u64()).or_else(|| {
        data.get("type")
            .and_then(|t| t.as_str())
            .and_then(|s| s.parse().ok())
    }) {
        Some(0) => AggressorSide::Buy,
        Some(1) => AggressorSide::Sell,
        _ => AggressorSide::Unknown,
    };
    Ok(Decoded::Trade(TradeRow {
        pair,
        trade_id,
        price: Price(price),
        quantity: Quantity(quantity),
        aggressor,
        exchange_ts_us: microtimestamp(data),
    }))
}

fn decode_book_data(obj: &serde_json::Map<String, Value>) -> Result<Decoded, String> {
    let channel = obj
        .get("channel")
        .and_then(|c| c.as_str())
        .ok_or_else(|| "book missing channel".to_string())?;
    let data = obj
        .get("data")
        .and_then(|d| d.as_object())
        .ok_or_else(|| "book missing data".to_string())?;
    let bids = decode_levels(data.get("bids").unwrap_or(&Value::Null))?;
    let asks = decode_levels(data.get("asks").unwrap_or(&Value::Null))?;
    let exchange_ts_us = microtimestamp(data);
    if let Some(pair) = channel.strip_prefix("diff_order_book_") {
        return Ok(Decoded::BookDelta {
            pair: pair.to_string(),
            bids,
            asks,
            exchange_ts_us,
        });
    }
    if let Some(pair) = channel.strip_prefix("order_book_") {
        return Ok(Decoded::BookSnapshot {
            pair: pair.to_string(),
            bids,
            asks,
            exchange_ts_us,
        });
    }
    Ok(Decoded::Unknown)
}

fn decode_levels(v: &Value) -> Result<Vec<(Price, Quantity)>, String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "book levels not array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let pair = row
            .as_array()
            .ok_or_else(|| "book level not [price, amount]".to_string())?;
        if pair.len() < 2 {
            return Err("book level short".into());
        }
        out.push((
            Price(fixed_from_json(&pair[0])?),
            Quantity(fixed_from_json(&pair[1])?),
        ));
    }
    Ok(out)
}

fn fixed_prefer_str(
    data: &serde_json::Map<String, Value>,
    str_key: &str,
    num_key: &str,
) -> Result<Fixed, String> {
    if let Some(Value::String(s)) = data.get(str_key) {
        return Fixed::parse_str(s).map_err(|e| e.to_string());
    }
    fixed_from_json(
        data.get(num_key)
            .ok_or_else(|| format!("missing {num_key}"))?,
    )
}

fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("fixed value not string/number".into()),
    }
}

fn microtimestamp(data: &serde_json::Map<String, Value>) -> i64 {
    data.get("microtimestamp")
        .and_then(|t| t.as_str())
        .and_then(|s| s.parse().ok())
        .or_else(|| data.get("microtimestamp").and_then(|t| t.as_i64()))
        .or_else(|| {
            data.get("timestamp")
                .and_then(|t| t.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .map(|s| s.saturating_mul(1_000_000))
        })
        .unwrap_or(0)
}

pub fn trade_id_source(id: &str) -> SourceId {
    SourceId(id.to_string())
}

pub fn us_to_ts(us: i64) -> marketfeed_model::TimestampNs {
    marketfeed_model::TimestampNs(us.saturating_mul(1_000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_trade_exact_fixed() {
        let raw = br#"{"channel":"live_trades_btcusd","event":"trade","data":{"id":123,"amount":"0.10000000","amount_str":"0.10000000","price":"29000.12","price_str":"29000.12","type":0,"timestamp":"1609459200","microtimestamp":"1609459200123456"}}"#;
        let Decoded::Trade(t) = decode_text(raw).unwrap() else {
            panic!("expected trade");
        };
        assert_eq!(t.pair, "btcusd");
        assert_eq!(t.price.0, Fixed::parse_str("29000.12").unwrap());
        assert_eq!(t.quantity.0, Fixed::parse_str("0.10000000").unwrap());
        assert_eq!(t.aggressor, AggressorSide::Buy);
        assert_eq!(t.exchange_ts_us, 1_609_459_200_123_456);
    }

    #[test]
    fn decode_order_book_and_diff() {
        let snap = br#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"1609459200","microtimestamp":"1609459200123456","bids":[["29000.00","1.5"]],"asks":[["29001.00","2.0"]]}}"#;
        let Decoded::BookSnapshot {
            pair, bids, asks, ..
        } = decode_text(snap).unwrap()
        else {
            panic!("snapshot");
        };
        assert_eq!(pair, "btcusd");
        assert_eq!(bids[0].0.0, Fixed::parse_str("29000.00").unwrap());
        assert_eq!(asks[0].1.0, Fixed::parse_str("2.0").unwrap());

        let delta = br#"{"channel":"diff_order_book_btcusd","event":"data","data":{"timestamp":"1609459201","microtimestamp":"1609459201123456","bids":[["29000.00","0"]],"asks":[]}}"#;
        let Decoded::BookDelta { bids, .. } = decode_text(delta).unwrap() else {
            panic!("delta");
        };
        assert_eq!(bids[0].1.0, Fixed::parse_str("0").unwrap());
    }

    #[test]
    fn decode_ohlc_rest_exact_fixed() {
        let raw = br#"{"data":{"pair":"BTC/USD","ohlc":[{"timestamp":"1609459200","open":"0.0010","high":"0.0025","low":"0.0015","close":"0.0020","volume":"1000"}]}}"#;
        let Decoded::Candle {
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
        } = decode_ohlc_rest(raw, CandleInterval::M1).unwrap()
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
    fn decode_ticker_rest_prefers_open_24() {
        let raw = br#"{"timestamp":"1609459200","open":"64000.00","open_24":"64100.00","high":"66000.50","low":"63000.25","last":"65000.12","volume":"12.5","vwap":"64500.00","bid":"65000.00","ask":"65000.10"}"#;
        let Decoded::Statistics24h {
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            exchange_ts,
        } = decode_ticker_rest(raw).unwrap()
        else {
            panic!("stats");
        };
        assert_eq!(open.unwrap().0, Fixed::parse_str("64100.00").unwrap());
        assert_eq!(high.unwrap().0, Fixed::parse_str("66000.50").unwrap());
        assert_eq!(low.unwrap().0, Fixed::parse_str("63000.25").unwrap());
        assert_eq!(close.unwrap().0, Fixed::parse_str("65000.12").unwrap());
        assert_eq!(volume.unwrap().0, Fixed::parse_str("12.5").unwrap());
        assert!(quote_volume.is_none());
        assert_eq!(exchange_ts, Some(TimestampNs(1_609_459_200_000_000_000)));
    }
}
