//! Bitfinex public WS v2 decode (exact Fixed; no f64 arithmetic).
//!
//! Data frames are `[chanId, …]`; channel kind comes from the session's
//! `subscribed` map (`ChanBinding`).
//! Candles: WS `candles` channel with key `trade:{tf}:{symbol}`
//! (wire is MTS,OPEN,CLOSE,HIGH,LOW,VOL). REST `decode_candles_rest` kept as
//! a unit-test helper only.
//! Ticker indices 6..9 (LAST/VOLUME/HIGH/LOW) → optional `Statistics24h`.

use std::collections::HashMap;

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{AggressorSide, Fixed, Price, Quantity, Rate, SourceId, TimestampNs};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChanKind {
    Trades,
    Ticker,
    Book,
    Candles,
    /// Public WS `status` key `liq:global`.
    StatusLiq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChanBinding {
    pub kind: ChanKind,
    pub symbol: String,
    /// Set for `ChanKind::Candles` only.
    pub candle_interval: Option<CandleInterval>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    Info {
        /// Info code (e.g. 20051 = reconnect).
        code: Option<u32>,
        version: Option<u32>,
    },
    Subscribed {
        chan_id: u32,
        kind: ChanKind,
        symbol: String,
        candle_interval: Option<CandleInterval>,
    },
    Error {
        msg: String,
        code: Option<i64>,
    },
    Pong,
    Heartbeat {
        chan_id: u32,
    },
    Trade(TradeRow),
    Ticker(TickerRow),
    BookSnapshot {
        symbol: String,
        entries: Vec<BookEntry>,
    },
    BookUpdate {
        symbol: String,
        entry: BookEntry,
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
    },
    Liquidation(LiquidationRow),
    Unknown,
}

/// One row from WS `status` / `liq:global` (or REST liquidations hist shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiquidationRow {
    pub symbol: String,
    pub price: Price,
    pub quantity: Quantity,
    pub side: AggressorSide,
    pub exchange_ts_ms: i64,
}

/// Public derivatives status row (mark / index / funding / OI).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivStatusRow {
    pub symbol: String,
    pub mark_price: Price,
    pub index_price: Price,
    pub funding_rate: Rate,
    pub next_funding_ts: Option<TimestampNs>,
    pub open_interest: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeRow {
    pub symbol: String,
    pub trade_id: String,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub exchange_ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickerRow {
    pub symbol: String,
    pub bid_price: Price,
    pub bid_qty: Quantity,
    pub ask_price: Price,
    pub ask_qty: Quantity,
    /// LAST / VOLUME / HIGH / LOW (indices 6..9); None when all zero.
    pub last: Option<Price>,
    pub volume: Option<Quantity>,
    pub high: Option<Price>,
    pub low: Option<Price>,
}

impl TickerRow {
    /// True when any 24h field is present (non-zero on wire).
    pub fn has_stats24h(&self) -> bool {
        self.last.is_some() || self.volume.is_some() || self.high.is_some() || self.low.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookEntry {
    pub price: Price,
    pub count: u64,
    /// Signed: >0 bid, <0 ask (Bitfinex trading book).
    pub amount: Fixed,
}

pub fn decode_text(
    bytes: &[u8],
    channels: &HashMap<u32, ChanBinding>,
) -> Result<Vec<Decoded>, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if let Some(obj) = v.as_object() {
        return Ok(vec![decode_event(obj)?]);
    }
    let arr = v
        .as_array()
        .ok_or_else(|| "bitfinex frame not object/array".to_string())?;
    if arr.is_empty() {
        return Err("bitfinex empty array".into());
    }
    let chan_id = arr[0]
        .as_u64()
        .ok_or_else(|| "bitfinex chanId not u64".to_string())? as u32;
    if arr.len() >= 2 && arr[1].as_str() == Some("hb") {
        return Ok(vec![Decoded::Heartbeat { chan_id }]);
    }
    let binding = channels
        .get(&chan_id)
        .ok_or_else(|| format!("bitfinex unknown chanId {chan_id}"))?;
    match binding.kind {
        ChanKind::Trades => decode_trades(arr, &binding.symbol),
        ChanKind::Ticker => Ok(vec![decode_ticker(arr, &binding.symbol)?]),
        ChanKind::Book => Ok(vec![decode_book(arr, &binding.symbol)?]),
        ChanKind::Candles => decode_candles_ws(arr, binding),
        ChanKind::StatusLiq => decode_status_liq(arr),
    }
}

fn decode_event(obj: &serde_json::Map<String, Value>) -> Result<Decoded, String> {
    let event = obj.get("event").and_then(|e| e.as_str()).unwrap_or("");
    match event {
        "info" => Ok(Decoded::Info {
            code: obj.get("code").and_then(|c| c.as_u64()).map(|c| c as u32),
            version: obj
                .get("version")
                .and_then(|c| c.as_u64())
                .map(|c| c as u32),
        }),
        "subscribed" => decode_subscribed(obj),
        "error" => Ok(Decoded::Error {
            msg: obj
                .get("msg")
                .and_then(|m| m.as_str())
                .unwrap_or("error")
                .to_string(),
            code: obj.get("code").and_then(|c| c.as_i64()),
        }),
        "pong" => Ok(Decoded::Pong),
        _ => Ok(Decoded::Unknown),
    }
}

fn decode_subscribed(obj: &serde_json::Map<String, Value>) -> Result<Decoded, String> {
    let chan_id = obj
        .get("chanId")
        .and_then(|c| c.as_u64())
        .ok_or_else(|| "subscribed missing chanId".to_string())? as u32;
    let channel = obj
        .get("channel")
        .and_then(|c| c.as_str())
        .ok_or_else(|| "subscribed missing channel".to_string())?;
    match channel {
        "trades" => Ok(Decoded::Subscribed {
            chan_id,
            kind: ChanKind::Trades,
            symbol: required_symbol(obj)?,
            candle_interval: None,
        }),
        "ticker" => Ok(Decoded::Subscribed {
            chan_id,
            kind: ChanKind::Ticker,
            symbol: required_symbol(obj)?,
            candle_interval: None,
        }),
        "book" => Ok(Decoded::Subscribed {
            chan_id,
            kind: ChanKind::Book,
            symbol: required_symbol(obj)?,
            candle_interval: None,
        }),
        // Candles subscribed ack carries `key`, not `symbol`.
        "candles" => {
            let key = obj
                .get("key")
                .and_then(|k| k.as_str())
                .ok_or_else(|| "subscribed candles missing key".to_string())?;
            let (interval, symbol) = parse_candle_key(key)?;
            Ok(Decoded::Subscribed {
                chan_id,
                kind: ChanKind::Candles,
                symbol,
                candle_interval: Some(interval),
            })
        }
        // Status ack carries `key` (`liq:global` or `deriv:SYMBOL`).
        "status" => {
            let key = obj
                .get("key")
                .and_then(|k| k.as_str())
                .ok_or_else(|| "subscribed status missing key".to_string())?;
            if key == "liq:global" {
                Ok(Decoded::Subscribed {
                    chan_id,
                    kind: ChanKind::StatusLiq,
                    symbol: key.to_string(),
                    candle_interval: None,
                })
            } else {
                // ponytail: deriv:SYMBOL WS upgrade is AVAILABLE; REST status/deriv already HAVE.
                Err(format!("subscribed status key not handled: {key}"))
            }
        }
        other => Err(format!("subscribed unknown channel {other}")),
    }
}

fn required_symbol(obj: &serde_json::Map<String, Value>) -> Result<String, String> {
    obj.get("symbol")
        .and_then(|c| c.as_str())
        .ok_or_else(|| "subscribed missing symbol".to_string())
        .map(str::to_string)
}

/// Parse WS/REST candle key `trade:{tf}:{symbol}`.
pub fn parse_candle_key(key: &str) -> Result<(CandleInterval, String), String> {
    let mut parts = key.splitn(3, ':');
    let prefix = parts.next().unwrap_or("");
    let tf = parts.next().unwrap_or("");
    let symbol = parts.next().unwrap_or("");
    if prefix != "trade" || tf.is_empty() || symbol.is_empty() {
        return Err(format!("bad candle key {key}"));
    }
    let interval = match tf {
        "1m" => CandleInterval::M1,
        "5m" => CandleInterval::M5,
        "15m" => CandleInterval::M15,
        "1h" => CandleInterval::H1,
        "1D" => CandleInterval::D1,
        other => return Err(format!("unsupported candle tf {other}")),
    };
    Ok((interval, symbol.to_string()))
}

fn decode_trades(arr: &[Value], symbol: &str) -> Result<Vec<Decoded>, String> {
    if arr.len() < 2 {
        return Err("trades frame short".into());
    }
    // Update: [chanId, "te"|"tu", [id, mts, amount, price]]
    if let Some(msg_type) = arr[1].as_str() {
        match msg_type {
            "te" => {
                let row = arr.get(2).ok_or_else(|| "te missing trade".to_string())?;
                Ok(vec![Decoded::Trade(parse_trade(row, symbol)?)])
            }
            // ponytail: skip "tu" to avoid double-counting with "te".
            "tu" => Ok(Vec::new()),
            _ => Ok(vec![Decoded::Unknown]),
        }
    } else if let Some(snapshot) = arr[1].as_array() {
        // Snapshot: [chanId, [[id, mts, amount, price], ...]]
        let mut out = Vec::with_capacity(snapshot.len());
        for row in snapshot {
            out.push(Decoded::Trade(parse_trade(row, symbol)?));
        }
        Ok(out)
    } else {
        Err("trades payload not te/snapshot".into())
    }
}

fn parse_trade(row: &Value, symbol: &str) -> Result<TradeRow, String> {
    let a = row
        .as_array()
        .ok_or_else(|| "trade not array".to_string())?;
    if a.len() < 4 {
        return Err("trade array short".into());
    }
    let trade_id = match &a[0] {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => return Err("trade id bad".into()),
    };
    let exchange_ts_ms = a[1]
        .as_i64()
        .or_else(|| a[1].as_u64().map(|u| u as i64))
        .ok_or_else(|| "trade mts bad".to_string())?;
    let amount = fixed_from_json(&a[2])?;
    let price = Price(fixed_from_json(&a[3])?);
    let aggressor = match amount.coefficient.cmp(&0) {
        std::cmp::Ordering::Greater => AggressorSide::Buy,
        std::cmp::Ordering::Less => AggressorSide::Sell,
        std::cmp::Ordering::Equal => AggressorSide::Unknown,
    };
    Ok(TradeRow {
        symbol: symbol.to_string(),
        trade_id,
        price,
        quantity: Quantity(abs_fixed(amount)),
        aggressor,
        exchange_ts_ms,
    })
}

fn decode_ticker(arr: &[Value], symbol: &str) -> Result<Decoded, String> {
    let update = arr
        .get(1)
        .and_then(|v| v.as_array())
        .ok_or_else(|| "ticker missing update".to_string())?;
    if update.len() < 4 {
        return Err("ticker update short".into());
    }
    // Indices: BID BID_SIZE ASK ASK_SIZE … LAST(6) VOLUME(7) HIGH(8) LOW(9).
    // ponytail: zero wire values → None (no free open/quote_volume on ticker).
    let (last, volume, high, low) = if update.len() >= 10 {
        (
            nonzero_price(&update[6])?,
            nonzero_qty(&update[7])?,
            nonzero_price(&update[8])?,
            nonzero_price(&update[9])?,
        )
    } else {
        (None, None, None, None)
    };
    Ok(Decoded::Ticker(TickerRow {
        symbol: symbol.to_string(),
        bid_price: Price(fixed_from_json(&update[0])?),
        bid_qty: Quantity(abs_fixed(fixed_from_json(&update[1])?)),
        ask_price: Price(fixed_from_json(&update[2])?),
        ask_qty: Quantity(abs_fixed(fixed_from_json(&update[3])?)),
        last,
        volume,
        high,
        low,
    }))
}

fn nonzero_price(v: &Value) -> Result<Option<Price>, String> {
    let p = Price(fixed_from_json(v)?);
    if p.0.coefficient == 0 {
        Ok(None)
    } else {
        Ok(Some(p))
    }
}

fn nonzero_qty(v: &Value) -> Result<Option<Quantity>, String> {
    let q = Quantity(abs_fixed(fixed_from_json(v)?));
    if q.0.coefficient == 0 {
        Ok(None)
    } else {
        Ok(Some(q))
    }
}

fn decode_book(arr: &[Value], symbol: &str) -> Result<Decoded, String> {
    let body = arr.get(1).ok_or_else(|| "book missing body".to_string())?;
    let body_arr = body
        .as_array()
        .ok_or_else(|| "book body not array".to_string())?;
    // Snapshot: [[price, count, amount], ...] — first elem is array.
    if body_arr.first().is_some_and(|e| e.is_array()) {
        let mut entries = Vec::with_capacity(body_arr.len());
        for row in body_arr {
            entries.push(parse_book_entry(row)?);
        }
        return Ok(Decoded::BookSnapshot {
            symbol: symbol.to_string(),
            entries,
        });
    }
    // Update: [price, count, amount]
    Ok(Decoded::BookUpdate {
        symbol: symbol.to_string(),
        entry: parse_book_entry(body)?,
    })
}

fn decode_candles_ws(arr: &[Value], binding: &ChanBinding) -> Result<Vec<Decoded>, String> {
    let interval = binding
        .candle_interval
        .ok_or_else(|| "candles binding missing interval".to_string())?;
    let body = arr
        .get(1)
        .ok_or_else(|| "candles missing body".to_string())?;
    let body_arr = body
        .as_array()
        .ok_or_else(|| "candles body not array".to_string())?;
    // Snapshot: [[MTS,OPEN,CLOSE,HIGH,LOW,VOL], ...]
    if body_arr.first().is_some_and(|e| e.is_array()) {
        let mut out = Vec::with_capacity(body_arr.len());
        for row in body_arr {
            out.push(parse_candle_row(row, &binding.symbol, interval)?);
        }
        return Ok(out);
    }
    // Update: [MTS,OPEN,CLOSE,HIGH,LOW,VOL]
    Ok(vec![parse_candle_row(body, &binding.symbol, interval)?])
}

fn parse_candle_row(
    row: &Value,
    symbol: &str,
    interval: CandleInterval,
) -> Result<Decoded, String> {
    let a = row
        .as_array()
        .ok_or_else(|| "candle row not array".to_string())?;
    if a.len() < 6 {
        return Err("candle row short".into());
    }
    let start_ms = match &a[0] {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|u| u as i64))
            .ok_or_else(|| "candle mts not i64".to_string())?,
        Value::String(s) => s.parse().map_err(|e| format!("candle mts: {e}"))?,
        _ => return Err("candle mts not number/string".into()),
    };
    Ok(Decoded::Candle {
        symbol: symbol.to_string(),
        open: Price(fixed_from_json(&a[1])?),
        // Wire: OPEN, CLOSE, HIGH, LOW, VOLUME
        close: Price(fixed_from_json(&a[2])?),
        high: Price(fixed_from_json(&a[3])?),
        low: Price(fixed_from_json(&a[4])?),
        volume: Quantity(fixed_from_json(&a[5])?),
        interval_ns: candle_interval_ns(interval),
        start_ts: ms_to_ts(start_ms),
    })
}

fn parse_book_entry(row: &Value) -> Result<BookEntry, String> {
    let a = row
        .as_array()
        .ok_or_else(|| "book entry not array".to_string())?;
    if a.len() < 3 {
        return Err("book entry short".into());
    }
    let count = a[1]
        .as_u64()
        .or_else(|| a[1].as_i64().map(|i| i.max(0) as u64))
        .ok_or_else(|| "book count bad".to_string())?;
    Ok(BookEntry {
        price: Price(fixed_from_json(&a[0])?),
        count,
        amount: fixed_from_json(&a[2])?,
    })
}

fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("fixed value not string/number".into()),
    }
}

fn abs_fixed(f: Fixed) -> Fixed {
    Fixed {
        coefficient: f.coefficient.unsigned_abs() as i128,
        scale: f.scale,
    }
}

pub fn trade_id_source(id: &str) -> SourceId {
    SourceId(id.to_string())
}

pub fn ms_to_ts(ms: i64) -> TimestampNs {
    TimestampNs(ms.saturating_mul(1_000_000))
}

/// Bitfinex candle timeframe token for key `trade:{tf}:{symbol}`.
pub fn candle_time_frame(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "1m",
        CandleInterval::M5 => "5m",
        CandleInterval::M15 => "15m",
        CandleInterval::H1 => "1h",
        CandleInterval::D1 => "1D",
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

/// Decode REST `hist` (`[[MTS,OPEN,CLOSE,HIGH,LOW,VOL],…]`) or `last` (`[MTS,…]`).
///
/// Unit-test helper only (session uses WS candles). Wire order OPEN, **CLOSE**, HIGH, LOW.
pub fn decode_candles_rest(bytes: &[u8], interval: CandleInterval) -> Result<Decoded, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let rows = v
        .as_array()
        .ok_or_else(|| "bitfinex candles not array".to_string())?;
    let row = if rows.first().and_then(|r| r.as_array()).is_some() {
        rows.first()
            .and_then(|r| r.as_array())
            .ok_or_else(|| "bitfinex candles empty".to_string())?
    } else {
        // `/last` returns a single flat candle row.
        rows
    };
    if row.len() < 6 {
        return Err("bitfinex candle row short".into());
    }
    let start_ms = match &row[0] {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|u| u as i64))
            .ok_or_else(|| "candle mts not i64".to_string())?,
        Value::String(s) => s.parse().map_err(|e| format!("candle mts: {e}"))?,
        _ => return Err("candle mts not number/string".into()),
    };
    Ok(Decoded::Candle {
        symbol: String::new(),
        open: Price(fixed_from_json(&row[1])?),
        // Wire: OPEN, CLOSE, HIGH, LOW, VOLUME
        close: Price(fixed_from_json(&row[2])?),
        high: Price(fixed_from_json(&row[3])?),
        low: Price(fixed_from_json(&row[4])?),
        volume: Quantity(fixed_from_json(&row[5])?),
        interval_ns: candle_interval_ns(interval),
        start_ts: ms_to_ts(start_ms),
    })
}

pub fn decode_status_deriv(bytes: &[u8]) -> Result<Vec<DerivStatusRow>, String> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let rows = v
        .as_array()
        .ok_or_else(|| "bitfinex status/deriv not array".to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let a = row
            .as_array()
            .ok_or_else(|| "bitfinex status/deriv row not array".to_string())?;
        if a.len() < 19 {
            return Err("bitfinex status/deriv row short".into());
        }
        let symbol = a[0]
            .as_str()
            .ok_or_else(|| "bitfinex status/deriv symbol bad".to_string())?
            .to_string();
        let mark = if !a[15].is_null() { &a[15] } else { &a[3] };
        let next_funding_ts = match &a[8] {
            Value::Null => None,
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_u64().map(|u| u as i64))
                .map(ms_to_ts),
            Value::String(s) => s.parse::<i64>().ok().map(ms_to_ts),
            _ => None,
        };
        out.push(DerivStatusRow {
            symbol,
            mark_price: Price(fixed_from_json(mark)?),
            index_price: Price(fixed_from_json(&a[4])?),
            funding_rate: Rate(fixed_from_json(&a[12])?),
            next_funding_ts,
            open_interest: Quantity(fixed_from_json(&a[18])?),
        });
    }
    Ok(out)
}

/// Decode WS `status` / `liq:global` payload `[chanId, [[…liq…], …]]`.
fn decode_status_liq(arr: &[Value]) -> Result<Vec<Decoded>, String> {
    let body = arr
        .get(1)
        .and_then(|v| v.as_array())
        .ok_or_else(|| "status liq missing body".to_string())?;
    let mut out = Vec::with_capacity(body.len());
    for row in body {
        if let Some(liq) = parse_liquidation_row(row)? {
            out.push(Decoded::Liquidation(liq));
        }
    }
    Ok(out)
}

/// Parse one liquidation array. Skips non-`pos` / non-match rows (no emit).
///
/// Wire: MSG_TYPE, POS_ID, TIME_MS, _, SYMBOL, AMOUNT, BASE_PRICE, _, IS_MATCH,
/// IS_MARKET_SOLD, _, LIQUIDATION_PRICE.
///
/// # ponytail
/// Only `IS_MATCH==1` (market execution) — skips initial trigger to avoid doubles.
fn parse_liquidation_row(row: &Value) -> Result<Option<LiquidationRow>, String> {
    let a = row
        .as_array()
        .ok_or_else(|| "liq row not array".to_string())?;
    if a.len() < 7 {
        return Err("liq row short".into());
    }
    if a[0].as_str() != Some("pos") {
        return Ok(None);
    }
    // IS_MATCH at [8] when present; absent → treat as match (hist / short rows).
    if a.len() > 8 {
        let is_match = match &a[8] {
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_u64().map(|u| u as i64))
                .unwrap_or(0),
            Value::Bool(b) => i64::from(*b),
            _ => 0,
        };
        if is_match != 1 {
            return Ok(None);
        }
    }
    let symbol = a[4]
        .as_str()
        .ok_or_else(|| "liq symbol bad".to_string())?
        .to_string();
    let exchange_ts_ms = json_ms(&a[2])?;
    let amount = fixed_from_json(&a[5])?;
    // Long liquidated (amount > 0) → forced sell; short → forced buy (peer of Bybit).
    let side = match amount.coefficient.cmp(&0) {
        std::cmp::Ordering::Greater => AggressorSide::Sell,
        std::cmp::Ordering::Less => AggressorSide::Buy,
        std::cmp::Ordering::Equal => AggressorSide::Unknown,
    };
    // Bitfinex documents the liquidation trigger price at [11]. BASE_PRICE
    // [6] is the position entry/base price and is only a legacy-row fallback.
    let price_v = if a.len() > 11 && !a[11].is_null() {
        &a[11]
    } else if !a[6].is_null() {
        &a[6]
    } else {
        return Err("liq price missing".into());
    };
    Ok(Some(LiquidationRow {
        symbol,
        price: Price(fixed_from_json(price_v)?),
        quantity: Quantity(abs_fixed(amount)),
        side,
        exchange_ts_ms,
    }))
}

fn json_ms(v: &Value) -> Result<i64, String> {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|u| u as i64))
            .or_else(|| n.as_f64().map(|f| f as i64))
            .ok_or_else(|| "mts not i64".into()),
        Value::String(s) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(i)
            } else {
                s.parse::<f64>()
                    .map(|f| f as i64)
                    .map_err(|e| format!("mts: {e}"))
            }
        }
        _ => Err("mts not number/string".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_one(
        id: u32,
        kind: ChanKind,
        symbol: &str,
        candle_interval: Option<CandleInterval>,
    ) -> HashMap<u32, ChanBinding> {
        let mut m = HashMap::new();
        m.insert(
            id,
            ChanBinding {
                kind,
                symbol: symbol.into(),
                candle_interval,
            },
        );
        m
    }

    #[test]
    fn decode_subscribed_and_trade_te_exact() {
        let empty = HashMap::new();
        let Decoded::Subscribed {
            chan_id,
            kind,
            symbol,
            candle_interval,
        } = &decode_text(
            br#"{"event":"subscribed","channel":"trades","chanId":17470,"symbol":"tBTCUSD","pair":"BTCUSD"}"#,
            &empty,
        )
        .unwrap()[0]
        else {
            panic!("subscribed");
        };
        assert_eq!(*chan_id, 17470);
        assert_eq!(*kind, ChanKind::Trades);
        assert_eq!(symbol, "tBTCUSD");
        assert!(candle_interval.is_none());

        let m = map_one(17470, ChanKind::Trades, "tBTCUSD", None);
        let Decoded::Trade(t) = &decode_text(
            br#"[17470,"te",[401597395,1574694478808,0.005,7245.3]]"#,
            &m,
        )
        .unwrap()[0] else {
            panic!("te");
        };
        assert_eq!(t.price.0, Fixed::parse_str("7245.3").unwrap());
        assert_eq!(t.quantity.0, Fixed::parse_str("0.005").unwrap());
        assert_eq!(t.aggressor, AggressorSide::Buy);
        assert_eq!(t.exchange_ts_ms, 1_574_694_478_808);
    }

    #[test]
    fn decode_ticker_and_book() {
        let m = map_one(1, ChanKind::Ticker, "tBTCUSD", None);
        let Decoded::Ticker(q) =
            &decode_text(br#"[1,[29000.12,1.5,29001.00,2.0,0,0,0,0,0,0]]"#, &m).unwrap()[0]
        else {
            panic!("ticker");
        };
        assert_eq!(q.bid_price.0, Fixed::parse_str("29000.12").unwrap());
        assert_eq!(q.ask_qty.0, Fixed::parse_str("2.0").unwrap());
        assert!(!q.has_stats24h());

        let Decoded::Ticker(q) = &decode_text(
            br#"[1,[29000.12,1.5,29001.00,2.0,0,0,29050.5,100.25,29100.0,28900.0]]"#,
            &m,
        )
        .unwrap()[0] else {
            panic!("ticker stats");
        };
        assert!(q.has_stats24h());
        assert_eq!(q.last.unwrap().0, Fixed::parse_str("29050.5").unwrap());
        assert_eq!(q.volume.unwrap().0, Fixed::parse_str("100.25").unwrap());
        assert_eq!(q.high.unwrap().0, Fixed::parse_str("29100.0").unwrap());
        assert_eq!(q.low.unwrap().0, Fixed::parse_str("28900.0").unwrap());

        let m = map_one(2, ChanKind::Book, "tBTCUSD", None);
        let Decoded::BookSnapshot { entries, .. } =
            &decode_text(br#"[2,[[29000.0,2,1.5],[29001.0,1,-2.0]]]"#, &m).unwrap()[0]
        else {
            panic!("snap");
        };
        assert_eq!(entries.len(), 2);
        assert!(entries[0].amount.coefficient > 0);
        assert!(entries[1].amount.coefficient < 0);

        let Decoded::BookUpdate { entry, .. } =
            &decode_text(br#"[2,[29000.0,0,1]]"#, &m).unwrap()[0]
        else {
            panic!("upd");
        };
        assert_eq!(entry.count, 0);
    }

    #[test]
    fn decode_candles_ws_subscribed_key_and_update() {
        let empty = HashMap::new();
        let Decoded::Subscribed {
            chan_id,
            kind,
            symbol,
            candle_interval,
        } = &decode_text(
            br#"{"event":"subscribed","channel":"candles","chanId":341561,"key":"trade:1m:tBTCUSD"}"#,
            &empty,
        )
        .unwrap()[0]
        else {
            panic!("subscribed candles");
        };
        assert_eq!(*chan_id, 341561);
        assert_eq!(*kind, ChanKind::Candles);
        assert_eq!(symbol, "tBTCUSD");
        assert_eq!(*candle_interval, Some(CandleInterval::M1));

        let m = map_one(
            341561,
            ChanKind::Candles,
            "tBTCUSD",
            Some(CandleInterval::M1),
        );
        let Decoded::Candle {
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
            symbol,
        } = &decode_text(
            br#"[341561,[1609459200000,28901.57,28800.01,28902.46,28800,49.3149836]]"#,
            &m,
        )
        .unwrap()[0]
        else {
            panic!("candle");
        };
        assert_eq!(symbol, "tBTCUSD");
        assert_eq!(open.0, Fixed::parse_str("28901.57").unwrap());
        assert_eq!(close.0, Fixed::parse_str("28800.01").unwrap());
        assert_eq!(high.0, Fixed::parse_str("28902.46").unwrap());
        assert_eq!(low.0, Fixed::parse_str("28800").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("49.3149836").unwrap());
        assert_eq!(*interval_ns, 60_000_000_000);
        assert_eq!(*start_ts, TimestampNs(1_609_459_200_000_000_000));
    }

    #[test]
    fn skip_tu_avoids_double_count() {
        let m = map_one(1, ChanKind::Trades, "tBTCUSD", None);
        let out = decode_text(br#"[1,"tu",[1,1,-0.1,100]]"#, &m).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn decode_candles_hist_wire_order_ochl() {
        // Wire: MTS, OPEN, CLOSE, HIGH, LOW, VOLUME
        let Decoded::Candle {
            open,
            high,
            low,
            close,
            volume,
            interval_ns,
            start_ts,
            ..
        } = decode_candles_rest(
            br#"[[1609459200000,28901.57,28800.01,28902.46,28800,49.3149836]]"#,
            CandleInterval::M1,
        )
        .unwrap()
        else {
            panic!("candle");
        };
        assert_eq!(open.0, Fixed::parse_str("28901.57").unwrap());
        assert_eq!(close.0, Fixed::parse_str("28800.01").unwrap());
        assert_eq!(high.0, Fixed::parse_str("28902.46").unwrap());
        assert_eq!(low.0, Fixed::parse_str("28800").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("49.3149836").unwrap());
        assert_eq!(interval_ns, 60_000_000_000);
        assert_eq!(start_ts, TimestampNs(1_609_459_200_000_000_000));
    }
    #[test]
    fn decode_status_deriv_exact_fixed() {
        let rows = decode_status_deriv(
            br#"[["tBTCF0:USTF0",1700000000000,null,65924.12,65889,null,0,null,1700010000000,0,0,null,0.00006854,null,null,65885.8908,null,null,8875.366]]"#,
        ).unwrap();
        assert_eq!(rows[0].symbol, "tBTCF0:USTF0");
        assert_eq!(
            rows[0].mark_price.0,
            Fixed::parse_str("65885.8908").unwrap()
        );
        assert_eq!(
            rows[0].funding_rate.0,
            Fixed::parse_str("0.00006854").unwrap()
        );
    }

    #[test]
    fn decode_liq_global_exact_fixed() {
        let empty = HashMap::new();
        let Decoded::Subscribed {
            chan_id,
            kind,
            symbol,
            ..
        } = &decode_text(
            br#"{"event":"subscribed","channel":"status","chanId":91684,"key":"liq:global"}"#,
            &empty,
        )
        .unwrap()[0]
        else {
            panic!("subscribed liq");
        };
        assert_eq!(*chan_id, 91684);
        assert_eq!(*kind, ChanKind::StatusLiq);
        assert_eq!(symbol, "liq:global");

        let m = map_one(91684, ChanKind::StatusLiq, "liq:global", None);
        let Decoded::Liquidation(l) = &decode_text(
            br#"[91684,[["pos",142397657,1574697680828.2002,null,"tBTCF0:USTF0",-2.62932,91.583875238719,null,1,1,null,112.27]]]"#,
            &m,
        )
        .unwrap()[0]
        else {
            panic!("liq");
        };
        assert_eq!(l.symbol, "tBTCF0:USTF0");
        assert_eq!(l.price.0, Fixed::parse_str("112.27").unwrap());
        assert_eq!(l.quantity.0, Fixed::parse_str("2.62932").unwrap());
        assert_eq!(l.side, AggressorSide::Buy); // short liquidated → buy
        assert_eq!(l.exchange_ts_ms, 1_574_697_680_828);

        // IS_MATCH=0 (trigger) skipped.
        let skip = decode_text(
            br#"[91684,[["pos",1,1574697680828,null,"tBTCF0:USTF0",1.0,100.0,null,0,0,null,99.0]]]"#,
            &m,
        )
        .unwrap();
        assert!(skip.is_empty());
    }
}
