//! Deribit JSON-RPC subscription decoding (exact Fixed; no f64 arithmetic).

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{
    AggressorSide, Fixed, Funding, OpenInterest, Price, PricePoint, Quantity, Quote, Rate,
    SourceId, TimestampNs, Trade,
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedEvent {
    Trades(Vec<TradeRow>),
    /// Boxed: `TickerRow` is large (quote/mark/index/funding/OI); avoid fat enum.
    Ticker(Box<TickerRow>),
    /// Dedicated `deribit_price_index.{index_name}` (peer OKX `index-tickers`).
    IndexPrice {
        index_name: String,
        price: Price,
        exchange_ts_ms: i64,
    },
    /// First `book.*` notification for an instrument: full book, no `prev_change_id`.
    BookSnapshot {
        instrument: String,
        change_id: u64,
        bids: Vec<BookLevelChange>,
        asks: Vec<BookLevelChange>,
        exchange_ts_ms: i64,
    },
    /// Subsequent `book.*` notification: incremental change against `prev_change_id`.
    BookChange {
        instrument: String,
        change_id: u64,
        prev_change_id: u64,
        bids: Vec<BookLevelChange>,
        asks: Vec<BookLevelChange>,
        exchange_ts_ms: i64,
    },
    Candle {
        instrument: String,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
        exchange_ts_ms: i64,
    },
    SubscribeAck,
    Heartbeat {
        /// When true, session must reply with `public/test`.
        needs_test: bool,
    },
    Unknown,
}

/// One `[action, price, amount]` order-book tuple from a `book.*` notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookLevelAction {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevelChange {
    pub action: BookLevelAction,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeRow {
    pub instrument: String,
    pub trade_id: String,
    pub trade_seq: Option<u64>,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub exchange_ts_ms: i64,
    /// True when Deribit marks the trade as a liquidation (`"T"` taker / `"M"` maker).
    /// No dedicated public liquidation channel — liq is a trade attribute.
    pub liquidation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickerRow {
    pub instrument: String,
    pub exchange_ts_ms: i64,
    pub quote: Option<Quote>,
    pub mark: Option<PricePoint>,
    pub index: Option<PricePoint>,
    pub funding: Option<Funding>,
    pub open_interest: Option<OpenInterest>,
    /// From `stats` + `last_price` when present.
    pub open: Option<Price>,
    pub high: Option<Price>,
    pub low: Option<Price>,
    pub close: Option<Price>,
    pub volume: Option<Quantity>,
    pub quote_volume: Option<Quantity>,
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

    if let Some(method) = obj.get("method").and_then(|m| m.as_str()) {
        match method {
            "subscription" => return decode_subscription(obj),
            "heartbeat" => {
                let params = obj.get("params").and_then(|p| p.as_object());
                let kind = params
                    .and_then(|p| p.get("type"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("");
                return Ok(DecodedEvent::Heartbeat {
                    needs_test: kind == "test_request",
                });
            }
            _ => {}
        }
    }

    if obj.contains_key("result") && obj.contains_key("id") {
        return Ok(DecodedEvent::SubscribeAck);
    }
    if let Some(err) = obj.get("error") {
        return Err(format!("deribit rpc error: {err}"));
    }

    Ok(DecodedEvent::Unknown)
}

fn decode_subscription(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let params = obj
        .get("params")
        .and_then(|p| p.as_object())
        .ok_or_else(|| "subscription missing params".to_string())?;
    let channel = params.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    let data = params
        .get("data")
        .ok_or_else(|| "subscription missing data".to_string())?;

    if channel.starts_with("trades.") {
        return decode_trades(data);
    }
    if channel.starts_with("ticker.") {
        return decode_ticker(data);
    }
    if channel.starts_with("deribit_price_index.") {
        return decode_price_index(channel, data);
    }
    if channel.starts_with("book.") {
        return decode_book(data);
    }
    if channel.starts_with("chart.trades.") {
        return decode_chart_trades(channel, data);
    }
    Ok(DecodedEvent::Unknown)
}

/// Map instrument → Deribit index channel suffix (`btc_usd`, `btc_usdc`, …).
///
/// ponytail: naming heuristics from public instrument ids; ceiling = exotic
/// underlyings / non-USD settlement; upgrade = carry `price_index` from
/// `get_instruments` into session config.
pub fn deribit_index_name(instrument: &str) -> String {
    if let Some((head, _)) = instrument.split_once('-') {
        if head.contains('_') {
            return head.to_ascii_lowercase();
        }
        return format!("{}_usd", head.to_ascii_lowercase());
    }
    format!("{}_usd", instrument.to_ascii_lowercase())
}

fn decode_price_index(channel: &str, data: &Value) -> Result<DecodedEvent, String> {
    let index_name = channel
        .strip_prefix("deribit_price_index.")
        .ok_or_else(|| format!("bad deribit_price_index channel {channel}"))?
        .to_string();
    let o = data
        .as_object()
        .ok_or_else(|| "price_index data not object".to_string())?;
    let index_name = o
        .get("index_name")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or(index_name);
    let price = o
        .get("price")
        .ok_or_else(|| "price_index missing price".to_string())?;
    Ok(DecodedEvent::IndexPrice {
        index_name,
        price: Price(fixed_from_json(price)?),
        exchange_ts_ms: o.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0),
    })
}

/// Deribit `chart.trades.{instrument}.{resolution}` resolution segment.
pub fn chart_resolution(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "1",
        CandleInterval::M5 => "5",
        CandleInterval::M15 => "15",
        CandleInterval::H1 => "60",
        CandleInterval::D1 => "1D",
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

fn interval_ns_from_resolution(res: &str) -> Result<i64, String> {
    let interval = match res {
        "1" => CandleInterval::M1,
        "5" => CandleInterval::M5,
        "15" => CandleInterval::M15,
        "60" => CandleInterval::H1,
        "1D" => CandleInterval::D1,
        other => return Err(format!("unsupported chart resolution {other}")),
    };
    Ok(candle_interval_ns(interval))
}

/// # ponytail
/// Deribit pushes many in-period updates with no close flag. Ceiling = partial bars;
/// upgrade = emit only when `tick` advances.
fn decode_chart_trades(channel: &str, data: &Value) -> Result<DecodedEvent, String> {
    // channel: chart.trades.{instrument}.{resolution}
    let rest = channel
        .strip_prefix("chart.trades.")
        .ok_or_else(|| format!("bad chart channel {channel}"))?;
    let (instrument, resolution) = rest
        .rsplit_once('.')
        .ok_or_else(|| format!("bad chart channel {channel}"))?;
    let o = data
        .as_object()
        .ok_or_else(|| "chart data not object".to_string())?;
    let tick_ms = o
        .get("tick")
        .and_then(|t| t.as_i64())
        .ok_or_else(|| "chart missing tick".to_string())?;
    Ok(DecodedEvent::Candle {
        instrument: instrument.to_string(),
        open: Price(fixed_from_json(
            o.get("open")
                .ok_or_else(|| "chart missing open".to_string())?,
        )?),
        high: Price(fixed_from_json(
            o.get("high")
                .ok_or_else(|| "chart missing high".to_string())?,
        )?),
        low: Price(fixed_from_json(
            o.get("low")
                .ok_or_else(|| "chart missing low".to_string())?,
        )?),
        close: Price(fixed_from_json(
            o.get("close")
                .ok_or_else(|| "chart missing close".to_string())?,
        )?),
        volume: Quantity(fixed_from_json(
            o.get("volume")
                .ok_or_else(|| "chart missing volume".to_string())?,
        )?),
        interval_ns: interval_ns_from_resolution(resolution)?,
        start_ts: ms_to_ts(tick_ms),
        exchange_ts_ms: tick_ms,
    })
}

fn decode_book(data: &Value) -> Result<DecodedEvent, String> {
    let o = data
        .as_object()
        .ok_or_else(|| "book data not object".to_string())?;
    let instrument = o
        .get("instrument_name")
        .and_then(|s| s.as_str())
        .ok_or_else(|| "book missing instrument_name".to_string())?
        .to_string();
    let change_id = o
        .get("change_id")
        .and_then(|c| c.as_u64())
        .ok_or_else(|| "book missing change_id".to_string())?;
    let prev_change_id = o.get("prev_change_id").and_then(|c| c.as_u64());
    let exchange_ts_ms = o.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0);
    let bids = decode_book_levels(
        o.get("bids")
            .ok_or_else(|| "book missing bids".to_string())?,
    )?;
    let asks = decode_book_levels(
        o.get("asks")
            .ok_or_else(|| "book missing asks".to_string())?,
    )?;

    let is_snapshot = match o.get("type").and_then(|t| t.as_str()) {
        Some("snapshot") => true,
        Some("change") => false,
        _ => prev_change_id.is_none(),
    };
    if is_snapshot {
        Ok(DecodedEvent::BookSnapshot {
            instrument,
            change_id,
            bids,
            asks,
            exchange_ts_ms,
        })
    } else {
        let prev_change_id =
            prev_change_id.ok_or_else(|| "book change missing prev_change_id".to_string())?;
        Ok(DecodedEvent::BookChange {
            instrument,
            change_id,
            prev_change_id,
            bids,
            asks,
            exchange_ts_ms,
        })
    }
}

fn decode_book_levels(v: &Value) -> Result<Vec<BookLevelChange>, String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "book levels not array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let tuple = row
            .as_array()
            .ok_or_else(|| "book level not array".to_string())?;
        if tuple.len() != 3 {
            return Err("book level tuple must have 3 elements".into());
        }
        let action = match tuple[0].as_str() {
            Some("new") => BookLevelAction::New,
            Some("change") => BookLevelAction::Change,
            Some("delete") => BookLevelAction::Delete,
            other => return Err(format!("unknown book level action: {other:?}")),
        };
        out.push(BookLevelChange {
            action,
            price: Price(fixed_from_json(&tuple[1])?),
            quantity: Quantity(fixed_from_json(&tuple[2])?),
        });
    }
    Ok(out)
}

fn decode_trades(data: &Value) -> Result<DecodedEvent, String> {
    let arr = data
        .as_array()
        .ok_or_else(|| "trades data not array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let o = row
            .as_object()
            .ok_or_else(|| "trade row not object".to_string())?;
        let instrument = o
            .get("instrument_name")
            .and_then(|s| s.as_str())
            .ok_or_else(|| "trade missing instrument_name".to_string())?
            .to_string();
        let trade_id = match o.get("trade_id") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            _ => return Err("trade missing trade_id".into()),
        };
        let direction = o.get("direction").and_then(|d| d.as_str()).unwrap_or("");
        let aggressor = match direction {
            "buy" => AggressorSide::Buy,
            "sell" => AggressorSide::Sell,
            _ => AggressorSide::Unknown,
        };
        // Deribit: `"T"` = taker liquidated, `"M"` = maker liquidated; absent/null = normal.
        let liquidation = matches!(
            o.get("liquidation").and_then(|v| v.as_str()),
            Some("T") | Some("M")
        );
        out.push(TradeRow {
            instrument,
            trade_id,
            trade_seq: o.get("trade_seq").and_then(|t| t.as_u64()),
            price: Price(fixed_from_json(
                o.get("price")
                    .ok_or_else(|| "trade missing price".to_string())?,
            )?),
            quantity: Quantity(fixed_from_json(
                o.get("amount")
                    .ok_or_else(|| "trade missing amount".to_string())?,
            )?),
            aggressor,
            exchange_ts_ms: o
                .get("timestamp")
                .and_then(|t| t.as_i64())
                .ok_or_else(|| "trade missing timestamp".to_string())?,
            liquidation,
        });
    }
    Ok(DecodedEvent::Trades(out))
}

fn decode_ticker(data: &Value) -> Result<DecodedEvent, String> {
    let o = data
        .as_object()
        .ok_or_else(|| "ticker data not object".to_string())?;
    let instrument = o
        .get("instrument_name")
        .and_then(|s| s.as_str())
        .ok_or_else(|| "ticker missing instrument_name".to_string())?
        .to_string();
    let exchange_ts_ms = o.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0);

    let quote = match (
        o.get("best_bid_price"),
        o.get("best_ask_price"),
        o.get("best_bid_amount"),
        o.get("best_ask_amount"),
    ) {
        (Some(bp), Some(ap), bq, aq) if !bp.is_null() && !ap.is_null() => Some(Quote {
            bid_price: Price(fixed_from_json(bp)?),
            ask_price: Price(fixed_from_json(ap)?),
            bid_quantity: match bq {
                Some(v) if !v.is_null() => Some(Quantity(fixed_from_json(v)?)),
                _ => None,
            },
            ask_quantity: match aq {
                Some(v) if !v.is_null() => Some(Quantity(fixed_from_json(v)?)),
                _ => None,
            },
        }),
        _ => None,
    };

    let mark = match o.get("mark_price") {
        Some(v) if !v.is_null() => Some(PricePoint {
            price: Price(fixed_from_json(v)?),
        }),
        _ => None,
    };
    let index = match o.get("index_price") {
        Some(v) if !v.is_null() => Some(PricePoint {
            price: Price(fixed_from_json(v)?),
        }),
        _ => None,
    };
    let funding = match o.get("funding_8h") {
        Some(v) if !v.is_null() => Some(Funding {
            rate: Rate(fixed_from_json(v)?),
            next_funding_ts: None,
        }),
        _ => None,
    };
    let open_interest = match o.get("open_interest") {
        Some(v) if !v.is_null() => Some(OpenInterest {
            quantity: Quantity(fixed_from_json(v)?),
        }),
        _ => None,
    };

    // `stats` carries 24h high/low/volume; `last_price` → close. No free open.
    let (high, low, volume, quote_volume) = match o.get("stats").and_then(|s| s.as_object()) {
        Some(stats) => (
            match stats.get("high") {
                Some(v) if !v.is_null() => Some(Price(fixed_from_json(v)?)),
                _ => None,
            },
            match stats.get("low") {
                Some(v) if !v.is_null() => Some(Price(fixed_from_json(v)?)),
                _ => None,
            },
            match stats.get("volume") {
                Some(v) if !v.is_null() => Some(Quantity(fixed_from_json(v)?)),
                _ => None,
            },
            match stats.get("volume_usd") {
                Some(v) if !v.is_null() => Some(Quantity(fixed_from_json(v)?)),
                _ => None,
            },
        ),
        None => (None, None, None, None),
    };
    let close = match o.get("last_price") {
        Some(v) if !v.is_null() => Some(Price(fixed_from_json(v)?)),
        _ => None,
    };

    Ok(DecodedEvent::Ticker(Box::new(TickerRow {
        instrument,
        exchange_ts_ms,
        quote,
        mark,
        index,
        funding,
        open_interest,
        open: None,
        high,
        low,
        close,
        volume,
        quote_volume,
    })))
}

/// ponytail: JSON numbers via `Number::to_string`; ceiling = f64 round-trip loss on
/// pathological decimals; upgrade = raw-token decimal scan.
pub fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("expected number or string".into()),
    }
}

pub fn trade_id_source(id: &str) -> SourceId {
    SourceId(id.to_string())
}

pub fn ms_to_ts(ms: i64) -> TimestampNs {
    TimestampNs(ms.saturating_mul(1_000_000))
}

pub fn to_market_trade(row: &TradeRow) -> Trade {
    Trade {
        price: row.price,
        quantity: row.quantity,
        aggressor: row.aggressor,
        trade_id: Some(trade_id_source(&row.trade_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_trades_batch() {
        let raw = br#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"trades.BTC-PERPETUAL.raw","data":[{
            "trade_seq":1,"trade_id":"555","timestamp":1623060194301,
            "price":36457.5,"amount":10,"direction":"buy",
            "instrument_name":"BTC-PERPETUAL"
          }]}
        }"#;
        let DecodedEvent::Trades(rows) = decode_text(raw).unwrap() else {
            panic!("trades");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].aggressor, AggressorSide::Buy);
        assert_eq!(rows[0].price.0, Fixed::parse_str("36457.5").unwrap());
    }

    #[test]
    fn decode_ticker_fields() {
        let raw = br#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"ticker.BTC-PERPETUAL.100ms","data":{
            "timestamp":1623060194301,"instrument_name":"BTC-PERPETUAL",
            "best_bid_price":36442.5,"best_bid_amount":5000,
            "best_ask_price":36443,"best_ask_amount":100,
            "mark_price":36446.51,"index_price":36441.64,
            "funding_8h":0.0000211,"open_interest":502097590,
            "last_price":36450.0,
            "stats":{"high":37000.0,"low":35000.0,"volume":1234.5,"volume_usd":45000000.0}
          }}
        }"#;
        let DecodedEvent::Ticker(t) = decode_text(raw).unwrap() else {
            panic!("ticker");
        };
        assert!(t.quote.is_some());
        assert!(t.mark.is_some());
        assert!(t.index.is_some());
        assert!(t.funding.is_some());
        assert!(t.open_interest.is_some());
        assert_eq!(
            t.high.as_ref().unwrap().0,
            Fixed::parse_str("37000.0").unwrap()
        );
        assert_eq!(
            t.low.as_ref().unwrap().0,
            Fixed::parse_str("35000.0").unwrap()
        );
        assert_eq!(
            t.close.as_ref().unwrap().0,
            Fixed::parse_str("36450.0").unwrap()
        );
        assert_eq!(
            t.volume.as_ref().unwrap().0,
            Fixed::parse_str("1234.5").unwrap()
        );
        assert_eq!(
            t.quote_volume.as_ref().unwrap().0,
            Fixed::parse_str("45000000.0").unwrap()
        );
    }

    #[test]
    fn decode_deribit_price_index() {
        assert_eq!(deribit_index_name("BTC-PERPETUAL"), "btc_usd");
        assert_eq!(deribit_index_name("BTC_USDC-PERPETUAL"), "btc_usdc");
        let raw = br#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"deribit_price_index.btc_usd","data":{
            "timestamp":1535098298227,"price":6521.17,"index_name":"btc_usd"
          }}
        }"#;
        let DecodedEvent::IndexPrice {
            index_name, price, ..
        } = decode_text(raw).unwrap()
        else {
            panic!("price_index");
        };
        assert_eq!(index_name, "btc_usd");
        assert_eq!(price.0, Fixed::parse_str("6521.17").unwrap());
    }

    #[test]
    fn decode_book_snapshot_no_prev_change_id() {
        let raw = br#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"book.BTC-PERPETUAL.100ms","data":{
            "type":"snapshot","timestamp":1554373962454,"instrument_name":"BTC-PERPETUAL",
            "change_id":297217,
            "bids":[["new",5042.34,30],["new",5041.94,20]],
            "asks":[["new",5042.64,40],["new",5043.3,40]]
          }}
        }"#;
        let DecodedEvent::BookSnapshot {
            change_id,
            bids,
            asks,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("snapshot");
        };
        assert_eq!(change_id, 297217);
        assert_eq!(bids.len(), 2);
        assert_eq!(bids[0].action, BookLevelAction::New);
        assert_eq!(bids[0].price.0, Fixed::parse_str("5042.34").unwrap());
        assert_eq!(asks[1].quantity.0, Fixed::parse_str("40").unwrap());
    }

    #[test]
    fn decode_book_change_with_prev_change_id() {
        let raw = br#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"book.BTC-PERPETUAL.100ms","data":{
            "timestamp":1535098298327,"instrument_name":"BTC-PERPETUAL",
            "prev_change_id":123456,"change_id":123457,
            "bids":[["change",50000.0,9.8],["delete",49999.5,0]],
            "asks":[["new",50002.0,3.5]]
          }}
        }"#;
        let DecodedEvent::BookChange {
            change_id,
            prev_change_id,
            bids,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("change");
        };
        assert_eq!(prev_change_id, 123456);
        assert_eq!(change_id, 123457);
        assert_eq!(bids[1].action, BookLevelAction::Delete);
    }

    #[test]
    fn decode_heartbeat_test_request() {
        let raw = br#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#;
        assert!(matches!(
            decode_text(raw).unwrap(),
            DecodedEvent::Heartbeat { needs_test: true }
        ));
    }

    #[test]
    fn decode_chart_trades_candle_exact_fixed() {
        let raw = br#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"chart.trades.BTC-PERPETUAL.1","data":{
            "volume":0.05219351,"tick":1573645080000,"open":8869.79,
            "low":8788.25,"high":8870.31,"cost":460,"close":8791.25
          }}
        }"#;
        let DecodedEvent::Candle {
            instrument,
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
        assert_eq!(instrument, "BTC-PERPETUAL");
        assert_eq!(open.0, Fixed::parse_str("8869.79").unwrap());
        assert_eq!(high.0, Fixed::parse_str("8870.31").unwrap());
        assert_eq!(low.0, Fixed::parse_str("8788.25").unwrap());
        assert_eq!(close.0, Fixed::parse_str("8791.25").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("0.05219351").unwrap());
        assert_eq!(interval_ns, candle_interval_ns(CandleInterval::M1));
        assert_eq!(chart_resolution(CandleInterval::M1), "1");
        assert_eq!(start_ts, ms_to_ts(1_573_645_080_000));
    }

    /// Active backend must match the serde oracle (default = both serde).
    #[test]
    fn decode_text_matches_serde_oracle() {
        for raw in deribit_parity_fixtures() {
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
        for raw in deribit_parity_fixtures() {
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

    fn deribit_parity_fixtures() -> &'static [&'static [u8]] {
        &[
            br#"{"jsonrpc":"2.0","id":1,"result":["trades.BTC-PERPETUAL.raw"]}"#,
            br#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#,
            br#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.raw","data":[{"trade_seq":1,"trade_id":"555","timestamp":1623060194301,"price":36457.5,"amount":10,"direction":"buy","instrument_name":"BTC-PERPETUAL"}]}}"#,
            br#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"book.BTC-PERPETUAL.100ms","data":{"type":"snapshot","timestamp":1554373962454,"instrument_name":"BTC-PERPETUAL","change_id":297217,"bids":[["new",5042.34,30],["new",5041.94,20]],"asks":[["new",5042.64,40],["new",5043.3,40]]}}}"#,
            br#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"chart.trades.BTC-PERPETUAL.1","data":{"volume":0.05219351,"tick":1573645080000,"open":8869.79,"low":8788.25,"high":8870.31,"cost":460,"close":8791.25}}}"#,
        ]
    }
}
