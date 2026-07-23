//! OKX JSON decoding (exact Fixed; no f64) — shared by Spot/SWAP/Futures sessions.

use marketfeed_adapter_api::CandleInterval;
use marketfeed_model::{
    AggressorSide, BookLevel, BookOperation, BookSide, Fixed, Price, Quantity, Rate, SourceId,
    TimestampNs,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedEvent {
    Trade {
        inst_id: String,
        trade_id: String,
        price: Price,
        quantity: Quantity,
        aggressor: AggressorSide,
        exchange_ts_ms: i64,
        seq_id: Option<u64>,
    },
    Quote {
        inst_id: String,
        bid_price: Price,
        bid_qty: Quantity,
        ask_price: Price,
        ask_qty: Quantity,
        exchange_ts_ms: i64,
        /// Native 24h stats from `tickers` (`open24h`/`high24h`/`low24h`/`last`/`vol24h`/`volCcy24h`).
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        quote_volume: Quantity,
    },
    Candle {
        inst_id: String,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        interval_ns: i64,
        start_ts: TimestampNs,
        exchange_ts_ms: i64,
        /// Venue confirm flag (`"1"` = bar closed).
        is_closed: bool,
    },
    BookSnapshot {
        inst_id: String,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        seq_id: u64,
        exchange_ts_ms: i64,
        /// Venue checksum; `0` / absent after OKX deprecation.
        checksum: i64,
    },
    BookUpdate {
        inst_id: String,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        prev_seq_id: u64,
        seq_id: u64,
        exchange_ts_ms: i64,
        checksum: i64,
    },
    /// Derivatives-only: `mark-price` channel.
    MarkPrice {
        inst_id: String,
        mark_px: Price,
        exchange_ts_ms: i64,
    },
    /// Derivatives-only: `index-tickers` channel.
    IndexPrice {
        inst_id: String,
        idx_px: Price,
        exchange_ts_ms: i64,
    },
    /// Derivatives-only: `funding-rate` channel.
    Funding {
        inst_id: String,
        rate: Rate,
        next_funding_ts_ms: Option<i64>,
        exchange_ts_ms: i64,
    },
    /// Derivatives-only: `open-interest` channel.
    OpenInterest {
        inst_id: String,
        quantity: Quantity,
        exchange_ts_ms: i64,
    },
    /// Derivatives-only: `liquidation-orders` channel (instType firehose).
    Liquidations(Vec<LiquidationDecoded>),
    SubscribeAck {
        channel: Option<String>,
        inst_id: Option<String>,
    },
    Error {
        code: Option<String>,
        msg: Option<String>,
    },
    Ping,
    Pong,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiquidationDecoded {
    pub inst_id: String,
    pub price: Price,
    pub quantity: Quantity,
    /// Aggressor of the forced order (`side` field, not `posSide`).
    pub side: AggressorSide,
    pub exchange_ts_ms: i64,
}

#[derive(Debug, Deserialize)]
struct Arg {
    channel: Option<String>,
    #[serde(rename = "instId")]
    inst_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TradeRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "tradeId")]
    trade_id: String,
    px: String,
    sz: String,
    side: String,
    ts: String,
    #[serde(rename = "seqId")]
    seq_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TickerRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "bidPx")]
    bid_px: String,
    #[serde(rename = "bidSz")]
    bid_sz: String,
    #[serde(rename = "askPx")]
    ask_px: String,
    #[serde(rename = "askSz")]
    ask_sz: String,
    #[serde(rename = "open24h")]
    open24h: String,
    #[serde(rename = "high24h")]
    high24h: String,
    #[serde(rename = "low24h")]
    low24h: String,
    last: String,
    #[serde(rename = "vol24h")]
    vol24h: String,
    #[serde(rename = "volCcy24h")]
    vol_ccy24h: String,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct BookRow {
    asks: Vec<Vec<String>>,
    bids: Vec<Vec<String>>,
    ts: String,
    #[serde(rename = "seqId")]
    seq_id: u64,
    #[serde(rename = "prevSeqId")]
    prev_seq_id: Option<i64>,
    /// Deprecated by OKX (always 0 after 2026-06); still parsed for legacy non-zero path.
    /// Continuity is `seqId`/`prevSeqId`; non-zero checksum fail-closes in the session.
    #[serde(default)]
    checksum: i64,
}

#[derive(Debug, Deserialize)]
struct MarkPriceRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "markPx")]
    mark_px: String,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct IndexTickerRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "idxPx")]
    idx_px: String,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct FundingRateRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingTime")]
    funding_time: Option<String>,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct OpenInterestRow {
    #[serde(rename = "instId")]
    inst_id: String,
    oi: String,
    ts: String,
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
    if bytes == b"ping" {
        return Ok(DecodedEvent::Ping);
    }
    if bytes == b"pong" {
        return Ok(DecodedEvent::Pong);
    }
    let v = parse(bytes)?;
    decode_value(&v)
}

fn decode_value(v: &Value) -> Result<DecodedEvent, String> {
    let Some(obj) = v.as_object() else {
        return Ok(DecodedEvent::Unknown);
    };

    if let Some(event) = obj.get("event").and_then(|x| x.as_str()) {
        match event {
            "subscribe" | "unsubscribe" => {
                let arg: Option<Arg> = obj
                    .get("arg")
                    .and_then(|a| serde_json::from_value(a.clone()).ok());
                return Ok(DecodedEvent::SubscribeAck {
                    channel: arg.as_ref().and_then(|a| a.channel.clone()),
                    inst_id: arg.as_ref().and_then(|a| a.inst_id.clone()),
                });
            }
            "error" => {
                return Ok(DecodedEvent::Error {
                    code: obj.get("code").and_then(|c| c.as_str()).map(str::to_string),
                    msg: obj.get("msg").and_then(|m| m.as_str()).map(str::to_string),
                });
            }
            _ => return Ok(DecodedEvent::Unknown),
        }
    }

    let channel = obj
        .get("arg")
        .and_then(|a| a.get("channel"))
        .and_then(|c| c.as_str())
        .unwrap_or("");

    match channel {
        "trades" => decode_trades(obj),
        "tickers" => decode_tickers(obj),
        "books" | "books5" | "bbo-tbt" | "books-l2-tbt" | "books50-l2-tbt" => {
            decode_books(obj, channel)
        }
        "mark-price" => decode_mark_price(obj),
        "index-tickers" => decode_index_price(obj),
        "funding-rate" => decode_funding_rate(obj),
        "open-interest" => decode_open_interest(obj),
        "liquidation-orders" => decode_liquidation_orders(obj),
        ch if ch.starts_with("candle") => decode_candle(obj, ch),
        _ => Ok(DecodedEvent::Unknown),
    }
}

/// OKX candle channel name for a canonical interval.
pub fn candle_channel(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "candle1m",
        CandleInterval::M5 => "candle5m",
        CandleInterval::M15 => "candle15m",
        CandleInterval::H1 => "candle1H",
        CandleInterval::D1 => "candle1D",
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

fn interval_ns_from_channel(channel: &str) -> Result<i64, String> {
    let interval = match channel {
        "candle1m" => CandleInterval::M1,
        "candle5m" => CandleInterval::M5,
        "candle15m" => CandleInterval::M15,
        "candle1H" => CandleInterval::H1,
        "candle1D" | "candle1Dutc" => CandleInterval::D1,
        other => return Err(format!("unsupported candle channel {other}")),
    };
    Ok(candle_interval_ns(interval))
}

fn decode_candle(
    obj: &serde_json::Map<String, Value>,
    channel: &str,
) -> Result<DecodedEvent, String> {
    let inst_id = obj
        .get("arg")
        .and_then(|a| a.get("instId"))
        .and_then(|i| i.as_str())
        .unwrap_or("")
        .to_string();
    let rows: Vec<Vec<String>> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    // [ts, o, h, l, c, vol, volCcy, volCcyQuote?, confirm]
    if row.len() < 6 {
        return Err("candle row too short".into());
    }
    let confirm_idx = if row.len() >= 9 {
        8
    } else if row.len() >= 8 {
        7
    } else {
        usize::MAX
    };
    let is_closed = confirm_idx != usize::MAX && row[confirm_idx] == "1";
    let start_ms = parse_ts_ms(&row[0])?;
    Ok(DecodedEvent::Candle {
        inst_id,
        open: Price(parse_fixed(&row[1])?),
        high: Price(parse_fixed(&row[2])?),
        low: Price(parse_fixed(&row[3])?),
        close: Price(parse_fixed(&row[4])?),
        volume: Quantity(parse_fixed(&row[5])?),
        interval_ns: interval_ns_from_channel(channel)?,
        start_ts: TimestampNs(start_ms.saturating_mul(1_000_000)),
        exchange_ts_ms: start_ms,
        is_closed,
    })
}

fn decode_trades(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let rows: Vec<TradeRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    let aggressor = match row.side.as_str() {
        "buy" => AggressorSide::Buy,
        "sell" => AggressorSide::Sell,
        _ => AggressorSide::Unknown,
    };
    Ok(DecodedEvent::Trade {
        inst_id: row.inst_id,
        trade_id: row.trade_id,
        price: Price(parse_fixed(&row.px)?),
        quantity: Quantity(parse_fixed(&row.sz)?),
        aggressor,
        exchange_ts_ms: parse_ts_ms(&row.ts)?,
        seq_id: row.seq_id,
    })
}

fn decode_tickers(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let rows: Vec<TickerRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    if row.bid_px.is_empty() || row.ask_px.is_empty() {
        return Ok(DecodedEvent::Unknown);
    }
    Ok(DecodedEvent::Quote {
        inst_id: row.inst_id,
        bid_price: Price(parse_fixed(&row.bid_px)?),
        bid_qty: Quantity(parse_fixed(&row.bid_sz)?),
        ask_price: Price(parse_fixed(&row.ask_px)?),
        ask_qty: Quantity(parse_fixed(&row.ask_sz)?),
        exchange_ts_ms: parse_ts_ms(&row.ts)?,
        open: Price(parse_fixed(&row.open24h)?),
        high: Price(parse_fixed(&row.high24h)?),
        low: Price(parse_fixed(&row.low24h)?),
        close: Price(parse_fixed(&row.last)?),
        volume: Quantity(parse_fixed(&row.vol24h)?),
        quote_volume: Quantity(parse_fixed(&row.vol_ccy24h)?),
    })
}

fn decode_books(
    obj: &serde_json::Map<String, Value>,
    channel: &str,
) -> Result<DecodedEvent, String> {
    // ponytail: books5/bbo-tbt reuse books decoder; full depth sync only for `books`.
    let _ = channel;
    let action = obj
        .get("action")
        .and_then(|a| a.as_str())
        .unwrap_or("snapshot");
    let rows: Vec<BookRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    let inst_id = obj
        .get("arg")
        .and_then(|a| a.get("instId"))
        .and_then(|i| i.as_str())
        .unwrap_or("")
        .to_string();
    let bids = parse_book_levels(&row.bids)?;
    let asks = parse_book_levels(&row.asks)?;
    let exchange_ts_ms = parse_ts_ms(&row.ts)?;
    match action {
        "snapshot" => Ok(DecodedEvent::BookSnapshot {
            inst_id,
            bids,
            asks,
            seq_id: row.seq_id,
            exchange_ts_ms,
            checksum: row.checksum,
        }),
        "update" => {
            let prev = row.prev_seq_id.unwrap_or(-1);
            if prev < 0 {
                return Err("books update missing prevSeqId".into());
            }
            Ok(DecodedEvent::BookUpdate {
                inst_id,
                bids,
                asks,
                prev_seq_id: prev as u64,
                seq_id: row.seq_id,
                exchange_ts_ms,
                checksum: row.checksum,
            })
        }
        _ => Ok(DecodedEvent::Unknown),
    }
}

fn decode_mark_price(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let rows: Vec<MarkPriceRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    Ok(DecodedEvent::MarkPrice {
        inst_id: row.inst_id,
        mark_px: Price(parse_fixed(&row.mark_px)?),
        exchange_ts_ms: parse_ts_ms(&row.ts)?,
    })
}

fn decode_index_price(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let rows: Vec<IndexTickerRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    Ok(DecodedEvent::IndexPrice {
        inst_id: row.inst_id,
        idx_px: Price(parse_fixed(&row.idx_px)?),
        exchange_ts_ms: parse_ts_ms(&row.ts)?,
    })
}

fn decode_funding_rate(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let rows: Vec<FundingRateRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    let next_funding_ts_ms = row
        .funding_time
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(parse_ts_ms)
        .transpose()?;
    Ok(DecodedEvent::Funding {
        inst_id: row.inst_id,
        rate: Rate(parse_fixed(&row.funding_rate)?),
        next_funding_ts_ms,
        exchange_ts_ms: parse_ts_ms(&row.ts)?,
    })
}

fn decode_open_interest(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let rows: Vec<OpenInterestRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(DecodedEvent::Unknown);
    };
    Ok(DecodedEvent::OpenInterest {
        inst_id: row.inst_id,
        quantity: Quantity(parse_fixed(&row.oi)?),
        exchange_ts_ms: parse_ts_ms(&row.ts)?,
    })
}

#[derive(Debug, Deserialize)]
struct LiquidationOrderRow {
    #[serde(rename = "instId")]
    inst_id: String,
    details: Vec<LiquidationDetailRow>,
}

#[derive(Debug, Deserialize)]
struct LiquidationDetailRow {
    #[serde(rename = "bkPx")]
    bk_px: String,
    sz: String,
    /// Forced-order side (`buy` / `sell`), not position side.
    side: String,
    ts: String,
}

fn decode_liquidation_orders(obj: &serde_json::Map<String, Value>) -> Result<DecodedEvent, String> {
    let rows: Vec<LiquidationOrderRow> =
        serde_json::from_value(obj.get("data").cloned().unwrap_or(Value::Null))
            .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in rows {
        for d in row.details {
            let side = match d.side.as_str() {
                "buy" => AggressorSide::Buy,
                "sell" => AggressorSide::Sell,
                _ => AggressorSide::Unknown,
            };
            out.push(LiquidationDecoded {
                inst_id: row.inst_id.clone(),
                price: Price(parse_fixed(&d.bk_px)?),
                quantity: Quantity(parse_fixed(&d.sz)?),
                side,
                exchange_ts_ms: parse_ts_ms(&d.ts)?,
            });
        }
    }
    if out.is_empty() {
        return Ok(DecodedEvent::Unknown);
    }
    Ok(DecodedEvent::Liquidations(out))
}

fn parse_book_levels(rows: &[Vec<String>]) -> Result<Vec<(Price, Quantity)>, String> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() < 2 {
            return Err("book level needs price+size".into());
        }
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

fn parse_ts_ms(s: &str) -> Result<i64, String> {
    s.parse::<i64>().map_err(|e| e.to_string())
}

pub fn level_op(qty: Quantity) -> (BookOperation, Option<Quantity>) {
    if qty.0.coefficient == 0 {
        (BookOperation::Delete, None)
    } else {
        (BookOperation::Upsert, Some(qty))
    }
}

pub fn to_book_levels(_side: BookSide, levels: &[(Price, Quantity)]) -> Vec<BookLevel> {
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

pub fn trade_id_u64(id: &str) -> Option<u64> {
    id.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_trade_side_is_aggressor() {
        let raw = br#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"10","px":"100.5","sz":"0.01","side":"sell","ts":"2","seqId":9}]}"#;
        let DecodedEvent::Trade {
            aggressor, price, ..
        } = decode_text(raw).unwrap()
        else {
            panic!("trade");
        };
        assert_eq!(aggressor, AggressorSide::Sell);
        assert_eq!(price.0, Fixed::new(1005, 1));
    }

    #[test]
    fn decode_ticker_quote_with_stats24h() {
        let raw = br#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","bidPx":"100.0","bidSz":"1","askPx":"100.1","askSz":"2","ts":"1","last":"100.05","lastSz":"0","instType":"SPOT","open24h":"99.0","high24h":"101.5","low24h":"98.5","volCcy24h":"2500.0","vol24h":"25.5","sodUtc0":"0","sodUtc8":"0"}]}"#;
        let DecodedEvent::Quote {
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("quote");
        };
        assert_eq!(open.0, Fixed::parse_str("99.0").unwrap());
        assert_eq!(high.0, Fixed::parse_str("101.5").unwrap());
        assert_eq!(low.0, Fixed::parse_str("98.5").unwrap());
        assert_eq!(close.0, Fixed::parse_str("100.05").unwrap());
        assert_eq!(volume.0, Fixed::parse_str("25.5").unwrap());
        assert_eq!(quote_volume.0, Fixed::parse_str("2500.0").unwrap());
    }

    #[test]
    fn decode_books_snapshot_and_ping() {
        assert_eq!(decode_text(b"ping").unwrap(), DecodedEvent::Ping);
        let raw = br#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["101","1","0","1"]],"bids":[["100","2","0","1"]],"ts":"1","checksum":0,"prevSeqId":-1,"seqId":10}]}"#;
        let DecodedEvent::BookSnapshot {
            seq_id,
            bids,
            checksum,
            ..
        } = decode_text(raw).unwrap()
        else {
            panic!("snap");
        };
        assert_eq!(seq_id, 10);
        assert_eq!(checksum, 0);
        assert_eq!(bids[0].1.0.coefficient, 2);
    }

    #[test]
    fn zero_qty_is_delete() {
        let (op, q) = level_op(Quantity(Fixed::new(0, 0)));
        assert_eq!(op, BookOperation::Delete);
        assert!(q.is_none());
    }

    #[test]
    fn decode_candle1m_exact_fixed() {
        let raw = br#"{"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1597026383085","3.721","3.743","3.677","3.708","8422410","22698348.04828491","0"]]}"#;
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
            panic!("candle");
        };
        assert_eq!(open.0, Fixed::new(3721, 3));
        assert_eq!(high.0, Fixed::new(3743, 3));
        assert_eq!(low.0, Fixed::new(3677, 3));
        assert_eq!(close.0, Fixed::new(3708, 3));
        assert_eq!(volume.0, Fixed::new(8422410, 0));
        assert_eq!(interval_ns, candle_interval_ns(CandleInterval::M1));
        assert_eq!(start_ts, TimestampNs(1_597_026_383_085_000_000));
        assert!(!is_closed);
    }

    #[test]
    fn decode_mark_index_and_funding() {
        let mark = br#"{"arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","markPx":"65010.5","ts":"1"}]}"#;
        let DecodedEvent::MarkPrice { mark_px, .. } = decode_text(mark).unwrap() else {
            panic!("mark-price");
        };
        assert_eq!(mark_px.0, Fixed::new(650105, 1));

        let index = br#"{"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","idxPx":"65000.0","high24h":"0","sodUtc0":"0","open24h":"0","low24h":"0","sodUtc8":"0","ts":"2"}]}"#;
        let DecodedEvent::IndexPrice { idx_px, .. } = decode_text(index).unwrap() else {
            panic!("index-tickers");
        };
        assert_eq!(idx_px.0, Fixed::new(650000, 1));

        let funding = br#"{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","fundingRate":"0.0001515","nextFundingRate":"0.0003","fundingTime":"1622822400000","minFundingRate":"-0.00375","maxFundingRate":"0.00375","settFundingRate":"0.0001515","settState":"settled","method":"next_period","premium":"","ts":"1622813835000"}]}"#;
        let DecodedEvent::Funding {
            rate,
            next_funding_ts_ms,
            ..
        } = decode_text(funding).unwrap()
        else {
            panic!("funding-rate");
        };
        assert_eq!(rate.0, Fixed::new(1515, 7));
        assert_eq!(next_funding_ts_ms, Some(1_622_822_400_000));

        let oi = br#"{"arg":{"channel":"open-interest","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","oi":"10659.509","oiCcy":"10659.509","ts":"1589437530011"}]}"#;
        let DecodedEvent::OpenInterest { quantity, .. } = decode_text(oi).unwrap() else {
            panic!("open-interest");
        };
        assert_eq!(quantity.0, Fixed::new(10659509, 3));

        let liq = br#"{"arg":{"channel":"liquidation-orders","instType":"SWAP"},"data":[{"details":[{"bkLoss":"0","bkPx":"23523.9","ccy":"","posSide":"short","side":"buy","sz":"0.01","ts":"1672738134824"}],"instFamily":"BTC-USDT","instId":"BTC-USDT-SWAP","instType":"SWAP","uly":"BTC-USDT"}]}"#;
        let DecodedEvent::Liquidations(rows) = decode_text(liq).unwrap() else {
            panic!("liquidation-orders");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].inst_id, "BTC-USDT-SWAP");
        assert_eq!(rows[0].side, AggressorSide::Buy);
        assert_eq!(rows[0].price.0, Fixed::new(235239, 1));
        assert_eq!(rows[0].quantity.0, Fixed::new(1, 2));
        assert_eq!(rows[0].exchange_ts_ms, 1_672_738_134_824);
    }

    /// Active backend must match the serde oracle (default = both serde).
    #[test]
    fn decode_text_matches_serde_oracle() {
        for raw in okx_parity_fixtures() {
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
        for raw in okx_parity_fixtures() {
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

    fn okx_parity_fixtures() -> &'static [&'static [u8]] {
        &[
            b"ping",
            b"pong",
            br#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"10","px":"100.5","sz":"0.01","side":"sell","ts":"2","seqId":9}]}"#,
            br#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","bidPx":"100.0","bidSz":"1","askPx":"100.1","askSz":"2","ts":"1","last":"100.05","lastSz":"0","instType":"SPOT","open24h":"99.0","high24h":"101.5","low24h":"98.5","volCcy24h":"2500.0","vol24h":"25.5","sodUtc0":"0","sodUtc8":"0"}]}"#,
            br#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["101","1","0","1"]],"bids":[["100","2","0","1"]],"ts":"1","checksum":0,"prevSeqId":-1,"seqId":10}]}"#,
            br#"{"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1597026383085","3.721","3.743","3.677","3.708","8422410","22698348.04828491","0"]]}"#,
            br#"{"arg":{"channel":"open-interest","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","oi":"10659.509","oiCcy":"10659.509","ts":"1589437530011"}]}"#,
            br#"{"arg":{"channel":"liquidation-orders","instType":"SWAP"},"data":[{"details":[{"bkLoss":"0","bkPx":"23523.9","ccy":"","posSide":"short","side":"buy","sz":"0.01","ts":"1672738134824"}],"instFamily":"BTC-USDT","instId":"BTC-USDT-SWAP","instType":"SWAP","uly":"BTC-USDT"}]}"#,
            br#"{"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"}}"#,
        ]
    }
}
