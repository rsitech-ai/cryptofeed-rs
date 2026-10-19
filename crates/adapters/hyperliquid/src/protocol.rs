//! Shared native protocol model. Numeric JSON tokens and string decimals are
//! parsed exactly, including exponents. Native attributes remain available to
//! consumers (order counts, trade hash/ID, candle end/count, asset context).
use marketfeed_adapter_api::AdapterError;
use marketfeed_model::Fixed;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, value::RawValue};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Decimal(pub Fixed);
impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(d)?;
        let s = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(serde::de::Error::custom)?
        } else {
            raw.get().to_owned()
        };
        Fixed::parse_str(&s)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct Trade {
    pub coin: String,
    pub side: String,
    pub px: Decimal,
    pub sz: Decimal,
    pub hash: String,
    pub time: i64,
    pub tid: u64,
    #[serde(default)]
    pub users: Vec<String>,
}
impl Trade {
    /// The native tid alone is not globally unique (official API contract).
    pub fn unique_id(&self) -> String {
        format!("{}:{}:{}", self.coin, self.time, self.tid)
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct Level {
    pub px: Decimal,
    pub sz: Decimal,
    pub n: u64,
}
#[derive(Debug, Clone, Deserialize)]
pub struct Bbo {
    pub coin: String,
    pub time: i64,
    pub bbo: [Option<Level>; 2],
}
#[derive(Debug, Clone, Deserialize)]
pub struct Book {
    pub coin: String,
    pub time: i64,
    pub levels: [Vec<Level>; 2],
}
#[derive(Debug, Clone, Deserialize)]
pub struct Candle {
    pub t: i64,
    #[serde(rename = "T")]
    pub end: i64,
    pub s: String,
    pub i: String,
    pub o: Decimal,
    pub h: Decimal,
    pub l: Decimal,
    pub c: Decimal,
    pub v: Decimal,
    pub n: u64,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetContext {
    pub day_ntl_vlm: Decimal,
    pub prev_day_px: Decimal,
    pub mark_px: Decimal,
    pub mid_px: Option<Decimal>,
    pub circulating_supply: Option<Decimal>,
    pub funding: Option<Decimal>,
    pub open_interest: Option<Decimal>,
    pub oracle_px: Option<Decimal>,
    pub day_base_vlm: Option<Decimal>,
    pub premium: Option<Decimal>,
    pub impact_pxs: Option<[Decimal; 2]>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct Context {
    pub coin: String,
    pub ctx: AssetContext,
}
#[derive(Debug, Clone)]
pub enum Message {
    Trades(Vec<Trade>),
    Bbo(Bbo),
    Book(Book),
    Candle(Candle),
    Context(Context),
    AllMids(BTreeMap<String, Decimal>),
    Subscription(Value),
    Pong,
}
fn error(s: impl ToString) -> AdapterError {
    AdapterError::Parse(s.to_string())
}
fn positive(d: Decimal) -> Result<(), AdapterError> {
    if d.0.coefficient <= 0 {
        return Err(error("non-positive price/size"));
    }
    Ok(())
}
fn nonnegative(d: Decimal) -> Result<(), AdapterError> {
    if d.0.coefficient < 0 {
        return Err(error("negative quantity"));
    }
    Ok(())
}
fn timestamp(ms: i64) -> Result<(), AdapterError> {
    if ms < 0 || ms.checked_mul(1_000_000).is_none() {
        return Err(error("timestamp overflow"));
    }
    Ok(())
}
fn level(l: &Level) -> Result<(), AdapterError> {
    positive(l.px)?;
    positive(l.sz)
}
pub fn interval_ns(s: &str) -> Result<i64, AdapterError> {
    let minutes: i64 = match s {
        "1m" => 1,
        "3m" => 3,
        "5m" => 5,
        "15m" => 15,
        "30m" => 30,
        "1h" => 60,
        "2h" => 120,
        "4h" => 240,
        "8h" => 480,
        "12h" => 720,
        "1d" => 1440,
        "3d" => 4320,
        "1w" => 10080,
        // Calendar months cannot be represented as a fixed duration.
        _ => return Err(error("unsupported fixed candle interval")),
    };
    Ok(minutes * 60_000_000_000)
}
/// Validates the entire frame before returning any rows. Malformed trailing
/// rows cannot leave an accepted prefix that looks like a complete batch.
pub fn decode(bytes: &[u8]) -> Result<Message, AdapterError> {
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(error("Hyperliquid frame exceeds 4 MiB"));
    }
    #[derive(Deserialize)]
    struct Envelope {
        channel: String,
        #[serde(default)]
        data: Option<Box<RawValue>>,
    }
    let e: Envelope = serde_json::from_slice(bytes).map_err(error)?;
    let data = e.data.as_deref().map(RawValue::get).unwrap_or("null");
    Ok(match e.channel.as_str() {
        "trades" => {
            let rows: Vec<Trade> = serde_json::from_str(data).map_err(error)?;
            if rows.len() > u16::MAX as usize {
                return Err(error("trade batch too large"));
            }
            for t in &rows {
                positive(t.px)?;
                positive(t.sz)?;
                timestamp(t.time)?;
                if !matches!(t.side.as_str(), "B" | "A") {
                    return Err(error("unknown aggressor side"));
                }
                if t.coin.is_empty() || t.hash.is_empty() {
                    return Err(error("missing native identity"));
                }
            }
            Message::Trades(rows)
        }
        "bbo" => {
            let b: Bbo = serde_json::from_str(data).map_err(error)?;
            timestamp(b.time)?;
            for l in b.bbo.iter().flatten() {
                level(l)?;
            }
            if let [Some(bid), Some(ask)] = &b.bbo {
                if bid.px.0.numeric_cmp(ask.px.0).is_gt() {
                    return Err(error("crossed bbo"));
                }
            }
            Message::Bbo(b)
        }
        "l2Book" => {
            let b: Book = serde_json::from_str(data).map_err(error)?;
            timestamp(b.time)?;
            for (side, levels) in b.levels.iter().enumerate() {
                if levels.len() > 20 {
                    return Err(error("book exceeds native depth"));
                }
                for l in levels {
                    level(l)?;
                }
                for p in levels.windows(2) {
                    let cmp = p[0].px.0.numeric_cmp(p[1].px.0);
                    if (side == 0 && !cmp.is_gt()) || (side == 1 && !cmp.is_lt()) {
                        return Err(error("unsorted or duplicate book levels"));
                    }
                }
            }
            if let (Some(bid), Some(ask)) = (b.levels[0].first(), b.levels[1].first()) {
                if bid.px.0.numeric_cmp(ask.px.0).is_gt() {
                    return Err(error("crossed book"));
                }
            }
            Message::Book(b)
        }
        "candle" => {
            let c: Candle = serde_json::from_str(data).map_err(error)?;
            timestamp(c.t)?;
            timestamp(c.end)?;
            interval_ns(&c.i)?;
            for p in [c.o, c.h, c.l, c.c] {
                positive(p)?;
            }
            nonnegative(c.v)?;
            if c.end < c.t
                || c.l.0.numeric_cmp(c.h.0).is_gt()
                || [c.o, c.c]
                    .iter()
                    .any(|p| p.0.numeric_cmp(c.l.0).is_lt() || p.0.numeric_cmp(c.h.0).is_gt())
            {
                return Err(error("invalid candle bounds"));
            }
            Message::Candle(c)
        }
        "activeAssetCtx" | "activeSpotAssetCtx" => {
            let c: Context = serde_json::from_str(data).map_err(error)?;
            positive(c.ctx.mark_px)?;
            nonnegative(c.ctx.day_ntl_vlm)?;
            if let Some(v) = c.ctx.day_base_vlm {
                nonnegative(v)?;
            }
            if let Some(v) = c.ctx.mid_px {
                positive(v)?;
            }
            if let Some(v) = c.ctx.oracle_px {
                positive(v)?;
            }
            if let Some(v) = c.ctx.impact_pxs {
                for price in v {
                    positive(price)?;
                }
            }
            nonnegative(c.ctx.prev_day_px)?;
            if let Some(v) = c.ctx.circulating_supply {
                nonnegative(v)?;
            }
            if let Some(v) = c.ctx.open_interest {
                nonnegative(v)?;
            }
            Message::Context(c)
        }
        "allMids" => {
            #[derive(Deserialize)]
            struct Mids {
                mids: BTreeMap<String, Decimal>,
            }
            let m: Mids = serde_json::from_str(data).map_err(error)?;
            for v in m.mids.values() {
                positive(*v)?;
            }
            Message::AllMids(m.mids)
        }
        "subscriptionResponse" => Message::Subscription(serde_json::from_str(data).map_err(error)?),
        "pong" => Message::Pong,
        "error" => return Err(AdapterError::Subscription(data.to_string())),
        c => return Err(error(format!("unknown Hyperliquid channel {c}"))),
    })
}
