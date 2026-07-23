//! Shared `EventEnvelope` ↔ JSON codec aligned with `proto/marketfeed/v1/market_event.proto`.
//!
//! Used by:
//! - **MFNE-JSON1** ([`crate::NormalizedEventWriter`]): newline-delimited JSON
//! - **MFPE-JSON1** (`marketfeed-sinks::ProtobufFileSink`): length-prefixed JSON
//!
//! Field names match the protobuf JSON mapping (snake_case message fields).
//! Rust `MarketEvent` / `EventEnvelope` remain the SoT; this is a persistence view.

use marketfeed_model::{AggressorSide, BookOperation, BookSide, EventEnvelope, Fixed, MarketEvent};
use serde_json::{Value, json};

use crate::format::RecordingError;

/// Encode [`EventEnvelope`] using proto3 JSON field names from `market_event.proto`.
pub fn event_envelope_json(env: &EventEnvelope) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "schema_version".into(),
        json!(u32::from(env.schema_version)),
    );
    obj.insert("venue_id".into(), json!(u32::from(env.venue.0)));
    if let Some(inst) = env.instrument {
        obj.insert("instrument_id".into(), json!(inst.0));
    }
    obj.insert("connection_id".into(), json!(env.connection.0));
    obj.insert("session_id".into(), json!(env.session.0));
    obj.insert("frame_seq".into(), json!(env.frame_seq));
    obj.insert("event_index".into(), json!(u32::from(env.event_index)));
    if let Some(ts) = env.exchange_ts {
        obj.insert("exchange_ts".into(), json!({ "ns": ts.0 }));
    }
    obj.insert("receive_ts".into(), json!({ "ns": env.receive_ts.0 }));
    if let Some(seq) = env.source_sequence {
        obj.insert(
            "source_sequence".into(),
            json!({ "first": seq.first, "last": seq.last }),
        );
    }
    obj.insert("flags".into(), json!(env.flags.0));
    obj.insert("payload".into(), market_event_json(&env.payload));
    Value::Object(obj)
}

fn market_event_json(ev: &MarketEvent) -> Value {
    match ev {
        MarketEvent::Trade(t) => json!({
            "trade": {
                "price": { "value": fixed_json(t.price.0) },
                "quantity": { "value": fixed_json(t.quantity.0) },
                "aggressor": aggressor_json(t.aggressor),
                "trade_id": t.trade_id.as_ref().map(|s| &s.0),
            }
        }),
        MarketEvent::Quote(q) => {
            let mut quote = serde_json::Map::new();
            quote.insert(
                "bid_price".into(),
                json!({ "value": fixed_json(q.bid_price.0) }),
            );
            if let Some(qty) = q.bid_quantity {
                quote.insert("bid_quantity".into(), json!({ "value": fixed_json(qty.0) }));
            }
            quote.insert(
                "ask_price".into(),
                json!({ "value": fixed_json(q.ask_price.0) }),
            );
            if let Some(qty) = q.ask_quantity {
                quote.insert("ask_quantity".into(), json!({ "value": fixed_json(qty.0) }));
            }
            json!({ "quote": quote })
        }
        MarketEvent::BookSnapshot(s) => {
            let mut snap = serde_json::Map::new();
            snap.insert(
                "bids".into(),
                Value::Array(
                    s.bids
                        .iter()
                        .map(|l| {
                            json!({
                                "price": { "value": fixed_json(l.price.0) },
                                "quantity": { "value": fixed_json(l.quantity.0) },
                            })
                        })
                        .collect(),
                ),
            );
            snap.insert(
                "asks".into(),
                Value::Array(
                    s.asks
                        .iter()
                        .map(|l| {
                            json!({
                                "price": { "value": fixed_json(l.price.0) },
                                "quantity": { "value": fixed_json(l.quantity.0) },
                            })
                        })
                        .collect(),
                ),
            );
            if let Some(d) = s.depth {
                snap.insert("depth".into(), json!(d));
            }
            if let Some(c) = &s.checksum {
                snap.insert("checksum".into(), json!(c.0));
            }
            json!({ "book_snapshot": snap })
        }
        MarketEvent::BookDelta(d) => {
            let changes: Vec<Value> = d
                .changes
                .iter()
                .map(|c| {
                    let mut ch = serde_json::Map::new();
                    ch.insert("side".into(), json!(book_side_json(c.side)));
                    ch.insert("operation".into(), json!(book_op_json(c.operation)));
                    ch.insert("price".into(), json!({ "value": fixed_json(c.price.0) }));
                    if let Some(qty) = c.quantity {
                        ch.insert("quantity".into(), json!({ "value": fixed_json(qty.0) }));
                    }
                    Value::Object(ch)
                })
                .collect();
            let mut delta = serde_json::Map::new();
            delta.insert("changes".into(), Value::Array(changes));
            if let Some(c) = &d.checksum {
                delta.insert("checksum".into(), json!(c.0));
            }
            json!({ "book_delta": delta })
        }
        MarketEvent::Candle(c) => json!({
            "candle": {
                "open": { "value": fixed_json(c.open.0) },
                "high": { "value": fixed_json(c.high.0) },
                "low": { "value": fixed_json(c.low.0) },
                "close": { "value": fixed_json(c.close.0) },
                "volume": { "value": fixed_json(c.volume.0) },
                "interval_ns": c.interval_ns,
                "start_ts": { "ns": c.start_ts.0 },
            }
        }),
        MarketEvent::MarkPrice(p) => {
            json!({ "mark_price": { "price": { "value": fixed_json(p.price.0) } } })
        }
        MarketEvent::IndexPrice(p) => {
            json!({ "index_price": { "price": { "value": fixed_json(p.price.0) } } })
        }
        MarketEvent::Funding(f) => {
            let mut funding = serde_json::Map::new();
            funding.insert("rate".into(), json!({ "value": fixed_json(f.rate.0) }));
            if let Some(ts) = f.next_funding_ts {
                funding.insert("next_funding_ts".into(), json!({ "ns": ts.0 }));
            }
            json!({ "funding": funding })
        }
        MarketEvent::OpenInterest(o) => {
            json!({ "open_interest": { "quantity": { "value": fixed_json(o.quantity.0) } } })
        }
        MarketEvent::Liquidation(l) => json!({
            "liquidation": {
                "price": { "value": fixed_json(l.price.0) },
                "quantity": { "value": fixed_json(l.quantity.0) },
                "side": aggressor_json(l.side),
            }
        }),
        MarketEvent::Statistics24h(s) => {
            let mut st = serde_json::Map::new();
            if let Some(p) = s.open {
                st.insert("open".into(), json!({ "value": fixed_json(p.0) }));
            }
            if let Some(p) = s.high {
                st.insert("high".into(), json!({ "value": fixed_json(p.0) }));
            }
            if let Some(p) = s.low {
                st.insert("low".into(), json!({ "value": fixed_json(p.0) }));
            }
            if let Some(p) = s.close {
                st.insert("close".into(), json!({ "value": fixed_json(p.0) }));
            }
            if let Some(q) = s.volume {
                st.insert("volume".into(), json!({ "value": fixed_json(q.0) }));
            }
            if let Some(q) = s.quote_volume {
                st.insert("quote_volume".into(), json!({ "value": fixed_json(q.0) }));
            }
            json!({ "statistics_24h": st })
        }
        MarketEvent::InstrumentUpdate(u) => {
            json!({ "instrument_update": { "status": format!("{:?}", u.status) } })
        }
        MarketEvent::VenueStatus(v) => {
            json!({ "venue_status": { "message": v.message } })
        }
    }
}

fn fixed_json(f: Fixed) -> Value {
    let lo = f.coefficient as i64;
    let hi = (f.coefficient >> 64) as i64;
    json!({
        "coefficient_lo": lo,
        "coefficient_hi": hi,
        "scale": u32::from(f.scale),
    })
}

fn aggressor_json(side: AggressorSide) -> &'static str {
    match side {
        AggressorSide::Buy => "AGGRESSOR_SIDE_BUY",
        AggressorSide::Sell => "AGGRESSOR_SIDE_SELL",
        AggressorSide::Unknown => "AGGRESSOR_SIDE_UNKNOWN",
    }
}

fn book_side_json(side: BookSide) -> &'static str {
    match side {
        BookSide::Bid => "BOOK_SIDE_BID",
        BookSide::Ask => "BOOK_SIDE_ASK",
    }
}

fn book_op_json(op: BookOperation) -> &'static str {
    match op {
        BookOperation::Upsert => "BOOK_OPERATION_UPSERT",
        BookOperation::Delete => "BOOK_OPERATION_DELETE",
    }
}

/// Read all MFNE-JSON1 records (one JSON object per non-empty line).
pub fn read_normalized_jsonl(bytes: &[u8]) -> Result<Vec<Value>, RecordingError> {
    let text = std::str::from_utf8(bytes).map_err(|e| RecordingError::Io(e.to_string()))?;
    let mut out = Vec::new();
    for (lineno, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(line)
            .map_err(|e| RecordingError::Io(format!("MFNE-JSON1 line {}: {e}", lineno + 1)))?;
        out.push(v);
    }
    Ok(out)
}

/// Read all length-prefixed MFPE-JSON1 records from a byte slice (tests / inspect).
pub fn read_length_prefixed_json(bytes: &[u8]) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes.len() - i < 4 {
            return Err("truncated length prefix".into());
        }
        let len = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if bytes.len() - i < len {
            return Err("truncated record body".into());
        }
        let v: Value = serde_json::from_slice(&bytes[i..i + len]).map_err(|e| e.to_string())?;
        out.push(v);
        i += len;
    }
    Ok(out)
}
