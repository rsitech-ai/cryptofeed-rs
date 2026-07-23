//! Hand-written protobuf3 wire encoder for `marketfeed.v1.EventEnvelope`.
//!
//! # ponytail
//! No `prost` / `prost-build` — tags match [`proto/marketfeed/v1/market_event.proto`].
//! Ceiling = hand-maintained; upgrade = feature-gated prost codegen when a
//! consumer needs generated stubs.

use marketfeed_model::{
    AggressorSide, BookChange, BookLevel, BookOperation, BookSide, EventEnvelope, Fixed,
    MarketEvent, Price, Quantity, Rate, SequenceRange, TimestampNs,
};

const WIRE_VARINT: u32 = 0;
const WIRE_LEN: u32 = 2;

/// Encode [`EventEnvelope`] as a protobuf3 message body (no length prefix).
pub fn encode_event_envelope(env: &EventEnvelope) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    write_event_envelope(&mut out, env);
    out
}

fn write_event_envelope(out: &mut Vec<u8>, env: &EventEnvelope) {
    write_u32(out, 1, u32::from(env.schema_version));
    write_u32(out, 2, u32::from(env.venue.0));
    if let Some(inst) = env.instrument {
        write_u32(out, 3, inst.0);
    }
    write_u64(out, 4, env.connection.0);
    write_u64(out, 5, env.session.0);
    write_u64(out, 6, env.frame_seq);
    write_u32(out, 7, u32::from(env.event_index));
    if let Some(ts) = env.exchange_ts {
        write_msg(out, 8, &encode_timestamp_ns(ts));
    }
    write_msg(out, 9, &encode_timestamp_ns(env.receive_ts));
    if let Some(seq) = env.source_sequence {
        write_msg(out, 10, &encode_sequence_range(seq));
    }
    write_u32(out, 11, env.flags.0);
    write_msg(out, 12, &encode_market_event(&env.payload));
}

fn encode_market_event(ev: &MarketEvent) -> Vec<u8> {
    let mut out = Vec::new();
    match ev {
        MarketEvent::Trade(t) => write_msg(&mut out, 1, &encode_trade(t)),
        MarketEvent::Quote(q) => write_msg(&mut out, 2, &encode_quote(q)),
        MarketEvent::BookSnapshot(s) => write_msg(&mut out, 3, &encode_book_snapshot(s)),
        MarketEvent::BookDelta(d) => write_msg(&mut out, 4, &encode_book_delta(d)),
        MarketEvent::Candle(c) => write_msg(&mut out, 5, &encode_candle(c)),
        MarketEvent::MarkPrice(p) => write_msg(&mut out, 6, &encode_price_point(p)),
        MarketEvent::IndexPrice(p) => write_msg(&mut out, 7, &encode_price_point(p)),
        MarketEvent::Funding(f) => write_msg(&mut out, 8, &encode_funding(f)),
        MarketEvent::OpenInterest(o) => write_msg(&mut out, 9, &encode_open_interest(o)),
        MarketEvent::Liquidation(l) => write_msg(&mut out, 10, &encode_liquidation(l)),
        MarketEvent::Statistics24h(s) => write_msg(&mut out, 11, &encode_statistics_24h(s)),
        MarketEvent::InstrumentUpdate(u) => {
            write_msg(&mut out, 12, &encode_instrument_update(u));
        }
        MarketEvent::VenueStatus(v) => write_msg(&mut out, 13, &encode_venue_status(v)),
    }
    out
}

fn encode_trade(t: &marketfeed_model::Trade) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_price(t.price));
    write_msg(&mut out, 2, &encode_quantity(t.quantity));
    write_enum(&mut out, 3, aggressor_num(t.aggressor));
    if let Some(id) = &t.trade_id {
        write_string(&mut out, 4, &id.0);
    }
    out
}

fn encode_quote(q: &marketfeed_model::Quote) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_price(q.bid_price));
    if let Some(qty) = q.bid_quantity {
        write_msg(&mut out, 2, &encode_quantity(qty));
    }
    write_msg(&mut out, 3, &encode_price(q.ask_price));
    if let Some(qty) = q.ask_quantity {
        write_msg(&mut out, 4, &encode_quantity(qty));
    }
    out
}

fn encode_book_level(l: &BookLevel) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_price(l.price));
    write_msg(&mut out, 2, &encode_quantity(l.quantity));
    out
}

fn encode_book_change(c: &BookChange) -> Vec<u8> {
    let mut out = Vec::new();
    write_enum(&mut out, 1, book_side_num(c.side));
    write_enum(&mut out, 2, book_op_num(c.operation));
    write_msg(&mut out, 3, &encode_price(c.price));
    if let Some(qty) = c.quantity {
        write_msg(&mut out, 4, &encode_quantity(qty));
    }
    out
}

fn encode_book_snapshot(s: &marketfeed_model::BookSnapshot) -> Vec<u8> {
    let mut out = Vec::new();
    for l in &s.bids {
        write_msg(&mut out, 1, &encode_book_level(l));
    }
    for l in &s.asks {
        write_msg(&mut out, 2, &encode_book_level(l));
    }
    if let Some(d) = s.depth {
        write_u32(&mut out, 3, d);
    }
    if let Some(c) = &s.checksum {
        write_string(&mut out, 4, &c.0);
    }
    out
}

fn encode_book_delta(d: &marketfeed_model::BookDelta) -> Vec<u8> {
    let mut out = Vec::new();
    for c in &d.changes {
        write_msg(&mut out, 1, &encode_book_change(c));
    }
    if let Some(c) = &d.checksum {
        write_string(&mut out, 2, &c.0);
    }
    out
}

fn encode_candle(c: &marketfeed_model::Candle) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_price(c.open));
    write_msg(&mut out, 2, &encode_price(c.high));
    write_msg(&mut out, 3, &encode_price(c.low));
    write_msg(&mut out, 4, &encode_price(c.close));
    write_msg(&mut out, 5, &encode_quantity(c.volume));
    write_i64(&mut out, 6, c.interval_ns);
    write_msg(&mut out, 7, &encode_timestamp_ns(c.start_ts));
    out
}

fn encode_price_point(p: &marketfeed_model::PricePoint) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_price(p.price));
    out
}

fn encode_funding(f: &marketfeed_model::Funding) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_rate(f.rate));
    if let Some(ts) = f.next_funding_ts {
        write_msg(&mut out, 2, &encode_timestamp_ns(ts));
    }
    out
}

fn encode_open_interest(o: &marketfeed_model::OpenInterest) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_quantity(o.quantity));
    out
}

fn encode_liquidation(l: &marketfeed_model::Liquidation) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_price(l.price));
    write_msg(&mut out, 2, &encode_quantity(l.quantity));
    write_enum(&mut out, 3, aggressor_num(l.side));
    out
}

fn encode_statistics_24h(s: &marketfeed_model::Statistics24h) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(p) = s.open {
        write_msg(&mut out, 1, &encode_price(p));
    }
    if let Some(p) = s.high {
        write_msg(&mut out, 2, &encode_price(p));
    }
    if let Some(p) = s.low {
        write_msg(&mut out, 3, &encode_price(p));
    }
    if let Some(p) = s.close {
        write_msg(&mut out, 4, &encode_price(p));
    }
    if let Some(q) = s.volume {
        write_msg(&mut out, 5, &encode_quantity(q));
    }
    if let Some(q) = s.quote_volume {
        write_msg(&mut out, 6, &encode_quantity(q));
    }
    out
}

fn encode_instrument_update(u: &marketfeed_model::InstrumentUpdate) -> Vec<u8> {
    let mut out = Vec::new();
    // Matches JSON sink: Debug form of InstrumentStatus.
    write_string(&mut out, 1, &format!("{:?}", u.status));
    out
}

fn encode_venue_status(v: &marketfeed_model::VenueStatus) -> Vec<u8> {
    let mut out = Vec::new();
    write_string(&mut out, 1, &v.message);
    out
}

fn encode_price(p: Price) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_fixed(p.0));
    out
}

fn encode_quantity(q: Quantity) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_fixed(q.0));
    out
}

fn encode_rate(r: Rate) -> Vec<u8> {
    let mut out = Vec::new();
    write_msg(&mut out, 1, &encode_fixed(r.0));
    out
}

fn encode_fixed(f: Fixed) -> Vec<u8> {
    let mut out = Vec::new();
    let lo = f.coefficient as i64;
    let hi = (f.coefficient >> 64) as i64;
    write_i64(&mut out, 1, lo);
    write_i64(&mut out, 2, hi);
    write_u32(&mut out, 3, u32::from(f.scale));
    out
}

fn encode_timestamp_ns(ts: TimestampNs) -> Vec<u8> {
    let mut out = Vec::new();
    write_i64(&mut out, 1, ts.0);
    out
}

fn encode_sequence_range(seq: SequenceRange) -> Vec<u8> {
    let mut out = Vec::new();
    write_u64(&mut out, 1, seq.first);
    write_u64(&mut out, 2, seq.last);
    out
}

fn aggressor_num(side: AggressorSide) -> i32 {
    match side {
        AggressorSide::Buy => 1,     // AGGRESSOR_SIDE_BUY
        AggressorSide::Sell => 2,    // AGGRESSOR_SIDE_SELL
        AggressorSide::Unknown => 3, // AGGRESSOR_SIDE_UNKNOWN
    }
}

fn book_side_num(side: BookSide) -> i32 {
    match side {
        BookSide::Bid => 1,
        BookSide::Ask => 2,
    }
}

fn book_op_num(op: BookOperation) -> i32 {
    match op {
        BookOperation::Upsert => 1,
        BookOperation::Delete => 2,
    }
}

fn write_key(out: &mut Vec<u8>, field: u32, wire: u32) {
    write_varint(out, u64::from((field << 3) | wire));
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn write_u32(out: &mut Vec<u8>, field: u32, v: u32) {
    if v == 0 {
        return;
    }
    write_key(out, field, WIRE_VARINT);
    write_varint(out, u64::from(v));
}

fn write_u64(out: &mut Vec<u8>, field: u32, v: u64) {
    if v == 0 {
        return;
    }
    write_key(out, field, WIRE_VARINT);
    write_varint(out, v);
}

fn write_i64(out: &mut Vec<u8>, field: u32, v: i64) {
    if v == 0 {
        return;
    }
    write_key(out, field, WIRE_VARINT);
    write_varint(out, v as u64);
}

fn write_enum(out: &mut Vec<u8>, field: u32, v: i32) {
    if v == 0 {
        return;
    }
    write_key(out, field, WIRE_VARINT);
    write_varint(out, v as u64);
}

fn write_string(out: &mut Vec<u8>, field: u32, s: &str) {
    if s.is_empty() {
        return;
    }
    write_bytes(out, field, s.as_bytes());
}

fn write_bytes(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    write_key(out, field, WIRE_LEN);
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn write_msg(out: &mut Vec<u8>, field: u32, msg: &[u8]) {
    // Nested messages are always written (proto3 presence for message fields).
    write_bytes(out, field, msg);
}

/// Split a length-prefixed MFPE-PB1 byte stream into record bodies.
pub fn read_length_prefixed_records(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
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
        out.push(bytes[i..i + len].to_vec());
        i += len;
    }
    Ok(out)
}

#[cfg(test)]
/// Minimal varint field scanner for tests (returns first matching field value bytes).
fn find_field(msg: &[u8], field: u32) -> Option<(u32, &[u8])> {
    let mut i = 0;
    while i < msg.len() {
        let (key, n) = read_varint(msg, i)?;
        i += n;
        let f = (key >> 3) as u32;
        let wire = (key & 7) as u32;
        let (val, n) = match wire {
            WIRE_VARINT => {
                let (v, n) = read_varint(msg, i)?;
                // Return empty slice; value carried via length encoding below.
                let _ = v;
                (&msg[i..i + n], n)
            }
            WIRE_LEN => {
                let (len, n) = read_varint(msg, i)?;
                i += n;
                let len = len as usize;
                if i + len > msg.len() {
                    return None;
                }
                let slice = &msg[i..i + len];
                if f == field {
                    return Some((wire, slice));
                }
                i += len;
                continue;
            }
            _ => return None,
        };
        if f == field {
            return Some((wire, val));
        }
        i += n;
    }
    None
}

#[cfg(test)]
fn read_varint(buf: &[u8], mut i: usize) -> Option<(u64, usize)> {
    let start = i;
    let mut result = 0u64;
    let mut shift = 0u32;
    while i < buf.len() {
        let b = buf[i];
        i += 1;
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((result, i - start));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use marketfeed_model::{
        ConnectionId, EventFlags, Price, Quantity, SessionId, TimestampNs, Trade, VenueId,
    };

    use super::*;

    #[test]
    fn encodes_trade_envelope_tags() {
        let env = EventEnvelope {
            schema_version: 1,
            venue: VenueId(2),
            instrument: Some(marketfeed_model::InstrumentId(7)),
            connection: ConnectionId(3),
            session: SessionId(4),
            frame_seq: 9,
            event_index: 0,
            exchange_ts: Some(TimestampNs(1_000)),
            receive_ts: TimestampNs(1_100),
            source_sequence: None,
            flags: EventFlags(0),
            payload: MarketEvent::Trade(Trade {
                price: Price(Fixed::new(100_00, 2)),
                quantity: Quantity(Fixed::new(1_5, 1)),
                aggressor: AggressorSide::Buy,
                trade_id: Some(marketfeed_model::SourceId("t1".into())),
            }),
        };
        let bytes = encode_event_envelope(&env);
        // venue_id field 2 = 2
        let (_, venue) = find_field(&bytes, 2).expect("venue_id");
        assert_eq!(venue, &[2]); // varint bytes for value 2 (single byte)
        // payload field 12 length-delimited
        let (_, payload) = find_field(&bytes, 12).expect("payload");
        let (_, trade) = find_field(payload, 1).expect("trade oneof");
        let (_, aggressor) = find_field(trade, 3).expect("aggressor");
        assert_eq!(aggressor, &[1]); // BUY
        let (_, trade_id) = find_field(trade, 4).expect("trade_id");
        assert_eq!(trade_id, b"t1");
    }

    #[test]
    fn fixed_skips_zero_limbs() {
        let f = Fixed::new(100, 2);
        let bytes = encode_fixed(f);
        // coefficient_lo=100, hi omitted (0), scale=2
        let (_, lo) = find_field(&bytes, 1).expect("lo");
        assert_eq!(lo, &[100]);
        assert!(find_field(&bytes, 2).is_none());
        let (_, scale) = find_field(&bytes, 3).expect("scale");
        assert_eq!(scale, &[2]);
    }
}
