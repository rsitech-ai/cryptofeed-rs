//! Binance Spot user-data stream migration scaffold.
//!
//! Binance retired the REST listen-key bootstrap used by the original
//! implementation. The session therefore fails closed on connection until the
//! authenticated WebSocket API subscription flow is implemented end to end.
//! Legacy payload decoding remains fixture-only and cannot initiate wire I/O.

use std::collections::HashMap;

use marketfeed_adapter_api::{SessionAction, SessionInput, StopReason};
use marketfeed_model::{
    Fixed, InstrumentId, Price, Quantity, SessionId, SystemEvent, TimestampNs, VenueId,
};
use serde_json::Value;

use crate::PrivateError;
use crate::account::{AccountEvent, Balance, BalanceDelta, Fill, OrderUpdate};
use crate::session::{PrivateActionBuffer, PrivateSessionMachine};

/// Canonical Binance Spot public VenueId (matches adapters/binance).
pub const BINANCE_SPOT_VENUE_ID: VenueId = VenueId(2);

#[derive(Debug, Clone)]
pub struct BinanceSpotUserDataConfig {
    pub session: SessionId,
    pub venue: VenueId,
    /// Native symbol → instrument id for private account events.
    pub instrument_ids: HashMap<String, InstrumentId>,
}

impl Default for BinanceSpotUserDataConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTCUSDT".into(), InstrumentId(1));
        Self {
            session: SessionId(1),
            venue: BINANCE_SPOT_VENUE_ID,
            instrument_ids,
        }
    }
}

/// Binance Spot private-event decoder and fail-closed migration boundary.
#[derive(Debug)]
pub struct BinanceSpotUserDataSession {
    cfg: BinanceSpotUserDataConfig,
    live: bool,
}

impl BinanceSpotUserDataSession {
    pub fn new(cfg: BinanceSpotUserDataConfig) -> Self {
        Self { cfg, live: false }
    }

    pub fn is_live(&self) -> bool {
        self.live
    }

    fn on_text(
        &mut self,
        bytes: &[u8],
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let v: Value =
            serde_json::from_slice(bytes).map_err(|e| PrivateError::Parse(e.to_string()))?;
        let payload = v.get("event").unwrap_or(&v);
        let event = payload.get("e").and_then(|x| x.as_str()).unwrap_or("");
        match event {
            "outboundAccountPosition" => self.decode_outbound_account(payload, output),
            "balanceUpdate" => self.decode_balance_update(payload, output),
            "executionReport" => self.decode_execution_report(payload, output),
            "" => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "private frame missing e".into(),
                }));
                Ok(())
            }
            other => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("private event {other}"),
                }));
                Ok(())
            }
        }
    }

    fn decode_outbound_account(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let ts = event_ts_ns(v);
        let balances = v
            .get("B")
            .and_then(|x| x.as_array())
            .ok_or_else(|| PrivateError::Parse("outboundAccountPosition missing B".into()))?;
        for row in balances {
            let asset = row
                .get("a")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PrivateError::Parse("balance missing a".into()))?;
            let free = parse_qty(row.get("f").and_then(|x| x.as_str()).unwrap_or("0"))?;
            let locked = parse_qty(row.get("l").and_then(|x| x.as_str()).unwrap_or("0"))?;
            output.push_account(AccountEvent::Balance(Balance {
                venue: self.cfg.venue,
                asset: asset.into(),
                free,
                locked,
                exchange_ts: ts,
            }));
        }
        Ok(())
    }

    fn decode_balance_update(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let asset = v
            .get("a")
            .and_then(|x| x.as_str())
            .ok_or_else(|| PrivateError::Parse("balanceUpdate missing a".into()))?;
        let delta = parse_qty(
            v.get("d")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PrivateError::Parse("balanceUpdate missing d".into()))?,
        )?;
        output.push_account(AccountEvent::BalanceDelta(BalanceDelta {
            venue: self.cfg.venue,
            asset: asset.into(),
            delta,
            exchange_ts: clear_ts_ns(v).or_else(|| event_ts_ns(v)),
        }));
        Ok(())
    }

    fn decode_execution_report(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let symbol = v
            .get("s")
            .and_then(|x| x.as_str())
            .ok_or_else(|| PrivateError::Parse("executionReport missing s".into()))?;
        let instrument = self
            .cfg
            .instrument_ids
            .get(symbol)
            .copied()
            .unwrap_or(InstrumentId(0));
        let ts = event_ts_ns(v);
        let exec_type = v.get("x").and_then(|x| x.as_str()).map(str::to_string);
        let status = v.get("X").and_then(|x| x.as_str()).map(str::to_string);
        let price = optional_price(v.get("p").and_then(|x| x.as_str()))?;
        let qty = optional_qty(v.get("q").and_then(|x| x.as_str()))?;
        let filled = optional_qty(v.get("z").and_then(|x| x.as_str()))?;
        output.push_account(AccountEvent::Order(OrderUpdate {
            venue: self.cfg.venue,
            instrument,
            client_order_id: v.get("c").and_then(|x| x.as_str()).map(str::to_string),
            exchange_order_id: v.get("i").map(|x| match x {
                Value::Number(n) => n.to_string(),
                Value::String(s) => s.clone(),
                _ => x.to_string(),
            }),
            execution_type: exec_type.clone(),
            status,
            price,
            quantity: qty,
            filled_quantity: filled,
            exchange_ts: ts,
        }));
        if exec_type.as_deref() == Some("TRADE") {
            let last_price = parse_price(
                v.get("L")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| PrivateError::Parse("TRADE missing L".into()))?,
            )?;
            let last_qty = parse_qty(
                v.get("l")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| PrivateError::Parse("TRADE missing l".into()))?,
            )?;
            let fee = optional_qty(v.get("n").and_then(|x| x.as_str()))?;
            output.push_account(AccountEvent::Fill(Fill {
                venue: self.cfg.venue,
                instrument,
                price: last_price,
                quantity: last_qty,
                fee,
                exchange_order_id: v.get("i").map(|x| match x {
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    _ => x.to_string(),
                }),
                trade_id: v.get("t").map(|x| match x {
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    _ => x.to_string(),
                }),
                exchange_ts: ts,
            }));
        }
        Ok(())
    }
}

impl PrivateSessionMachine for BinanceSpotUserDataSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        match input {
            SessionInput::Connected { .. } => {
                self.live = false;
                output.push_session(SessionAction::StopSession(StopReason::FatalProtocol));
                Err(PrivateError::NotImplemented)
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                output.push_session(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "disconnected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::TextFrame { bytes, .. } => self.on_text(bytes, output),
            SessionInput::HttpResponse { .. } | SessionInput::Timer { .. } => {
                Err(PrivateError::NotImplemented)
            }
            SessionInput::Control { command } => {
                use marketfeed_adapter_api::SessionCommand;
                if matches!(command, SessionCommand::Stop) {
                    output.push_session(SessionAction::StopSession(StopReason::Control));
                }
                Ok(())
            }
            SessionInput::BinaryFrame { .. } | SessionInput::Pong { .. } => Ok(()),
        }
    }
}

fn event_ts_ns(v: &Value) -> Option<TimestampNs> {
    v.get("E")
        .and_then(|x| x.as_i64())
        .map(|ms| TimestampNs(ms.saturating_mul(1_000_000)))
}

fn clear_ts_ns(v: &Value) -> Option<TimestampNs> {
    v.get("T")
        .and_then(|x| x.as_i64())
        .map(|ms| TimestampNs(ms.saturating_mul(1_000_000)))
}

fn parse_fixed(s: &str) -> Result<Fixed, PrivateError> {
    Fixed::parse_str(s).map_err(|e| PrivateError::Parse(e.to_string()))
}

fn parse_qty(s: &str) -> Result<Quantity, PrivateError> {
    Ok(Quantity(parse_fixed(s)?))
}

fn parse_price(s: &str) -> Result<Price, PrivateError> {
    Ok(Price(parse_fixed(s)?))
}

fn optional_qty(s: Option<&str>) -> Result<Option<Quantity>, PrivateError> {
    match s {
        None | Some("") => Ok(None),
        Some(v) => Ok(Some(parse_qty(v)?)),
    }
}

fn optional_price(s: Option<&str>) -> Result<Option<Price>, PrivateError> {
    match s {
        None | Some("") => Ok(None),
        Some(v) => Ok(Some(parse_price(v)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_rejects_retired_listen_key_protocol() {
        let mut s = BinanceSpotUserDataSession::new(BinanceSpotUserDataConfig::default());
        let mut out = PrivateActionBuffer::new();
        let result = s.on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut out,
        );

        assert_eq!(result, Err(PrivateError::NotImplemented));
        assert!(out.as_slice().iter().all(|action| !matches!(
            action,
            crate::session::PrivateSessionAction::Session(SessionAction::RequestHttp(_))
        )));
    }
}
