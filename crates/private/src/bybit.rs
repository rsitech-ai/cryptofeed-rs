//! Bybit private account stream — fixture SM + optional live WS auth (`live` feature).
//!
//! Flow (no order placement):
//! 1. On `Connected` → redacted/non-recorded auth (fixture or HMAC-signed live payload)
//! 2. On auth success → subscribe `wallet` / `order` / `execution` + `MarkLive`
//! 3. Text frames → balances / order updates / fills
//!
//! Fixture `auth_payload` is a placeholder. Live path builds a fresh HMAC payload
//! from env credentials (see `credentials::sign`).

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ReconnectReason, SensitiveBytes, SessionAction, SessionInput, StopReason,
};
use marketfeed_model::{
    Fixed, InstrumentId, Price, Quantity, SessionId, SystemEvent, TimestampNs, VenueId,
};
use serde_json::Value;

use crate::PrivateError;
use crate::account::{AccountEvent, Balance, Fill, OrderUpdate};
use crate::session::{PrivateActionBuffer, PrivateSessionMachine};

/// Canonical Bybit Spot public VenueId (matches adapters/bybit).
pub const BYBIT_SPOT_VENUE_ID: VenueId = VenueId(6);

/// Bybit v5 private WebSocket URL (mainnet).
pub const BYBIT_PRIVATE_WS_URL: &str = "wss://stream.bybit.com/v5/private";

/// Default fixture auth body (no real signature).
pub const FIXTURE_AUTH_PAYLOAD: &str =
    r#"{"op":"auth","args":["fixture-api-key",1700000000000,"fixture-sign"]}"#;

#[derive(Debug, Clone)]
pub struct BybitPrivateConfig {
    pub session: SessionId,
    pub venue: VenueId,
    /// Pre-built auth JSON for a sensitive text send (fixture placeholder or live HMAC body).
    /// Live bodies contain secrets — never log or record.
    pub auth_payload: String,
    /// Private WS URL (live connect).
    pub ws_url: String,
    /// Native symbol → instrument id for `order` / `execution`.
    pub instrument_ids: HashMap<String, InstrumentId>,
}

impl Default for BybitPrivateConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTCUSDT".into(), InstrumentId(1));
        Self {
            session: SessionId(1),
            venue: BYBIT_SPOT_VENUE_ID,
            auth_payload: FIXTURE_AUTH_PAYLOAD.into(),
            ws_url: BYBIT_PRIVATE_WS_URL.into(),
            instrument_ids,
        }
    }
}

/// Fixture-driven Bybit private account state machine.
#[derive(Debug)]
pub struct BybitPrivateSession {
    cfg: BybitPrivateConfig,
    live: bool,
    authed: bool,
}

impl BybitPrivateSession {
    pub fn new(cfg: BybitPrivateConfig) -> Self {
        Self {
            cfg,
            live: false,
            authed: false,
        }
    }

    pub fn is_live(&self) -> bool {
        self.live
    }

    pub fn is_authed(&self) -> bool {
        self.authed
    }

    /// Private WS URL for live connect.
    pub fn ws_url(&self) -> &str {
        &self.cfg.ws_url
    }

    fn send_auth(&self, output: &mut PrivateActionBuffer) {
        output.push_session(SessionAction::SendSensitiveText(SensitiveBytes::new(
            Bytes::from(self.cfg.auth_payload.clone()),
        )));
    }

    fn send_subscribe(&self, output: &mut PrivateActionBuffer) {
        let body = r#"{"op":"subscribe","args":["wallet","order","execution"]}"#;
        output.push_session(SessionAction::SendText(Bytes::from_static(body.as_bytes())));
    }

    fn on_text(
        &mut self,
        bytes: &[u8],
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let v: Value =
            serde_json::from_slice(bytes).map_err(|e| PrivateError::Parse(e.to_string()))?;

        if let Some(op) = v.get("op").and_then(|x| x.as_str()) {
            return self.on_op(op, &v, output);
        }

        let topic = v.get("topic").and_then(|x| x.as_str()).unwrap_or("");
        // Topics may be `wallet`, `order`, `order.spot`, `execution.linear`, …
        let kind = topic.split('.').next().unwrap_or(topic);
        match kind {
            "wallet" => self.decode_wallet(&v, output),
            "order" => self.decode_order(&v, output),
            "execution" => self.decode_execution(&v, output),
            "" => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "bybit private frame missing topic".into(),
                }));
                Ok(())
            }
            other => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("bybit private topic {other}"),
                }));
                Ok(())
            }
        }
    }

    fn on_op(
        &mut self,
        op: &str,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        match op {
            "auth" => {
                let ok = v.get("success").and_then(|x| x.as_bool()).unwrap_or(false);
                if ok {
                    self.authed = true;
                    self.live = true;
                    self.send_subscribe(output);
                    output.push_session(SessionAction::MarkLive);
                    output.push_session(SessionAction::EmitSystem(
                        SystemEvent::ConnectionStateChanged {
                            state: "authed".into(),
                        },
                    ));
                } else {
                    self.authed = false;
                    self.live = false;
                    let msg = v
                        .get("ret_msg")
                        .and_then(|x| x.as_str())
                        .unwrap_or("auth failed");
                    output.push_session(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("bybit auth: {msg}"),
                    }));
                    output.push_session(SessionAction::Reconnect(ReconnectReason::Protocol));
                }
                Ok(())
            }
            "subscribe" | "pong" => Ok(()),
            other => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("bybit private op {other}"),
                }));
                Ok(())
            }
        }
    }

    fn decode_wallet(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let ts = v
            .get("ts")
            .and_then(|x| x.as_i64())
            .map(|ms| TimestampNs(ms.saturating_mul(1_000_000)));
        let rows = v
            .get("data")
            .and_then(|x| x.as_array())
            .ok_or_else(|| PrivateError::Parse("wallet missing data".into()))?;
        for row in rows {
            let coins = row
                .get("coin")
                .and_then(|x| x.as_array())
                .ok_or_else(|| PrivateError::Parse("wallet missing coin".into()))?;
            for c in coins {
                let asset = c
                    .get("coin")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| PrivateError::Parse("wallet coin missing coin".into()))?;
                // Prefer availableToWithdraw as free; fall back to walletBalance.
                let free = parse_qty(
                    c.get("availableToWithdraw")
                        .and_then(|x| x.as_str())
                        .or_else(|| c.get("walletBalance").and_then(|x| x.as_str()))
                        .unwrap_or("0"),
                )?;
                let locked = parse_qty(c.get("locked").and_then(|x| x.as_str()).unwrap_or("0"))?;
                output.push_account(AccountEvent::Balance(Balance {
                    venue: self.cfg.venue,
                    asset: asset.into(),
                    free,
                    locked,
                    exchange_ts: ts,
                }));
            }
        }
        Ok(())
    }

    fn decode_order(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let rows = v
            .get("data")
            .and_then(|x| x.as_array())
            .ok_or_else(|| PrivateError::Parse("order missing data".into()))?;
        for row in rows {
            let symbol = row
                .get("symbol")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PrivateError::Parse("order missing symbol".into()))?;
            let instrument = self
                .cfg
                .instrument_ids
                .get(symbol)
                .copied()
                .unwrap_or(InstrumentId(0));
            let ts = ms_ts_field(row.get("updatedTime"));
            let status = row
                .get("orderStatus")
                .and_then(|x| x.as_str())
                .map(str::to_string);
            output.push_account(AccountEvent::Order(OrderUpdate {
                venue: self.cfg.venue,
                instrument,
                client_order_id: row
                    .get("orderLinkId")
                    .and_then(|x| x.as_str())
                    .map(str::to_string),
                exchange_order_id: row
                    .get("orderId")
                    .and_then(|x| x.as_str())
                    .map(str::to_string),
                execution_type: status.clone(),
                status,
                price: optional_price(row.get("price").and_then(|x| x.as_str()))?,
                quantity: optional_qty(row.get("qty").and_then(|x| x.as_str()))?,
                filled_quantity: optional_qty(row.get("cumExecQty").and_then(|x| x.as_str()))?,
                exchange_ts: ts,
            }));
        }
        Ok(())
    }

    fn decode_execution(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let rows = v
            .get("data")
            .and_then(|x| x.as_array())
            .ok_or_else(|| PrivateError::Parse("execution missing data".into()))?;
        for row in rows {
            let symbol = row
                .get("symbol")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PrivateError::Parse("execution missing symbol".into()))?;
            let instrument = self
                .cfg
                .instrument_ids
                .get(symbol)
                .copied()
                .unwrap_or(InstrumentId(0));
            let ts = ms_ts_field(row.get("execTime"));
            output.push_account(AccountEvent::Fill(Fill {
                venue: self.cfg.venue,
                instrument,
                price: parse_price(
                    row.get("execPrice")
                        .and_then(|x| x.as_str())
                        .ok_or_else(|| PrivateError::Parse("execution missing execPrice".into()))?,
                )?,
                quantity: parse_qty(
                    row.get("execQty")
                        .and_then(|x| x.as_str())
                        .ok_or_else(|| PrivateError::Parse("execution missing execQty".into()))?,
                )?,
                fee: optional_qty(row.get("execFee").and_then(|x| x.as_str()))?,
                exchange_order_id: row
                    .get("orderId")
                    .and_then(|x| x.as_str())
                    .map(str::to_string),
                trade_id: row
                    .get("execId")
                    .and_then(|x| x.as_str())
                    .map(str::to_string),
                exchange_ts: ts,
            }));
        }
        Ok(())
    }
}

impl PrivateSessionMachine for BybitPrivateSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.authed = false;
                output.push_session(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "connected".into(),
                    },
                ));
                self.send_auth(output);
                let _ = now;
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.authed = false;
                output.push_session(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "disconnected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::TextFrame { bytes, .. } => self.on_text(bytes, output),
            SessionInput::Control { command } => {
                use marketfeed_adapter_api::SessionCommand;
                if matches!(command, SessionCommand::Stop) {
                    output.push_session(SessionAction::StopSession(StopReason::Control));
                }
                Ok(())
            }
            SessionInput::HttpResponse { .. }
            | SessionInput::Timer { .. }
            | SessionInput::BinaryFrame { .. }
            | SessionInput::Pong { .. } => Ok(()),
        }
    }
}

fn ms_ts_field(v: Option<&Value>) -> Option<TimestampNs> {
    match v {
        Some(Value::String(s)) => s
            .parse::<i64>()
            .ok()
            .map(|ms| TimestampNs(ms.saturating_mul(1_000_000))),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(|ms| TimestampNs(ms.saturating_mul(1_000_000))),
        _ => None,
    }
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
    fn connect_emits_sensitive_auth_text() {
        let mut s = BybitPrivateSession::new(BybitPrivateConfig::default());
        let mut out = PrivateActionBuffer::new();
        s.on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut out,
        )
        .unwrap();
        let text = out.as_slice().iter().find_map(|a| match a {
            crate::session::PrivateSessionAction::Session(SessionAction::SendSensitiveText(b)) => {
                Some(std::str::from_utf8(b.expose()).unwrap().to_string())
            }
            _ => None,
        });
        let text = text.expect("sensitive auth text");
        assert!(text.contains(r#""op":"auth""#));
        assert!(text.contains("fixture-api-key"));
    }
}
