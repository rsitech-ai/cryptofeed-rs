//! OKX private account stream — fixture SM + optional live WS auth (`live` feature).
//!
//! Flow (no order placement):
//! 1. On `Connected` → redacted/non-recorded login (fixture or HMAC-signed live payload)
//! 2. On login success → subscribe `account` + `orders` via `SendText` + `MarkLive`
//! 3. Text frames → balances / order updates / fills
//!
//! Fixture `login_payload` is a placeholder. Live path builds a fresh HMAC payload
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

/// Canonical OKX Spot public VenueId (matches adapters/okx).
pub const OKX_SPOT_VENUE_ID: VenueId = VenueId(4);

/// OKX v5 private WebSocket URL (mainnet).
pub const OKX_PRIVATE_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/private";

/// Default fixture login body (no real signature).
pub const FIXTURE_LOGIN_PAYLOAD: &str = r#"{"op":"login","args":[{"apiKey":"fixture-api-key","passphrase":"fixture-pass","timestamp":"1700000000","sign":"fixture-sign"}]}"#;

#[derive(Debug, Clone)]
pub struct OkxPrivateConfig {
    pub session: SessionId,
    pub venue: VenueId,
    /// Pre-built login JSON for a sensitive text send (fixture placeholder or live HMAC body).
    /// Live bodies contain secrets — never log or record.
    pub login_payload: String,
    /// Private WS URL (live connect).
    pub ws_url: String,
    /// Native instId → instrument id for `orders.instId`.
    pub instrument_ids: HashMap<String, InstrumentId>,
}

impl Default for OkxPrivateConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTC-USDT".into(), InstrumentId(1));
        Self {
            session: SessionId(1),
            venue: OKX_SPOT_VENUE_ID,
            login_payload: FIXTURE_LOGIN_PAYLOAD.into(),
            ws_url: OKX_PRIVATE_WS_URL.into(),
            instrument_ids,
        }
    }
}

/// Fixture-driven OKX private account state machine.
#[derive(Debug)]
pub struct OkxPrivateSession {
    cfg: OkxPrivateConfig,
    live: bool,
    authed: bool,
}

impl OkxPrivateSession {
    pub fn new(cfg: OkxPrivateConfig) -> Self {
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

    fn send_login(&self, output: &mut PrivateActionBuffer) {
        output.push_session(SessionAction::SendSensitiveText(SensitiveBytes::new(
            Bytes::from(self.cfg.login_payload.clone()),
        )));
    }

    fn send_subscribe(&self, output: &mut PrivateActionBuffer) {
        // ponytail: Spot orders only; add SWAP/instType when multi-segment private lands.
        let body = r#"{"op":"subscribe","args":[{"channel":"account"},{"channel":"orders","instType":"SPOT"}]}"#;
        output.push_session(SessionAction::SendText(Bytes::from_static(body.as_bytes())));
    }

    fn on_text(
        &mut self,
        bytes: &[u8],
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let v: Value =
            serde_json::from_slice(bytes).map_err(|e| PrivateError::Parse(e.to_string()))?;

        if let Some(event) = v.get("event").and_then(|x| x.as_str()) {
            return self.on_event(event, &v, output);
        }

        let channel = v
            .pointer("/arg/channel")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        match channel {
            "account" => self.decode_account(&v, output),
            "orders" => self.decode_orders(&v, output),
            "" => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "okx private frame missing channel".into(),
                }));
                Ok(())
            }
            other => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("okx private channel {other}"),
                }));
                Ok(())
            }
        }
    }

    fn on_event(
        &mut self,
        event: &str,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        match event {
            "login" => {
                let code = v.get("code").and_then(|x| x.as_str()).unwrap_or("1");
                if code == "0" {
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
                    output.push_session(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("okx login code {code}"),
                    }));
                    output.push_session(SessionAction::Reconnect(ReconnectReason::Protocol));
                }
                Ok(())
            }
            "subscribe" | "channel-conn-count" => Ok(()),
            "error" => {
                let msg = v
                    .get("msg")
                    .and_then(|x| x.as_str())
                    .unwrap_or("okx private error");
                output.push_session(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: msg.into(),
                }));
                output.push_session(SessionAction::Reconnect(ReconnectReason::Protocol));
                Ok(())
            }
            other => {
                output.push_session(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("okx private event {other}"),
                }));
                Ok(())
            }
        }
    }

    fn decode_account(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let rows = v
            .get("data")
            .and_then(|x| x.as_array())
            .ok_or_else(|| PrivateError::Parse("account missing data".into()))?;
        for row in rows {
            let ts = ms_ts(row.get("uTime").and_then(|x| x.as_str()));
            let details = row
                .get("details")
                .and_then(|x| x.as_array())
                .ok_or_else(|| PrivateError::Parse("account missing details".into()))?;
            for d in details {
                let asset = d
                    .get("ccy")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| PrivateError::Parse("account detail missing ccy".into()))?;
                let free = parse_qty(d.get("availBal").and_then(|x| x.as_str()).unwrap_or("0"))?;
                let locked = parse_qty(d.get("frozenBal").and_then(|x| x.as_str()).unwrap_or("0"))?;
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

    fn decode_orders(
        &self,
        v: &Value,
        output: &mut PrivateActionBuffer,
    ) -> Result<(), PrivateError> {
        let rows = v
            .get("data")
            .and_then(|x| x.as_array())
            .ok_or_else(|| PrivateError::Parse("orders missing data".into()))?;
        for row in rows {
            let inst = row
                .get("instId")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PrivateError::Parse("orders missing instId".into()))?;
            let instrument = self
                .cfg
                .instrument_ids
                .get(inst)
                .copied()
                .unwrap_or(InstrumentId(0));
            let ts = ms_ts(row.get("uTime").and_then(|x| x.as_str()));
            let status = row
                .get("state")
                .and_then(|x| x.as_str())
                .map(str::to_string);
            let fill_sz = row.get("fillSz").and_then(|x| x.as_str()).unwrap_or("0");
            let has_fill = fill_sz != "0" && !fill_sz.is_empty();
            output.push_account(AccountEvent::Order(OrderUpdate {
                venue: self.cfg.venue,
                instrument,
                client_order_id: row
                    .get("clOrdId")
                    .and_then(|x| x.as_str())
                    .map(str::to_string),
                exchange_order_id: row
                    .get("ordId")
                    .and_then(|x| x.as_str())
                    .map(str::to_string),
                execution_type: if has_fill {
                    Some("TRADE".into())
                } else {
                    status.clone()
                },
                status,
                price: optional_price(row.get("px").and_then(|x| x.as_str()))?,
                quantity: optional_qty(row.get("sz").and_then(|x| x.as_str()))?,
                filled_quantity: optional_qty(row.get("accFillSz").and_then(|x| x.as_str()))?,
                exchange_ts: ts,
            }));
            if has_fill {
                let fill_px = row
                    .get("fillPx")
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| PrivateError::Parse("fill missing fillPx".into()))?;
                output.push_account(AccountEvent::Fill(Fill {
                    venue: self.cfg.venue,
                    instrument,
                    price: parse_price(fill_px)?,
                    quantity: parse_qty(fill_sz)?,
                    fee: optional_qty(row.get("fee").and_then(|x| x.as_str()))?,
                    exchange_order_id: row
                        .get("ordId")
                        .and_then(|x| x.as_str())
                        .map(str::to_string),
                    trade_id: row
                        .get("tradeId")
                        .and_then(|x| x.as_str())
                        .map(str::to_string),
                    exchange_ts: ts,
                }));
            }
        }
        Ok(())
    }
}

impl PrivateSessionMachine for OkxPrivateSession {
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
                self.send_login(output);
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

fn ms_ts(s: Option<&str>) -> Option<TimestampNs> {
    s.and_then(|x| x.parse::<i64>().ok())
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
    fn connect_emits_sensitive_login_text() {
        let mut s = OkxPrivateSession::new(OkxPrivateConfig::default());
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
        let text = text.expect("sensitive login text");
        assert!(text.contains(r#""op":"login""#));
        assert!(text.contains("fixture-api-key"));
    }
}
