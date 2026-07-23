//! Account event types (separate from public [`marketfeed_model::MarketEvent`]).

use marketfeed_model::{
    InstrumentId, Price, Quantity, SessionId, SystemEvent, TimestampNs, VenueId,
};

use crate::PrivateError;

/// Balance snapshot for one asset on a venue account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    pub venue: VenueId,
    pub asset: String,
    pub free: Quantity,
    pub locked: Quantity,
    pub exchange_ts: Option<TimestampNs>,
}

/// Signed balance delta (e.g. Binance `balanceUpdate` deposit/withdrawal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceDelta {
    pub venue: VenueId,
    pub asset: String,
    /// Signed; negative = debit. Uses [`Quantity`]'s signed [`marketfeed_model::Fixed`].
    pub delta: Quantity,
    pub exchange_ts: Option<TimestampNs>,
}

/// Order state update (observe-only; no order-entry API).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderUpdate {
    pub venue: VenueId,
    pub instrument: InstrumentId,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    /// Venue execution type (`x` on Binance), e.g. `NEW` / `TRADE` / `CANCELED`.
    pub execution_type: Option<String>,
    /// Venue order status (`X` on Binance), e.g. `NEW` / `FILLED` / `CANCELED`.
    pub status: Option<String>,
    pub price: Option<Price>,
    pub quantity: Option<Quantity>,
    pub filled_quantity: Option<Quantity>,
    pub exchange_ts: Option<TimestampNs>,
}

/// Fill / trade against an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub venue: VenueId,
    pub instrument: InstrumentId,
    pub price: Price,
    pub quantity: Quantity,
    pub fee: Option<Quantity>,
    pub exchange_order_id: Option<String>,
    pub trade_id: Option<String>,
    pub exchange_ts: Option<TimestampNs>,
}

/// Position snapshot (derivatives; unused by Spot user-data Phase 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub venue: VenueId,
    pub instrument: InstrumentId,
    pub quantity: Quantity,
    pub entry_price: Option<Price>,
    pub exchange_ts: Option<TimestampNs>,
}

/// Private account payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountEvent {
    Balance(Balance),
    BalanceDelta(BalanceDelta),
    Order(OrderUpdate),
    Fill(Fill),
    Position(Position),
}

/// Consumer of private account events (bounded implementations required later).
pub trait AccountEventSink {
    fn push_account(&mut self, event: AccountEvent) -> Result<(), PrivateError>;
    /// Operational events from the authenticated session. Implementations must
    /// not silently ignore these because auth failures and reconnect requests
    /// are carried on this lane.
    fn push_system(&mut self, event: SystemEvent) -> Result<(), PrivateError>;
}

/// Authenticated private stream handle — **not implemented for live I/O**.
///
/// # Safety / policy
/// - Lives in this feature-gated crate; public market data must not depend on it.
/// - Raw recording of private payloads disabled by default.
/// - No order-entry / execution path in this trait.
pub trait PrivateStream {
    fn session(&self) -> SessionId;

    /// Connect and authenticate. Always returns [`PrivateError::NotImplemented`].
    fn connect(&mut self) -> Result<(), PrivateError> {
        Err(PrivateError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RejectSink;

    impl AccountEventSink for RejectSink {
        fn push_account(&mut self, _event: AccountEvent) -> Result<(), PrivateError> {
            Err(PrivateError::NotImplemented)
        }

        fn push_system(&mut self, _event: SystemEvent) -> Result<(), PrivateError> {
            Err(PrivateError::NotImplemented)
        }
    }

    #[test]
    fn stubs_surface_not_implemented() {
        let mut sink = RejectSink;
        let err = sink
            .push_account(AccountEvent::Balance(Balance {
                venue: VenueId(1),
                asset: "USDT".into(),
                free: Quantity(marketfeed_model::Fixed::ZERO),
                locked: Quantity(marketfeed_model::Fixed::ZERO),
                exchange_ts: None,
            }))
            .unwrap_err();
        assert_eq!(err, PrivateError::NotImplemented);
    }
}
