//! Collector-wide Hyperliquid connection-attempt and application-write budgets.
//! Automatic Pong/close controls and other processes/IP users are outside these
//! counters. Causal static admission additionally allows one market each and a
//! combined bounded subscription plan; it does not prove a total IP wire rate.
use marketfeed_transport::{
    CloseReason, FrameBuffer, InboundFrame, OutboundFrame, TransportError, TungsteniteWebSocket,
    WebSocketSpec, WebSocketTransport,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
#[derive(Debug, Default)]
pub(crate) struct HyperliquidLimits {
    connections: Mutex<VecDeque<Instant>>,
    messages: Mutex<VecDeque<Instant>>,
    causal: Mutex<CausalReservations>,
}
#[derive(Debug, Default)]
struct CausalReservations {
    markets: [bool; 2],
    subscriptions: usize,
}
impl HyperliquidLimits {
    pub(crate) fn reserve_causal(
        &self,
        spot: bool,
        subscriptions: usize,
    ) -> Result<(), TransportError> {
        let mut state = self
            .causal
            .lock()
            .map_err(|_| TransportError::Io("Hyperliquid causal admission lock poisoned".into()))?;
        let market = usize::from(!spot);
        if subscriptions == 0
            || subscriptions > 1000
            || state.markets[market]
            || state.subscriptions > 1000 - subscriptions
        {
            return Err(TransportError::Io(
                "Hyperliquid causal market/subscription admission refused".into(),
            ));
        }
        state.markets[market] = true;
        state.subscriptions += subscriptions;
        Ok(())
    }
    pub(crate) fn release_causal(&self, spot: bool, subscriptions: usize) {
        let mut state = self.causal.lock().unwrap_or_else(|p| p.into_inner());
        state.markets[usize::from(!spot)] = false;
        state.subscriptions = state.subscriptions.saturating_sub(subscriptions);
    }
}
fn reserve(
    queue: &Mutex<VecDeque<Instant>>,
    limit: usize,
    now: Instant,
) -> Result<(), TransportError> {
    let mut q = queue
        .lock()
        .map_err(|_| TransportError::Io("Hyperliquid rate-budget lock poisoned".into()))?;
    while q
        .front()
        .is_some_and(|t| now.saturating_duration_since(*t) >= Duration::from_secs(60))
    {
        q.pop_front();
    }
    if q.len() >= limit {
        return Err(TransportError::Io(
            "Hyperliquid collector rolling-minute budget exhausted; refusing send".into(),
        ));
    }
    q.push_back(now);
    Ok(())
}
pub(crate) struct BudgetedWebSocket {
    inner: TungsteniteWebSocket,
    limits: Option<Arc<HyperliquidLimits>>,
}
impl BudgetedWebSocket {
    pub(crate) fn new(limits: Option<Arc<HyperliquidLimits>>) -> Self {
        Self {
            inner: TungsteniteWebSocket::new(),
            limits,
        }
    }

    pub(crate) fn new_causal(limits: Arc<HyperliquidLimits>) -> Self {
        Self {
            inner: TungsteniteWebSocket::new_causal(),
            limits: Some(limits),
        }
    }
}
impl WebSocketTransport for BudgetedWebSocket {
    async fn connect(&mut self, spec: &WebSocketSpec) -> Result<(), TransportError> {
        if let Some(limits) = &self.limits {
            reserve(&limits.connections, 30, Instant::now())?;
        }
        self.inner.connect(spec).await
    }
    async fn read_frame(
        &mut self,
        buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        self.inner.read_frame(buffer).await
    }
    async fn write_frame(&mut self, frame: OutboundFrame) -> Result<(), TransportError> {
        if let Some(limits) = &self.limits {
            reserve(&limits.messages, 2000, Instant::now())?;
        }
        self.inner.write_frame(frame).await
    }
    async fn close(&mut self, reason: CloseReason) -> Result<(), TransportError> {
        self.inner.close(reason).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rolling_budget_is_shared_and_releases_only_expired_entries() {
        let q = Mutex::new(VecDeque::new());
        let now = Instant::now();
        reserve(&q, 2, now).unwrap();
        reserve(&q, 2, now + Duration::from_secs(1)).unwrap();
        assert!(reserve(&q, 2, now + Duration::from_secs(59)).is_err());
        reserve(&q, 2, now + Duration::from_secs(60)).unwrap();
        assert!(reserve(&q, 2, now + Duration::from_secs(60)).is_err());
        reserve(&q, 2, now + Duration::from_secs(61)).unwrap();
    }

    #[test]
    fn causal_admission_shares_market_and_total_subscription_bounds() {
        let limits = Arc::new(HyperliquidLimits::default());
        let clone = limits.clone();
        assert!(limits.reserve_causal(true, 0).is_err());
        limits.reserve_causal(true, 600).unwrap();
        assert!(clone.reserve_causal(true, 1).is_err());
        assert!(clone.reserve_causal(false, 401).is_err());
        clone.reserve_causal(false, 400).unwrap();
        limits.release_causal(true, 600);
        clone.reserve_causal(true, 600).unwrap();
        assert!(limits.reserve_causal(false, 1).is_err());
    }
}
