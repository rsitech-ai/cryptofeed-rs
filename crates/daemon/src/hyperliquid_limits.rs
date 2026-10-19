//! Collector-wide Hyperliquid budgets. Other applications behind the same IP
//! require their own coordination; this process never sends above its budget.
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
}
