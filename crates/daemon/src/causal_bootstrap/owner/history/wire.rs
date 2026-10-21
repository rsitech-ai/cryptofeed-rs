//! Opaque one-use application-write authority, created only after actor ACK.
use super::*;
use marketfeed_transport::{FrameOpcode, OutboundFrame, WebSocketTransport};
#[derive(Debug, thiserror::Error)]
pub enum WireSendError {
    #[error("wire authorization revoked, unavailable or not durably settled")]
    Unavailable,
}
#[derive(Debug)]
pub struct WireSendCompletion {
    pub outcome: WireOutcomeDto,
    pub receipt: CommitReceipt,
    pub detail: Option<WireErrorDto>,
}
pub(super) struct WireRuntime {
    pub ledger: WireLedger,
    pub staged: Option<WireLedger>,
    pub deadline: Instant,
    pub intent_queued: bool,
    pub ready: bool,
    pub claimed: bool,
    pub invocation_queued: bool,
    pub result_queued: bool,
    pub outcome: Option<WireOutcomeDto>,
    pub detail: Option<WireErrorDto>,
    pub waiter: Option<std::sync::mpsc::Sender<WireSendCompletion>>,
}
impl WireRuntime {
    pub fn new(policy: WirePolicyDto) -> Self {
        Self {
            ledger: WireLedger::new(policy),
            staged: None,
            deadline: Instant::now(),
            intent_queued: false,
            ready: false,
            claimed: false,
            invocation_queued: false,
            result_queued: false,
            outcome: None,
            detail: None,
            waiter: None,
        }
    }
    pub fn permitted(&self, s: &Data) -> bool {
        self.ledger.first().is_some_and(|b| {
            s.status.failure.is_none()
                && s.source_active
                && s.source.0 == b.connection
                && b.input_id >= s.visibility_fence
                && Instant::now() < self.deadline
        })
    }
}
/// Not Clone; no raw frame/payload accessor or caller-supplied success API.
/// Its future invokes the actual transport method. Dropping an invocation future
/// retains an Unknown observation; it cannot revoke/undo bytes already sent.
pub struct WireAuthorization {
    shared: Arc<HistoryShared>,
    wire_id: u64,
    armed: bool,
}
impl HistoryJournalOwner {
    pub fn try_take_wire(&self) -> Option<WireAuthorization> {
        let mut s = state(&self.ingress.shared);
        refresh(&self.ingress.shared, &mut s);
        let wire = s.wire.as_ref()?;
        if !wire.ready || wire.claimed || !wire.permitted(&s) {
            return None;
        }
        let wire_id = wire.ledger.intent()?.0;
        let wire = s.wire.as_mut()?;
        wire.ready = false;
        wire.claimed = true;
        Some(WireAuthorization {
            shared: self.ingress.shared.clone(),
            wire_id,
            armed: true,
        })
    }
    pub fn wire_progress(&self) -> Option<(usize, usize, bool)> {
        state(&self.ingress.shared)
            .wire
            .as_ref()
            .map(|w| (w.ledger.count(), w.ledger.bytes(), w.ledger.clean()))
    }
}
impl WireAuthorization {
    pub fn id(&self) -> u64 {
        self.wire_id
    }
    /// Permit must be durably committed before this method invokes write_frame.
    /// The driver must own the transport for this owner's physical connection;
    /// no portable trait proves a socket's remote identity/OS loaded image.
    /// The transport method and first poll must not synchronously reenter this
    /// owner: both run under its fence lock. A blocking method/poll or accepted
    /// storage ticket can make shutdown/join unbounded.
    pub async fn invoke<T: WebSocketTransport>(
        mut self,
        transport: &mut T,
    ) -> Result<WireSendCompletion, WireSendError> {
        let (tx, rx) = std::sync::mpsc::channel();
        {
            let mut s = state(&self.shared);
            refresh(&self.shared, &mut s);
            let wire = s.wire.as_ref().ok_or(WireSendError::Unavailable)?;
            if wire.ledger.intent().map(|x| x.0) != Some(self.wire_id) || !wire.permitted(&s) {
                return Err(WireSendError::Unavailable);
            }
            let intent_ordinal = wire.ledger.intent().expect("checked").1;
            let wire = s.wire.as_mut().expect("checked");
            wire.waiter = Some(tx);
            if !push_wire(
                &self.shared,
                &mut s,
                WireEventDto::InvocationPermitted {
                    wire_id: self.wire_id,
                    intent_ordinal,
                },
            ) {
                return Err(WireSendError::Unavailable);
            }
            s.wire.as_mut().expect("checked").invocation_queued = true;
        }
        let frame = loop {
            let decision = {
                let mut s = state(&self.shared);
                refresh(&self.shared, &mut s);
                if s.status.phase == OwnerPhase::Stopped {
                    return Err(WireSendError::Unavailable);
                }
                let wire = s.wire.as_ref().ok_or(WireSendError::Unavailable)?;
                if wire.ledger.invocation().is_none() {
                    None
                } else if !wire.permitted(&s) {
                    s.wire.as_mut().expect("checked").outcome = Some(WireOutcomeDto::NotInvoked);
                    Some(None)
                } else {
                    let binding = wire.ledger.first().ok_or(WireSendError::Unavailable)?;
                    let opcode = match binding.frame.opcode {
                        OpcodeDto::Text => FrameOpcode::Text,
                        OpcodeDto::Binary => FrameOpcode::Binary,
                        _ => return Err(WireSendError::Unavailable),
                    };
                    Some(Some(OutboundFrame {
                        opcode,
                        payload: binding
                            .frame
                            .payload
                            .bytes()
                            .map_err(|_| WireSendError::Unavailable)?,
                    }))
                }
            };
            if let Some(frame) = decision {
                break frame;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        };
        if let Some(frame) = frame {
            let mut transport = Some(transport);
            let mut frame = Some(frame);
            let mut future = None;
            let outcome = std::future::poll_fn(|cx| {
                if future.is_none() {
                    let mut s = state(&self.shared);
                    refresh(&self.shared, &mut s);
                    if !s.wire.as_ref().is_some_and(|w| w.permitted(&s)) {
                        return std::task::Poll::Ready((WireOutcomeDto::NotInvoked, None));
                    }
                    // Both METHOD CALL and first future poll occur under the
                    // fresh eligibility lock: a trait method may execute before
                    // returning its future. Later safety is already-invoked I/O.
                    future = Some(Box::pin(
                        transport
                            .take()
                            .expect("one invocation")
                            .write_frame(frame.take().expect("one frame")),
                    ));
                    return match std::future::Future::poll(
                        future.as_mut().expect("constructed").as_mut(),
                        cx,
                    ) {
                        std::task::Poll::Pending => std::task::Poll::Pending,
                        std::task::Poll::Ready(result) => {
                            std::task::Poll::Ready(wire_result(result))
                        }
                    };
                }
                match std::future::Future::poll(future.as_mut().expect("constructed").as_mut(), cx)
                {
                    std::task::Poll::Pending => std::task::Poll::Pending,
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(wire_result(result)),
                }
            })
            .await;
            let mut s = state(&self.shared);
            if let Some(wire) = s.wire.as_mut() {
                wire.outcome = Some(outcome.0);
                wire.detail = outcome.1;
            }
            if matches!(outcome.0, WireOutcomeDto::Failed | WireOutcomeDto::Unknown) {
                // Observed unsafe I/O fences public state immediately, while
                // the exact complete result remains retained for its ACK.
                revoke_history(&self.shared, &mut s, OwnerFailure::Revoked);
            }
        }
        self.armed = false;
        self.shared.wake.notify_all();
        loop {
            match rx.try_recv() {
                Ok(done) => return Ok(done),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(WireSendError::Unavailable);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
            if state(&self.shared).status.phase == OwnerPhase::Stopped {
                return Err(WireSendError::Unavailable);
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
impl Drop for WireAuthorization {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut s = state(&self.shared);
        let mut uncertain = false;
        if let Some(wire) = s.wire.as_mut() {
            if wire.ledger.intent().map(|x| x.0) == Some(self.wire_id) && wire.outcome.is_none() {
                uncertain = wire.invocation_queued;
                wire.outcome = Some(if uncertain {
                    WireOutcomeDto::Unknown
                } else {
                    WireOutcomeDto::NotInvoked
                });
            }
        }
        if uncertain {
            revoke_history(&self.shared, &mut s, OwnerFailure::Revoked);
        }
        self.shared.wake.notify_all();
    }
}
/// Called with the owner lock. Returns true while native execution must wait.
pub(super) fn tick(shared: &HistoryShared, s: &mut Data) -> bool {
    let Some(wire) = s.wire.as_ref() else {
        return false;
    };
    let Some(binding) = wire.ledger.first() else {
        return false;
    };
    if wire.ledger.intent().is_none() {
        if wire.intent_queued {
            return true;
        }
        let event = if wire.permitted(s) {
            let Some(wire_id) = wire.ledger.next_id() else {
                revoke_history(shared, s, OwnerFailure::InputOrdinalExhausted);
                return true;
            };
            WireEventDto::Intent {
                wire_id,
                binding: Box::new(binding.clone()),
            }
        } else {
            WireEventDto::Discarded {
                binding: Box::new(binding.clone()),
            }
        };
        if push_wire(shared, s, event) {
            s.wire.as_mut().expect("checked").intent_queued = true;
        }
        return true;
    }
    let (wire_id, intent_ordinal) = wire.ledger.intent().expect("checked");
    let outcome = wire.outcome.or_else(|| {
        (!wire.invocation_queued && !wire.permitted(s)).then_some(WireOutcomeDto::NotInvoked)
    });
    if let Some(outcome) = outcome {
        if !wire.result_queued && (!wire.invocation_queued || wire.ledger.invocation().is_some()) {
            let event = WireEventDto::Settled {
                wire_id,
                intent_ordinal,
                invocation_ordinal: wire.ledger.invocation(),
                outcome,
                detail: wire.detail.clone(),
            };
            if push_wire(shared, s, event) {
                s.wire.as_mut().expect("checked").result_queued = true;
            }
        }
    }
    true
}
fn push_wire(shared: &HistoryShared, s: &mut Data, event: WireEventDto) -> bool {
    let Some(wire) = s.wire.as_ref() else {
        return false;
    };
    if wire.staged.is_some() {
        return false;
    }
    let mut staged = wire.ledger.clone();
    let Some(ordinal) = push(
        shared,
        s,
        Event::Control(OwnerEventDto::Wire(event.clone())),
        0,
    ) else {
        return false;
    };
    let observed = s.observations.back().expect("just queued").observed_ns;
    if staged
        .apply(
            &event,
            ordinal,
            observed,
            s.source.0,
            s.visibility_fence,
            s.source_active && s.status.failure.is_none(),
        )
        .is_err()
    {
        s.observations.pop_back();
        s.next_ordinal -= 1;
        return false;
    }
    s.wire.as_mut().expect("checked").staged = Some(staged);
    true
}
pub(super) fn committed(
    shared: &HistoryShared,
    s: &mut Data,
    event: &WireEventDto,
    receipt: CommitReceipt,
) {
    let Some(wire) = s.wire.as_mut() else {
        return;
    };
    let Some(staged) = wire.staged.take() else {
        revoke_history(shared, s, OwnerFailure::InvalidReceipt);
        return;
    };
    wire.ledger = staged;
    match event {
        WireEventDto::Intent { .. } => {
            wire.ready = true;
            wire.intent_queued = false;
        }
        WireEventDto::Settled {
            outcome, detail, ..
        } => {
            if let Some(tx) = wire.waiter.take() {
                let _ = tx.send(WireSendCompletion {
                    outcome: *outcome,
                    receipt,
                    detail: detail.clone(),
                });
            }
            wire.ready = false;
            wire.claimed = false;
            wire.invocation_queued = false;
            wire.result_queued = false;
            wire.outcome = None;
            wire.detail = None;
            if matches!(outcome, WireOutcomeDto::Failed | WireOutcomeDto::Unknown) {
                revoke_history(shared, s, OwnerFailure::Revoked);
            }
        }
        WireEventDto::Discarded { .. } => wire.intent_queued = false,
        WireEventDto::InvocationPermitted { .. } => {}
    }
}

fn wire_result(
    result: Result<(), marketfeed_transport::TransportError>,
) -> (WireOutcomeDto, Option<WireErrorDto>) {
    match result {
        Ok(()) => (WireOutcomeDto::Succeeded, None),
        Err(error) => {
            let detail = WireErrorDto::from_transport(error);
            (
                if matches!(detail, WireErrorDto::CapacityExceeded { .. }) {
                    WireOutcomeDto::Unknown
                } else {
                    WireOutcomeDto::Failed
                },
                Some(detail),
            )
        }
    }
}
