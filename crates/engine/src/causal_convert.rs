use super::*;
fn n(value: usize) -> u64 {
    value as u64
}
fn batch(value: &EventBatch) -> Result<BatchJson, CodecError> {
    Ok(BatchJson(RecordJson::new(&FeedRecord::Batch(
        value.clone(),
    ))?))
}
fn system(value: &SystemEvent, session: u64) -> Result<SystemJson, CodecError> {
    Ok(SystemJson(RecordJson::new(&FeedRecord::System {
        session: SessionId(session),
        event: value.clone(),
    })?))
}
impl From<&SessionCommand> for CommandDto {
    fn from(value: &SessionCommand) -> Self {
        match value {
            SessionCommand::Subscribe(v) => Self::Subscribe(v.clone()),
            SessionCommand::Unsubscribe(v) => Self::Unsubscribe(v.clone()),
            SessionCommand::Replace(v) => Self::Replace(v.clone()),
            SessionCommand::Resync(v) => Self::Resync(v.0),
            SessionCommand::Stop => Self::Stop,
        }
    }
}
impl From<&HttpRequestSpec> for HttpRequestDto {
    fn from(value: &HttpRequestSpec) -> Self {
        Self {
            id: value.id,
            method: value.method.into(),
            url: value.url.clone(),
            headers: value.headers.clone(),
            body: value.body.as_ref().map(|b| BytesDto::new(b)),
        }
    }
}
impl From<&HttpResponse> for HttpResponseDto {
    fn from(value: &HttpResponse) -> Self {
        Self {
            status: value.status,
            headers: value.headers.clone(),
            body: BytesDto::new(&value.body),
        }
    }
}
impl From<&OutboundFrame> for WriteDto {
    fn from(value: &OutboundFrame) -> Self {
        Self {
            opcode: value.opcode.into(),
            payload: BytesDto::new(&value.payload),
        }
    }
}
impl InputDto {
    pub(super) fn input(value: &CaptureInput, session: u64) -> Result<Self, CodecError> {
        Ok(match value {
            CaptureInput::Connected { connection, now } => Self::Connected {
                connection: connection.0,
                now: now.0,
            },
            CaptureInput::Disconnected { reason, now } => Self::Disconnected {
                reason: reason.clone().into(),
                now: now.0,
            },
            CaptureInput::TransportLost { reason, now } => Self::TransportLost {
                reason: reason.clone().into(),
                now: now.0,
            },
            CaptureInput::Text { payload, received } => Self::Text {
                payload: BytesDto::new(payload),
                received: *received,
            },
            CaptureInput::Binary { payload, received } => Self::Binary {
                payload: BytesDto::new(payload),
                received: *received,
            },
            CaptureInput::Ping { payload, received } => Self::Ping {
                payload: BytesDto::new(payload),
                received: *received,
            },
            CaptureInput::Pong { payload, received } => Self::Pong {
                payload: BytesDto::new(payload),
                received: *received,
            },
            CaptureInput::Close { payload, received } => Self::Close {
                payload: BytesDto::new(payload),
                received: *received,
            },
            CaptureInput::Http {
                request_id,
                response,
                received,
            } => Self::Http {
                request_id: *request_id,
                response: response.into(),
                received: *received,
            },
            CaptureInput::ForwardDispatch { results } => Self::ForwardDispatch {
                results: results
                    .iter()
                    .map(|r| r.as_ref().map(Into::into).map_err(Into::into))
                    .collect(),
            },
            CaptureInput::NullDispatch => Self::NullDispatch,
            CaptureInput::TakeWrites => Self::TakeWrites,
            CaptureInput::TakeHttp => Self::TakeHttp,
            CaptureInput::TimerPoll { now } => Self::TimerPoll { now: now.0 },
            CaptureInput::Control { command, now } => Self::Control {
                command: command.into(),
                now: now.0,
            },
            CaptureInput::System(value) => Self::System(system(value, session)?),
            CaptureInput::Degraded { message, now } => Self::Degraded {
                message: message.clone(),
                now: now.0,
            },
            CaptureInput::Live { now } => Self::Live { now: now.0 },
            CaptureInput::Catalog { catalog, now } => Self::Catalog {
                catalog: catalog.into(),
                now: now.0,
            },
            CaptureInput::Stop => Self::Stop,
            CaptureInput::Lifecycle(value) => Self::Lifecycle((*value).into()),
        })
    }
}
impl From<&InvocationKind> for InvocationDto {
    fn from(value: &InvocationKind) -> Self {
        match value {
            InvocationKind::Connected => Self::Connected,
            InvocationKind::Disconnected(v) => Self::Disconnected(v.clone().into()),
            InvocationKind::Text(v) => Self::Text(*v),
            InvocationKind::Binary(v) => Self::Binary(*v),
            InvocationKind::Pong(v) => Self::Pong(*v),
            InvocationKind::Http {
                request_id,
                received,
            } => Self::Http {
                request_id: *request_id,
                received: *received,
            },
            InvocationKind::Timer { timer_id, now } => Self::Timer {
                timer_id: *timer_id,
                now: now.0,
            },
            InvocationKind::Control(v) => Self::Control(v.into()),
        }
    }
}
impl ActionDto {
    fn action(value: &SessionAction, session: u64) -> Result<Self, CodecError> {
        Ok(match value {
            SessionAction::SendText(v) => Self::SendText(BytesDto::new(v)),
            SessionAction::SendBinary(v) => Self::SendBinary(BytesDto::new(v)),
            SessionAction::SendPing(v) => Self::SendPing(BytesDto::new(v)),
            SessionAction::SendSensitiveText(_) => {
                return Err(invalid("sensitive action cannot be encoded"));
            }
            SessionAction::RequestHttp(v) => Self::RequestHttp(v.into()),
            SessionAction::ScheduleTimer(v) => Self::ScheduleTimer {
                timer_id: v.timer_id,
                fire_at: v.fire_at.0,
            },
            SessionAction::CancelTimer(v) => Self::CancelTimer(*v),
            SessionAction::EmitBatch(v) => Self::EmitBatch(batch(v)?),
            SessionAction::EmitObservedBatch(v) => {
                Self::EmitObservedBatch(ObservedBatchJson::new(v)?)
            }
            SessionAction::EmitSystem(v) => Self::EmitSystem(system(v, session)?),
            SessionAction::MarkLive => Self::MarkLive,
            SessionAction::MarkDegraded => Self::MarkDegraded,
            SessionAction::ResyncInstrument(v) => Self::ResyncInstrument(v.0),
            SessionAction::Reconnect(v) => Self::Reconnect(v.clone().into()),
            SessionAction::DisableSubscription(v) => Self::DisableSubscription(v.0),
            SessionAction::StopSession(v) => Self::StopSession(v.clone().into()),
        })
    }
}
impl From<&ActionDisposition> for DispositionDto {
    fn from(value: &ActionDisposition) -> Self {
        match value {
            ActionDisposition::Buffered => Self::Buffered,
            ActionDisposition::DiscardedAfterCallbackError => Self::DiscardedAfterCallbackError,
            ActionDisposition::Started => Self::Started,
            ActionDisposition::Completed(v) => {
                Self::Completed(v.as_ref().map(|_| ()).map_err(Into::into))
            }
        }
    }
}
impl ItemDto {
    fn item(value: &CaptureQueueItem, session: u64) -> Result<Self, CodecError> {
        Ok(match value {
            CaptureQueueItem::Market(v) => Self::Market(batch(v)?),
            CaptureQueueItem::ObservedMarket(v) => Self::ObservedMarket(ObservedBatchJson::new(v)?),
            CaptureQueueItem::System(v) => Self::System(system(v, session)?),
            CaptureQueueItem::Write(v) => Self::Write(v.into()),
            CaptureQueueItem::Http(v) => Self::Http(v.into()),
        })
    }
    fn lane(&self) -> LaneDto {
        match self {
            Self::Market(_) | Self::ObservedMarket(_) => LaneDto::Market,
            Self::System(_) => LaneDto::System,
            Self::Write(_) => LaneDto::Write,
            Self::Http(_) => LaneDto::Http,
        }
    }
}
impl From<&CaptureOrigin> for OriginDto {
    fn from(value: &CaptureOrigin) -> Self {
        match value {
            CaptureOrigin::Runner => Self::Runner,
            CaptureOrigin::Invocation { ordinal } => Self::Invocation { ordinal: *ordinal },
            CaptureOrigin::Action { invocation, index } => Self::Action {
                invocation: *invocation,
                index: n(*index),
            },
        }
    }
}
impl From<&SubscriptionWireAction> for WireActionDto {
    fn from(value: &SubscriptionWireAction) -> Self {
        match value {
            SubscriptionWireAction::Text(v) => Self::Text(BytesDto::new(v)),
            SubscriptionWireAction::Binary(v) => Self::Binary(BytesDto::new(v)),
            SubscriptionWireAction::Ping(v) => Self::Ping(BytesDto::new(v)),
        }
    }
}
impl EffectDto {
    fn effect(value: &CaptureEntry, session: u64) -> Result<Self, CodecError> {
        Ok(match value {
            CaptureEntry::ProducedRecord(v) => Self::ProducedRecord(RecordJson::original(v)?),
            CaptureEntry::Invocation {
                kind,
                retained_actions,
                dropped_before,
                dropped_after,
                actions,
                outcome,
            } => {
                let CallbackOutcome::Returned(result) = outcome;
                Self::Invocation {
                    kind: kind.into(),
                    retained_actions: n(*retained_actions),
                    dropped_before: *dropped_before,
                    dropped_after: *dropped_after,
                    actions: actions
                        .iter()
                        .map(|a| {
                            Ok(BufferedDto {
                                action: ActionDto::action(&a.action, session)?,
                                disposition: (&a.disposition).into(),
                            })
                        })
                        .collect::<Result<_, CodecError>>()?,
                    outcome: result.as_ref().map(|_| ()).map_err(Into::into),
                }
            }
            CaptureEntry::QueueAttempt {
                lane,
                incoming,
                evicted_front,
                result,
            } => Self::QueueAttempt {
                lane: lane.clone().into(),
                incoming: ItemDto::item(incoming, session)?,
                evicted_front: evicted_front
                    .as_ref()
                    .map(|v| ItemDto::item(v, session))
                    .transpose()?,
                result: result
                    .as_ref()
                    .map(|r| r.as_ref().map(Into::into).map_err(Into::into)),
            },
            CaptureEntry::QueueDrain {
                lane,
                items,
                completed,
            } => Self::QueueDrain {
                lane: lane.clone().into(),
                items: items
                    .iter()
                    .map(|v| ItemDto::item(v, session))
                    .collect::<Result<_, _>>()?,
                completed: *completed,
            },
            CaptureEntry::SinkAttempt {
                lane,
                incoming,
                result,
            } => Self::SinkAttempt {
                lane: lane.clone().into(),
                incoming: ItemDto::item(incoming, session)?,
                result: result
                    .as_ref()
                    .map(|r| r.as_ref().map(Into::into).map_err(Into::into)),
            },
            CaptureEntry::ActionBufferDropsConsumed(v) => Self::ActionBufferDropsConsumed(*v),
            CaptureEntry::SuppressedDiagnostic(v) => Self::SuppressedDiagnostic(v.into()),
            CaptureEntry::SubscriptionPrepared(v) => {
                Self::SubscriptionPrepared(v.as_ref().map(Into::into).map_err(Into::into))
            }
            CaptureEntry::SubscriptionReservation(v) => {
                Self::SubscriptionReservation(v.as_ref().map(|_| ()).map_err(Into::into))
            }
            CaptureEntry::SubscriptionCommitted => Self::SubscriptionCommitted,
        })
    }
}
impl StateDto {
    fn state(value: &CapturedState, session: u64) -> Result<Self, CodecError> {
        Ok(Self {
            lifecycle: value.lifecycle.into(),
            connection: value.connection.0,
            frame_seq: value.frame_seq,
            stop_requested: value.stop_requested,
            reconnect_requested: value.reconnect_requested,
            timers: value.timers.iter().map(|(id, ts)| (*id, ts.0)).collect(),
            pending_market: n(value.pending_market),
            pending_system: n(value.pending_system),
            pending_writes: n(value.pending_writes),
            pending_http: n(value.pending_http),
            valid_books: value.valid_books,
            queues: value
                .queues
                .as_ref()
                .ok_or_else(|| invalid("incomplete queue snapshot"))?
                .iter()
                .map(|(lane, items)| {
                    Ok((
                        lane.clone().into(),
                        items
                            .iter()
                            .map(|v| ItemDto::item(v, session))
                            .collect::<Result<_, _>>()?,
                    ))
                })
                .collect::<Result<_, CodecError>>()?,
        })
    }
    fn validate(&self) -> Result<(), CodecError> {
        let expected = [
            (LaneDto::Market, self.pending_market),
            (LaneDto::System, self.pending_system),
            (LaneDto::Write, self.pending_writes),
            (LaneDto::Http, self.pending_http),
        ];
        if self.queues.len() != 4
            || self.timers.len() > 32
            || self.timers.windows(2).any(|w| w[0].0 >= w[1].0)
        {
            return Err(invalid("invalid queue/timer state shape"));
        }
        for ((lane, items), (expected, count)) in self.queues.iter().zip(expected) {
            if *lane != expected
                || n(items.len()) != count
                || items.len() > 65536
                || items.iter().any(|item| item.lane() != *lane)
            {
                return Err(invalid("queue lane/count mismatch"));
            }
        }
        Ok(())
    }
}
impl OperationDto {
    pub fn from_capture(
        value: &CapturedOperation,
        identity: &OperationIdentity,
    ) -> Result<Self, CodecError> {
        Self::from_capture_schema(value, identity, 1)
    }
    /// Only MFCJ4 owner completions may persist this operation schema.
    pub fn from_observed_capture(
        value: &CapturedOperation,
        identity: &OperationIdentity,
    ) -> Result<Self, CodecError> {
        Self::from_capture_schema(value, identity, 2)
    }
    fn from_capture_schema(
        value: &CapturedOperation,
        identity: &OperationIdentity,
        schema_version: u16,
    ) -> Result<Self, CodecError> {
        if value.venue.0 != identity.venue || value.session.0 != identity.session {
            return Err(invalid("capture owner identity mismatch"));
        }
        if value.entries.len() > 65536
            || value.observed_entries != n(value.entries.len())
            || value.retained_bytes_bound > 64 * 1024 * 1024
        {
            return Err(invalid("capture structure/limits"));
        }
        if value.completeness != CaptureCompleteness::Complete {
            return Err(invalid("incomplete capture cannot be journaled"));
        }
        let CaptureOutcome::Returned(result) = &value.outcome else {
            return Err(invalid("missing complete native outcome"));
        };
        let dto = Self {
            schema_version,
            route: CausalRoute {
                venue: identity.venue,
                session: identity.session,
                ordinal: value.ordinal,
                generation: value.generation,
                bootstrap_id: identity.bootstrap_id.clone(),
            },
            input: InputDto::input(&value.original_input, identity.session)?,
            drain_policy: value.drain_policy.into(),
            before: StateDto::state(&value.before, identity.session)?,
            entries: value
                .entries
                .iter()
                .map(|e| {
                    Ok(TraceDto {
                        origin: (&e.origin).into(),
                        effect: EffectDto::effect(&e.effect, identity.session)?,
                    })
                })
                .collect::<Result<_, CodecError>>()?,
            outcome: result.as_ref().map(|_| ()).map_err(Into::into),
            after: StateDto::state(&value.after, identity.session)?,
            observed_entries: value.observed_entries,
            retained_bytes_bound: n(value.retained_bytes_bound),
        };
        dto.validate()?;
        Ok(dto)
    }
    pub(super) fn validate(&self) -> Result<(), CodecError> {
        if !matches!(self.schema_version, 1 | 2)
            || self.route.ordinal == 0
            || self.route.session == 0
            || self.route.bootstrap_id.len() != 64
            || !self
                .route
                .bootstrap_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.entries.len() > 65536
            || self.observed_entries != n(self.entries.len())
            || self.retained_bytes_bound > 64 * 1024 * 1024
        {
            return Err(invalid("operation structure/limits"));
        }
        if self.schema_version == 1 && self.has_native_observations() {
            return Err(invalid(
                "native observation batches require an enriched causal profile",
            ));
        }
        self.before.validate()?;
        self.after.validate()?;
        if matches!(self.input, InputDto::Connected { connection: 0, .. }) {
            return Err(invalid("zero recorded connection"));
        }
        if let InputDto::Catalog { catalog, .. } = &self.input {
            catalog.catalog()?;
        }
        let mut invocations = Vec::new();
        for trace in &self.entries {
            if let EffectDto::Invocation {
                retained_actions,
                actions,
                ..
            } = &trace.effect
            {
                if *retained_actions != n(actions.len())
                    || actions.len() > 65536
                    || actions
                        .iter()
                        .any(|a| a.disposition == DispositionDto::Started)
                {
                    return Err(invalid("invocation action count"));
                }
                invocations.push(actions.len());
                if trace.origin
                    != (OriginDto::Invocation {
                        ordinal: n(invocations.len()),
                    })
                {
                    return Err(invalid("invocation origin/order"));
                }
            }
            match trace.origin {
                OriginDto::Runner => {}
                OriginDto::Invocation { ordinal } => {
                    if ordinal == 0 || ordinal > n(invocations.len()) {
                        return Err(invalid("unknown invocation origin"));
                    }
                }
                OriginDto::Action { invocation, index } => {
                    let count = invocation
                        .checked_sub(1)
                        .and_then(|i| usize::try_from(i).ok())
                        .and_then(|i| invocations.get(i));
                    if count.is_none_or(|count| index >= n(*count)) {
                        return Err(invalid("unknown action origin"));
                    }
                }
            }
            match &trace.effect {
                EffectDto::QueueAttempt {
                    lane,
                    incoming,
                    evicted_front,
                    result,
                } => {
                    if result.is_none()
                        || incoming.lane() != *lane
                        || evicted_front.as_ref().is_some_and(|i| i.lane() != *lane)
                    {
                        return Err(invalid("queue attempt lane"));
                    }
                }
                EffectDto::QueueDrain {
                    lane,
                    items,
                    completed,
                } => {
                    if !completed || items.len() > 65536 || items.iter().any(|i| i.lane() != *lane)
                    {
                        return Err(invalid("queue drain lane/bound"));
                    }
                }
                EffectDto::SinkAttempt {
                    lane,
                    incoming,
                    result,
                } => {
                    if result.is_none() || incoming.lane() != *lane {
                        return Err(invalid("sink attempt lane"));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
impl From<&AdapterError> for AdapterErrorDto {
    fn from(value: &AdapterError) -> Self {
        match value {
            AdapterError::UnsupportedCapability(v) => Self::UnsupportedCapability(v.clone()),
            AdapterError::Protocol(v) => Self::Protocol(v.clone()),
            AdapterError::Parse(v) => Self::Parse(v.clone()),
            AdapterError::Subscription(v) => Self::Subscription(v.clone()),
            AdapterError::Catalog(v) => Self::Catalog(v.clone()),
            AdapterError::BookInvariant(v) => Self::BookInvariant(v.clone()),
            AdapterError::Internal(v) => Self::Internal(v.clone()),
        }
    }
}
impl From<&TransportError> for TransportErrorDto {
    fn from(value: &TransportError) -> Self {
        match value {
            TransportError::Tls(v) => Self::Tls(v.clone()),
            TransportError::Protocol(v) => Self::Protocol(v.clone()),
            TransportError::Io(v) => Self::Io(v.clone()),
            TransportError::Stub(v) => Self::Stub(v.clone()),
            TransportError::NotConnected => Self::NotConnected,
            TransportError::Closed => Self::Closed,
            TransportError::Timeout => Self::Timeout,
            TransportError::FrameTooLarge(v) => Self::FrameTooLarge(n(*v)),
        }
    }
}
impl From<&SinkError> for SinkErrorDto {
    fn from(value: &SinkError) -> Self {
        match value {
            SinkError::Io(v) => Self::Io(v.clone()),
            SinkError::FailEngine => Self::FailEngine,
            SinkError::DeadlineExceeded => Self::DeadlineExceeded,
            SinkError::UnsupportedPolicy(v) => Self::UnsupportedPolicy((*v).into()),
            SinkError::Unsupported(v) => Self::Unsupported((*v).into()),
        }
    }
}
impl From<&DispatchError> for DispatchErrorDto {
    fn from(value: &DispatchError) -> Self {
        match value {
            DispatchError::FailEngine => Self::FailEngine,
            DispatchError::DeadlineExceeded => Self::DeadlineExceeded,
            DispatchError::UnsupportedPolicy(v) => Self::UnsupportedPolicy((*v).into()),
        }
    }
}
impl From<&RecordingError> for RecordingErrorDto {
    fn from(value: &RecordingError) -> Self {
        match value {
            RecordingError::Io(v) => Self::Io(v.clone()),
            RecordingError::PublicationIndeterminate(v) => {
                Self::PublicationIndeterminate(v.clone())
            }
            RecordingError::InvalidControlCommand(v) => Self::InvalidControlCommand(v.clone()),
            RecordingError::UnsupportedOverflow(v) => Self::UnsupportedOverflow(v.clone()),
            RecordingError::BadMagic => Self::BadMagic,
            RecordingError::Truncated => Self::Truncated,
            RecordingError::CrcMismatch => Self::CrcMismatch,
            RecordingError::InvalidHeader => Self::InvalidHeader,
            RecordingError::QueueFull => Self::QueueFull,
            RecordingError::DiskFull => Self::DiskFull,
            RecordingError::UnsupportedVersion(v) => Self::UnsupportedVersion(*v),
            RecordingError::MetadataConflict { key } => Self::MetadataConflict { key: key.clone() },
            RecordingError::RecordTooLarge { record_len, max } => Self::RecordTooLarge {
                record_len: *record_len,
                max: *max,
            },
            RecordingError::ShutdownTimeout { remaining } => Self::ShutdownTimeout {
                remaining: n(*remaining),
            },
            RecordingError::NormalizedBoundExceeded { kind, limit } => {
                Self::NormalizedBoundExceeded {
                    kind: (*kind).into(),
                    limit: *limit,
                }
            }
        }
    }
}
impl From<&EngineError> for EngineErrorDto {
    fn from(value: &EngineError) -> Self {
        match value {
            EngineError::Internal(v) => Self::Internal(v.clone()),
            EngineError::Stopped => Self::Stopped,
            EngineError::SessionNotFound => Self::SessionNotFound,
            EngineError::Adapter(v) => Self::Adapter(v.into()),
            EngineError::Dispatch(v) => Self::Dispatch(v.into()),
            EngineError::Recording(v) => Self::Recording(v.into()),
            EngineError::Transport(v) => Self::Transport(v.into()),
        }
    }
}
impl From<&PushOutcome> for PushDto {
    fn from(value: &PushOutcome) -> Self {
        match value {
            PushOutcome::Accepted => Self::Accepted,
            PushOutcome::DroppedNewest => Self::DroppedNewest,
            PushOutcome::DroppedOldest { dropped } => Self::DroppedOldest {
                dropped: n(*dropped),
            },
        }
    }
}
