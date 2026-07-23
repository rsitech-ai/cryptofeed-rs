//! Fast-as-possible replay runner (pace modes later).
//!
//! Default overflow is [`OverflowPolicy::FailEngine`] — same as the live session
//! runner until Drop* policies are soak-tested (spec §3.5 / WP-A).

use std::collections::BTreeMap;

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, DisconnectReason, EventBatch, SessionAction, SessionInput,
    SessionMachine,
};
use marketfeed_dispatch::{DispatchError, EventDispatcher, PushOutcome};
use marketfeed_model::{FrameStamp, OverflowPolicy, SystemEvent, TimestampNs};
use marketfeed_recording::{
    Direction, FrameOpcode, MetadataRecord, RawRecord, RawSegmentReader, RecordingError,
    decode_http_response, decode_metadata, decode_subscription_command,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error("recorded dynamic-subscription wire action does not match the adapter")]
    SubscriptionWireMismatch,
}

#[derive(Debug, Clone, Default)]
pub struct ReplayOutcome {
    pub market_batches: Vec<EventBatch>,
    pub system_events: Vec<SystemEvent>,
    pub other_actions: Vec<SessionAction>,
    pub metadata: Vec<MetadataRecord>,
    pub frames_applied: u64,
    /// Exact aggregate loss count, independent of bounded diagnostic queues.
    pub dropped_events: u64,
}

/// Replays inbound text/binary frames into `machine`, collecting dispatched output.
pub struct ReplayRunner {
    dispatch: EventDispatcher,
    actions: ActionBuffer,
    drop_counts: BTreeMap<String, u64>,
}

impl ReplayRunner {
    pub fn new(dispatch_capacity: usize) -> Self {
        Self::with_overflow(dispatch_capacity, OverflowPolicy::FailEngine)
    }

    pub fn with_overflow(dispatch_capacity: usize, overflow: OverflowPolicy) -> Self {
        Self {
            dispatch: EventDispatcher::new(dispatch_capacity, dispatch_capacity, overflow),
            actions: ActionBuffer::new(),
            drop_counts: BTreeMap::new(),
        }
    }

    pub fn replay_bytes(
        &mut self,
        machine: &mut dyn SessionMachine,
        bytes: Vec<u8>,
        connect_ts: TimestampNs,
    ) -> Result<ReplayOutcome, ReplayError> {
        let mut reader = RawSegmentReader::from_bytes(bytes)?;
        let records = reader.read_all()?;
        self.replay_records(machine, &records, connect_ts)
    }

    pub fn replay_records(
        &mut self,
        machine: &mut dyn SessionMachine,
        records: &[RawRecord],
        connect_ts: TimestampNs,
    ) -> Result<ReplayOutcome, ReplayError> {
        let mut outcome = ReplayOutcome::default();
        let _ = self.dispatch.drain_batches();
        let _ = self.dispatch.drain_systems();
        self.actions.clear();
        let _ = self.actions.take_dropped();
        self.drop_counts.clear();
        machine.on_replay_start(connect_ts, &mut self.actions)?;
        self.note_action_buffer_drops()?;
        self.absorb_actions(&mut outcome)?;

        for rec in records {
            if rec.header.direction != Direction::Inbound {
                continue;
            }
            let stamp = FrameStamp {
                receive_ts: TimestampNs(rec.header.receive_ts_ns),
                mono_ns: rec.header.monotonic_ns,
            };
            let mut payload = rec.payload.clone();
            self.actions.clear();
            match rec.header.opcode {
                FrameOpcode::Text => {
                    machine.on_input(
                        SessionInput::TextFrame {
                            bytes: &mut payload,
                            received: stamp,
                        },
                        &mut self.actions,
                    )?;
                }
                FrameOpcode::Binary => {
                    machine.on_input(
                        SessionInput::BinaryFrame {
                            bytes: &mut payload,
                            received: stamp,
                        },
                        &mut self.actions,
                    )?;
                }
                FrameOpcode::Pong => {
                    machine.on_input(
                        SessionInput::Pong {
                            payload: &payload,
                            received: stamp,
                        },
                        &mut self.actions,
                    )?;
                }
                FrameOpcode::HttpResponse => {
                    let (request_id, response) = decode_http_response(&payload)?;
                    machine.on_input(
                        SessionInput::HttpResponse {
                            request_id,
                            response: &response,
                            received: stamp,
                        },
                        &mut self.actions,
                    )?;
                }
                FrameOpcode::Metadata => {
                    outcome.metadata.push(decode_metadata(&payload)?);
                    continue;
                }
                FrameOpcode::SubscriptionCommand => {
                    let (command, recorded_wire) = decode_subscription_command(&payload)?;
                    let prepared_wire = machine.prepare_dynamic_subscription(&command)?;
                    if prepared_wire != recorded_wire {
                        return Err(ReplayError::SubscriptionWireMismatch);
                    }
                    machine.commit_dynamic_subscription(&command);
                }
                FrameOpcode::Ping | FrameOpcode::Close => {
                    // Engine owns ping/close; skip for adapter replay.
                    continue;
                }
            }
            self.note_action_buffer_drops()?;
            self.absorb_actions(&mut outcome)?;
            outcome.frames_applied += 1;
        }

        self.finish_outcome(&mut outcome);
        Ok(outcome)
    }

    pub fn replay_disconnect(
        &mut self,
        machine: &mut dyn SessionMachine,
        now: TimestampNs,
        outcome: &mut ReplayOutcome,
    ) -> Result<(), ReplayError> {
        self.actions.clear();
        let _ = self.actions.take_dropped();
        machine.on_input(
            SessionInput::Disconnected {
                reason: DisconnectReason::LocalStop,
                now,
            },
            &mut self.actions,
        )?;
        self.note_action_buffer_drops()?;
        self.absorb_actions(outcome)?;
        self.finish_outcome(outcome);
        Ok(())
    }

    fn absorb_actions(&mut self, outcome: &mut ReplayOutcome) -> Result<(), ReplayError> {
        let actions: Vec<_> = self.actions.drain().collect();
        for action in actions {
            match action {
                SessionAction::EmitBatch(batch) => {
                    let push = self.dispatch.push_batch(batch)?;
                    self.note_push_outcome("market_batch", push)?;
                }
                SessionAction::EmitSystem(ev) => {
                    let push = self.dispatch.push_system(ev)?;
                    self.note_push_outcome("system_event", push)?;
                }
                other => outcome.other_actions.push(other),
            }
        }
        Ok(())
    }

    fn note_action_buffer_drops(&mut self) -> Result<(), ReplayError> {
        let dropped = self.actions.take_dropped();
        if dropped == 0 {
            return Ok(());
        }
        self.record_drop("ActionBuffer DropNewest", dropped);
        Ok(())
    }

    fn finish_outcome(&mut self, outcome: &mut ReplayOutcome) {
        outcome.market_batches.extend(self.dispatch.drain_batches());
        outcome.system_events.extend(self.dispatch.drain_systems());
        for (detail, count) in std::mem::take(&mut self.drop_counts) {
            outcome.dropped_events = outcome.dropped_events.saturating_add(count);
            outcome
                .system_events
                .push(SystemEvent::EventsDropped { count, detail });
        }
    }

    /// Spec §3.5: Drop* must never be silent — emit `EventsDropped`.
    fn note_push_outcome(&mut self, lane: &str, push: PushOutcome) -> Result<(), ReplayError> {
        let (count, detail) = match push {
            PushOutcome::Accepted => return Ok(()),
            PushOutcome::DroppedNewest => (1u64, format!("{lane} DropNewest")),
            PushOutcome::DroppedOldest { dropped } => {
                (dropped as u64, format!("{lane} DropOldest"))
            }
        };
        self.record_drop(&detail, count);
        Ok(())
    }

    fn record_drop(&mut self, detail: &str, count: u64) {
        let total = self.drop_counts.entry(detail.to_string()).or_default();
        *total = total.saturating_add(count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::{
        AdapterError, HttpResponse, SessionCommand, SubscriptionWireAction,
    };
    use marketfeed_model::{
        AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
        Price, Quantity, SessionId, Trade, VenueId,
    };
    use marketfeed_recording::{
        Direction, FrameOpcode, RawRecordHeader, encode_http_response, encode_metadata,
        encode_subscription_command,
    };

    struct EmitTradeMachine {
        remaining: usize,
        next_frame: u64,
    }

    struct OverflowActionBufferMachine;
    struct FailAfterOverflowMachine {
        fail_next: bool,
    }
    #[derive(Default)]
    struct SubscriptionAwareMachine {
        subscribed: bool,
    }
    #[derive(Default)]
    struct HttpCaptureMachine {
        seen: Option<(u64, HttpResponse, FrameStamp)>,
    }

    impl SessionMachine for HttpCaptureMachine {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            _output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if let SessionInput::HttpResponse {
                request_id,
                response,
                received,
            } = input
            {
                self.seen = Some((request_id, response.clone(), received));
            }
            Ok(())
        }
    }

    impl SessionMachine for SubscriptionAwareMachine {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::TextFrame { .. }) && self.subscribed {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: "replayed-subscription-active".into(),
                    },
                ));
            }
            Ok(())
        }

        fn prepare_dynamic_subscription(
            &self,
            command: &SessionCommand,
        ) -> Result<SubscriptionWireAction, AdapterError> {
            match command {
                SessionCommand::Subscribe(_) => Ok(SubscriptionWireAction::Text(
                    b"subscribe".as_slice().to_vec().into(),
                )),
                _ => Err(AdapterError::UnsupportedCapability("test command".into())),
            }
        }

        fn commit_dynamic_subscription(&mut self, command: &SessionCommand) {
            if matches!(command, SessionCommand::Subscribe(_)) {
                self.subscribed = true;
            }
        }
    }

    impl SessionMachine for OverflowActionBufferMachine {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::TextFrame { .. }) {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "first".into(),
                    },
                ));
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "second".into(),
                    },
                ));
            }
            Ok(())
        }
    }

    impl SessionMachine for FailAfterOverflowMachine {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::TextFrame { .. }) && self.fail_next {
                self.fail_next = false;
                output.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                output.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                return Err(AdapterError::Parse("injected replay failure".into()));
            }
            Ok(())
        }
    }

    impl SessionMachine for EmitTradeMachine {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::TextFrame { .. }) && self.remaining > 0 {
                self.remaining -= 1;
                let frame_seq = self.next_frame;
                self.next_frame += 1;
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: SessionId(1),
                    frame_seq,
                    events: vec![EventEnvelope {
                        schema_version: 1,
                        venue: VenueId(1),
                        instrument: Some(InstrumentId(1)),
                        connection: ConnectionId(1),
                        session: SessionId(1),
                        frame_seq,
                        event_index: 0,
                        exchange_ts: None,
                        receive_ts: TimestampNs(0),
                        source_sequence: None,
                        flags: EventFlags::empty(),
                        payload: MarketEvent::Trade(Trade {
                            price: Price(Fixed::new(1, 0)),
                            quantity: Quantity(Fixed::new(1, 0)),
                            aggressor: AggressorSide::Buy,
                            trade_id: None,
                        }),
                    }],
                }));
            }
            Ok(())
        }
    }

    #[test]
    fn drop_newest_emits_events_dropped() {
        let mut runner = ReplayRunner::with_overflow(1, OverflowPolicy::DropNewest);
        let mut machine = EmitTradeMachine {
            remaining: 3,
            next_frame: 1,
        };
        let records = vec![text_record(1, b"{}"), text_record(2, b"{}")];
        let outcome = runner
            .replay_records(&mut machine, &records, TimestampNs(0))
            .unwrap();
        assert!(
            outcome
                .system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::EventsDropped { .. })),
            "expected EventsDropped, got {:?}",
            outcome.system_events
        );
        assert_eq!(
            outcome.market_batches.len(),
            1,
            "replay outcome must contain only the batch retained by dispatch"
        );
    }

    #[test]
    fn drop_oldest_outcome_contains_only_the_latest_retained_batch() {
        let mut runner = ReplayRunner::with_overflow(1, OverflowPolicy::DropOldest);
        let mut machine = EmitTradeMachine {
            remaining: 3,
            next_frame: 1,
        };
        let outcome = runner
            .replay_records(
                &mut machine,
                &[
                    text_record(1, b"{}"),
                    text_record(2, b"{}"),
                    text_record(3, b"{}"),
                ],
                TimestampNs(0),
            )
            .unwrap();

        assert_eq!(outcome.market_batches.len(), 1);
        assert_eq!(outcome.market_batches[0].frame_seq, 3);
        assert_eq!(outcome.dropped_events, 2);
        assert!(outcome.system_events.iter().any(|event| matches!(
            event,
            SystemEvent::EventsDropped { count: 2, detail }
                if detail == "market_batch DropOldest"
        )));
    }

    #[test]
    fn action_buffer_overflow_is_reported_in_replay_outcome() {
        let mut runner = ReplayRunner::with_overflow(4, OverflowPolicy::DropNewest);
        runner.actions = ActionBuffer::with_capacity(1);
        let outcome = runner
            .replay_records(
                &mut OverflowActionBufferMachine,
                &[text_record(1, b"{}")],
                TimestampNs(0),
            )
            .unwrap();

        assert!(outcome.system_events.iter().any(|event| matches!(
            event,
            SystemEvent::EventsDropped { count: 1, detail }
                if detail == "ActionBuffer DropNewest"
        )));
    }

    #[test]
    fn repeated_batch_overflow_reports_the_exact_total_out_of_band() {
        let mut runner = ReplayRunner::with_overflow(1, OverflowPolicy::DropNewest);
        let mut machine = EmitTradeMachine {
            remaining: 3,
            next_frame: 1,
        };
        let outcome = runner
            .replay_records(
                &mut machine,
                &[
                    text_record(1, b"{}"),
                    text_record(2, b"{}"),
                    text_record(3, b"{}"),
                ],
                TimestampNs(0),
            )
            .unwrap();

        assert_eq!(outcome.dropped_events, 2);
        assert!(outcome.system_events.iter().any(|event| matches!(
            event,
            SystemEvent::EventsDropped { count: 2, detail }
                if detail == "market_batch DropNewest"
        )));
    }

    #[test]
    fn system_lane_overflow_is_reported_out_of_band() {
        let mut runner = ReplayRunner::with_overflow(1, OverflowPolicy::DropNewest);
        let outcome = runner
            .replay_records(
                &mut OverflowActionBufferMachine,
                &[text_record(1, b"{}")],
                TimestampNs(0),
            )
            .unwrap();

        assert_eq!(outcome.dropped_events, 1);
        assert!(outcome.system_events.iter().any(|event| matches!(
            event,
            SystemEvent::EventsDropped { count: 1, detail }
                if detail == "system_event DropNewest"
        )));
    }

    #[test]
    fn failed_replay_does_not_leak_action_buffer_drops_into_retry() {
        let mut runner = ReplayRunner::with_overflow(4, OverflowPolicy::DropNewest);
        runner.actions = ActionBuffer::with_capacity(1);
        let mut machine = FailAfterOverflowMachine { fail_next: true };

        runner
            .replay_records(&mut machine, &[text_record(1, b"{}")], TimestampNs(0))
            .expect_err("first replay is intentionally rejected");
        let outcome = runner
            .replay_records(&mut machine, &[], TimestampNs(0))
            .expect("retry");

        assert_eq!(outcome.dropped_events, 0);
        assert!(outcome.system_events.is_empty());
    }

    #[test]
    fn replays_http_response_with_exact_payload_and_receive_stamp() {
        let response = HttpResponse {
            status: 206,
            headers: vec![("content-type".into(), "application/octet-stream".into())],
            body: vec![0, 1, 255].into(),
        };
        let payload = encode_http_response(77, &response).unwrap();
        let record = RawRecord {
            header: RawRecordHeader {
                record_len: 0,
                session: SessionId(1),
                frame_seq: 9,
                receive_ts_ns: 123,
                monotonic_ns: 456,
                direction: Direction::Inbound,
                opcode: FrameOpcode::HttpResponse,
                flags: 0,
                payload_len: payload.len() as u32,
                payload_crc32c: 0,
            },
            payload,
        };
        let mut machine = HttpCaptureMachine::default();
        let outcome = ReplayRunner::new(4)
            .replay_records(&mut machine, &[record], TimestampNs(0))
            .unwrap();

        assert_eq!(outcome.frames_applied, 1);
        assert_eq!(
            machine.seen,
            Some((
                77,
                response,
                FrameStamp {
                    receive_ts: TimestampNs(123),
                    mono_ns: 456,
                },
            ))
        );
    }

    #[test]
    fn exposes_metadata_without_applying_it_as_an_adapter_frame() {
        let metadata = MetadataRecord::current_build();
        let payload = encode_metadata(&metadata).unwrap();
        let record = RawRecord {
            header: RawRecordHeader {
                record_len: 0,
                session: SessionId(0),
                frame_seq: 0,
                receive_ts_ns: 123,
                monotonic_ns: 0,
                direction: Direction::Inbound,
                opcode: FrameOpcode::Metadata,
                flags: 0,
                payload_len: payload.len() as u32,
                payload_crc32c: 0,
            },
            payload,
        };
        let mut machine = EmitTradeMachine {
            remaining: 1,
            next_frame: 1,
        };
        let outcome = ReplayRunner::new(4)
            .replay_records(&mut machine, &[record], TimestampNs(0))
            .unwrap();

        assert_eq!(outcome.metadata, vec![metadata]);
        assert_eq!(outcome.frames_applied, 0);
        assert!(outcome.market_batches.is_empty());
    }

    #[test]
    fn replays_accepted_dynamic_subscription_before_later_inputs() {
        let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
        let wire = SubscriptionWireAction::Text(b"subscribe".as_slice().to_vec().into());
        let payload = encode_subscription_command(&command, &wire).unwrap();
        let control = RawRecord {
            header: RawRecordHeader {
                record_len: 0,
                session: SessionId(1),
                frame_seq: 0,
                receive_ts_ns: 10,
                monotonic_ns: 10,
                direction: Direction::Inbound,
                opcode: FrameOpcode::SubscriptionCommand,
                flags: 0,
                payload_len: payload.len() as u32,
                payload_crc32c: 0,
            },
            payload,
        };
        let mut machine = SubscriptionAwareMachine::default();
        let outcome = ReplayRunner::new(4)
            .replay_records(
                &mut machine,
                &[control, text_record(1, b"{}")],
                TimestampNs(0),
            )
            .unwrap();

        assert_eq!(outcome.frames_applied, 2);
        assert!(outcome.system_events.iter().any(|event| matches!(
            event,
            SystemEvent::SubscriptionStateChanged { state }
                if state == "replayed-subscription-active"
        )));
    }

    #[test]
    fn replay_rejects_dynamic_subscription_wire_drift() {
        let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
        let recorded_wire =
            SubscriptionWireAction::Text(b"different-wire".as_slice().to_vec().into());
        let payload = encode_subscription_command(&command, &recorded_wire).unwrap();
        let control = RawRecord {
            header: RawRecordHeader {
                record_len: 0,
                session: SessionId(1),
                frame_seq: 0,
                receive_ts_ns: 10,
                monotonic_ns: 10,
                direction: Direction::Inbound,
                opcode: FrameOpcode::SubscriptionCommand,
                flags: 0,
                payload_len: payload.len() as u32,
                payload_crc32c: 0,
            },
            payload,
        };

        let error = ReplayRunner::new(4)
            .replay_records(
                &mut SubscriptionAwareMachine::default(),
                &[control],
                TimestampNs(0),
            )
            .unwrap_err();

        assert!(matches!(error, ReplayError::SubscriptionWireMismatch));
    }

    fn text_record(seq: u64, payload: &'static [u8]) -> RawRecord {
        RawRecord {
            header: RawRecordHeader {
                record_len: 0,
                session: SessionId(1),
                frame_seq: seq,
                receive_ts_ns: seq as i64,
                monotonic_ns: seq,
                direction: Direction::Inbound,
                opcode: FrameOpcode::Text,
                flags: 0,
                payload_len: payload.len() as u32,
                payload_crc32c: 0,
            },
            payload: payload.to_vec(),
        }
    }
}
