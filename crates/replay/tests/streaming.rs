use std::{
    cell::Cell,
    io::{self, Cursor, Read},
    rc::Rc,
};

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, EventBatch, HttpMethod, HttpRequestSpec, HttpResponse,
    SensitiveBytes, SessionAction, SessionCommand, SessionInput, SessionMachine,
    SubscriptionWireAction, TimerSpec,
};
use marketfeed_model::{
    AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
    Price, Quantity, SessionId, SystemEvent, TimestampNs, Trade, VenueId,
};
use marketfeed_recording::{
    Direction, FrameOpcode, HEADER_SIZE, MetadataRecord, RawSegmentWriter, RecordingError,
    SessionRecordingMetadata, encode_http_response, encode_metadata, encode_subscription_command,
};
use marketfeed_replay::*;

#[derive(Clone, Copy)]
enum Mode {
    Normal,
    Fail,
    ActionOverflow,
    EventOverflow,
    SystemOverflow,
    HeaderOverflow,
    ByteOverflow,
    Sensitive,
    Panic,
}

struct Machine {
    session: u64,
    seen: u64,
    subscribed: bool,
    mode: Mode,
}
impl SessionMachine for Machine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let state = match input {
            SessionInput::Connected { now } => {
                out.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 1,
                    fire_at: TimestampNs(now.0 + 1),
                }));
                format!("{}:start:{}", self.session, now.0)
            }
            SessionInput::TextFrame { bytes, received }
            | SessionInput::BinaryFrame { bytes, received } => {
                self.seen += 1;
                let original = String::from_utf8_lossy(bytes).into_owned();
                bytes.fill(b'x');
                match self.mode {
                    Mode::Fail => {
                        out.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                        return Err(AdapterError::Parse("failed after state mutation".into()));
                    }
                    Mode::ActionOverflow => {
                        for _ in 0..3 {
                            out.push(SessionAction::MarkLive);
                        }
                        return Ok(());
                    }
                    Mode::EventOverflow => {
                        out.push(SessionAction::EmitBatch(batch(self.seen, 2)));
                        return Ok(());
                    }
                    Mode::SystemOverflow => {
                        for _ in 0..2 {
                            out.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                        }
                        return Ok(());
                    }
                    Mode::HeaderOverflow => {
                        out.push(SessionAction::RequestHttp(HttpRequestSpec {
                            id: 1,
                            method: HttpMethod::Get,
                            url: String::new(),
                            headers: vec![(String::new(), String::new()); 1024],
                            body: None,
                        }));
                        return Ok(());
                    }
                    Mode::ByteOverflow => {
                        out.push(SessionAction::SendText(vec![b'a'; 4096].into()));
                        return Ok(());
                    }
                    Mode::Sensitive => {
                        out.push(SessionAction::SendSensitiveText(SensitiveBytes::new(
                            b"test-only".as_slice().to_vec().into(),
                        )));
                        return Ok(());
                    }
                    Mode::Panic => panic!("adapter panic after state mutation"),
                    Mode::Normal => {}
                }
                out.push(SessionAction::EmitBatch(batch(self.seen, 1)));
                format!(
                    "{}:frame:{}:{}:{}:{}:{}",
                    self.session,
                    self.seen,
                    original,
                    received.receive_ts.0,
                    received.mono_ns,
                    self.subscribed
                )
            }
            SessionInput::HttpResponse {
                request_id,
                response,
                received,
            } => {
                format!(
                    "{}:http:{}:{}:{:?}:{}:{}",
                    self.session,
                    request_id,
                    response.status,
                    response.body,
                    received.receive_ts.0,
                    received.mono_ns
                )
            }
            SessionInput::Pong { payload, received } => format!(
                "{}:pong:{:?}:{}",
                self.session, payload, received.receive_ts.0
            ),
            SessionInput::Timer { .. } => {
                panic!("legacy stream must not synthesize timer callbacks")
            }
            SessionInput::Disconnected { .. } => {
                panic!("legacy stream must not synthesize disconnect callbacks")
            }
            _ => return Ok(()),
        };
        out.push(SessionAction::EmitSystem(
            SystemEvent::SubscriptionStateChanged { state },
        ));
        Ok(())
    }
    fn prepare_dynamic_subscription(
        &self,
        _command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        Ok(SubscriptionWireAction::Text(
            b"wire".as_slice().to_vec().into(),
        ))
    }
    fn commit_dynamic_subscription(&mut self, _command: &SessionCommand) {
        self.subscribed = true;
    }
}

fn batch(seq: u64, events: usize) -> EventBatch {
    EventBatch {
        session: SessionId(99),
        frame_seq: seq,
        events: (0..events)
            .map(|i| EventEnvelope {
                schema_version: 1,
                venue: VenueId(99),
                instrument: Some(InstrumentId(1)),
                connection: ConnectionId(77),
                session: SessionId(99),
                frame_seq: seq,
                event_index: i as u16,
                exchange_ts: None,
                receive_ts: TimestampNs(0),
                source_sequence: None,
                flags: EventFlags::empty(),
                payload: MarketEvent::Trade(Trade {
                    provenance: None,
                    price: Price(Fixed::new(9007199254740993123456789, 12)),
                    quantity: Quantity(Fixed::new(1234567890123456789, 18)),
                    aggressor: AggressorSide::Buy,
                    trade_id: None,
                }),
            })
            .collect(),
    }
}

fn factory(
    mode: Mode,
) -> impl FnMut(&SessionRecordingMetadata) -> Result<LegacyReplaySession, AdapterError> {
    move |m| {
        Ok(LegacyReplaySession {
            machine: Box::new(Machine {
                session: m.session_id,
                seen: 0,
                subscribed: false,
                mode,
            }),
            assumed_start: TimestampNs(7),
        })
    }
}
fn limits() -> LegacyReplayLimits {
    LegacyReplayLimits {
        max_sessions: 2,
        max_actions_per_step: 2,
        max_events_per_step: 2,
        ..Default::default()
    }
}
fn metadata(id: u64) -> MetadataRecord {
    MetadataRecord::Session(SessionRecordingMetadata {
        schema_version: 1,
        session_id: id,
        venue_id: id as u16,
        adapter: "test".into(),
        environment: "test".into(),
        endpoint: "memory://".into(),
        catalog_version: 1,
        catalog: vec![],
        initial_subscriptions: vec![],
    })
}
struct Spec {
    session: u64,
    seq: u64,
    now: i64,
    mono: u64,
    direction: Direction,
    opcode: FrameOpcode,
    payload: Vec<u8>,
}
fn raw(session: u64, seq: u64, opcode: FrameOpcode, payload: &[u8]) -> Spec {
    Spec {
        session,
        seq,
        now: seq as i64,
        mono: seq,
        direction: Direction::Inbound,
        opcode,
        payload: payload.to_vec(),
    }
}
fn meta(id: u64) -> Spec {
    raw(
        id,
        0,
        FrameOpcode::Metadata,
        &encode_metadata(&metadata(id)).unwrap(),
    )
}
fn segment(records: &[Spec]) -> (Vec<u8>, Vec<usize>) {
    let mut w = RawSegmentWriter::create(Vec::new(), 500).unwrap();
    let mut ends = Vec::new();
    for r in records {
        w.write_record(
            SessionId(r.session),
            r.seq,
            r.now,
            r.mono,
            r.direction,
            r.opcode,
            0,
            &r.payload,
        )
        .unwrap();
        ends.push(w.get_ref().len());
    }
    (w.into_inner(), ends)
}
fn states(actions: &[SessionAction]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitSystem(SystemEvent::SubscriptionStateChanged { state }) => {
                Some(state.clone())
            }
            _ => None,
        })
        .collect()
}

struct FencedReader {
    bytes: Cursor<Vec<u8>>,
    ends: Vec<usize>,
    acknowledged: Rc<Cell<usize>>,
}
impl Read for FencedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let pos = self.bytes.position() as usize;
        let end = self
            .ends
            .get(self.acknowledged.get())
            .copied()
            .unwrap_or(self.bytes.get_ref().len());
        if pos == end && pos < self.bytes.get_ref().len() {
            return Err(io::Error::other("read ahead of sink acknowledgement"));
        }
        let cap = end.saturating_sub(pos).min(buf.len());
        self.bytes.read(&mut buf[..cap])
    }
}

#[test]
fn streams_thousands_past_legacy_capacity_without_read_ahead_or_output_retention() {
    let mut records = vec![meta(1)];
    records.extend((1..=2048).map(|seq| raw(1, seq, FrameOpcode::Text, b"tick")));
    let (bytes, ends) = segment(&records);
    let mut old = ReplayRunner::new(2);
    let mut old_machine = Machine {
        session: 1,
        seen: 0,
        subscribed: false,
        mode: Mode::Normal,
    };
    assert!(matches!(
        old.replay_bytes(&mut old_machine, bytes.clone(), TimestampNs(7)),
        Err(ReplayError::Dispatch(_))
    ));
    let ack = Rc::new(Cell::new(0));
    let source = FencedReader {
        bytes: Cursor::new(bytes),
        ends,
        acknowledged: ack.clone(),
    };
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    let mut batches = 0;
    let report = replay
        .replay_segment(source, |step| {
            assert!(step.actions.len() <= 2);
            for action in step.actions {
                if let SessionAction::EmitBatch(batch) = action {
                    batches += 1;
                    assert_eq!(batch.frame_seq, batches);
                    // Legacy reconstruction never fabricates a physical identity.
                    assert_eq!(batch.session, SessionId(99));
                }
            }
            if step.kind == LegacyReplayStepKind::AdapterInput {
                assert_eq!(step.record.payload, b"tick");
            }
            ack.set(ack.get() + 1);
            Ok(())
        })
        .unwrap();
    assert_eq!(batches, 2048);
    assert_eq!(report.records_read, 2049);
    assert_eq!(report.steps_acknowledged, 2049);
    assert_eq!(report.sessions_started, 1);
}

#[test]
fn routes_interleaved_colliding_ids_and_preserves_backwards_wall_clock_order() {
    let mut a = raw(1, 1, FrameOpcode::Text, b"A");
    a.now = 100;
    let mut b = raw(2, 1, FrameOpcode::Text, b"B");
    b.now = 50;
    let (bytes, _) = segment(&[meta(1), meta(2), a, b, raw(1, 2, FrameOpcode::Binary, b"C")]);
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    let mut observed = Vec::new();
    replay
        .replay_segment(Cursor::new(bytes), |step| {
            observed.extend(states(step.actions));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        observed,
        vec![
            "1:start:7",
            "2:start:7",
            "1:frame:1:A:100:1:false",
            "2:frame:1:B:50:1:false",
            "1:frame:2:C:2:2:false"
        ]
    );
}

#[test]
fn repeated_metadata_at_rotation_does_not_restart_or_spend_metadata_budget_twice() {
    let creates = Rc::new(Cell::new(0));
    let count = creates.clone();
    let mut make = factory(Mode::Normal);
    let mut config = limits();
    config.max_metadata_bytes = meta(1).payload.len();
    let mut replay = LegacyStreamingReplay::new(config, move |m: &SessionRecordingMetadata| {
        count.set(count.get() + 1);
        make(m)
    })
    .unwrap();
    let mut observed = Vec::new();
    for (seq, text) in [(1, b"A"), (2, b"B")] {
        let (bytes, _) = segment(&[meta(1), raw(1, seq, FrameOpcode::Text, text)]);
        replay
            .replay_segment(Cursor::new(bytes), |step| {
                observed.extend(states(step.actions));
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(creates.get(), 1);
    assert_eq!(
        observed,
        vec![
            "1:start:7",
            "1:frame:1:A:1:1:false",
            "1:frame:2:B:2:2:false"
        ]
    );
    assert_eq!(replay.progress().segments_completed, 2);
    assert_eq!(
        replay.progress().metadata_bytes_retained,
        config.max_metadata_bytes
    );
}

#[test]
fn late_metadata_is_not_prescanned_or_used_to_redecode_earlier_frames() {
    let creates = Rc::new(Cell::new(0));
    let count = creates.clone();
    let mut make = factory(Mode::Normal);
    let mut replay = LegacyStreamingReplay::new(limits(), move |m: &SessionRecordingMetadata| {
        count.set(count.get() + 1);
        make(m)
    })
    .unwrap();
    let (bytes, _) = segment(&[raw(1, 1, FrameOpcode::Text, b"early"), meta(1)]);
    let error = replay
        .replay_segment(Cursor::new(bytes), |_| panic!("no earlier output eligible"))
        .unwrap_err();
    assert!(matches!(
        error.cause,
        LegacyReplayError::MissingMetadata(SessionId(1))
    ));
    assert_eq!(creates.get(), 0);
    assert_eq!(error.at.record, 1);
    assert_eq!(error.progress.callbacks_invoked, 0);
    let (bytes, _) = segment(&[meta(1)]);
    let retry = replay
        .replay_segment(Cursor::new(bytes), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(retry.cause, LegacyReplayError::Poisoned));
    assert_eq!(retry.progress, error.progress);
}

#[test]
fn header_identity_schema_and_metadata_conflicts_fail_before_rebinding() {
    for variant in 0..4 {
        let mut bad = metadata(1);
        if let MetadataRecord::Session(m) = &mut bad {
            match variant {
                0 => m.session_id = 2,
                1 => m.schema_version = 99,
                2 => m.catalog_version = 2,
                _ => m.session_id = 0,
            }
        }
        let header = if variant == 3 { 0 } else { 1 };
        let (bytes, _) = segment(&[
            meta(1),
            raw(
                header,
                0,
                FrameOpcode::Metadata,
                &encode_metadata(&bad).unwrap(),
            ),
        ]);
        let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
        let mut delivered = 0;
        let error = replay
            .replay_segment(Cursor::new(bytes), |_| {
                delivered += 1;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(
            error.cause,
            LegacyReplayError::InvalidMetadata | LegacyReplayError::ConflictingMetadata
        ));
        assert_eq!(error.progress.sessions_started, 1);
        assert_eq!(delivered, 1);
    }
}

#[test]
fn session_and_metadata_limits_stop_before_factory_invocation() {
    for limit in ["sessions", "metadata"] {
        let creates = Rc::new(Cell::new(0));
        let count = creates.clone();
        let mut make = factory(Mode::Normal);
        let mut config = limits();
        let records = if limit == "sessions" {
            config.max_sessions = 1;
            vec![meta(1), meta(2)]
        } else {
            config.max_metadata_bytes = meta(1).payload.len() - 1;
            vec![meta(1)]
        };
        let mut replay = LegacyStreamingReplay::new(config, move |m: &SessionRecordingMetadata| {
            count.set(count.get() + 1);
            make(m)
        })
        .unwrap();
        let (bytes, _) = segment(&records);
        let error = replay
            .replay_segment(Cursor::new(bytes), |_| Ok(()))
            .unwrap_err();
        if limit == "sessions" {
            assert!(matches!(error.cause, LegacyReplayError::SessionBudget));
            assert_eq!(creates.get(), 1)
        } else {
            assert!(matches!(error.cause, LegacyReplayError::MetadataBudget));
            assert_eq!(creates.get(), 0)
        }
    }
}

#[test]
fn adapter_failure_discards_all_step_actions_and_exposes_separate_positions() {
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Fail)).unwrap();
    let (bytes, _) = segment(&[
        meta(1),
        raw(1, 1, FrameOpcode::Text, b"fail"),
        raw(1, 2, FrameOpcode::Text, b"unread"),
    ]);
    let mut delivered = 0;
    let error = replay
        .replay_segment(Cursor::new(bytes), |_| {
            delivered += 1;
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(error.cause, LegacyReplayError::Adapter(_)));
    assert_eq!(delivered, 1);
    assert_eq!(error.progress.records_read, 2);
    assert_eq!(error.progress.callbacks_invoked, 2);
    assert_eq!(error.progress.callbacks_completed, 1);
    assert_eq!(error.progress.last_invoked.unwrap().record, 2);
    assert_eq!(error.progress.last_completed.unwrap().record, 1);
    assert_eq!(error.progress.last_acknowledged.unwrap().record, 1);
}

#[test]
fn action_event_byte_and_sensitive_limits_reject_whole_steps() {
    for mode in [
        Mode::ActionOverflow,
        Mode::EventOverflow,
        Mode::SystemOverflow,
        Mode::Normal,
        Mode::HeaderOverflow,
        Mode::ByteOverflow,
        Mode::Sensitive,
    ] {
        let mut config = limits();
        config.max_events_per_step = 1;
        config.max_output_bytes_per_step = 2048;
        let mut replay = LegacyStreamingReplay::new(config, factory(mode)).unwrap();
        let (bytes, _) = segment(&[meta(1), raw(1, 1, FrameOpcode::Text, b"input")]);
        let mut delivered = 0;
        let error = replay
            .replay_segment(Cursor::new(bytes), |_| {
                delivered += 1;
                Ok(())
            })
            .unwrap_err();
        match mode {
            Mode::ActionOverflow => {
                assert!(matches!(error.cause, LegacyReplayError::ActionOverflow(1)))
            }
            Mode::EventOverflow | Mode::SystemOverflow | Mode::Normal => {
                assert!(matches!(error.cause, LegacyReplayError::EventBudget))
            }
            Mode::ByteOverflow | Mode::HeaderOverflow => {
                assert!(matches!(error.cause, LegacyReplayError::OutputByteBudget))
            }
            Mode::Sensitive => assert!(matches!(error.cause, LegacyReplayError::SensitiveAction)),
            _ => unreachable!(),
        }
        assert_eq!(delivered, 1);
        assert_eq!(error.progress.callbacks_completed, 2);
    }
}

#[test]
fn sink_failure_is_terminal_and_does_not_claim_its_partial_side_effect_was_rolled_back() {
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    let (bytes, _) = segment(&[
        meta(1),
        raw(1, 1, FrameOpcode::Text, b"visible"),
        raw(1, 2, FrameOpcode::Text, b"unread"),
    ]);
    let mut visible = Vec::new();
    let error = replay
        .replay_segment(Cursor::new(bytes), |step| {
            visible.push(step.position);
            if visible.len() == 2 {
                Err("injected after external write".into())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(matches!(error.cause, LegacyReplayError::Sink(_)));
    assert_eq!(visible.len(), 2);
    assert_eq!(error.progress.steps_acknowledged, 1);
    assert_eq!(error.progress.last_completed.unwrap().record, 2);
    assert_eq!(error.progress.last_acknowledged.unwrap().record, 1);
    assert_eq!(error.progress.records_read, 2);
}

#[test]
fn exact_input_byte_quota_accepts_eof_and_excess_consumes_only_one_probe_byte() {
    let (bytes, _) = segment(&[meta(1), raw(1, 1, FrameOpcode::Text, b"A")]);
    let mut config = limits();
    config.max_input_bytes = bytes.len() as u64;
    let mut replay = LegacyStreamingReplay::new(config, factory(Mode::Normal)).unwrap();
    let report = replay
        .replay_segment(Cursor::new(bytes.clone()), |_| Ok(()))
        .unwrap();
    assert_eq!(report.input_bytes_read, bytes.len() as u64);
    config.max_input_bytes -= 1;
    let mut replay = LegacyStreamingReplay::new(config, factory(Mode::Normal)).unwrap();
    let error = replay
        .replay_segment(Cursor::new(bytes), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(error.cause, LegacyReplayError::InputByteBudget));
    assert_eq!(error.progress.input_bytes_read, config.max_input_bytes + 1);
    assert_eq!(error.progress.steps_acknowledged, 1);
}

#[test]
fn record_quota_rejects_the_extra_record_before_adapter_delivery() {
    let mut config = limits();
    config.max_records = 1;
    let mut replay = LegacyStreamingReplay::new(config, factory(Mode::Normal)).unwrap();
    let (bytes, _) = segment(&[meta(1), raw(1, 1, FrameOpcode::Text, b"A")]);
    let error = replay
        .replay_segment(Cursor::new(bytes), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(error.cause, LegacyReplayError::RecordBudget));
    assert_eq!(error.progress.records_read, 2);
    assert_eq!(error.progress.callbacks_invoked, 1);
    assert_eq!(error.progress.steps_acknowledged, 1);
}

#[test]
fn strict_stream_rejects_torn_tail_and_late_crc_corruption_after_the_delivered_prefix() {
    let (original, _) = segment(&[meta(1), raw(1, 1, FrameOpcode::Text, b"A")]);
    for n in 1..4 {
        let mut bytes = original.clone();
        bytes.extend_from_slice(&[50, 0, 0][..n]);
        let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
        let error = replay
            .replay_segment(Cursor::new(bytes), |_| Ok(()))
            .unwrap_err();
        assert!(matches!(
            error.cause,
            LegacyReplayError::Recording(RecordingError::Truncated)
        ));
        assert_eq!(error.at.record, 3);
        assert_eq!(error.progress.steps_acknowledged, 2);
    }
    let (mut bytes, _) = segment(&[
        meta(1),
        raw(1, 1, FrameOpcode::Text, b"A"),
        raw(1, 2, FrameOpcode::Text, b"B"),
    ]);
    *bytes.last_mut().unwrap() ^= 1;
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    let error = replay
        .replay_segment(Cursor::new(bytes), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(
        error.cause,
        LegacyReplayError::Recording(RecordingError::CrcMismatch)
    ));
    assert_eq!(error.progress.records_read, 2);
    assert_eq!(error.progress.steps_acknowledged, 2);
}

#[test]
fn http_subscription_and_control_observation_keep_record_order_without_timer_simulation() {
    let command = SessionCommand::Subscribe(vec!["BTC".into()]);
    let wire = SubscriptionWireAction::Text(b"wire".as_slice().to_vec().into());
    let control = encode_subscription_command(&command, &wire).unwrap();
    let response = HttpResponse {
        status: 206,
        headers: vec![("x-test".into(), "value".into())],
        body: vec![0, 1, 255].into(),
    };
    let mut outbound = raw(1, 1, FrameOpcode::Text, b"outbound-only");
    outbound.direction = Direction::Outbound;
    let (bytes, _) = segment(&[
        meta(1),
        raw(1, 0, FrameOpcode::SubscriptionCommand, &control),
        outbound,
        raw(
            1,
            2,
            FrameOpcode::HttpResponse,
            &encode_http_response(42, &response).unwrap(),
        ),
        raw(1, 3, FrameOpcode::Pong, b"p"),
        raw(1, 4, FrameOpcode::Ping, b"p"),
        raw(1, 5, FrameOpcode::Close, b"close"),
        raw(1, 6, FrameOpcode::Text, b"after"),
    ]);
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    let mut observed = Vec::new();
    let mut kinds = Vec::new();
    replay
        .replay_segment(Cursor::new(bytes), |step| {
            observed.extend(states(step.actions));
            kinds.push(step.kind);
            Ok(())
        })
        .unwrap();
    assert_eq!(kinds[1], LegacyReplayStepKind::SubscriptionMutation);
    for i in [2, 5, 6] {
        assert_eq!(kinds[i], LegacyReplayStepKind::ObservedOnly)
    }
    assert!(observed[1].starts_with("1:http:42:206:"));
    assert!(observed[2].starts_with("1:pong:"));
    assert_eq!(observed[3], "1:frame:1:after:6:6:true");
}

#[test]
fn wire_mismatch_and_malformed_http_do_not_commit_or_claim_callback_completion() {
    let command = SessionCommand::Subscribe(vec!["BTC".into()]);
    let wrong = SubscriptionWireAction::Text(b"different".as_slice().to_vec().into());
    for (opcode, payload) in [
        (
            FrameOpcode::SubscriptionCommand,
            encode_subscription_command(&command, &wrong).unwrap(),
        ),
        (FrameOpcode::HttpResponse, b"invalid".to_vec()),
    ] {
        let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
        let (bytes, _) = segment(&[meta(1), raw(1, 1, opcode, &payload)]);
        let error = replay
            .replay_segment(Cursor::new(bytes), |_| Ok(()))
            .unwrap_err();
        assert_eq!(error.progress.callbacks_completed, 1);
        assert_eq!(error.progress.steps_acknowledged, 1);
        if opcode == FrameOpcode::HttpResponse {
            assert_eq!(error.progress.callbacks_invoked, 1)
        } else {
            assert!(matches!(
                error.cause,
                LegacyReplayError::SubscriptionWireMismatch
            ));
            assert_eq!(error.progress.callbacks_invoked, 2)
        }
    }
}

#[test]
fn legacy_v1_remains_available_only_through_the_explicit_single_machine_api() {
    let (mut bytes, _) = segment(&[raw(1, 1, FrameOpcode::Text, b"A")]);
    bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    assert!(matches!(
        replay
            .replay_segment(Cursor::new(bytes.clone()), |_| Ok(()))
            .unwrap_err()
            .cause,
        LegacyReplayError::MissingMetadata(_)
    ));
    let mut machine = Machine {
        session: 1,
        seen: 0,
        subscribed: false,
        mode: Mode::Normal,
    };
    assert_eq!(
        ReplayRunner::new(4)
            .replay_bytes(&mut machine, bytes, TimestampNs(7))
            .unwrap()
            .frames_applied,
        1
    );
}

#[test]
fn fragmented_and_interrupted_reads_preserve_complete_record_boundaries() {
    struct Fragments {
        source: Cursor<Vec<u8>>,
        interrupted: bool,
    }
    impl Read for Fragments {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(io::ErrorKind::Interrupted.into());
            }
            let cap = buf.len().min(1);
            self.source.read(&mut buf[..cap])
        }
    }
    let (bytes, _) = segment(&[meta(1), raw(1, 1, FrameOpcode::Text, b"A")]);
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    assert_eq!(
        replay
            .replay_segment(
                Fragments {
                    source: Cursor::new(bytes),
                    interrupted: false
                },
                |_| Ok(())
            )
            .unwrap()
            .steps_acknowledged,
        2
    );
}

#[test]
fn adding_future_metadata_with_an_earlier_timestamp_cannot_change_the_prefix() {
    let mut prefix = vec![meta(1), raw(1, 1, FrameOpcode::Text, b"A")];
    let mut outcomes = Vec::new();
    for future in [false, true] {
        if future {
            prefix.push(meta(2));
            prefix.push(raw(1, 2, FrameOpcode::Text, b"B"));
        }
        let (bytes, _) = segment(&prefix);
        let mut output = Vec::new();
        LegacyStreamingReplay::new(limits(), factory(Mode::Normal))
            .unwrap()
            .replay_segment(Cursor::new(bytes), |step| {
                output.push((
                    step.position,
                    step.kind,
                    step.record.header.session,
                    step.actions.to_vec(),
                ));
                Ok(())
            })
            .unwrap();
        outcomes.push(output);
    }
    assert_eq!(outcomes[0], outcomes[1][..2]);
}

#[test]
fn empty_exact_quota_and_invalid_limit_boundaries_are_explicit() {
    let (bytes, _) = segment(&[]);
    assert_eq!(bytes.len(), HEADER_SIZE);
    let mut config = limits();
    config.max_input_bytes = HEADER_SIZE as u64;
    assert_eq!(
        LegacyStreamingReplay::new(config, factory(Mode::Normal))
            .unwrap()
            .replay_segment(Cursor::new(bytes), |_| Ok(()))
            .unwrap()
            .segments_completed,
        1
    );
    config.max_actions_per_step = 0;
    assert!(matches!(
        LegacyStreamingReplay::new(config, factory(Mode::Normal)),
        Err(LegacyReplayError::InvalidLimits)
    ));
}

#[test]
fn missing_factory_context_rejects_without_startup_or_retained_binding() {
    let calls = Cell::new(0);
    let mut replay = LegacyStreamingReplay::new(limits(), |_: &SessionRecordingMetadata| {
        calls.set(calls.get() + 1);
        Err(AdapterError::Parse(
            "missing native conversion artifact".into(),
        ))
    })
    .unwrap();
    let (bytes, _) = segment(&[meta(1)]);
    let error = replay
        .replay_segment(Cursor::new(bytes), |_| panic!("no eligible output"))
        .unwrap_err();
    assert!(matches!(error.cause, LegacyReplayError::Adapter(_)));
    assert_eq!(calls.get(), 1);
    assert_eq!(error.progress.callbacks_invoked, 0);
    assert_eq!(error.progress.steps_acknowledged, 0);
    assert_eq!(error.progress.sessions_started, 0);
    assert_eq!(error.progress.metadata_bytes_retained, 0);
    assert!(matches!(
        replay
            .replay_segment(io::empty(), |_| Ok(()))
            .unwrap_err()
            .cause,
        LegacyReplayError::Poisoned
    ));
    assert_eq!(calls.get(), 1);
}

#[test]
fn build_metadata_repeats_once_and_rejects_conflict_and_wrong_schema() {
    let original = MetadataRecord::current_build();
    let encoded = encode_metadata(&original).unwrap();
    let build = || raw(0, 0, FrameOpcode::Metadata, &encoded);
    let (bytes, _) = segment(&[build(), build()]);
    let mut replay = LegacyStreamingReplay::new(limits(), factory(Mode::Normal)).unwrap();
    let progress = replay
        .replay_segment(Cursor::new(bytes), |_| Ok(()))
        .unwrap();
    assert_eq!(progress.metadata_bytes_retained, encoded.len());
    assert_eq!(progress.callbacks_invoked, 0);
    for schema_conflict in [false, true] {
        let mut changed = original.clone();
        let MetadataRecord::Build(ref mut fields) = changed else {
            unreachable!()
        };
        if schema_conflict {
            fields.schema_version = 2;
        } else {
            fields.package_version.push_str("-changed");
        }
        let changed = raw(
            0,
            0,
            FrameOpcode::Metadata,
            &encode_metadata(&changed).unwrap(),
        );
        let (bytes, _) = segment(&[build(), changed]);
        let error = LegacyStreamingReplay::new(limits(), factory(Mode::Normal))
            .unwrap()
            .replay_segment(Cursor::new(bytes), |_| Ok(()))
            .unwrap_err();
        if schema_conflict {
            assert!(matches!(error.cause, LegacyReplayError::InvalidMetadata));
        } else {
            assert!(matches!(
                error.cause,
                LegacyReplayError::ConflictingMetadata
            ));
        }
        assert_eq!(error.progress.steps_acknowledged, 1);
        assert_eq!(error.progress.metadata_bytes_retained, encoded.len());
    }
}

#[test]
fn caught_factory_adapter_and_sink_panics_leave_terminal_poison() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    for location in 0..3 {
        let mut make = factory(if location == 1 {
            Mode::Panic
        } else {
            Mode::Normal
        });
        let mut replay =
            LegacyStreamingReplay::new(limits(), move |m: &SessionRecordingMetadata| {
                assert_ne!(location, 0, "factory panic");
                make(m)
            })
            .unwrap();
        let (bytes, _) = segment(&[meta(1), raw(1, 1, FrameOpcode::Text, b"input")]);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            replay.replay_segment(Cursor::new(bytes), |_| {
                assert_ne!(location, 2, "sink panic");
                Ok(())
            })
        }));
        assert!(panic.is_err());
        assert!(matches!(
            replay
                .replay_segment(io::empty(), |_| Ok(()))
                .unwrap_err()
                .cause,
            LegacyReplayError::Poisoned
        ));
    }
}

#[test]
fn input_byte_quota_is_shared_across_rotated_segments() {
    let (first, _) = segment(&[meta(1)]);
    let (second, _) = segment(&[raw(1, 1, FrameOpcode::Text, b"input")]);
    let mut config = limits();
    config.max_input_bytes = (first.len() + second.len() - 1) as u64;
    let mut replay = LegacyStreamingReplay::new(config, factory(Mode::Normal)).unwrap();
    replay
        .replay_segment(Cursor::new(first), |_| Ok(()))
        .unwrap();
    let error = replay
        .replay_segment(Cursor::new(second), |_| panic!("incomplete second segment"))
        .unwrap_err();
    assert!(matches!(error.cause, LegacyReplayError::InputByteBudget));
    assert_eq!(error.at.segment, 1);
    assert_eq!(error.progress.input_bytes_read, config.max_input_bytes + 1);
    assert_eq!(error.progress.steps_acknowledged, 1);
    assert_eq!(error.progress.callbacks_invoked, 1);
}
