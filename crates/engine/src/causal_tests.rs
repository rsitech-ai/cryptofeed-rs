use super::*;
use bytes::Bytes;

fn identity() -> OperationIdentity {
    OperationIdentity {
        venue: 21,
        session: 77,
        bootstrap_id: "a".repeat(64),
    }
}
pub(super) fn config(policy: OverflowPolicy) -> CaptureConfig {
    CaptureConfig {
        venue: VenueId(21),
        session: SessionId(77),
        dispatch_capacity: 1,
        overflow: policy,
        max_bytes: 8 * 1024 * 1024,
        max_entries: 8192,
    }
}
pub(super) fn stamp() -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(i64::MIN),
        mono_ns: u64::MAX,
    }
}
fn batch(value: i128, scale: u8) -> EventBatch {
    EventBatch {
        session: SessionId(999),
        frame_seq: u64::MAX,
        events: vec![EventEnvelope {
            schema_version: 1,
            venue: VenueId(999),
            instrument: Some(InstrumentId(u32::MAX)),
            connection: ConnectionId(u64::MAX),
            session: SessionId(999),
            frame_seq: u64::MAX,
            event_index: u16::MAX,
            exchange_ts: Some(TimestampNs(i64::MAX)),
            receive_ts: TimestampNs(i64::MIN),
            source_sequence: Some(SequenceRange {
                first: u64::MAX,
                last: u64::MAX,
            }),
            flags: EventFlags::empty(),
            payload: MarketEvent::Trade(Trade {
                provenance: None,
                price: Price(Fixed::new(value, scale)),
                quantity: Quantity(Fixed::new(value, scale)),
                aggressor: AggressorSide::Unknown,
                trade_id: Some(SourceId("id\0quoted\"".into())),
            }),
        }],
    }
}
pub(super) struct Emit {
    pub(super) fail: bool,
}
impl SessionMachine for Emit {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { bytes, .. } = input {
            bytes.fill(0);
            for value in [i128::MIN, i128::MAX] {
                out.push(SessionAction::EmitBatch(batch(value, 255)));
            }
            out.push(SessionAction::SendBinary(Bytes::from_static(b"\0\xffwire")));
            out.push(SessionAction::RequestHttp(HttpRequestSpec {
                id: u64::MAX,
                method: HttpMethod::Post,
                url: "https://example.invalid/public?x=1&x=2".into(),
                headers: vec![
                    ("X-Test".into(), "one".into()),
                    ("X-Test".into(), "two".into()),
                ],
                body: Some(Bytes::from_static(b"\0\xffbody")),
            }));
            out.push(SessionAction::ScheduleTimer(TimerSpec {
                timer_id: 9,
                fire_at: TimestampNs(i64::MAX),
            }));
            if self.fail {
                return Err(AdapterError::Parse("late callback failure\0".into()));
            }
        }
        Ok(())
    }
}
fn capture(drain: CaptureDrainPolicy) -> CapturedOperation {
    let mut session = CaptureSession::new(
        Box::new(Emit { fail: false }),
        config(OverflowPolicy::DropOldest),
    )
    .unwrap();
    session
        .execute_turn(
            CaptureInput::Text {
                payload: b"original\0\xff".to_vec(),
                received: stamp(),
            },
            drain,
        )
        .unwrap()
}
fn roundtrip(capture: &CapturedOperation) -> OperationDto {
    let envelope = encode_operation(capture, &identity(), MAX_OPERATION_BYTES).unwrap();
    let decoded = decode_operation(&envelope, &identity()).unwrap();
    assert!(decoded.matches_capture(capture, &identity()).unwrap());
    assert_eq!(
        serde_json::to_vec(&decoded).unwrap(),
        envelope.payload().unwrap()
    );
    decoded
}
fn mutate(capture: &CapturedOperation, f: impl FnOnce(&mut OperationDto)) -> CausalEnvelope {
    let mut dto = OperationDto::from_capture(capture, &identity()).unwrap();
    f(&mut dto);
    CausalEnvelope {
        schema_version: 1,
        phase: CausalPhase::Operation,
        route: dto.route.clone(),
        payload_base64: STANDARD.encode(serde_json::to_vec(&dto).unwrap()),
    }
}
#[test]
fn actual_callback_queue_eviction_snapshots_and_compound_drains_keep_full_i128_and_bytes() {
    for drain in [
        CaptureDrainPolicy::Retain,
        CaptureDrainPolicy::NullDispatchTakeOutbound,
    ] {
        let operation = capture(drain);
        let dto = roundtrip(&operation);
        let InputDto::Text { payload, received } = &dto.input else {
            panic!()
        };
        assert_eq!(payload.bytes().unwrap(), b"original\0\xff");
        assert_eq!(*received, stamp());
        assert!(dto.entries.iter().any(|e| matches!(
            e.effect,
            EffectDto::QueueAttempt {
                evicted_front: Some(_),
                ..
            }
        )));
        let mut coefficients = Vec::new();
        for entry in &dto.entries {
            if let EffectDto::Invocation { actions, .. } = &entry.effect {
                for a in actions {
                    if let ActionDto::EmitBatch(leaf) = &a.action {
                        let FeedRecord::Batch(b) = leaf.record().unwrap() else {
                            panic!()
                        };
                        let MarketEvent::Trade(t) = &b.events[0].payload else {
                            panic!()
                        };
                        coefficients.push(t.price.0.coefficient);
                        assert_eq!(t.price.0.scale, 255);
                    }
                }
            }
        }
        assert_eq!(coefficients, [i128::MIN, i128::MAX]);
        assert_eq!(
            dto.after.pending_market,
            if drain == CaptureDrainPolicy::Retain {
                1
            } else {
                0
            }
        );
        assert!(
            dto.entries
                .iter()
                .filter_map(
                    |e| if let EffectDto::QueueDrain { completed, .. } = e.effect {
                        Some(completed)
                    } else {
                        None
                    }
                )
                .all(|v| v)
        );
    }
}
#[test]
fn actual_partial_callback_failure_preserves_discarded_actions_and_typed_error() {
    let mut session = CaptureSession::new(
        Box::new(Emit { fail: true }),
        config(OverflowPolicy::FailEngine),
    )
    .unwrap();
    let operation = session
        .execute(CaptureInput::Text {
            payload: b"input".to_vec(),
            received: stamp(),
        })
        .unwrap();
    let dto = roundtrip(&operation);
    assert_eq!(
        dto.outcome,
        Err(EngineErrorDto::Adapter(AdapterErrorDto::Parse(
            "late callback failure\0".into()
        )))
    );
    assert!(dto.entries.iter().any(|e|matches!(&e.effect,EffectDto::Invocation{actions,outcome:Err(AdapterErrorDto::Parse(_)),..} if actions.iter().all(|a|a.disposition==DispositionDto::DiscardedAfterCallbackError))));
    assert_eq!(dto.after.pending_market, 0);
}
#[test]
fn actual_sink_error_label_is_owned_and_rejected_front_is_preserved() {
    let mut session = CaptureSession::new(
        Box::new(Emit { fail: false }),
        config(OverflowPolicy::DropOldest),
    )
    .unwrap();
    session
        .execute(CaptureInput::Text {
            payload: b"input".to_vec(),
            received: stamp(),
        })
        .unwrap();
    let operation = session
        .execute(CaptureInput::ForwardDispatch {
            results: vec![Err(SinkError::Unsupported("unwired\0backend"))],
        })
        .unwrap();
    let dto = roundtrip(&operation);
    assert!(
        matches!(&dto.input,InputDto::ForwardDispatch{results} if results==&[Err(SinkErrorDto::Unsupported("unwired\0backend".into()))])
    );
    assert_eq!(dto.before.pending_market, dto.after.pending_market);
}
#[test]
fn all_catalog_fields_distinct_key_and_row_metadata_and_decimal_domain_roundtrip() {
    let mut instruments = Vec::new();
    for (i, kind) in [
        InstrumentKind::Spot,
        InstrumentKind::PerpetualLinear,
        InstrumentKind::PerpetualInverse,
        InstrumentKind::FutureLinear,
        InstrumentKind::FutureInverse,
        InstrumentKind::Option,
    ]
    .into_iter()
    .enumerate()
    {
        instruments.push(Instrument {
            id: InstrumentId(i as u32),
            key: InstrumentKey {
                venue: VenueCode(format!("venue{i}")),
                native_symbol: format!("symbol{i}"),
                kind,
                settlement: Some(AssetCode("KEY-SETTLE".into())),
                expiry_ns: Some(i64::MIN),
            },
            base: AssetCode("BASE".into()),
            quote: AssetCode("QUOTE".into()),
            settlement: Some(AssetCode("ROW-SETTLE".into())),
            price_scale: 255,
            quantity_scale: 254,
            price_increment: Fixed::new(i128::MIN, 255),
            quantity_increment: Fixed::new(i128::MAX, 254),
            min_quantity: Some(Fixed::new(0, 253)),
            max_quantity: Some(Fixed::new(1, 252)),
            min_notional: Some(Fixed::new(-1, 251)),
            contract_size: Some(Fixed::new(i128::MAX, 250)),
            expiry_ns: Some(i64::MAX),
            status: InstrumentStatus::Unknown,
            inverse: true,
            catalog_version: CatalogVersion(u64::MAX - i as u64),
        });
    }
    let catalog =
        CatalogView::with_instruments(VenueId(u16::MAX), CatalogVersion(u64::MAX), instruments);
    let dto = CatalogDto::from(&catalog);
    assert_eq!(dto.catalog().unwrap(), catalog);
    let decoded: CatalogDto = serde_json::from_slice(&serde_json::to_vec(&dto).unwrap()).unwrap();
    assert_eq!(decoded.catalog().unwrap(), catalog);
    let mut session = CaptureSession::new(
        Box::new(Emit { fail: false }),
        config(OverflowPolicy::DropOldest),
    )
    .unwrap();
    let operation = session
        .execute(CaptureInput::Catalog {
            catalog: catalog.clone(),
            now: TimestampNs(i64::MAX),
        })
        .unwrap();
    let dto = roundtrip(&operation);
    let InputDto::Catalog {
        catalog: actual, ..
    } = dto.input
    else {
        panic!()
    };
    assert_eq!(actual.catalog().unwrap(), catalog);
    for scale in 0..=255 {
        for coefficient in [i128::MIN, i128::MAX, 0, -1, 1] {
            let fixed = Fixed::new(coefficient, scale);
            let dto = FixedDto::from(fixed);
            let decoded: FixedDto =
                serde_json::from_slice(&serde_json::to_vec(&dto).unwrap()).unwrap();
            assert_eq!(decoded.fixed().unwrap(), fixed)
        }
    }
}
#[test]
fn malformed_coefficient_tokens_and_missing_or_unknown_catalog_fields_fail() {
    for coefficient in [
        "+1",
        "01",
        "-0",
        "1e2",
        "1.0",
        "170141183460469231731687303715884105728",
    ] {
        let json = format!(r#"{{"coefficient":"{coefficient}","scale":0}}"#);
        assert!(serde_json::from_str::<FixedDto>(&json).is_err());
    }
    for json in [
        r#"{"coefficient":1,"scale":0}"#,
        r#"{"coefficient":"1","scale":256}"#,
        r#"{"coefficient":"1"}"#,
        r#"{"coefficient":"1","scale":0,"unknown":0}"#,
    ] {
        assert!(serde_json::from_str::<FixedDto>(json).is_err())
    }
}
#[test]
fn incomplete_sensitive_and_missing_error_details_never_form_operations() {
    let operation = capture(CaptureDrainPolicy::Retain);
    for fault in [
        CaptureFault::Bytes,
        CaptureFault::Entries,
        CaptureFault::SensitiveAction,
        CaptureFault::Panicked,
    ] {
        let mut bad = operation.clone();
        bad.completeness = CaptureCompleteness::Incomplete(fault);
        assert!(encode_operation(&bad, &identity(), MAX_OPERATION_BYTES).is_err());
    }
    for outcome in [
        CaptureOutcome::Panicked,
        CaptureOutcome::ReturnedErrorDetailsOmitted(CaptureErrorClass::Internal),
    ] {
        let mut bad = operation.clone();
        bad.outcome = outcome;
        assert!(encode_operation(&bad, &identity(), MAX_OPERATION_BYTES).is_err());
    }
    let mut bad = operation.clone();
    bad.before.queues = None;
    assert!(encode_operation(&bad, &identity(), MAX_OPERATION_BYTES).is_err());
    let mut bad = operation;
    let invocation = bad
        .entries
        .iter_mut()
        .find_map(|e| {
            if let CaptureEntry::Invocation { actions, .. } = &mut e.effect {
                Some(actions)
            } else {
                None
            }
        })
        .unwrap();
    invocation[0].action = SessionAction::SendSensitiveText(SensitiveBytes::new(
        Bytes::from_static(b"NEVER_SERIALIZE_THIS"),
    ));
    assert!(encode_operation(&bad, &identity(), MAX_OPERATION_BYTES).is_err());
}
#[test]
fn encoded_budget_boundary_counts_whole_dto_and_never_truncates() {
    let operation = capture(CaptureDrainPolicy::Retain);
    let bytes = encode_operation(&operation, &identity(), MAX_OPERATION_BYTES)
        .unwrap()
        .payload()
        .unwrap();
    assert!(encode_operation(&operation, &identity(), bytes.len()).is_ok());
    assert!(matches!(
        encode_operation(&operation, &identity(), bytes.len() - 1),
        Err(CodecError::TooLarge)
    ));
    assert!(matches!(
        encode_operation(&operation, &identity(), 0),
        Err(CodecError::TooLarge)
    ));
    assert!(matches!(
        encode_operation(&operation, &identity(), MAX_OPERATION_BYTES + 1),
        Err(CodecError::TooLarge)
    ));
}
#[test]
fn identity_route_schema_origins_lanes_and_counts_are_checked_before_acceptance() {
    let operation = capture(CaptureDrainPolicy::Retain);
    let mutations: Vec<Box<dyn FnOnce(&mut OperationDto)>> = vec![
        Box::new(|o| o.schema_version = 2),
        Box::new(|o| o.observed_entries += 1),
        Box::new(|o| o.after.pending_market += 1),
        Box::new(|o| o.after.queues.swap(0, 1)),
        Box::new(|o| {
            o.entries[0].origin = OriginDto::Action {
                invocation: u64::MAX,
                index: 0,
            }
        }),
        Box::new(|o| o.route.session = 0),
        Box::new(|o| o.route.bootstrap_id = "A".repeat(64)),
        Box::new(|o| o.retained_bytes_bound = u64::MAX),
    ];
    for mutation in mutations {
        let envelope = mutate(&operation, mutation);
        assert!(decode_operation(&envelope, &identity()).is_err())
    }
    let mut envelope = encode_operation(&operation, &identity(), MAX_OPERATION_BYTES).unwrap();
    envelope.route.generation += 1;
    assert!(decode_operation(&envelope, &identity()).is_err());
    let mut other = identity();
    other.venue += 1;
    assert!(
        decode_operation(
            &encode_operation(&operation, &identity(), MAX_OPERATION_BYTES).unwrap(),
            &other
        )
        .is_err()
    );
}
#[test]
fn noncanonical_unknown_duplicate_and_wrong_leaf_content_are_rejected() {
    let operation = capture(CaptureDrainPolicy::Retain);
    let envelope = encode_operation(&operation, &identity(), MAX_OPERATION_BYTES).unwrap();
    let payload = String::from_utf8(envelope.payload().unwrap()).unwrap();
    for changed in [
        format!(" {payload}"),
        payload.replacen(
            "\"schema_version\":1",
            "\"schema_version\":1,\"unknown\":1",
            1,
        ),
        payload.replacen(
            "\"schema_version\":1",
            "\"schema_version\":1,\"schema_version\":1",
            1,
        ),
        payload.replacen(
            "\"mono_ns\":18446744073709551615",
            "\"mono_ns\":18446744073709551615,\"unknown\":1",
            1,
        ),
    ] {
        let bad = CausalEnvelope::new(
            CausalPhase::Operation,
            envelope.route.clone(),
            changed.as_bytes(),
        )
        .unwrap();
        assert!(decode_operation(&bad, &identity()).is_err());
    }
    let json =
        serde_json::to_string(&RecordJson::new(&FeedRecord::artifact("x", b"bytes")).unwrap())
            .unwrap();
    assert!(serde_json::from_str::<BatchJson>(&json).is_err());
    assert!(serde_json::from_str::<SystemJson>(&json).is_err());
    let leaf = serde_json::to_string(&format!(
        " {}",
        serde_json::to_string(&FeedRecord::Batch(batch(i128::MAX, 255))).unwrap()
    ))
    .unwrap();
    assert!(serde_json::from_str::<RecordJson>(&leaf).is_err());
}
#[test]
fn all_native_error_categories_and_fields_survive_owned_wire_roundtrip() {
    let errors = vec![
        EngineError::Stopped,
        EngineError::SessionNotFound,
        EngineError::Internal("detail".into()),
        EngineError::Adapter(AdapterError::Parse("parse".into())),
        EngineError::Dispatch(DispatchError::UnsupportedPolicy(
            OverflowPolicy::SpillToDisk,
        )),
        EngineError::Transport(TransportError::FrameTooLarge(usize::MAX)),
        EngineError::Recording(RecordingError::NormalizedBoundExceeded {
            kind: "static-kind",
            limit: u64::MAX,
        }),
        EngineError::Recording(RecordingError::PublicationIndeterminate(
            "after rename".into(),
        )),
        EngineError::Recording(RecordingError::RecordTooLarge {
            record_len: u32::MAX,
            max: 1,
        }),
    ];
    for error in errors {
        let mut operation = capture(CaptureDrainPolicy::Retain);
        operation.outcome = CaptureOutcome::Returned(Err(error.clone()));
        let dto = roundtrip(&operation);
        assert_eq!(dto.outcome, Err((&error).into()));
    }
}

#[test]
fn actual_nonfinancial_inputs_keep_transport_bytes_ordered_headers_and_native_coordinates() {
    let mut inputs = vec![
        CaptureInput::Connected {
            connection: ConnectionId(u64::MAX),
            now: TimestampNs(i64::MIN),
        },
        CaptureInput::Binary {
            payload: b"\xff\0binary".to_vec(),
            received: stamp(),
        },
        CaptureInput::Ping {
            payload: b"\xff\0ping".to_vec(),
            received: stamp(),
        },
        CaptureInput::Pong {
            payload: b"\xff\0pong".to_vec(),
            received: stamp(),
        },
        CaptureInput::Close {
            payload: b"\xff\0close".to_vec(),
            received: stamp(),
        },
        CaptureInput::Http {
            request_id: u64::MAX,
            response: HttpResponse {
                status: 599,
                headers: vec![
                    ("X-Repeat".into(), "first".into()),
                    ("X-Repeat".into(), "second".into()),
                ],
                body: Bytes::from_static(b"\xff\0response"),
            },
            received: stamp(),
        },
        CaptureInput::TimerPoll {
            now: TimestampNs(i64::MAX),
        },
        CaptureInput::NullDispatch,
        CaptureInput::TakeWrites,
        CaptureInput::TakeHttp,
        CaptureInput::System(SystemEvent::ClockJump { delta_ns: i64::MIN }),
        CaptureInput::Degraded {
            message: "degraded\0".into(),
            now: TimestampNs(i64::MAX),
        },
        CaptureInput::Live {
            now: TimestampNs(i64::MIN),
        },
        CaptureInput::Stop,
    ];
    for reason in [
        DisconnectReason::RemoteClose,
        DisconnectReason::TransportError,
        DisconnectReason::HeartbeatTimeout,
        DisconnectReason::LocalStop,
        DisconnectReason::ReconnectRequested,
    ] {
        inputs.push(CaptureInput::Disconnected {
            reason: reason.clone(),
            now: TimestampNs(i64::MIN),
        });
        inputs.push(CaptureInput::TransportLost {
            reason,
            now: TimestampNs(i64::MAX),
        });
    }
    for command in [
        SessionCommand::Subscribe(vec!["b".into(), "a".into(), "a".into()]),
        SessionCommand::Unsubscribe(vec!["c".into()]),
        SessionCommand::Replace(vec![]),
        SessionCommand::Resync(InstrumentId(u32::MAX)),
        SessionCommand::Stop,
    ] {
        inputs.push(CaptureInput::Control {
            command,
            now: TimestampNs(i64::MAX),
        });
    }
    for state in [
        SessionLifecycle::Planned,
        SessionLifecycle::Connecting,
        SessionLifecycle::Connected,
        SessionLifecycle::Subscribing,
        SessionLifecycle::Synchronizing,
        SessionLifecycle::Live,
        SessionLifecycle::Degraded,
        SessionLifecycle::Backoff,
        SessionLifecycle::Draining,
        SessionLifecycle::Stopped,
    ] {
        inputs.push(CaptureInput::Lifecycle(state));
    }
    for input in inputs {
        let mut session = CaptureSession::new(
            Box::new(Emit { fail: false }),
            config(OverflowPolicy::DropOldest),
        )
        .unwrap();
        let operation = session.execute(input).unwrap();
        let dto = roundtrip(&operation);
        if let InputDto::Http {
            request_id,
            response,
            received,
        } = dto.input
        {
            assert_eq!(request_id, u64::MAX);
            assert_eq!(response.status, 599);
            assert_eq!(
                response.headers,
                [
                    ("X-Repeat".into(), "first".into()),
                    ("X-Repeat".into(), "second".into())
                ]
            );
            assert_eq!(response.body.bytes().unwrap(), b"\xff\0response");
            assert_eq!(received, stamp());
        }
    }
}
