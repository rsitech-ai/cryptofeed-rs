use super::*;

fn identity() -> OperationIdentity {
    OperationIdentity {
        venue: 21,
        session: 77,
        bootstrap_id: "a".repeat(64),
    }
}
fn unit(mids: bool) -> ObservedEventBatch {
    // Independent typed wire golden; layout/codec test, not admitted market data.
    let mut native: NativeObservationAttachment = serde_json::from_str(include_str!(
        "../../adapter-api/tests/fixtures/native_observations_v1.json"
    ))
    .unwrap();
    if !mids {
        native.rows.pop();
    }
    let events = (0..5)
        .map(|index| EventEnvelope {
            schema_version: 1,
            venue: VenueId(21),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(9),
            session: SessionId(77),
            frame_seq: 5,
            event_index: index,
            exchange_ts: None,
            receive_ts: TimestampNs(i64::MIN),
            source_sequence: None,
            flags: EventFlags::empty(),
            payload: MarketEvent::Trade(Trade {
                provenance: None,
                price: Price(Fixed::new(i128::MAX, 255)),
                quantity: Quantity(Fixed::new(1, 20)),
                aggressor: AggressorSide::Unknown,
                trade_id: None,
            }),
        })
        .collect();
    ObservedEventBatch::try_new(
        EventBatch {
            session: SessionId(77),
            frame_seq: 5,
            events,
        },
        native,
        ObservedBatchSource {
            venue: VenueId(21),
            connection: ConnectionId(9),
            received: FrameStamp {
                receive_ts: TimestampNs(i64::MIN),
                mono_ns: u64::MAX,
            },
        },
        ObservedBatchLimits::HARD_MAX,
    )
    .unwrap()
}
struct Emit(bool);
impl SessionMachine for Emit {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            out.push(SessionAction::EmitObservedBatch(unit(self.0)));
            out.push(SessionAction::EmitObservedBatch(unit(self.0)));
        }
        Ok(())
    }
}
fn capture(mids: bool) -> CapturedOperation {
    let mut owner = CaptureSession::new(
        Box::new(Emit(mids)),
        CaptureConfig {
            venue: VenueId(21),
            session: SessionId(77),
            dispatch_capacity: 1,
            overflow: OverflowPolicy::DropOldest,
            max_bytes: 8 << 20,
            max_entries: 8192,
        },
    )
    .unwrap();
    owner
        .execute_turn(
            CaptureInput::Text {
                payload: b"native".to_vec(),
                received: FrameStamp {
                    receive_ts: TimestampNs(i64::MIN),
                    mono_ns: u64::MAX,
                },
            },
            CaptureDrainPolicy::NullDispatchTakeOutbound,
        )
        .unwrap()
}
fn history(operation: OperationDto) -> OwnerHistoryDto {
    OwnerHistoryDto {
        schema_version: 3,
        route: CausalRoute {
            venue: 21,
            session: 77,
            ordinal: 10,
            generation: operation.route.generation,
            bootstrap_id: identity().bootstrap_id,
        },
        observed_ns: 20,
        event: OwnerEventDto::ExecutionCompleted {
            input_id: 1,
            permit_ordinal: 5,
            operation: Box::new(operation),
        },
    }
}

#[test]
fn native_operation_schema_roundtrips_all_financial_leaves_and_actual_queue_fates() {
    let capture = capture(true);
    let operation = OperationDto::from_observed_capture(&capture, &identity()).unwrap();
    assert_eq!(operation.schema_version, 2);
    assert!(operation.matches_capture(&capture, &identity()).unwrap());
    let dto = history(operation.clone());
    let envelope = encode_owner_history(&dto, &identity(), MAX_OPERATION_BYTES).unwrap();
    assert_eq!(envelope.schema_version, 4);
    let restored = decode_owner_history(&envelope, &identity()).unwrap();
    assert_eq!(restored, dto);
    let leaf = ObservedBatchJson::new(&unit(true)).unwrap();
    let decoded: ObservedBatchJson =
        serde_json::from_slice(&serde_json::to_vec(&leaf).unwrap()).unwrap();
    assert_eq!(decoded.batch().unwrap(), unit(true));
    assert!(operation.entries.iter().any(|e| matches!(
        &e.effect,
        EffectDto::QueueAttempt {
            incoming: ItemDto::ObservedMarket(_),
            evicted_front: Some(ItemDto::ObservedMarket(_)),
            result: Some(Ok(PushDto::DroppedOldest { .. })),
            ..
        }
    )));
    assert!(operation.entries.iter().any(
        |e| matches!(&e.effect, EffectDto::QueueDrain { items, completed: true, .. }
        if items.iter().any(|i| matches!(i, ItemDto::ObservedMarket(_))))
    ));
}

#[test]
fn native_schema_downgrades_and_cross_profile_owner_wrapping_fail_closed() {
    let capture = capture(false);
    assert!(OperationDto::from_capture(&capture, &identity()).is_err());
    let native = OperationDto::from_observed_capture(&capture, &identity()).unwrap();
    let mut dto = history(native.clone());
    dto.schema_version = 2;
    assert!(encode_owner_history(&dto, &identity(), MAX_OPERATION_BYTES).is_err());
    let mut envelope =
        encode_owner_history(&history(native.clone()), &identity(), MAX_OPERATION_BYTES).unwrap();
    envelope.schema_version = 3;
    assert!(decode_owner_history(&envelope, &identity()).is_err());
    let mut downgrade = native;
    downgrade.schema_version = 1;
    assert!(downgrade.validate().is_err());
}

#[test]
fn native_leaves_refuse_noncanonical_content_and_unknown_fields_without_float_buffering() {
    let leaf = ObservedBatchJson::new(&unit(false)).unwrap();
    let spaced = serde_json::to_string(&format!(" {}", leaf.0)).unwrap();
    assert!(serde_json::from_str::<ObservedBatchJson>(&spaced).is_err());
    let unknown = leaf.0.replacen('{', "{\"unknown\":1,", 1);
    assert!(
        serde_json::from_str::<ObservedBatchJson>(&serde_json::to_string(&unknown).unwrap())
            .is_err()
    );
    let mut bad = unit(false).native().clone();
    bad.rows[0].canonical_positions = vec![1];
    assert!(
        ObservedEventBatch::try_new(
            unit(false).canonical().clone(),
            bad,
            unit(false).source(),
            ObservedBatchLimits::HARD_MAX
        )
        .is_err()
    );
}

#[test]
fn native_record_byte_cap_and_bootstrap_family_limits_are_exact() {
    let operation = OperationDto::from_observed_capture(&capture(false), &identity()).unwrap();
    operation
        .validate_observation_policy(Some(NativeObservationPolicyDto::HyperliquidV1 {
            limits: ObservedBatchLimits::HARD_MAX,
        }))
        .unwrap();
    assert!(
        operation
            .validate_observation_policy(Some(NativeObservationPolicyDto::CanonicalOnlyV1))
            .is_err()
    );
    assert!(
        operation
            .validate_observation_policy(Some(NativeObservationPolicyDto::HyperliquidV1 {
                limits: ObservedBatchLimits {
                    encoded_bytes: 1 << 20,
                    ..ObservedBatchLimits::HARD_MAX
                }
            }))
            .is_err()
    );
    let mids = OperationDto::from_observed_capture(&capture(true), &identity()).unwrap();
    assert!(
        mids.validate_observation_policy(Some(NativeObservationPolicyDto::HyperliquidV1 {
            limits: ObservedBatchLimits::HARD_MAX
        }))
        .is_err()
    );
    let dto = history(operation);
    let bytes = serde_json::to_vec(&dto).unwrap();
    assert_eq!(
        encode_owner_history(&dto, &identity(), bytes.len())
            .unwrap()
            .payload()
            .unwrap(),
        bytes
    );
    assert!(matches!(
        encode_owner_history(&dto, &identity(), bytes.len() - 1),
        Err(CodecError::TooLarge)
    ));
}
