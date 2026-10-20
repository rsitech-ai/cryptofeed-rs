//! Independent public-API challenges for complete, correctly bound operations.
use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, SessionAction, SessionInput, SessionMachine,
};
use marketfeed_engine::{
    ActionDisposition, CaptureConfig, CaptureDrainPolicy, CaptureEntry, CaptureInput,
    CaptureSession, CapturedOperation,
    causal::{
        DispositionDto, EffectDto, MAX_OPERATION_BYTES, OperationDto, OperationIdentity,
        decode_operation, encode_operation,
    },
};
use marketfeed_model::{FrameStamp, OverflowPolicy, SessionId, SystemEvent, TimestampNs, VenueId};
use marketfeed_recording::shared_feed::{CausalEnvelope, CausalPhase};
use marketfeed_sinks::SinkError;

struct Emit {
    overflow: bool,
}
impl SessionMachine for Emit {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        actions: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            actions.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
            if self.overflow {
                actions.push(SessionAction::EmitSystem(SystemEvent::RateLimited));
            }
            actions.push(SessionAction::SendText(Bytes::from_static(b"public-wire")));
        }
        Ok(())
    }
}
fn identity() -> OperationIdentity {
    OperationIdentity {
        venue: 21,
        session: 77,
        bootstrap_id: "a".repeat(64),
    }
}
fn owner(overflow: bool) -> CaptureSession {
    CaptureSession::new(
        Box::new(Emit { overflow }),
        CaptureConfig {
            venue: VenueId(21),
            session: SessionId(77),
            dispatch_capacity: 1,
            overflow: OverflowPolicy::FailEngine,
            max_bytes: 1024 * 1024,
            max_entries: 1024,
        },
    )
    .unwrap()
}
fn input() -> CaptureInput {
    CaptureInput::Text {
        payload: b"native-public-input".to_vec(),
        received: FrameStamp {
            receive_ts: TimestampNs(17),
            mono_ns: 23,
        },
    }
}
fn captured(drain: CaptureDrainPolicy) -> CapturedOperation {
    owner(false).execute_turn(input(), drain).unwrap()
}
fn rejects_wire_mutation(operation: &CapturedOperation, mutate: impl FnOnce(&mut OperationDto)) {
    let mut dto = OperationDto::from_capture(operation, &identity()).unwrap();
    mutate(&mut dto);
    // Serialize the altered DTO canonically, so rejection must be semantic.
    let envelope = CausalEnvelope::new(
        CausalPhase::Operation,
        dto.route.clone(),
        &serde_json::to_vec(&dto).unwrap(),
    )
    .unwrap();
    assert!(decode_operation(&envelope, &identity()).is_err());
}

#[test]
fn incomplete_queue_and_action_outcomes_fail_encoding_and_canonical_decoding() {
    let operation = captured(CaptureDrainPolicy::Retain);
    let mut missing = operation.clone();
    let effect = missing
        .entries
        .iter_mut()
        .find_map(|entry| match &mut entry.effect {
            CaptureEntry::QueueAttempt { result, .. } => Some(result),
            _ => None,
        })
        .unwrap();
    assert!(effect.take().is_some());
    assert!(encode_operation(&missing, &identity(), MAX_OPERATION_BYTES).is_err());
    rejects_wire_mutation(&operation, |dto| {
        let result = dto
            .entries
            .iter_mut()
            .find_map(|entry| match &mut entry.effect {
                EffectDto::QueueAttempt { result, .. } => Some(result),
                _ => None,
            })
            .unwrap();
        assert!(result.take().is_some());
    });

    let mut started = operation.clone();
    let actions = started
        .entries
        .iter_mut()
        .find_map(|entry| match &mut entry.effect {
            CaptureEntry::Invocation { actions, .. } => Some(actions),
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        actions[0].disposition,
        ActionDisposition::Completed(Ok(()))
    ));
    actions[0].disposition = ActionDisposition::Started;
    assert!(encode_operation(&started, &identity(), MAX_OPERATION_BYTES).is_err());
    rejects_wire_mutation(&operation, |dto| {
        let actions = dto
            .entries
            .iter_mut()
            .find_map(|entry| match &mut entry.effect {
                EffectDto::Invocation { actions, .. } => Some(actions),
                _ => None,
            })
            .unwrap();
        actions[0].disposition = DispositionDto::Started;
    });
}

#[test]
fn incomplete_drain_and_sink_outcomes_fail_encoding_and_canonical_decoding() {
    let operation = captured(CaptureDrainPolicy::NullDispatchTakeOutbound);
    let mut incomplete = operation.clone();
    let completed = incomplete
        .entries
        .iter_mut()
        .find_map(|entry| match &mut entry.effect {
            CaptureEntry::QueueDrain { completed, .. } => Some(completed),
            _ => None,
        })
        .unwrap();
    assert!(*completed);
    *completed = false;
    assert!(encode_operation(&incomplete, &identity(), MAX_OPERATION_BYTES).is_err());
    rejects_wire_mutation(&operation, |dto| {
        let completed = dto
            .entries
            .iter_mut()
            .find_map(|entry| match &mut entry.effect {
                EffectDto::QueueDrain { completed, .. } => Some(completed),
                _ => None,
            })
            .unwrap();
        *completed = false;
    });

    let mut session = owner(false);
    session.execute(input()).unwrap();
    let operation = session
        .execute(CaptureInput::ForwardDispatch {
            results: vec![Err(SinkError::Io("observed rejection".into()))],
        })
        .unwrap();
    let mut incomplete = operation.clone();
    let result = incomplete
        .entries
        .iter_mut()
        .find_map(|entry| match &mut entry.effect {
            CaptureEntry::SinkAttempt { result, .. } => Some(result),
            _ => None,
        })
        .unwrap();
    assert!(result.take().is_some());
    assert!(encode_operation(&incomplete, &identity(), MAX_OPERATION_BYTES).is_err());
    rejects_wire_mutation(&operation, |dto| {
        let result = dto
            .entries
            .iter_mut()
            .find_map(|entry| match &mut entry.effect {
                EffectDto::SinkAttempt { result, .. } => Some(result),
                _ => None,
            })
            .unwrap();
        assert!(result.take().is_some());
    });
}

#[test]
fn owning_venue_and_session_cannot_be_rebound_during_encoding() {
    let operation = captured(CaptureDrainPolicy::Retain);
    assert!(encode_operation(&operation, &identity(), MAX_OPERATION_BYTES).is_ok());
    for wrong in [
        OperationIdentity {
            venue: 22,
            ..identity()
        },
        OperationIdentity {
            session: 78,
            ..identity()
        },
    ] {
        assert!(OperationDto::from_capture(&operation, &wrong).is_err());
        assert!(encode_operation(&operation, &wrong, MAX_OPERATION_BYTES).is_err());
    }
}

#[test]
fn completed_failure_keeps_legitimate_buffered_suffix_encodable() {
    let operation = owner(true).execute(input()).unwrap();
    assert!(matches!(
        &operation.outcome,
        marketfeed_engine::CaptureOutcome::Returned(Err(_))
    ));
    let actions = operation
        .entries
        .iter()
        .find_map(|entry| match &entry.effect {
            CaptureEntry::Invocation { actions, .. } => Some(actions),
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        actions[0].disposition,
        ActionDisposition::Completed(Ok(()))
    ));
    assert!(matches!(
        actions[1].disposition,
        ActionDisposition::Completed(Err(_))
    ));
    assert_eq!(actions[2].disposition, ActionDisposition::Buffered);
    let envelope = encode_operation(&operation, &identity(), MAX_OPERATION_BYTES).unwrap();
    assert!(
        decode_operation(&envelope, &identity())
            .unwrap()
            .matches_capture(&operation, &identity())
            .unwrap()
    );
}
