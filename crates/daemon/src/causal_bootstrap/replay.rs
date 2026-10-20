//! Actual-engine replay of one committed MFCJ3 disk/null owner, in physical
//! order. No transport, external dispatch, live receipt or writable feed exists
//! here. Recorded release dispositions remain distinct from native execution.
use super::*;
use marketfeed_adapter_api::{DisconnectReason, HttpResponse, SessionCommand};
use marketfeed_engine::{CaptureDrainPolicy, CaptureInput, EngineError};
use marketfeed_model::{ConnectionId, InstrumentId, TimestampNs};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Native(#[from] EngineError),
    #[error(
        "native replay mismatch at owner record {journal_ordinal}, native operation {native_ordinal}"
    )]
    NativeMismatch {
        journal_ordinal: u64,
        native_ordinal: u64,
    },
    #[error("unsupported or invalid owner replay: {0}")]
    Invalid(&'static str),
    #[error("owner replay is poisoned; reopen and reconstruct the verified prefix")]
    Poisoned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayProgress {
    pub cursor: FeedCursor,
    pub journal_ordinal: u64,
    /// Completed native turns actually reexecuted and compared, not permits.
    pub native_ordinal: u64,
    pub pending_inputs: usize,
    pub terminal: bool,
    /// Storage closure only; this does not resolve execution uncertainties.
    pub sealed: bool,
    pub uncertainties: Vec<OwnerPrefixUncertainty>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayStep {
    pub cursor: FeedCursor,
    /// Only a committed HandoffObserved after exact native equality. Internal
    /// observations do not establish external delivery or production readiness.
    pub handoff: Option<RecordedOwnerHandoff>,
}

/// Private persistent engine, bounded structural ledger and committed disk
/// cursor. This first reader explicitly rejects other profiles and additional
/// owners. Historical artifacts require their matching executable/recipe.
pub struct HistoryReplay {
    reader: FeedReader,
    history: OwnerHistory,
    capture: CaptureSession,
    sealed: bool,
    poisoned: bool,
}
impl HistoryReplay {
    pub fn open(path: impl AsRef<Path>, limits: OwnerHistoryLimits) -> Result<Self, ReplayError> {
        Self::open_bound(path, limits, None)
    }
    fn open_bound(
        path: impl AsRef<Path>,
        limits: OwnerHistoryLimits,
        target: Option<&FeedCursor>,
    ) -> Result<Self, ReplayError> {
        if !cfg!(feature = "causal-journal") {
            return Err(ReplayError::Invalid("causal-journal feature disabled"));
        }
        let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV3)?;
        if let Some(target) = target {
            let bound = reader.observed_commit();
            if target.feed_id != bound.feed_id
                || target.sequence > bound.sequence
                || target.offset > bound.offset
                || target.offset <= reader.cursor().offset
            {
                return Err(ReplayError::Invalid(
                    "resume exceeds initial committed bound or changes feed",
                ));
            }
        }
        let Some(FeedRecord::Causal(envelope)) = reader.read_next()? else {
            return Err(ReplayError::Invalid(
                "first committed record must be the bootstrap",
            ));
        };
        if envelope.phase != CausalPhase::Bootstrap
            || envelope.route.ordinal != 0
            || envelope.route.generation != 0
        {
            return Err(ReplayError::Invalid(
                "first committed record must be a fresh bootstrap",
            ));
        }
        if target.is_some_and(|target| {
            reader.cursor().sequence > target.sequence
                || reader.cursor().offset > target.offset
                || (reader.cursor().sequence == target.sequence && reader.cursor() != target)
        }) {
            return Err(ReplayError::Invalid("resume boundary crosses bootstrap"));
        }
        // Bounds, supported factory/catalog/policies and artifact are checked
        // before any factory creation. No prescan for later metadata occurs.
        if envelope.payload()?.len() > limits.max_record_bytes {
            return Err(CodecError::TooLarge.into());
        }
        let history = OwnerHistory::from_bootstrap(&envelope, limits)?;
        let dto = decode_bootstrap(&envelope)?;
        validate_supported(&dto)?;
        if dto.build != current_build()? {
            return Err(
                BootstrapError::Unsupported("replay build or actual executable mismatch").into(),
            );
        }
        let capture = capture_from_supported(&dto)?;
        Ok(Self {
            reader,
            history,
            capture,
            sealed: false,
            poisoned: false,
        })
    }

    /// Reexecute from the bootstrap to the exact cursor; never initialize a
    /// fresh machine at a later storage cursor. Prior handoffs are discarded.
    /// This reconstructs a prefix, not a native checkpoint or restart authority.
    pub fn resume(
        path: impl AsRef<Path>,
        target: FeedCursor,
        limits: OwnerHistoryLimits,
    ) -> Result<Self, ReplayError> {
        if target.sequence == 0 {
            return Err(ReplayError::Invalid("resume cursor precedes bootstrap"));
        }
        let mut replay = Self::open_bound(path, limits, Some(&target))?;
        while replay.reader.cursor().sequence < target.sequence {
            if replay.advance(Some(&target))?.is_none() {
                return Err(ReplayError::Invalid("resume exceeds committed prefix"));
            }
        }
        if replay.reader.cursor() != &target {
            return Err(ReplayError::Invalid(
                "resume is not an exact physical boundary",
            ));
        }
        Ok(replay)
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn progress(&self) -> Result<ReplayProgress, ReplayError> {
        if self.poisoned {
            return Err(ReplayError::Poisoned);
        }
        Ok(ReplayProgress {
            cursor: self.reader.cursor().clone(),
            journal_ordinal: self.history.ordinal(),
            native_ordinal: self.history.native_ordinal(),
            pending_inputs: self.history.pending_inputs(),
            terminal: self.history.is_terminal(),
            sealed: self.sealed,
            uncertainties: self.history.uncertainties(),
        })
    }
    /// None means caught up to the durable prefix. A later append may supply
    /// another record. An error poisons every subsequent read/progress request.
    pub fn read_next(&mut self) -> Result<Option<ReplayStep>, ReplayError> {
        if self.poisoned {
            return Err(ReplayError::Poisoned);
        }
        let result = self.advance(None);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn advance(&mut self, ceiling: Option<&FeedCursor>) -> Result<Option<ReplayStep>, ReplayError> {
        let Some(record) = self.reader.read_next()? else {
            return Ok(None);
        };
        if ceiling.is_some_and(|target| {
            self.reader.cursor().sequence > target.sequence
                || self.reader.cursor().offset > target.offset
                || (self.reader.cursor().sequence == target.sequence
                    && self.reader.cursor() != target)
        }) {
            return Err(ReplayError::Invalid("record crosses exact resume boundary"));
        }
        let handoff = match record {
            FeedRecord::Causal(envelope) => {
                if envelope.phase == CausalPhase::Bootstrap {
                    return Err(ReplayError::Invalid(
                        "additional bootstrap/owner is unsupported",
                    ));
                }
                // Validate ingress/permit/order/source/bounds before executing
                // any untrusted completion. A subsequent mismatch is terminal.
                let handoff = self.history.apply(&envelope)?;
                if envelope.phase == CausalPhase::Operation {
                    let dto = decode_owner_history(&envelope, self.history.identity())?;
                    let OwnerEventDto::ExecutionCompleted { operation, .. } = dto.event else {
                        return Err(ReplayError::Invalid("expected MFCJ3 completion"));
                    };
                    let native = self.capture.execute_turn(
                        owner_input(&operation.input)?,
                        CaptureDrainPolicy::NullDispatchTakeOutbound,
                    )?;
                    let actual = OperationDto::from_capture(&native, self.history.identity())?;
                    if actual != *operation {
                        return Err(ReplayError::NativeMismatch {
                            journal_ordinal: envelope.route.ordinal,
                            native_ordinal: operation.route.ordinal,
                        });
                    }
                }
                handoff
            }
            FeedRecord::Seal { .. } => {
                self.sealed = true;
                None
            }
            _ => return Err(ReplayError::Invalid("non-owner record in owner journal")),
        };
        Ok(Some(ReplayStep {
            cursor: self.reader.cursor().clone(),
            handoff,
        }))
    }
}

fn disconnect(reason: DisconnectDto) -> DisconnectReason {
    match reason {
        DisconnectDto::RemoteClose => DisconnectReason::RemoteClose,
        DisconnectDto::TransportError => DisconnectReason::TransportError,
        DisconnectDto::HeartbeatTimeout => DisconnectReason::HeartbeatTimeout,
        DisconnectDto::LocalStop => DisconnectReason::LocalStop,
        DisconnectDto::ReconnectRequested => DisconnectReason::ReconnectRequested,
    }
}
/// Decode exactly the owner-admitted input subset. No error-string interning,
/// generic JSON/financial conversion or invented sink completions occur here.
fn owner_input(input: &InputDto) -> Result<CaptureInput, ReplayError> {
    Ok(match input {
        InputDto::Connected { connection, now } => CaptureInput::Connected {
            connection: ConnectionId(*connection),
            now: TimestampNs(*now),
        },
        InputDto::Disconnected { reason, now } => CaptureInput::Disconnected {
            reason: disconnect(*reason),
            now: TimestampNs(*now),
        },
        InputDto::TransportLost { reason, now } => CaptureInput::TransportLost {
            reason: disconnect(*reason),
            now: TimestampNs(*now),
        },
        InputDto::Text { payload, received } => CaptureInput::Text {
            payload: payload.bytes()?,
            received: *received,
        },
        InputDto::Binary { payload, received } => CaptureInput::Binary {
            payload: payload.bytes()?,
            received: *received,
        },
        InputDto::Ping { payload, received } => CaptureInput::Ping {
            payload: payload.bytes()?,
            received: *received,
        },
        InputDto::Pong { payload, received } => CaptureInput::Pong {
            payload: payload.bytes()?,
            received: *received,
        },
        InputDto::Close { payload, received } => CaptureInput::Close {
            payload: payload.bytes()?,
            received: *received,
        },
        InputDto::Http {
            request_id,
            response,
            received,
        } => CaptureInput::Http {
            request_id: *request_id,
            response: HttpResponse {
                status: response.status,
                headers: response.headers.clone(),
                body: response.body.bytes()?.into(),
            },
            received: *received,
        },
        InputDto::TimerPoll { now } => CaptureInput::TimerPoll {
            now: TimestampNs(*now),
        },
        InputDto::Control {
            command: CommandDto::Resync(instrument),
            now,
        } => CaptureInput::Control {
            command: SessionCommand::Resync(InstrumentId(*instrument)),
            now: TimestampNs(*now),
        },
        InputDto::Control {
            command: CommandDto::Stop,
            now,
        } => CaptureInput::Control {
            command: SessionCommand::Stop,
            now: TimestampNs(*now),
        },
        InputDto::Stop => CaptureInput::Stop,
        _ => {
            return Err(ReplayError::Invalid(
                "input is outside the negotiated owner subset",
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::FrameStamp;

    #[test]
    fn owner_input_roundtrip_preserves_every_admitted_variant_and_disconnect_cause() {
        let stamp = FrameStamp {
            receive_ts: TimestampNs(-7),
            mono_ns: u64::MAX,
        };
        let mut inputs = vec![
            CaptureInput::Connected {
                connection: ConnectionId(93),
                now: TimestampNs(-3),
            },
            CaptureInput::Text {
                payload: vec![0, 255, 13, 10],
                received: stamp,
            },
            CaptureInput::Binary {
                payload: vec![255, 0, 1],
                received: stamp,
            },
            CaptureInput::Ping {
                payload: vec![0, 128],
                received: stamp,
            },
            CaptureInput::Pong {
                payload: vec![9, 0, 255],
                received: stamp,
            },
            CaptureInput::Close {
                payload: vec![3, 232, 0, 255],
                received: stamp,
            },
            CaptureInput::Http {
                request_id: u64::MAX,
                response: HttpResponse {
                    status: 503,
                    headers: vec![
                        ("X-Order".into(), "second".into()),
                        ("X-Order".into(), "first".into()),
                    ],
                    body: vec![0, 255, 1].into(),
                },
                received: stamp,
            },
            CaptureInput::TimerPoll {
                now: TimestampNs(-99),
            },
            CaptureInput::Control {
                command: SessionCommand::Resync(InstrumentId(u32::MAX)),
                now: TimestampNs(-1),
            },
            CaptureInput::Control {
                command: SessionCommand::Stop,
                now: TimestampNs(101),
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
                now: TimestampNs(-100),
            });
            inputs.push(CaptureInput::TransportLost {
                reason,
                now: TimestampNs(7),
            });
        }
        let identity = OperationIdentity {
            venue: 1,
            session: 7,
            bootstrap_id: "a".repeat(64),
        };
        for original in inputs {
            let OwnerEventDto::Ingress { input, .. } =
                OwnerEventDto::ingress(1, ConnectionId(93), 100, 4096, &original, &identity)
                    .unwrap()
            else {
                panic!()
            };
            assert_eq!(owner_input(&input).unwrap(), original);
        }
        for unbound in [
            InputDto::NullDispatch,
            InputDto::TakeWrites,
            InputDto::TakeHttp,
            InputDto::ForwardDispatch { results: vec![] },
        ] {
            assert!(owner_input(&unbound).is_err());
        }
    }

    #[cfg(not(feature = "causal-journal"))]
    #[test]
    fn disabled_replay_rejects_before_filesystem_access() {
        assert!(matches!(
            HistoryReplay::open(
                "missing-causal-feed",
                OwnerHistoryLimits {
                    max_retained_input_bytes: 1,
                    max_input_bytes: 1,
                    max_record_bytes: 1
                }
            ),
            Err(ReplayError::Invalid("causal-journal feature disabled"))
        ));
    }
}
