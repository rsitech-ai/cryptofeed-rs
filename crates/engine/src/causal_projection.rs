//! MFOP1 compact consumer groups. Only the daemon's verified source projector
//! grants native release authority; this codec/reader validates target structure.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

#[path = "causal_projection_reader.rs"]
mod reader;
pub use reader::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionPolicy {
    pub schema_version: u16,
    pub source_profile: u16,
    pub clock_policy: ProjectionClockPolicy,
    pub limits: ProjectionLimits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionClockPolicy {
    PerOwnerStartedPhysicalOrderV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionLimits {
    pub max_group_bytes: usize,
    pub max_items: usize,
    pub max_owners: usize,
    pub max_bootstrap_bytes: usize,
    pub max_catalog_entries: usize,
    pub max_pending_inputs: usize,
}
impl ProjectionLimits {
    pub fn validate(&self) -> Result<(), CodecError> {
        if self.max_group_bytes == 0
            || self.max_group_bytes > MAX_PROJECTION_PAYLOAD
            || self.max_items == 0
            || self.max_items > 65_536
            || self.max_owners == 0
            || self.max_owners > 4096
            || self.max_bootstrap_bytes == 0
            || self.max_bootstrap_bytes > 128 << 20
            || self.max_catalog_entries == 0
            || self.max_catalog_entries > 65_536
            || self.max_pending_inputs == 0
            || self.max_pending_inputs > 65_536
        {
            return Err(invalid("projection limits"));
        }
        Ok(())
    }
    pub(super) fn within(&self, cap: &Self) -> bool {
        self.max_group_bytes <= cap.max_group_bytes
            && self.max_items <= cap.max_items
            && self.max_owners <= cap.max_owners
            && self.max_bootstrap_bytes <= cap.max_bootstrap_bytes
            && self.max_catalog_entries <= cap.max_catalog_entries
            && self.max_pending_inputs <= cap.max_pending_inputs
    }
}
impl ProjectionPolicy {
    pub fn validate(&self) -> Result<(), CodecError> {
        self.limits.validate()?;
        if self.schema_version != 1 || !matches!(self.source_profile, 3 | 4) {
            return Err(invalid("projection policy/version/source profile"));
        }
        Ok(())
    }
    pub fn source_feed_profile(&self) -> Result<FeedProfile, CodecError> {
        self.validate()?;
        Ok(if self.source_profile == 4 {
            FeedProfile::EngineCausalV4
        } else {
            FeedProfile::EngineCausalV3
        })
    }
    pub fn id(&self) -> Result<String, CodecError> {
        self.validate()?;
        let mut hash = Sha256::new();
        hash.update(b"marketfeed-projection-policy-v1\0");
        hash.update(serde_json::to_vec(self)?);
        Ok(format!("{:x}", hash.finalize()))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionClock {
    Unstarted,
    Owner {
        started_unix_ns: i64,
        observed_ns: u64,
        approximate_unix_ns: Option<i64>,
    },
    StorageSeal {
        recorded_now_ns: i64,
    },
}
impl ProjectionClock {
    pub fn owner(origin: i64, observed: u64) -> Self {
        Self::Owner {
            started_unix_ns: origin,
            observed_ns: observed,
            approximate_unix_ns: i64::try_from(i128::from(origin) + i128::from(observed)).ok(),
        }
    }
}
/// The owner's admitted control subset. Keep pending input references fixed-size;
/// subscription lists are neither admitted owner controls nor retained metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionCommand {
    Resync(u32),
    Stop,
}
/// Exact source clocks/control, with no raw market payload to decode again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionInput {
    Connected {
        connection: u64,
        now: i64,
    },
    Disconnected {
        reason: DisconnectDto,
        now: i64,
    },
    TransportLost {
        reason: DisconnectDto,
        now: i64,
    },
    Text(FrameStamp),
    Binary(FrameStamp),
    Ping(FrameStamp),
    Pong(FrameStamp),
    Close(FrameStamp),
    Http {
        request_id: u64,
        status: u16,
        received: FrameStamp,
    },
    TimerPoll {
        now: i64,
    },
    Control {
        command: ProjectionCommand,
        now: i64,
    },
    Stop,
}
impl ProjectionInput {
    pub fn from_input(input: &InputDto) -> Result<Self, CodecError> {
        Ok(match input {
            InputDto::Connected { connection, now } => Self::Connected {
                connection: *connection,
                now: *now,
            },
            InputDto::Disconnected { reason, now } => Self::Disconnected {
                reason: *reason,
                now: *now,
            },
            InputDto::TransportLost { reason, now } => Self::TransportLost {
                reason: *reason,
                now: *now,
            },
            InputDto::Text { received, .. } => Self::Text(*received),
            InputDto::Binary { received, .. } => Self::Binary(*received),
            InputDto::Ping { received, .. } => Self::Ping(*received),
            InputDto::Pong { received, .. } => Self::Pong(*received),
            InputDto::Close { received, .. } => Self::Close(*received),
            InputDto::Http {
                request_id,
                response,
                received,
            } => Self::Http {
                request_id: *request_id,
                status: response.status,
                received: *received,
            },
            InputDto::TimerPoll { now } => Self::TimerPoll { now: *now },
            InputDto::Control { command, now } => Self::Control {
                command: match command {
                    CommandDto::Resync(instrument) => ProjectionCommand::Resync(*instrument),
                    CommandDto::Stop => ProjectionCommand::Stop,
                    _ => return Err(invalid("projection control outside owner subset")),
                },
                now: *now,
            },
            InputDto::Stop => Self::Stop,
            _ => return Err(invalid("projection input outside owner subset")),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionOwnerEvent {
    Started {
        unix_ns: i64,
    },
    Ingress {
        input_id: u64,
        connection: u64,
        deadline_ns: u64,
        owned_bytes_bound: u64,
        input: ProjectionInput,
    },
    ExecutionPermitted {
        input_id: u64,
        native_ordinal: u64,
        generation: u64,
    },
    ExecutionCompleted {
        input_id: u64,
        permit_ordinal: u64,
        operation_route: CausalRoute,
        input: ProjectionInput,
        outcome: Result<(), EngineErrorDto>,
        stopped: bool,
        reconnect_requested: bool,
        unsafe_state: bool,
        connection: u64,
        frame_seq: u64,
    },
    ExecutionNotInvoked {
        input_id: u64,
        permit_ordinal: u64,
        cause: OwnerNotExecutedCause,
    },
    ExecutionIndeterminate {
        input_id: u64,
        permit_ordinal: u64,
        cause: OwnerIndeterminateCause,
    },
    ReleaseIntent {
        input_id: u64,
        operation_ordinal: u64,
        outputs_eligible: bool,
    },
    HandoffObserved {
        input_id: u64,
        intent_ordinal: u64,
    },
    Withheld {
        input_id: u64,
    },
    NotExecuted {
        input_id: u64,
        cause: OwnerNotExecutedCause,
    },
    Revoked {
        through_input_id: u64,
        cause: OwnerTerminalCause,
    },
}
impl ProjectionOwnerEvent {
    pub fn from_event(event: &OwnerEventDto) -> Result<Self, CodecError> {
        Ok(match event {
            OwnerEventDto::Started { unix_ns } => Self::Started { unix_ns: *unix_ns },
            OwnerEventDto::Ingress {
                input_id,
                connection,
                deadline_ns,
                owned_bytes_bound,
                input,
            } => Self::Ingress {
                input_id: *input_id,
                connection: *connection,
                deadline_ns: *deadline_ns,
                owned_bytes_bound: *owned_bytes_bound,
                input: ProjectionInput::from_input(input)?,
            },
            OwnerEventDto::ExecutionPermitted {
                input_id,
                native_ordinal,
                generation,
            } => Self::ExecutionPermitted {
                input_id: *input_id,
                native_ordinal: *native_ordinal,
                generation: *generation,
            },
            OwnerEventDto::ExecutionCompleted {
                input_id,
                permit_ordinal,
                operation,
            } => Self::ExecutionCompleted {
                input_id: *input_id,
                permit_ordinal: *permit_ordinal,
                operation_route: operation.route.clone(),
                input: ProjectionInput::from_input(&operation.input)?,
                outcome: operation.outcome.clone(),
                stopped: operation.after.stop_requested,
                reconnect_requested: operation.after.reconnect_requested,
                frame_seq: operation.after.frame_seq,
                connection: operation.after.connection,
                unsafe_state: operation.outcome.is_err()
                    || (operation.after.stop_requested && !operation.before.stop_requested)
                    || (operation.after.reconnect_requested
                        && !operation.before.reconnect_requested)
                    || operation.after.valid_books < operation.before.valid_books
                    || (operation.before.lifecycle == LifecycleDto::Live
                        && operation.after.lifecycle != LifecycleDto::Live),
            },
            OwnerEventDto::ExecutionNotInvoked {
                input_id,
                permit_ordinal,
                cause,
            } => Self::ExecutionNotInvoked {
                input_id: *input_id,
                permit_ordinal: *permit_ordinal,
                cause: *cause,
            },
            OwnerEventDto::ExecutionIndeterminate {
                input_id,
                permit_ordinal,
                cause,
            } => Self::ExecutionIndeterminate {
                input_id: *input_id,
                permit_ordinal: *permit_ordinal,
                cause: *cause,
            },
            OwnerEventDto::ReleaseIntent {
                input_id,
                operation_ordinal,
                outputs_eligible,
            } => Self::ReleaseIntent {
                input_id: *input_id,
                operation_ordinal: *operation_ordinal,
                outputs_eligible: *outputs_eligible,
            },
            OwnerEventDto::HandoffObserved {
                input_id,
                intent_ordinal,
            } => Self::HandoffObserved {
                input_id: *input_id,
                intent_ordinal: *intent_ordinal,
            },
            OwnerEventDto::Withheld { input_id } => Self::Withheld {
                input_id: *input_id,
            },
            OwnerEventDto::NotExecuted { input_id, cause } => Self::NotExecuted {
                input_id: *input_id,
                cause: *cause,
            },
            OwnerEventDto::Revoked {
                through_input_id,
                cause,
            } => Self::Revoked {
                through_input_id: *through_input_id,
                cause: *cause,
            },
            _ => return Err(invalid("legacy owner event in projection")),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionOwnerProgress {
    pub identity: OperationIdentity,
    pub journal_ordinal: u64,
    pub native_ordinal: u64,
    pub terminal: bool,
    pub uncertainties: Vec<OwnerPrefixUncertainty>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionSource {
    Bootstrap(Box<BootstrapDto>),
    Owner {
        route: CausalRoute,
        observed_ns: u64,
        event: ProjectionOwnerEvent,
        after: Box<ProjectionOwnerProgress>,
    },
    Seal {
        schema_version: u16,
        now_ns: i64,
        reason: String,
        owners: Vec<ProjectionOwnerProgress>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionRelease {
    pub input_id: u64,
    pub intent_ordinal: u64,
    pub observed_ns: u64,
    pub outputs_eligible: bool,
    pub operation_route: CausalRoute,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionOutbound {
    Write(WriteDto),
    Http(HttpRequestDto),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionEvidence {
    pub source_index: u32,
    pub value: ProjectionOutbound,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionGroup {
    pub schema_version: u16,
    pub policy: ProjectionPolicy,
    pub policy_id: String,
    pub source_before: FeedCursor,
    pub source_after: FeedCursor,
    pub owner: Option<OperationIdentity>,
    pub clock: ProjectionClock,
    pub source: ProjectionSource,
    pub release: Option<ProjectionRelease>,
    pub items: Vec<ItemDto>,
    /// Positions in the exact released drain stream, including outbound evidence.
    pub item_positions: Vec<u32>,
    /// Recorded intent only; not external delivery or market observations.
    pub outbound_evidence: Vec<ProjectionEvidence>,
}
impl ProjectionGroup {
    pub fn encode(&self) -> Result<ProjectionEnvelope, CodecError> {
        self.validate()?;
        let mut buffer = CappedBuffer {
            bytes: Vec::new(),
            max: self.policy.limits.max_group_bytes,
            overflow: false,
        };
        if let Err(error) = serde_json::to_writer(&mut buffer, self) {
            return Err(if buffer.overflow {
                CodecError::TooLarge
            } else {
                error.into()
            });
        }
        Ok(ProjectionEnvelope::new(
            self.source_before.clone(),
            self.source_after.clone(),
            matches!(self.source, ProjectionSource::Seal { .. }),
            &buffer.bytes,
        )?)
    }
    pub fn decode(envelope: &ProjectionEnvelope, limit: usize) -> Result<Self, CodecError> {
        envelope.validate()?;
        let length = envelope.payload_base64.len() / 4 * 3
            - envelope
                .payload_base64
                .bytes()
                .rev()
                .take_while(|b| *b == b'=')
                .count();
        if length > limit {
            return Err(CodecError::TooLarge);
        }
        let raw = envelope.payload()?;
        let group: Self = serde_json::from_slice(&raw)?;
        group.validate()?;
        if raw.len() > group.policy.limits.max_group_bytes
            || group.source_before != envelope.source_before
            || group.source_after != envelope.source_after
            || matches!(group.source, ProjectionSource::Seal { .. }) != envelope.source_sealed
        {
            return Err(invalid("projection inner/outer boundary"));
        }
        let mut canonical = Canonical {
            expected: &raw,
            offset: 0,
        };
        serde_json::to_writer(&mut canonical, &group)?;
        if canonical.offset != raw.len() {
            return Err(invalid("noncanonical projection group"));
        }
        Ok(group)
    }
    fn validate(&self) -> Result<(), CodecError> {
        self.policy.validate()?;
        if self.schema_version != 1
            || self.policy_id != self.policy.id()?
            || self
                .items
                .len()
                .saturating_add(self.outbound_evidence.len())
                > self.policy.limits.max_items
        {
            return Err(invalid("projection schema/policy/item bound"));
        }
        if self.item_positions.len() != self.items.len()
            || self.item_positions.windows(2).any(|p| p[0] >= p[1])
            || self
                .outbound_evidence
                .windows(2)
                .any(|p| p[0].source_index >= p[1].source_index)
        {
            return Err(invalid("projection output ordering"));
        }
        let mut indices = self.item_positions.clone();
        indices.extend(self.outbound_evidence.iter().map(|e| e.source_index));
        indices.sort_unstable();
        if indices.iter().enumerate().any(|(i, p)| *p as usize != i) {
            return Err(invalid("projection output position partition"));
        }
        if self.items.iter().any(|item| {
            !matches!(
                item,
                ItemDto::Market(_) | ItemDto::ObservedMarket(_) | ItemDto::System(_)
            )
        }) {
            return Err(invalid("outbound evidence is not a projected market value"));
        }
        let observed = matches!(
            self.source,
            ProjectionSource::Owner {
                event: ProjectionOwnerEvent::HandoffObserved { .. },
                ..
            }
        );
        if self.release.is_some() != observed
            || ((!self.items.is_empty() || !self.outbound_evidence.is_empty())
                && !self.release.as_ref().is_some_and(|r| r.outputs_eligible))
        {
            return Err(invalid(
                "projection values without observed eligible handoff",
            ));
        }
        match &self.source {
            ProjectionSource::Bootstrap(dto) => {
                let e = encode_bootstrap_for_profile(
                    dto,
                    MAX_OPERATION_BYTES,
                    self.policy.source_feed_profile()?,
                )?;
                if self.owner.as_ref()
                    != Some(&OperationIdentity {
                        venue: e.route.venue,
                        session: e.route.session,
                        bootstrap_id: e.route.bootstrap_id,
                    })
                    || self.clock != ProjectionClock::Unstarted
                {
                    return Err(invalid("projection bootstrap identity/clock"));
                }
            }
            ProjectionSource::Owner {
                route,
                observed_ns,
                event,
                after,
            } => {
                if self.owner.as_ref() != Some(&after.identity)
                    || route.venue != after.identity.venue
                    || route.session != after.identity.session
                    || route.bootstrap_id != after.identity.bootstrap_id
                    || route.ordinal != after.journal_ordinal
                    || after.uncertainties.len() > self.policy.limits.max_pending_inputs
                {
                    return Err(invalid("projection owner scope/status"));
                }
                if let ProjectionOwnerEvent::HandoffObserved {
                    input_id,
                    intent_ordinal,
                } = event
                {
                    let r = self.release.as_ref().expect("checked");
                    if r.input_id != *input_id
                        || r.intent_ordinal != *intent_ordinal
                        || r.observed_ns != *observed_ns
                        || r.operation_route.venue != route.venue
                        || r.operation_route.session != route.session
                        || r.operation_route.bootstrap_id != route.bootstrap_id
                    {
                        return Err(invalid("projection release lineage"));
                    }
                }
            }
            ProjectionSource::Seal {
                schema_version,
                now_ns,
                owners,
                ..
            } => {
                if *schema_version != 1
                    || self.owner.is_some()
                    || self.clock
                        != (ProjectionClock::StorageSeal {
                            recorded_now_ns: *now_ns,
                        })
                    || owners.len() > self.policy.limits.max_owners
                {
                    return Err(invalid("projection seal"));
                }
            }
        }
        Ok(())
    }
}
