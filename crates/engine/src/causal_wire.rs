//! MFCJ5 application-write evidence. No event/DTO grants live send authority.
//! Automatic transport connect/read/pong/close are explicitly outside this profile.
use super::*;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
pub const WIRE_POLICY_ROLE: &str = "hyperliquid-application-wire-v1";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WirePolicyDto {
    pub schema_version: u16,
    pub max_pending_writes: usize,
    pub max_pending_bytes: usize,
    pub transport_controls: TransportControlsDto,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportControlsDto {
    ManagedConnectReadAutomaticPongCloseUnjournaledV1,
}
impl WirePolicyDto {
    pub fn validate(&self, owner: &JournalOwnerPolicyDto) -> Result<(), CodecError> {
        if self.schema_version != 1
            || self.max_pending_writes == 0
            || self.max_pending_writes > 64
            || self.max_pending_writes > owner.max_pending_operations as usize
            || self.max_pending_bytes == 0
            || self.max_pending_bytes > owner.max_pending_bytes as usize
            || self.max_pending_bytes > MAX_OPERATION_BYTES
        {
            return Err(invalid("application wire policy bounds"));
        }
        Ok(())
    }
}
impl BootstrapDto {
    pub fn bind_wire_policy(&mut self, policy: WirePolicyDto) -> Result<(), CodecError> {
        if self
            .public_inputs
            .iter()
            .any(|p| p.role == WIRE_POLICY_ROLE)
        {
            return Err(invalid("duplicate application wire policy"));
        }
        policy.validate(
            self.owner_policy
                .as_ref()
                .ok_or_else(|| invalid("missing owner policy"))?,
        )?;
        self.public_inputs.push(PublicReproductionInputDto::new(
            WIRE_POLICY_ROLE,
            &serde_json::to_vec(&policy)?,
        ));
        Ok(())
    }
    pub fn wire_policy(&self) -> Result<WirePolicyDto, CodecError> {
        if !matches!(self.factory, FactoryDto::HyperliquidPublic { .. })
            || self.output_profile != OutputProfileDto::DiskNullDispatchV1
        {
            return Err(invalid(
                "wire profile requires public Hyperliquid/null dispatch",
            ));
        }
        let mut inputs = self
            .public_inputs
            .iter()
            .filter(|p| p.role == WIRE_POLICY_ROLE);
        let input = inputs
            .next()
            .ok_or_else(|| invalid("missing application wire policy"))?;
        if inputs.next().is_some() {
            return Err(invalid("duplicate application wire policy"));
        }
        if input.bytes > 4096 || input.payload.0.len() > 5464 {
            return Err(CodecError::TooLarge);
        }
        let raw = input.payload.bytes()?;
        if raw.len() as u64 != input.bytes || bootstrap_sha256(&raw) != input.sha256 {
            return Err(invalid("wire policy bytes/digest mismatch"));
        }
        let policy: WirePolicyDto = serde_json::from_slice(&raw)?;
        if serde_json::to_vec(&policy)? != raw {
            return Err(invalid("noncanonical application wire policy"));
        }
        policy.validate(
            self.owner_policy
                .as_ref()
                .ok_or_else(|| invalid("missing owner policy"))?,
        )?;
        if policy
            .max_pending_bytes
            .checked_add(MAX_WIRE_ERROR_UTF8 * 6 + 4096)
            .is_none_or(|n| {
                n > self
                    .history_policy()
                    .map(|p| p.max_record_bytes as usize)
                    .unwrap_or(0)
            })
        {
            return Err(invalid("wire policy cannot fit an owner record"));
        }
        Ok(policy)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireBindingDto {
    pub input_id: u64,
    pub completion_ordinal: u64,
    pub handoff_ordinal: u64,
    pub operation: CausalRoute,
    pub connection: u64,
    pub frame_seq: u64,
    pub input_stamp: Option<FrameStamp>,
    pub drain_position: u32,
    pub frame: WriteDto,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireOutcomeDto {
    Succeeded,
    Failed,
    Unknown,
    NotInvoked,
}
pub const MAX_WIRE_ERROR_UTF8: usize = 4096;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireErrorKindDto {
    Tls,
    Protocol,
    Io,
    Stub,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WireErrorDto {
    Transport(TransportErrorDto),
    /// Exact detail cannot be retained under this fixed profile bound. This is
    /// explicitly indeterminate-capacity evidence, never a truncated error.
    CapacityExceeded {
        kind: WireErrorKindDto,
        utf8_bytes: u64,
        sha256: String,
    },
}
impl WireErrorDto {
    pub fn from_transport(error: TransportError) -> Self {
        let (kind, text) = match error {
            TransportError::Tls(s) => (WireErrorKindDto::Tls, s),
            TransportError::Protocol(s) => (WireErrorKindDto::Protocol, s),
            TransportError::Io(s) => (WireErrorKindDto::Io, s),
            TransportError::Stub(s) => (WireErrorKindDto::Stub, s),
            other => return Self::Transport((&other).into()),
        };
        if text.len() > MAX_WIRE_ERROR_UTF8 {
            let digest = Sha256::digest(text.as_bytes());
            return Self::CapacityExceeded {
                kind,
                utf8_bytes: text.len() as u64,
                sha256: digest.iter().map(|b| format!("{b:02x}")).collect(),
            };
        }
        Self::Transport(match kind {
            WireErrorKindDto::Tls => TransportErrorDto::Tls(text),
            WireErrorKindDto::Protocol => TransportErrorDto::Protocol(text),
            WireErrorKindDto::Io => TransportErrorDto::Io(text),
            WireErrorKindDto::Stub => TransportErrorDto::Stub(text),
        })
    }
    pub fn validate(&self) -> Result<(), CodecError> {
        match self {
            Self::Transport(
                TransportErrorDto::Tls(s)
                | TransportErrorDto::Protocol(s)
                | TransportErrorDto::Io(s)
                | TransportErrorDto::Stub(s),
            ) if s.len() > MAX_WIRE_ERROR_UTF8 => Err(CodecError::TooLarge),
            Self::CapacityExceeded {
                utf8_bytes, sha256, ..
            } if *utf8_bytes <= MAX_WIRE_ERROR_UTF8 as u64
                || sha256.len() != 64
                || !sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
            {
                Err(invalid("wire result capacity evidence"))
            }
            _ => Ok(()),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WireEventDto {
    Discarded {
        binding: Box<WireBindingDto>,
    },
    Intent {
        wire_id: u64,
        binding: Box<WireBindingDto>,
    },
    InvocationPermitted {
        wire_id: u64,
        intent_ordinal: u64,
    },
    Settled {
        wire_id: u64,
        intent_ordinal: u64,
        invocation_ordinal: Option<u64>,
        outcome: WireOutcomeDto,
        detail: Option<WireErrorDto>,
    },
}
impl WireEventDto {
    pub fn validate(&self, route: &CausalRoute) -> Result<(), CodecError> {
        match self {
            Self::Intent { binding, .. } | Self::Discarded { binding } => {
                if matches!(self, Self::Intent { wire_id: 0, .. })
                    || binding.input_id == 0
                    || binding.completion_ordinal == 0
                    || binding.completion_ordinal >= binding.handoff_ordinal
                    || binding.handoff_ordinal >= route.ordinal
                    || binding.operation.venue != route.venue
                    || binding.operation.session != route.session
                    || binding.operation.bootstrap_id != route.bootstrap_id
                    || binding.operation.generation != route.generation
                    || binding.operation.ordinal == 0
                    || binding.connection == 0
                    || !matches!(binding.frame.opcode, OpcodeDto::Text | OpcodeDto::Binary)
                {
                    return Err(invalid("wire binding/ref/owner scope"));
                }
            }
            Self::InvocationPermitted {
                wire_id,
                intent_ordinal,
            }
            | Self::Settled {
                wire_id,
                intent_ordinal,
                ..
            } => {
                if *wire_id == 0 || *intent_ordinal == 0 || *intent_ordinal >= route.ordinal {
                    return Err(invalid("wire event reference"));
                }
                if let Self::Settled {
                    invocation_ordinal: Some(invocation),
                    ..
                } = self
                {
                    if *invocation <= *intent_ordinal || *invocation >= route.ordinal {
                        return Err(invalid("wire invocation reference"));
                    }
                }
            }
        }
        Ok(())
    }
}
/// Derive only from an eligible actual complete handoff, before allocating its
/// bounded copies. Native protocol-control frames remain source evidence only.
pub fn wire_candidates(
    operation: &OperationDto,
    input_id: u64,
    completion: u64,
    handoff: u64,
    policy: &WirePolicyDto,
) -> Result<Vec<WireBindingDto>, CodecError> {
    let stamp = match &operation.input {
        InputDto::Text { received, .. }
        | InputDto::Binary { received, .. }
        | InputDto::Ping { received, .. }
        | InputDto::Pong { received, .. }
        | InputDto::Close { received, .. } => Some(*received),
        _ => None,
    };
    let mut items = operation
        .entries
        .iter()
        .flat_map(|e| match &e.effect {
            EffectDto::QueueDrain {
                items,
                completed: true,
                ..
            } => items.as_slice(),
            _ => &[],
        })
        .enumerate();
    let mut result = Vec::new();
    let mut bytes = 0usize;
    for (position, item) in &mut items {
        if matches!(item, ItemDto::Http(_)) {
            return Err(invalid("runtime HTTP outside application wire profile"));
        }
        let ItemDto::Write(frame) = item else {
            continue;
        };
        if !matches!(frame.opcode, OpcodeDto::Text | OpcodeDto::Binary) {
            continue;
        }
        let encoded = frame
            .payload
            .0
            .len()
            .checked_add(1024)
            .ok_or(CodecError::TooLarge)?;
        bytes = bytes.checked_add(encoded).ok_or(CodecError::TooLarge)?;
        if result.len() >= policy.max_pending_writes || bytes > policy.max_pending_bytes {
            return Err(CodecError::TooLarge);
        }
        result.push(WireBindingDto {
            input_id,
            completion_ordinal: completion,
            handoff_ordinal: handoff,
            operation: operation.route.clone(),
            connection: operation.after.connection,
            frame_seq: operation.after.frame_seq,
            input_stamp: stamp,
            drain_position: u32::try_from(position).map_err(|_| CodecError::TooLarge)?,
            frame: frame.clone(),
        });
    }
    Ok(result)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireLedger {
    policy: WirePolicyDto,
    candidates: VecDeque<WireBindingDto>,
    deadline: u64,
    next_id: u64,
    intent: Option<(u64, u64)>,
    invocation: Option<u64>,
    uncertain: bool,
    uncertain_input: Option<u64>,
}
impl WireLedger {
    pub fn new(policy: WirePolicyDto) -> Self {
        Self {
            policy,
            candidates: VecDeque::new(),
            deadline: 0,
            next_id: 0,
            intent: None,
            invocation: None,
            uncertain: false,
            uncertain_input: None,
        }
    }
    pub fn install(
        &mut self,
        candidates: Vec<WireBindingDto>,
        deadline: u64,
    ) -> Result<(), CodecError> {
        if !self.candidates.is_empty() || self.uncertain {
            return Err(invalid("unsettled wire bundle"));
        }
        let bytes = candidates
            .iter()
            .try_fold(0usize, |n, c| n.checked_add(c.frame.payload.0.len() + 1024))
            .ok_or(CodecError::TooLarge)?;
        if candidates.len() > self.policy.max_pending_writes
            || bytes > self.policy.max_pending_bytes
        {
            return Err(CodecError::TooLarge);
        }
        self.candidates = candidates.into();
        self.deadline = deadline;
        Ok(())
    }
    pub fn policy(&self) -> &WirePolicyDto {
        &self.policy
    }
    pub fn first(&self) -> Option<&WireBindingDto> {
        self.candidates.front()
    }
    pub fn intent(&self) -> Option<(u64, u64)> {
        self.intent
    }
    pub fn deadline(&self) -> u64 {
        self.deadline
    }
    pub fn invocation(&self) -> Option<u64> {
        self.invocation
    }
    pub fn next_id(&self) -> Option<u64> {
        self.next_id.checked_add(1)
    }
    pub fn count(&self) -> usize {
        self.candidates.len()
    }
    pub fn bytes(&self) -> usize {
        self.candidates
            .iter()
            .map(|c| c.frame.payload.0.len() + 1024)
            .sum()
    }
    pub fn uncertain_input(&self) -> Option<u64> {
        self.uncertain_input
            .or_else(|| self.first().map(|b| b.input_id))
    }
    pub fn clean(&self) -> bool {
        self.candidates.is_empty() && !self.uncertain
    }
    pub fn apply(
        &mut self,
        event: &WireEventDto,
        ordinal: u64,
        now: u64,
        source: u64,
        fence: u64,
        active: bool,
    ) -> Result<(), CodecError> {
        let binding = self
            .first()
            .ok_or_else(|| invalid("wire event without eligible native handoff"))?;
        let input_id = binding.input_id;
        let permitted = active
            && source == binding.connection
            && binding.input_id >= fence
            && now < self.deadline
            && !self.uncertain;
        match event {
            WireEventDto::Discarded { binding: actual } => {
                if permitted || self.intent.is_some() || binding != actual.as_ref() {
                    return Err(invalid("wire discard scope/reference"));
                }
                self.candidates.pop_front();
            }
            WireEventDto::Intent {
                wire_id,
                binding: actual,
            } => {
                if !permitted
                    || self.intent.is_some()
                    || self.next_id() != Some(*wire_id)
                    || binding != actual.as_ref()
                {
                    return Err(invalid("wire intent binding/source/fence"));
                }
                self.next_id = *wire_id;
                self.intent = Some((*wire_id, ordinal));
            }
            WireEventDto::InvocationPermitted {
                wire_id,
                intent_ordinal,
            } => {
                if !permitted
                    || self.intent != Some((*wire_id, *intent_ordinal))
                    || self.invocation.is_some()
                {
                    return Err(invalid("wire invocation permit/source/fence"));
                }
                self.invocation = Some(ordinal);
            }
            WireEventDto::Settled {
                wire_id,
                intent_ordinal,
                invocation_ordinal,
                outcome,
                detail,
            } => {
                if let Some(detail) = detail {
                    detail.validate()?;
                }
                if !matches!(
                    (outcome, detail),
                    (WireOutcomeDto::Succeeded | WireOutcomeDto::NotInvoked, None)
                        | (
                            WireOutcomeDto::Unknown,
                            None | Some(WireErrorDto::CapacityExceeded { .. })
                        )
                        | (WireOutcomeDto::Failed, Some(WireErrorDto::Transport(_)))
                ) {
                    return Err(invalid("wire result outcome/detail mismatch"));
                }
                if self.intent != Some((*wire_id, *intent_ordinal))
                    || self.invocation != *invocation_ordinal
                    || (self.invocation.is_none() && *outcome != WireOutcomeDto::NotInvoked)
                {
                    return Err(invalid("wire settlement reference/outcome"));
                }
                self.uncertain |=
                    matches!(outcome, WireOutcomeDto::Failed | WireOutcomeDto::Unknown);
                if self.uncertain {
                    self.uncertain_input = Some(input_id);
                }
                self.candidates.pop_front();
                self.intent = None;
                self.invocation = None;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy() -> WirePolicyDto {
        WirePolicyDto {
            schema_version: 1,
            max_pending_writes: 2,
            max_pending_bytes: 4096,
            transport_controls:
                TransportControlsDto::ManagedConnectReadAutomaticPongCloseUnjournaledV1,
        }
    }
    fn binding() -> WireBindingDto {
        WireBindingDto {
            input_id: 1,
            completion_ordinal: 4,
            handoff_ordinal: 6,
            operation: CausalRoute {
                venue: 22,
                session: 31,
                ordinal: 1,
                generation: 1,
                bootstrap_id: "a".repeat(64),
            },
            connection: 1,
            frame_seq: 0,
            input_stamp: None,
            drain_position: 0,
            frame: WriteDto {
                opcode: OpcodeDto::Text,
                payload: BytesDto::new(b"exact application JSON ping"),
            },
        }
    }
    fn ledger() -> WireLedger {
        let mut l = WireLedger::new(policy());
        l.install(vec![binding()], 100).unwrap();
        l
    }
    #[test]
    fn wire_exact_native_binding_and_scope_mutations_never_get_intent() {
        for mutation in 0..7 {
            let mut l = ledger();
            let mut b = binding();
            match mutation {
                0 => b.frame.payload = BytesDto::new(b"forged"),
                1 => b.drain_position += 1,
                2 => b.handoff_ordinal += 1,
                3 => b.operation.ordinal += 1,
                4 => b.connection += 1,
                5 => b.frame_seq += 1,
                _ => {
                    b.input_stamp = Some(FrameStamp {
                        receive_ts: TimestampNs(1),
                        mono_ns: 2,
                    })
                }
            }
            assert!(
                l.apply(
                    &WireEventDto::Intent {
                        wire_id: 1,
                        binding: Box::new(b)
                    },
                    7,
                    10,
                    1,
                    0,
                    true
                )
                .is_err()
            );
            assert!(l.intent().is_none());
        }
        for (source, fence, active, now) in [
            (2, 0, true, 10),
            (1, 2, true, 10),
            (1, 0, false, 10),
            (1, 0, true, 100),
        ] {
            assert!(
                ledger()
                    .apply(
                        &WireEventDto::Intent {
                            wire_id: 1,
                            binding: Box::new(binding())
                        },
                        7,
                        now,
                        source,
                        fence,
                        active
                    )
                    .is_err()
            );
        }
    }
    #[test]
    fn wire_invocation_result_one_use_and_unknown_never_clean() {
        let mut l = ledger();
        let intent = WireEventDto::Intent {
            wire_id: 1,
            binding: Box::new(binding()),
        };
        l.apply(&intent, 7, 10, 1, 0, true).unwrap();
        assert!(l.apply(&intent, 8, 11, 1, 0, true).is_err());
        assert!(
            l.apply(
                &WireEventDto::Settled {
                    wire_id: 1,
                    intent_ordinal: 7,
                    invocation_ordinal: None,
                    outcome: WireOutcomeDto::Succeeded,
                    detail: None
                },
                8,
                11,
                1,
                0,
                true
            )
            .is_err()
        );
        let permit = WireEventDto::InvocationPermitted {
            wire_id: 1,
            intent_ordinal: 7,
        };
        l.apply(&permit, 8, 11, 1, 0, true).unwrap();
        assert!(l.apply(&permit, 9, 12, 1, 0, true).is_err());
        let done = WireEventDto::Settled {
            wire_id: 1,
            intent_ordinal: 7,
            invocation_ordinal: Some(8),
            outcome: WireOutcomeDto::Unknown,
            detail: None,
        };
        l.apply(&done, 9, 12, 1, 2, false).unwrap();
        assert_eq!(l.count(), 0);
        assert!(!l.clean());
        assert_eq!(l.uncertain_input(), Some(1));
        assert!(l.apply(&done, 10, 13, 1, 2, false).is_err());
    }
    #[test]
    fn wire_retention_caps_are_checked_before_install_and_can_refuse_old_profile_event() {
        let mut l = WireLedger::new(policy());
        assert!(l.install(vec![binding(); 3], 100).is_err());
        assert_eq!(l.count(), 0);
        let mut b = binding();
        b.frame.payload = BytesDto::new(&vec![b'x'; 4096]);
        assert!(l.install(vec![b], 100).is_err());
        assert_eq!(l.bytes(), 0);
        let dto = OwnerHistoryDto {
            schema_version: 3,
            route: CausalRoute {
                venue: 22,
                session: 31,
                ordinal: 7,
                generation: 1,
                bootstrap_id: "a".repeat(64),
            },
            observed_ns: 1,
            event: OwnerEventDto::Wire(WireEventDto::Intent {
                wire_id: 1,
                binding: Box::new(binding()),
            }),
        };
        let id = OperationIdentity {
            venue: 22,
            session: 31,
            bootstrap_id: "a".repeat(64),
        };
        assert!(encode_owner_history(&dto, &id, MAX_OPERATION_BYTES).is_err());
    }
}
