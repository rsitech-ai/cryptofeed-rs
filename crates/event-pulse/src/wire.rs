use std::{cmp::Ordering, collections::BTreeMap};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_I64_U64: u64 = i64::MAX as u64;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireError {
    #[error("invalid canonical decimal")]
    Decimal,
    #[error("invalid RFC3339 timestamp")]
    Time,
    #[error("invalid cursor")]
    Cursor,
    #[error("invalid stable identity")]
    Identity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDecimal(String);
impl CanonicalDecimal {
    pub fn parse(value: &str, max_integer: usize, max_fraction: usize) -> Result<Self, WireError> {
        let bytes = value.as_bytes();
        let max_len = max_integer
            .checked_add(max_fraction)
            .and_then(|value| value.checked_add(2))
            .unwrap_or(usize::MAX);
        if bytes.is_empty() || bytes.len() > max_len {
            return Err(WireError::Decimal);
        }
        let (negative, body) = match bytes[0] {
            b'-' => (true, &bytes[1..]),
            b'+' => return Err(WireError::Decimal),
            _ => (false, bytes),
        };
        let Some((integer, fraction)) = body.split_first().map(|_| {
            if let Some(dot) = body.iter().position(|&b| b == b'.') {
                (&body[..dot], Some(&body[dot + 1..]))
            } else {
                (body, None)
            }
        }) else {
            return Err(WireError::Decimal);
        };
        if integer.is_empty()
            || integer.len() > max_integer
            || integer.iter().any(|b| !b.is_ascii_digit())
            || integer.len() > 1 && integer[0] == b'0'
        {
            return Err(WireError::Decimal);
        }
        if let Some(fraction) = fraction {
            if fraction.is_empty()
                || fraction.len() > max_fraction
                || fraction.iter().any(|b| !b.is_ascii_digit())
            {
                return Err(WireError::Decimal);
            }
        }
        if negative
            && integer.iter().all(|b| *b == b'0')
            && fraction.is_none_or(|x| x.iter().all(|b| *b == b'0'))
        {
            return Err(WireError::Decimal);
        }
        Ok(Self(value.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct Rfc3339Time {
    canonical: String,
    utc_micros: i64,
}
impl Rfc3339Time {
    pub fn parse(value: &str) -> Result<Self, WireError> {
        if value.len() > MAX_INPUT_BYTES || !value.is_ascii() || value.contains(":60") {
            return Err(WireError::Time);
        }
        let time_start = value.find('T').ok_or(WireError::Time)?;
        let zone = value
            .rfind(['Z', '+', '-'])
            .filter(|index| *index > time_start)
            .ok_or(WireError::Time)?;
        let (body, offset) = value.split_at(zone);
        if offset != "Z"
            && (offset.len() != 6
                || offset.as_bytes()[3] != b':'
                || !offset[1..3].bytes().all(|b| b.is_ascii_digit())
                || !offset[4..].bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(WireError::Time);
        }
        let normalized_body = if let Some(dot) = body.rfind('.') {
            let (head, fraction) = body.split_at(dot);
            let digits = &fraction[1..];
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(WireError::Time);
            }
            let micros = format!("{:0<6}", &digits[..digits.len().min(6)]);
            if micros == "000000" {
                head.to_owned()
            } else {
                format!("{head}.{micros}")
            }
        } else {
            body.to_owned()
        };
        let canonical_offset = if offset == "Z" || offset == "+00:00" || offset == "-00:00" {
            "Z"
        } else {
            offset
        };
        let normalized = format!("{normalized_body}{canonical_offset}");
        let parse_input = format!("{normalized_body}{offset}");
        let parsed = OffsetDateTime::parse(&parse_input, &Rfc3339).map_err(|_| WireError::Time)?;
        let nanos = parsed.unix_timestamp_nanos();
        let micros = i64::try_from(nanos / 1_000).map_err(|_| WireError::Time)?;
        Ok(Self {
            canonical: normalized,
            utc_micros: micros,
        })
    }
    pub fn utc_micros(&self) -> i64 {
        self.utc_micros
    }
    pub fn as_str(&self) -> &str {
        &self.canonical
    }
    pub fn canonical(&self) -> &str {
        &self.canonical
    }
}

impl PartialEq for Rfc3339Time {
    fn eq(&self, other: &Self) -> bool {
        self.utc_micros == other.utc_micros
    }
}
impl Eq for Rfc3339Time {}
impl PartialOrd for Rfc3339Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Rfc3339Time {
    fn cmp(&self, other: &Self) -> Ordering {
        self.utc_micros.cmp(&other.utc_micros)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CursorV1 {
    NativeRange {
        start: u64,
        end: u64,
    },
    DerivedAction {
        frame_ordinal: u64,
        action_index: u32,
        item_index: u32,
    },
}
impl CursorV1 {
    pub fn native(start: u64, end: u64) -> Result<Self, WireError> {
        if start > end || end > MAX_I64_U64 {
            return Err(WireError::Cursor);
        }
        Ok(Self::NativeRange { start, end })
    }
    pub fn derived(
        frame_ordinal: u64,
        action_index: u32,
        item_index: u32,
    ) -> Result<Self, WireError> {
        if action_index == u16::MAX as u32 || item_index > u16::MAX as u32 {
            return Err(WireError::Cursor);
        }
        let display = ((u128::from(frame_ordinal) * 65536 + u128::from(action_index)) * 65536)
            + u128::from(item_index);
        if display > u128::from(MAX_I64_U64) {
            return Err(WireError::Cursor);
        }
        Ok(Self::DerivedAction {
            frame_ordinal,
            action_index,
            item_index,
        })
    }
    pub fn derived_drop(frame_ordinal: u64, item_index: u32) -> Result<Self, WireError> {
        if item_index > 2 {
            return Err(WireError::Cursor);
        }
        let cursor = Self::DerivedAction {
            frame_ordinal,
            action_index: u16::MAX as u32,
            item_index,
        };
        cursor.display_sequence()?;
        Ok(cursor)
    }
    pub fn display_sequence(&self) -> Result<u64, WireError> {
        let (frame, action, item) = match self {
            Self::NativeRange { end, .. } => return Ok(*end),
            Self::DerivedAction {
                frame_ordinal,
                action_index,
                item_index,
            } => (*frame_ordinal, *action_index, *item_index),
        };
        let value = ((u128::from(frame) * 65_536 + u128::from(action)) * 65_536) + u128::from(item);
        u64::try_from(value)
            .ok()
            .filter(|value| *value <= MAX_I64_U64)
            .ok_or(WireError::Cursor)
    }
}

/// Raw causal-system-chain preimages.  These bytes are intentionally not JSON.
pub struct SystemChainPreimage;
impl SystemChainPreimage {
    pub fn first(payload_hash: &str) -> Result<Vec<u8>, WireError> {
        Self::build(None, payload_hash)
    }
    pub fn next(predecessor: &str, payload_hash: &str) -> Result<Vec<u8>, WireError> {
        Self::build(Some(predecessor), payload_hash)
    }
    pub fn hash_first(payload_hash: &str) -> Result<String, WireError> {
        Ok(format!("{:x}", Sha256::digest(Self::first(payload_hash)?)))
    }
    pub fn hash_next(predecessor: &str, payload_hash: &str) -> Result<String, WireError> {
        Ok(format!(
            "{:x}",
            Sha256::digest(Self::next(predecessor, payload_hash)?)
        ))
    }
    fn build(predecessor: Option<&str>, payload_hash: &str) -> Result<Vec<u8>, WireError> {
        fn decoded(value: &str) -> Result<[u8; 32], WireError> {
            if value.len() != 64 {
                return Err(WireError::Identity);
            }
            let mut bytes = [0; 32];
            for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => Some(byte - b'0'),
                    b'a'..=b'f' => Some(byte - b'a' + 10),
                    _ => None,
                };
                bytes[index] = digit(pair[0])
                    .zip(digit(pair[1]))
                    .map(|(a, b)| a * 16 + b)
                    .ok_or(WireError::Identity)?;
            }
            Ok(bytes)
        }
        let mut output = b"event-pulse-system-chain-v1\0".to_vec();
        match predecessor {
            None => output.push(0),
            Some(hash) => {
                output.push(1);
                output.extend(decoded(hash)?);
            }
        }
        output.extend(decoded(payload_hash)?);
        Ok(output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionKeyV1(String);
impl ConnectionKeyV1 {
    pub fn new(source_id: &str) -> Result<Self, WireError> {
        if source_id.is_empty()
            || source_id.len() > 128
            || !source_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(WireError::Identity);
        }
        Ok(Self(source_id.into()))
    }
    pub fn source_id(&self) -> &str {
        &self.0
    }
}

fn bounded_identity(value: &str) -> Result<String, WireError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(WireError::Identity);
    }
    Ok(value.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentIdentityV1 {
    pub base_asset: String,
    pub quote_asset: String,
    pub market_type: String,
    pub venue: String,
    pub venue_symbol: String,
}
impl InstrumentIdentityV1 {
    pub fn validate(&self) -> Result<(), WireError> {
        for value in [
            &self.base_asset,
            &self.quote_asset,
            &self.market_type,
            &self.venue,
            &self.venue_symbol,
        ] {
            bounded_identity(value)?;
        }
        if self.base_asset == self.quote_asset {
            return Err(WireError::Identity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContributorKeyV1 {
    pub source_id: String,
    pub instrument: InstrumentIdentityV1,
}
impl ContributorKeyV1 {
    pub fn new(source_id: &str, instrument: InstrumentIdentityV1) -> Result<Self, WireError> {
        let source_id = ConnectionKeyV1::new(source_id)?.0;
        instrument.validate()?;
        Ok(Self {
            source_id,
            instrument,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributorV1 {
    pub key: ContributorKeyV1,
    pub connection_epoch: String,
    pub epoch_generation: u8,
}
impl ContributorV1 {
    pub fn new(
        key: ContributorKeyV1,
        connection_epoch: &str,
        epoch_generation: u8,
    ) -> Result<Self, WireError> {
        if !connection_epoch.starts_with("epoch_") {
            return Err(WireError::Identity);
        }
        let connection_epoch = bounded_identity(connection_epoch)?;
        Ok(Self {
            key,
            connection_epoch,
            epoch_generation,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FamilyV1 {
    Trade,
    Quote,
    Book,
    OpenInterest,
    Liquidation,
    ConfirmationPrice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CursorModeV1 {
    Native,
    Derived,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClockSourceKeyV1 {
    pub source_id: String,
    pub subject: ContributorKeyV1,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockSourceV1 {
    pub key: ClockSourceKeyV1,
    pub epoch: String,
    pub epoch_generation: u8,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockCursorV1(CursorV1);
impl ClockCursorV1 {
    pub fn native(start: u64, end: u64) -> Result<Self, WireError> {
        Ok(Self(CursorV1::native(start, end)?))
    }
    pub fn cursor(&self) -> &CursorV1 {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CoverageSourceKeyV1 {
    pub source_id: String,
    pub subject: ContributorKeyV1,
    pub family: FamilyV1,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageSourceV1 {
    pub key: CoverageSourceKeyV1,
    pub epoch: String,
    pub epoch_generation: u8,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageCursorV1(CursorV1);
impl CoverageCursorV1 {
    pub fn native(start: u64, end: u64) -> Result<Self, WireError> {
        Ok(Self(CursorV1::native(start, end)?))
    }
    pub fn cursor(&self) -> &CursorV1 {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfiguredTargetKeyV1 {
    Contributor(ContributorKeyV1),
    Connection(ConnectionKeyV1),
    Processor(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SystemSourceKeyV1 {
    pub source_id: String,
    pub scope_kind: FaultScopeKindV1,
    pub configured_target_key: ConfiguredTargetKeyV1,
    pub cursor_mode: CursorModeV1,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemSourceV1 {
    pub key: SystemSourceKeyV1,
    pub epoch: String,
    pub epoch_generation: u8,
}
pub type SystemCursorV1 = CursorV1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultScopeKindV1 {
    Contributor,
    ConnectionEpoch,
    Processor,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultScopeV1 {
    Contributor(ContributorV1),
    ConnectionEpoch {
        connection_key: ConnectionKeyV1,
        connection_epoch: String,
        epoch_generation: u8,
    },
    Processor {
        processor_id: String,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropCategoryV1 {
    ActionBuffer,
    MarketDispatch,
    SystemDispatch,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemFaultV1 {
    Disconnected,
    SequenceGap {
        expected: u64,
        actual: u64,
    },
    EventsDropped {
        count: u64,
        category: DropCategoryV1,
    },
    ChecksumMismatch,
    ClockJump {
        delta_ns: i64,
    },
    BookInvalidated,
    BookResynchronized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotAuthoringV1 {
    pub contract_id: String,
    pub lineage_id: String,
    pub event_cluster_id: String,
    pub primary_scope: InstrumentIdentityV1,
    pub revision_start: u64,
    pub predecessor_content_hash: Option<String>,
    pub expected_half_life_ms: u64,
    pub producer_version: String,
}
impl SnapshotAuthoringV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        contract_id: &str,
        lineage_id: &str,
        event_cluster_id: &str,
        primary_scope: InstrumentIdentityV1,
        revision_start: u64,
        predecessor_content_hash: Option<String>,
        expected_half_life_ms: u64,
        producer_version: &str,
    ) -> Result<Self, WireError> {
        primary_scope.validate()?;
        for value in [contract_id, lineage_id, event_cluster_id] {
            bounded_identity(value)?;
        }
        if !contract_id.starts_with("event_pulse_mechanics_")
            || !lineage_id.starts_with("lineage_")
            || !event_cluster_id.starts_with("event_cluster_")
            || producer_version.trim().is_empty()
            || producer_version.len() > 128
        {
            return Err(WireError::Identity);
        }
        let predecessor_valid = match (revision_start, predecessor_content_hash.as_deref()) {
            (1, None) => true,
            (2.., Some(hash)) => SystemChainPreimage::first(hash).is_ok(),
            _ => false,
        };
        if !predecessor_valid || expected_half_life_ms == 0 || expected_half_life_ms > 86_400_000 {
            return Err(WireError::Identity);
        }
        Ok(Self {
            contract_id: contract_id.to_owned(),
            lineage_id: lineage_id.to_owned(),
            event_cluster_id: event_cluster_id.to_owned(),
            primary_scope,
            revision_start,
            predecessor_content_hash,
            expected_half_life_ms,
            producer_version: producer_version.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MechanicsConfigV1 {
    pub processor_id: String,
    pub connections: Vec<ConnectionKeyV1>,
    pub contributors: Vec<ContributorKeyV1>,
    pub contributor_connections: BTreeMap<ContributorKeyV1, ConnectionKeyV1>,
    pub clock_sources: Vec<ClockSourceKeyV1>,
    pub coverage_sources: Vec<CoverageSourceKeyV1>,
    pub system_sources: Vec<SystemSourceKeyV1>,
}
impl MechanicsConfigV1 {
    pub fn new(
        processor_id: &str,
        connections: Vec<ConnectionKeyV1>,
        contributors: Vec<ContributorKeyV1>,
        contributor_connections: BTreeMap<ContributorKeyV1, ConnectionKeyV1>,
        clock_sources: Vec<ClockSourceKeyV1>,
        coverage_sources: Vec<CoverageSourceKeyV1>,
        system_sources: Vec<SystemSourceKeyV1>,
    ) -> Result<Self, WireError> {
        let processor_id = bounded_identity(processor_id)?;
        if connections.is_empty()
            || connections.len() > 32
            || contributors.is_empty()
            || contributors.len() > 32
            || clock_sources.len() > 32
            || coverage_sources.len() > 192
            || system_sources.len() > 130
        {
            return Err(WireError::Identity);
        }
        let contributors_set = contributors
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        let connections_set = connections
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        if connections_set.len() != connections.len()
            || contributors_set.len() != contributors.len()
            || contributor_connections.len() != contributors.len()
            || contributor_connections
                .iter()
                .any(|(contributor, connection)| {
                    !contributors_set.contains(contributor) || !connections_set.contains(connection)
                })
            || clock_sources.len() != contributors.len()
            || clock_sources
                .iter()
                .any(|source| !contributors_set.contains(&source.subject))
            || coverage_sources
                .iter()
                .any(|source| !contributors_set.contains(&source.subject))
        {
            return Err(WireError::Identity);
        }
        let mut source_ids = std::collections::BTreeSet::new();
        for source_id in contributors
            .iter()
            .map(|source| source.source_id.as_str())
            .chain(clock_sources.iter().map(|source| source.source_id.as_str()))
            .chain(
                coverage_sources
                    .iter()
                    .map(|source| source.source_id.as_str()),
            )
            .chain(
                system_sources
                    .iter()
                    .map(|source| source.source_id.as_str()),
            )
        {
            if !source_ids.insert(source_id) {
                return Err(WireError::Identity);
            }
        }
        let unique_clocks = clock_sources
            .iter()
            .map(|source| &source.subject)
            .collect::<std::collections::BTreeSet<_>>();
        let unique_coverage = coverage_sources
            .iter()
            .map(|source| (&source.subject, source.family))
            .collect::<std::collections::BTreeSet<_>>();
        let unique_system = system_sources
            .iter()
            .map(|source| (&source.configured_target_key, source.cursor_mode))
            .collect::<std::collections::BTreeSet<_>>();
        if unique_clocks.len() != clock_sources.len()
            || unique_coverage.len() != coverage_sources.len()
            || unique_system.len() != system_sources.len()
        {
            return Err(WireError::Identity);
        }
        Ok(Self {
            processor_id,
            connections,
            contributors,
            contributor_connections,
            clock_sources,
            coverage_sources,
            system_sources,
        })
    }
}

fn checked_source_epoch(
    source_id: &str,
    epoch: &str,
    epoch_generation: u8,
) -> Result<(String, String, u8), WireError> {
    let source_id = ConnectionKeyV1::new(source_id)?.0;
    if !epoch.starts_with("epoch_") {
        return Err(WireError::Identity);
    }
    Ok((source_id, bounded_identity(epoch)?, epoch_generation))
}

impl ClockSourceKeyV1 {
    pub fn new(source_id: &str, subject: ContributorKeyV1) -> Result<Self, WireError> {
        Ok(Self {
            source_id: ConnectionKeyV1::new(source_id)?.0,
            subject,
        })
    }
}
impl ClockSourceV1 {
    pub fn new(
        key: ClockSourceKeyV1,
        epoch: &str,
        epoch_generation: u8,
    ) -> Result<Self, WireError> {
        let (_, epoch, epoch_generation) =
            checked_source_epoch(&key.source_id, epoch, epoch_generation)?;
        Ok(Self {
            key,
            epoch,
            epoch_generation,
        })
    }
}
impl CoverageSourceKeyV1 {
    pub fn new(
        source_id: &str,
        subject: ContributorKeyV1,
        family: FamilyV1,
    ) -> Result<Self, WireError> {
        Ok(Self {
            source_id: ConnectionKeyV1::new(source_id)?.0,
            subject,
            family,
        })
    }
}
impl CoverageSourceV1 {
    pub fn new(
        key: CoverageSourceKeyV1,
        epoch: &str,
        epoch_generation: u8,
    ) -> Result<Self, WireError> {
        let (_, epoch, epoch_generation) =
            checked_source_epoch(&key.source_id, epoch, epoch_generation)?;
        Ok(Self {
            key,
            epoch,
            epoch_generation,
        })
    }
}
impl SystemSourceKeyV1 {
    pub fn new(
        source_id: &str,
        scope_kind: FaultScopeKindV1,
        configured_target_key: ConfiguredTargetKeyV1,
        cursor_mode: CursorModeV1,
    ) -> Result<Self, WireError> {
        let target_matches = matches!(
            (&scope_kind, &configured_target_key),
            (
                FaultScopeKindV1::Contributor,
                ConfiguredTargetKeyV1::Contributor(_)
            ) | (
                FaultScopeKindV1::ConnectionEpoch,
                ConfiguredTargetKeyV1::Connection(_)
            ) | (
                FaultScopeKindV1::Processor,
                ConfiguredTargetKeyV1::Processor(_)
            )
        );
        if !target_matches {
            return Err(WireError::Identity);
        }
        if let ConfiguredTargetKeyV1::Processor(id) = &configured_target_key {
            bounded_identity(id)?;
        }
        Ok(Self {
            source_id: ConnectionKeyV1::new(source_id)?.0,
            scope_kind,
            configured_target_key,
            cursor_mode,
        })
    }
}
impl SystemSourceV1 {
    pub fn new(
        key: SystemSourceKeyV1,
        epoch: &str,
        epoch_generation: u8,
    ) -> Result<Self, WireError> {
        let (_, epoch, epoch_generation) =
            checked_source_epoch(&key.source_id, epoch, epoch_generation)?;
        Ok(Self {
            key,
            epoch,
            epoch_generation,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockStateV1 {
    Synchronized,
    Degraded,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockQualityV1 {
    Validated,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplayCatalogV1 {
    pub venue_sources: BTreeMap<u16, String>,
    pub instruments: BTreeMap<u32, InstrumentIdentityV1>,
    pub connection_epochs: BTreeMap<(u32, u32), (String, u8)>,
}
impl ReplayCatalogV1 {
    pub fn validate(&self) -> Result<(), WireError> {
        for source in self.venue_sources.values() {
            ConnectionKeyV1::new(source)?;
        }
        for instrument in self.instruments.values() {
            instrument.validate()?;
        }
        for (epoch, _) in self.connection_epochs.values() {
            if !epoch.starts_with("epoch_") {
                return Err(WireError::Identity);
            }
            bounded_identity(epoch)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MechanicsInputV1 {
    Market {
        envelope: marketfeed_model::EventEnvelope,
        action_index: u32,
        catalog: ReplayCatalogV1,
        payload_hash: String,
    },
    System {
        system_source: SystemSourceV1,
        scope: FaultScopeV1,
        occurred_at: Rfc3339Time,
        available_at: Rfc3339Time,
        system_cursor: SystemCursorV1,
        fault: SystemFaultV1,
        predecessor_system_chain_hash: Option<String>,
        payload_hash: String,
    },
    Coverage {
        contributor: ContributorV1,
        coverage_source: CoverageSourceV1,
        family: FamilyV1,
        covered_from: Rfc3339Time,
        covered_through: Rfc3339Time,
        available_at: Rfc3339Time,
        coverage_cursor: CoverageCursorV1,
        payload_hash: String,
    },
    Clock {
        contributor: ContributorV1,
        clock_source: ClockSourceV1,
        observed_at: Rfc3339Time,
        available_at: Rfc3339Time,
        clock_cursor: ClockCursorV1,
        clock_state: ClockStateV1,
        observed_skew_ms: CanonicalDecimal,
        freshness_limit_ms: u64,
        quality_state: ClockQualityV1,
        reason_code: String,
        payload_hash: String,
    },
}

impl MechanicsInputV1 {
    pub fn validate_static(&self) -> Result<(), WireError> {
        let hash = |value: &str| SystemChainPreimage::first(value).map(|_| ());
        match self {
            Self::Market {
                catalog,
                payload_hash,
                ..
            } => {
                catalog.validate()?;
                hash(payload_hash)
            }
            Self::System {
                system_source,
                scope,
                occurred_at,
                available_at,
                system_cursor,
                fault,
                predecessor_system_chain_hash,
                payload_hash,
            } => {
                if occurred_at > available_at {
                    return Err(WireError::Time);
                }
                hash(payload_hash)?;
                if let Some(predecessor) = predecessor_system_chain_hash {
                    hash(predecessor)?;
                }
                validate_system_binding(system_source, scope, system_cursor, fault)
            }
            Self::Coverage {
                contributor,
                coverage_source,
                family,
                covered_from,
                covered_through,
                available_at,
                payload_hash,
                ..
            } => {
                if covered_from > covered_through
                    || covered_through > available_at
                    || coverage_source.key.subject != contributor.key
                    || coverage_source.key.family != *family
                {
                    return Err(WireError::Identity);
                }
                hash(payload_hash)
            }
            Self::Clock {
                contributor,
                clock_source,
                observed_at,
                available_at,
                freshness_limit_ms,
                reason_code,
                payload_hash,
                ..
            } => {
                if observed_at > available_at
                    || *freshness_limit_ms == 0
                    || clock_source.key.subject != contributor.key
                    || reason_code.is_empty()
                    || !reason_code.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
                {
                    return Err(WireError::Identity);
                }
                hash(payload_hash)
            }
        }
    }
}

fn validate_system_binding(
    source: &SystemSourceV1,
    scope: &FaultScopeV1,
    cursor: &SystemCursorV1,
    fault: &SystemFaultV1,
) -> Result<(), WireError> {
    let mode_matches = matches!(
        (source.key.cursor_mode, cursor),
        (CursorModeV1::Native, CursorV1::NativeRange { .. })
            | (CursorModeV1::Derived, CursorV1::DerivedAction { .. })
    );
    let scope_matches = match (&source.key.configured_target_key, scope) {
        (ConfiguredTargetKeyV1::Contributor(expected), FaultScopeV1::Contributor(actual)) => {
            expected == &actual.key
        }
        (
            ConfiguredTargetKeyV1::Connection(expected),
            FaultScopeV1::ConnectionEpoch {
                connection_key: actual,
                ..
            },
        ) => expected == actual,
        (
            ConfiguredTargetKeyV1::Processor(expected),
            FaultScopeV1::Processor {
                processor_id: actual,
            },
        ) => expected == actual,
        _ => false,
    };
    let scope_kind_matches = matches!(
        (source.key.scope_kind, scope),
        (FaultScopeKindV1::Contributor, FaultScopeV1::Contributor(_))
            | (
                FaultScopeKindV1::ConnectionEpoch,
                FaultScopeV1::ConnectionEpoch { .. }
            )
            | (FaultScopeKindV1::Processor, FaultScopeV1::Processor { .. })
    );
    let fault_matches = matches!(
        (fault, scope),
        (
            SystemFaultV1::SequenceGap { .. }
                | SystemFaultV1::ChecksumMismatch
                | SystemFaultV1::BookInvalidated
                | SystemFaultV1::BookResynchronized,
            FaultScopeV1::Contributor(_)
        ) | (
            SystemFaultV1::Disconnected,
            FaultScopeV1::ConnectionEpoch { .. }
        ) | (
            SystemFaultV1::EventsDropped { .. } | SystemFaultV1::ClockJump { .. },
            FaultScopeV1::Processor { .. }
        )
    );
    let reserved_matches = match (cursor, fault) {
        (
            CursorV1::DerivedAction {
                action_index,
                item_index,
                ..
            },
            SystemFaultV1::EventsDropped { category, .. },
        ) if *action_index == u16::MAX as u32 => {
            *item_index
                == match category {
                    DropCategoryV1::ActionBuffer => 0,
                    DropCategoryV1::MarketDispatch => 1,
                    DropCategoryV1::SystemDispatch => 2,
                }
        }
        (CursorV1::DerivedAction { action_index, .. }, _) => *action_index != u16::MAX as u32,
        _ => true,
    };
    if mode_matches && scope_matches && scope_kind_matches && fault_matches && reserved_matches {
        Ok(())
    } else {
        Err(WireError::Cursor)
    }
}
