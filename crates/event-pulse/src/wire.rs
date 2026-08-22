use std::{cmp::Ordering, collections::BTreeMap};

use serde::{Deserialize, Serialize, de::DeserializeSeed};
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
    pub fn parse_unbounded(value: &str) -> Result<Self, WireError> {
        Self::parse(value, value.len(), value.len())
    }
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
impl Serialize for CanonicalDecimal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de> Deserialize<'de> for CanonicalDecimal {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse_unbounded(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone)]
pub struct Rfc3339Time {
    canonical: String,
    utc_micros: i64,
}
impl Rfc3339Time {
    pub fn from_unix_nanos(value: i64) -> Result<Self, WireError> {
        let micros = value.div_euclid(1_000);
        let instant = OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
            .map_err(|_| WireError::Time)?;
        let rendered = instant.format(&Rfc3339).map_err(|_| WireError::Time)?;
        Self::parse(&rendered)
    }
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
impl Serialize for Rfc3339Time {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.canonical())
    }
}
impl<'de> Deserialize<'de> for Rfc3339Time {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
enum CursorKindV1 {
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct CursorV1(CursorKindV1);
impl CursorV1 {
    pub fn native(start: u64, end: u64) -> Result<Self, WireError> {
        if start > end || end > MAX_I64_U64 {
            return Err(WireError::Cursor);
        }
        Ok(Self(CursorKindV1::NativeRange { start, end }))
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
        Ok(Self(CursorKindV1::DerivedAction {
            frame_ordinal,
            action_index,
            item_index,
        }))
    }
    pub fn derived_drop(frame_ordinal: u64, item_index: u32) -> Result<Self, WireError> {
        if item_index > 2 {
            return Err(WireError::Cursor);
        }
        let cursor = Self(CursorKindV1::DerivedAction {
            frame_ordinal,
            action_index: u16::MAX as u32,
            item_index,
        });
        cursor.display_sequence()?;
        Ok(cursor)
    }
    pub fn display_sequence(&self) -> Result<u64, WireError> {
        let (frame, action, item) = match &self.0 {
            CursorKindV1::NativeRange { end, .. } => return Ok(*end),
            CursorKindV1::DerivedAction {
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
    pub fn native_range(&self) -> Option<(u64, u64)> {
        match self.0 {
            CursorKindV1::NativeRange { start, end } => Some((start, end)),
            CursorKindV1::DerivedAction { .. } => None,
        }
    }
    pub fn derived_coordinate(&self) -> Option<(u64, u32, u32)> {
        match self.0 {
            CursorKindV1::DerivedAction {
                frame_ordinal,
                action_index,
                item_index,
            } => Some((frame_ordinal, action_index, item_index)),
            CursorKindV1::NativeRange { .. } => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
enum CursorWireV1 {
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
impl<'de> Deserialize<'de> for CursorV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match CursorWireV1::deserialize(deserializer)? {
            CursorWireV1::NativeRange { start, end } => Self::native(start, end),
            CursorWireV1::DerivedAction {
                frame_ordinal,
                action_index,
                item_index,
            } if action_index == u16::MAX as u32 => Self::derived_drop(frame_ordinal, item_index),
            CursorWireV1::DerivedAction {
                frame_ordinal,
                action_index,
                item_index,
            } => Self::derived(frame_ordinal, action_index, item_index),
        }
        .map_err(serde::de::Error::custom)
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ConnectionKeyV1(String);
impl ConnectionKeyV1 {
    pub fn new(source_id: &str) -> Result<Self, WireError> {
        if source_id.is_empty()
            || !source_id.as_bytes()[0].is_ascii_lowercase()
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
impl<'de> Deserialize<'de> for ConnectionKeyV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(&String::deserialize(d)?).map_err(serde::de::Error::custom)
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct InstrumentIdentityV1 {
    base_asset: String,
    quote_asset: String,
    market_type: String,
    venue: String,
    venue_symbol: String,
}
impl InstrumentIdentityV1 {
    pub fn new(
        base_asset: &str,
        quote_asset: &str,
        market_type: &str,
        venue: &str,
        venue_symbol: &str,
    ) -> Result<Self, WireError> {
        if !matches!(base_asset, "BTC" | "ETH" | "SOL" | "BNB" | "HYPE")
            || !matches!(quote_asset, "USD" | "USDC" | "USDT")
            || !matches!(market_type, "SPOT" | "PERPETUAL")
            || !matches!(venue, "BINANCE" | "HYPERLIQUID")
            || base_asset == quote_asset
            || venue_symbol.is_empty()
            || !(venue_symbol.as_bytes()[0].is_ascii_uppercase()
                || venue_symbol.as_bytes()[0].is_ascii_digit())
            || !venue_symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
        {
            return Err(WireError::Identity);
        }
        Ok(Self {
            base_asset: base_asset.into(),
            quote_asset: quote_asset.into(),
            market_type: market_type.into(),
            venue: venue.into(),
            venue_symbol: venue_symbol.into(),
        })
    }
    pub fn validate(&self) -> Result<(), WireError> {
        Self::new(
            &self.base_asset,
            &self.quote_asset,
            &self.market_type,
            &self.venue,
            &self.venue_symbol,
        )
        .map(|_| ())
    }
    pub fn venue(&self) -> &str {
        &self.venue
    }
    pub fn base_asset(&self) -> &str {
        &self.base_asset
    }
    pub fn quote_asset(&self) -> &str {
        &self.quote_asset
    }
    pub fn market_type(&self) -> &str {
        &self.market_type
    }
    pub fn venue_symbol(&self) -> &str {
        &self.venue_symbol
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentWire {
    base_asset: String,
    quote_asset: String,
    market_type: String,
    venue: String,
    venue_symbol: String,
}
impl<'de> Deserialize<'de> for InstrumentIdentityV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = InstrumentWire::deserialize(d)?;
        Self::new(
            &w.base_asset,
            &w.quote_asset,
            &w.market_type,
            &w.venue,
            &w.venue_symbol,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ContributorKeyV1 {
    source_id: String,
    instrument: InstrumentIdentityV1,
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
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
    pub fn instrument(&self) -> &InstrumentIdentityV1 {
        &self.instrument
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContributorKeyWire {
    source_id: String,
    instrument: InstrumentIdentityV1,
}
impl<'de> Deserialize<'de> for ContributorKeyV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = ContributorKeyWire::deserialize(d)?;
        Self::new(&w.source_id, w.instrument).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContributorV1 {
    key: ContributorKeyV1,
    connection_epoch: String,
    epoch_generation: u8,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContributorWire {
    key: ContributorKeyV1,
    connection_epoch: String,
    epoch_generation: u8,
}
impl<'de> Deserialize<'de> for ContributorV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = ContributorWire::deserialize(d)?;
        Self::new(w.key, &w.connection_epoch, w.epoch_generation).map_err(serde::de::Error::custom)
    }
}
impl ContributorV1 {
    pub fn new(
        key: ContributorKeyV1,
        connection_epoch: &str,
        epoch_generation: u8,
    ) -> Result<Self, WireError> {
        let connection_epoch = checked_epoch(connection_epoch)?;
        Ok(Self {
            key,
            connection_epoch,
            epoch_generation,
        })
    }
    pub fn key(&self) -> &ContributorKeyV1 {
        &self.key
    }
    pub fn connection_epoch(&self) -> &str {
        &self.connection_epoch
    }
    pub fn epoch_generation(&self) -> u8 {
        self.epoch_generation
    }
}

fn checked_epoch(value: &str) -> Result<String, WireError> {
    let suffix = value.strip_prefix("epoch_").ok_or(WireError::Identity)?;
    if suffix.is_empty()
        || suffix.len() > 64
        || !suffix.as_bytes()[0].is_ascii_alphanumeric()
        || !suffix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(WireError::Identity);
    }
    Ok(value.into())
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ClockSourceKeyV1 {
    source_id: String,
    subject: ContributorKeyV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClockSourceV1 {
    key: ClockSourceKeyV1,
    epoch: String,
    epoch_generation: u8,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ClockCursorV1(CursorV1);
impl ClockCursorV1 {
    pub fn native(start: u64, end: u64) -> Result<Self, WireError> {
        Ok(Self(CursorV1::native(start, end)?))
    }
    pub fn cursor(&self) -> &CursorV1 {
        &self.0
    }
    pub fn validate_static(&self) -> Result<(), WireError> {
        self.0.native_range().map(|_| ()).ok_or(WireError::Cursor)
    }
}
impl<'de> Deserialize<'de> for ClockCursorV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let cursor = CursorV1::deserialize(deserializer)?;
        cursor
            .native_range()
            .map(|_| Self(cursor))
            .ok_or_else(|| serde::de::Error::custom(WireError::Cursor))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct CoverageSourceKeyV1 {
    source_id: String,
    subject: ContributorKeyV1,
    family: FamilyV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CoverageSourceV1 {
    key: CoverageSourceKeyV1,
    epoch: String,
    epoch_generation: u8,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct CoverageCursorV1(CursorV1);
impl CoverageCursorV1 {
    pub fn native(start: u64, end: u64) -> Result<Self, WireError> {
        Ok(Self(CursorV1::native(start, end)?))
    }
    pub fn cursor(&self) -> &CursorV1 {
        &self.0
    }
    pub fn validate_static(&self) -> Result<(), WireError> {
        self.0.native_range().map(|_| ()).ok_or(WireError::Cursor)
    }
}
impl<'de> Deserialize<'de> for CoverageCursorV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let cursor = CursorV1::deserialize(deserializer)?;
        cursor
            .native_range()
            .map(|_| Self(cursor))
            .ok_or_else(|| serde::de::Error::custom(WireError::Cursor))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(tag = "kind", content = "target", rename_all = "SCREAMING_SNAKE_CASE")]
enum ConfiguredTargetInner {
    Contributor(ContributorKeyV1),
    Connection(ConnectionKeyV1),
    Processor(String),
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ConfiguredTargetKeyV1(ConfiguredTargetInner);
impl ConfiguredTargetKeyV1 {
    pub fn contributor(k: ContributorKeyV1) -> Self {
        Self(ConfiguredTargetInner::Contributor(k))
    }
    pub fn connection(k: ConnectionKeyV1) -> Self {
        Self(ConfiguredTargetInner::Connection(k))
    }
    pub fn processor(id: &str) -> Result<Self, WireError> {
        Ok(Self(ConfiguredTargetInner::Processor(bounded_identity(
            id,
        )?)))
    }
    pub fn scope_kind(&self) -> FaultScopeKindV1 {
        match self.0 {
            ConfiguredTargetInner::Contributor(_) => FaultScopeKindV1::Contributor,
            ConfiguredTargetInner::Connection(_) => FaultScopeKindV1::ConnectionEpoch,
            ConfiguredTargetInner::Processor(_) => FaultScopeKindV1::Processor,
        }
    }
    pub fn contributor_key(&self) -> Option<&ContributorKeyV1> {
        match &self.0 {
            ConfiguredTargetInner::Contributor(key) => Some(key),
            _ => None,
        }
    }
    pub fn connection_key(&self) -> Option<&ConnectionKeyV1> {
        match &self.0 {
            ConfiguredTargetInner::Connection(key) => Some(key),
            _ => None,
        }
    }
    pub fn processor_id(&self) -> Option<&str> {
        match &self.0 {
            ConfiguredTargetInner::Processor(id) => Some(id),
            _ => None,
        }
    }
}
#[derive(Deserialize)]
#[serde(
    tag = "kind",
    content = "target",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
enum TargetWire {
    Contributor(ContributorKeyV1),
    Connection(ConnectionKeyV1),
    Processor(String),
}
impl<'de> Deserialize<'de> for ConfiguredTargetKeyV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match TargetWire::deserialize(d)? {
            TargetWire::Contributor(k) => Ok(Self::contributor(k)),
            TargetWire::Connection(k) => Ok(Self::connection(k)),
            TargetWire::Processor(id) => Self::processor(&id),
        }
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct SystemSourceKeyV1 {
    source_id: String,
    scope_kind: FaultScopeKindV1,
    configured_target_key: ConfiguredTargetKeyV1,
    cursor_mode: CursorModeV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SystemSourceV1 {
    key: SystemSourceKeyV1,
    epoch: String,
    epoch_generation: u8,
}
pub type SystemCursorV1 = CursorV1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FaultScopeKindV1 {
    Contributor,
    ConnectionEpoch,
    Processor,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
enum FaultScopeInner {
    Contributor {
        contributor: ContributorV1,
    },
    ConnectionEpoch {
        connection_key: ConnectionKeyV1,
        connection_epoch: String,
        epoch_generation: u8,
    },
    Processor {
        processor_id: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct FaultScopeV1(FaultScopeInner);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultScopeRefV1<'a> {
    Contributor {
        contributor: &'a ContributorV1,
    },
    ConnectionEpoch {
        connection_key: &'a ConnectionKeyV1,
        connection_epoch: &'a str,
        epoch_generation: u8,
    },
    Processor {
        processor_id: &'a str,
    },
}
impl FaultScopeV1 {
    pub fn contributor(c: ContributorV1) -> Self {
        Self(FaultScopeInner::Contributor { contributor: c })
    }
    pub fn connection(k: ConnectionKeyV1, e: &str, g: u8) -> Result<Self, WireError> {
        Ok(Self(FaultScopeInner::ConnectionEpoch {
            connection_key: k,
            connection_epoch: checked_epoch(e)?,
            epoch_generation: g,
        }))
    }
    pub fn processor(id: &str) -> Result<Self, WireError> {
        Ok(Self(FaultScopeInner::Processor {
            processor_id: bounded_identity(id)?,
        }))
    }
    pub fn kind(&self) -> FaultScopeKindV1 {
        match self.0 {
            FaultScopeInner::Contributor { .. } => FaultScopeKindV1::Contributor,
            FaultScopeInner::ConnectionEpoch { .. } => FaultScopeKindV1::ConnectionEpoch,
            FaultScopeInner::Processor { .. } => FaultScopeKindV1::Processor,
        }
    }
    pub fn view(&self) -> FaultScopeRefV1<'_> {
        match &self.0 {
            FaultScopeInner::Contributor { contributor } => {
                FaultScopeRefV1::Contributor { contributor }
            }
            FaultScopeInner::ConnectionEpoch {
                connection_key,
                connection_epoch,
                epoch_generation,
            } => FaultScopeRefV1::ConnectionEpoch {
                connection_key,
                connection_epoch,
                epoch_generation: *epoch_generation,
            },
            FaultScopeInner::Processor { processor_id } => {
                FaultScopeRefV1::Processor { processor_id }
            }
        }
    }
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
enum ScopeWire {
    Contributor {
        contributor: ContributorV1,
    },
    ConnectionEpoch {
        connection_key: ConnectionKeyV1,
        connection_epoch: String,
        epoch_generation: u8,
    },
    Processor {
        processor_id: String,
    },
}
impl<'de> Deserialize<'de> for FaultScopeV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match ScopeWire::deserialize(d)? {
            ScopeWire::Contributor { contributor } => Ok(Self::contributor(contributor)),
            ScopeWire::ConnectionEpoch {
                connection_key,
                connection_epoch,
                epoch_generation,
            } => Self::connection(connection_key, &connection_epoch, epoch_generation),
            ScopeWire::Processor { processor_id } => Self::processor(&processor_id),
        }
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DropCategoryV1 {
    ActionBuffer,
    MarketDispatch,
    SystemDispatch,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
enum SystemFaultInner {
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SystemFaultV1(SystemFaultInner);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemFaultRefV1 {
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
impl SystemFaultV1 {
    pub fn disconnected() -> Self {
        Self(SystemFaultInner::Disconnected)
    }
    pub fn sequence_gap(expected: u64, actual: u64) -> Self {
        Self(SystemFaultInner::SequenceGap { expected, actual })
    }
    pub fn events_dropped(count: u64, category: DropCategoryV1) -> Result<Self, WireError> {
        if count == 0 {
            return Err(WireError::Identity);
        }
        Ok(Self(SystemFaultInner::EventsDropped { count, category }))
    }
    pub fn checksum_mismatch() -> Self {
        Self(SystemFaultInner::ChecksumMismatch)
    }
    pub fn clock_jump(delta_ns: i64) -> Self {
        Self(SystemFaultInner::ClockJump { delta_ns })
    }
    pub fn book_invalidated() -> Self {
        Self(SystemFaultInner::BookInvalidated)
    }
    pub fn book_resynchronized() -> Self {
        Self(SystemFaultInner::BookResynchronized)
    }
    pub fn view(&self) -> SystemFaultRefV1 {
        match self.0 {
            SystemFaultInner::Disconnected => SystemFaultRefV1::Disconnected,
            SystemFaultInner::SequenceGap { expected, actual } => {
                SystemFaultRefV1::SequenceGap { expected, actual }
            }
            SystemFaultInner::EventsDropped { count, category } => {
                SystemFaultRefV1::EventsDropped { count, category }
            }
            SystemFaultInner::ChecksumMismatch => SystemFaultRefV1::ChecksumMismatch,
            SystemFaultInner::ClockJump { delta_ns } => SystemFaultRefV1::ClockJump { delta_ns },
            SystemFaultInner::BookInvalidated => SystemFaultRefV1::BookInvalidated,
            SystemFaultInner::BookResynchronized => SystemFaultRefV1::BookResynchronized,
        }
    }
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
enum FaultWire {
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
impl<'de> Deserialize<'de> for SystemFaultV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match FaultWire::deserialize(d)? {
            FaultWire::Disconnected => Ok(Self::disconnected()),
            FaultWire::SequenceGap { expected, actual } => Ok(Self::sequence_gap(expected, actual)),
            FaultWire::EventsDropped { count, category } => Self::events_dropped(count, category),
            FaultWire::ChecksumMismatch => Ok(Self::checksum_mismatch()),
            FaultWire::ClockJump { delta_ns } => Ok(Self::clock_jump(delta_ns)),
            FaultWire::BookInvalidated => Ok(Self::book_invalidated()),
            FaultWire::BookResynchronized => Ok(Self::book_resynchronized()),
        }
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotAuthoringV1 {
    contract_id: String,
    lineage_id: String,
    event_cluster_id: String,
    primary_scope: InstrumentIdentityV1,
    revision_start: u64,
    predecessor_content_hash: Option<String>,
    expected_half_life_ms: u64,
    producer_version: String,
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
        if !valid_prefixed_id(contract_id, "event_pulse_mechanics_")
            || !valid_prefixed_id(lineage_id, "lineage_")
            || !valid_prefixed_id(event_cluster_id, "event_cluster_")
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
    pub fn contract_id(&self) -> &str {
        &self.contract_id
    }
    pub fn lineage_id(&self) -> &str {
        &self.lineage_id
    }
    pub fn event_cluster_id(&self) -> &str {
        &self.event_cluster_id
    }
    pub fn primary_scope(&self) -> &InstrumentIdentityV1 {
        &self.primary_scope
    }
    pub fn revision_start(&self) -> u64 {
        self.revision_start
    }
    pub fn predecessor_content_hash(&self) -> Option<&str> {
        self.predecessor_content_hash.as_deref()
    }
    pub fn expected_half_life_ms(&self) -> u64 {
        self.expected_half_life_ms
    }
    pub fn producer_version(&self) -> &str {
        &self.producer_version
    }
}

fn valid_prefixed_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|s| {
        !s.is_empty()
            && s.as_bytes()[0].is_ascii_alphanumeric()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContributorRoleV1 {
    Primary,
    Confirmation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContributorSpecV1 {
    key: ContributorKeyV1,
    role: ContributorRoleV1,
    allowed_families: std::collections::BTreeSet<FamilyV1>,
}
impl ContributorSpecV1 {
    pub fn new(
        key: ContributorKeyV1,
        role: ContributorRoleV1,
        families: impl IntoIterator<Item = FamilyV1>,
    ) -> Result<Self, WireError> {
        let allowed_families = families
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let valid = match role {
            ContributorRoleV1::Primary => {
                !allowed_families.is_empty()
                    && allowed_families.iter().all(|f| {
                        matches!(
                            f,
                            FamilyV1::Trade
                                | FamilyV1::Quote
                                | FamilyV1::Book
                                | FamilyV1::OpenInterest
                                | FamilyV1::Liquidation
                        )
                    })
            }
            ContributorRoleV1::Confirmation => {
                allowed_families == [FamilyV1::ConfirmationPrice].into_iter().collect()
            }
        };
        if !valid {
            return Err(WireError::Identity);
        }
        Ok(Self {
            key,
            role,
            allowed_families,
        })
    }
    pub fn key(&self) -> &ContributorKeyV1 {
        &self.key
    }
    pub fn role(&self) -> ContributorRoleV1 {
        self.role
    }
    pub fn allowed_families(&self) -> &std::collections::BTreeSet<FamilyV1> {
        &self.allowed_families
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MechanicsConfigV1 {
    processor_id: String,
    connections: Vec<ConnectionKeyV1>,
    contributors: Vec<ContributorSpecV1>,
    contributor_connections: BTreeMap<ContributorKeyV1, ConnectionKeyV1>,
    clock_sources: Vec<ClockSourceKeyV1>,
    coverage_sources: Vec<CoverageSourceKeyV1>,
    system_sources: Vec<SystemSourceKeyV1>,
}
impl MechanicsConfigV1 {
    pub fn new(
        processor_id: &str,
        connections: Vec<ConnectionKeyV1>,
        contributors: Vec<ContributorSpecV1>,
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
            .map(ContributorSpecV1::key)
            .collect::<std::collections::BTreeSet<_>>();
        let connections_set = connections
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        let primary = contributors
            .iter()
            .find(|contributor| contributor.role() == ContributorRoleV1::Primary);
        let primary_count = contributors
            .iter()
            .filter(|contributor| contributor.role() == ContributorRoleV1::Primary)
            .count();
        let primary_instrument_valid = primary.is_some_and(|primary| {
            contributors
                .iter()
                .filter(|contributor| contributor.role() == ContributorRoleV1::Primary)
                .all(|contributor| contributor.key.instrument == primary.key.instrument)
        });
        let primary_family_owner_count = |family: FamilyV1| {
            contributors
                .iter()
                .filter(|contributor| {
                    contributor.role() == ContributorRoleV1::Primary
                        && contributor.allowed_families.contains(&family)
                })
                .count()
        };
        let primary_family_ownership_valid = [FamilyV1::Trade, FamilyV1::Quote, FamilyV1::Book]
            .into_iter()
            .all(|family| primary_family_owner_count(family) == 1)
            && [FamilyV1::OpenInterest, FamilyV1::Liquidation]
                .into_iter()
                .all(|family| primary_family_owner_count(family) <= 1);
        let confirmation_count = contributors
            .iter()
            .filter(|contributor| contributor.role() == ContributorRoleV1::Confirmation)
            .count();
        let confirmation_venues = contributors
            .iter()
            .filter(|contributor| contributor.role() == ContributorRoleV1::Confirmation)
            .map(|contributor| contributor.key.instrument.venue.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let confirmation_identity_valid = primary.is_some_and(|primary| {
            let primary = &primary.key.instrument;
            contributors
                .iter()
                .filter(|contributor| contributor.role() == ContributorRoleV1::Confirmation)
                .all(|contributor| {
                    let confirmation = &contributor.key.instrument;
                    confirmation.base_asset == primary.base_asset
                        && confirmation.quote_asset == primary.quote_asset
                        && confirmation.market_type == primary.market_type
                        && confirmation.venue != primary.venue
                })
        });
        if connections_set.len() != connections.len()
            || contributors_set.len() != contributors.len()
            || contributor_connections.len() != contributors.len()
            || contributor_connections
                .iter()
                .any(|(contributor, connection)| {
                    !contributors_set.contains(contributor) || !connections_set.contains(connection)
                })
            || primary_count == 0
            || !primary_instrument_valid
            || !primary_family_ownership_valid
            || confirmation_count > 15
            || confirmation_venues.len() != confirmation_count
            || !confirmation_identity_valid
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
            .map(|source| source.key.source_id.as_str())
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
        if system_sources
            .iter()
            .any(|source| match &source.configured_target_key.0 {
                ConfiguredTargetInner::Contributor(key) => !contributors_set.contains(key),
                ConfiguredTargetInner::Connection(key) => !connections_set.contains(key),
                ConfiguredTargetInner::Processor(id) => id != &processor_id,
            })
        {
            return Err(WireError::Identity);
        }
        let expected_coverage = contributors
            .iter()
            .flat_map(|c| c.allowed_families().iter().map(move |f| (c.key(), *f)))
            .collect::<std::collections::BTreeSet<_>>();
        if unique_clocks.len() != clock_sources.len()
            || unique_coverage.len() != coverage_sources.len()
            || unique_coverage != expected_coverage
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
    pub fn contributors(&self) -> &[ContributorSpecV1] {
        &self.contributors
    }
    pub fn processor_id(&self) -> &str {
        &self.processor_id
    }
    pub fn connections(&self) -> &[ConnectionKeyV1] {
        &self.connections
    }
    pub fn contributor_connections(&self) -> &BTreeMap<ContributorKeyV1, ConnectionKeyV1> {
        &self.contributor_connections
    }
    pub fn clock_sources(&self) -> &[ClockSourceKeyV1] {
        &self.clock_sources
    }
    pub fn coverage_sources(&self) -> &[CoverageSourceKeyV1] {
        &self.coverage_sources
    }
    pub fn system_sources(&self) -> &[SystemSourceKeyV1] {
        &self.system_sources
    }
}

fn checked_source_epoch(
    source_id: &str,
    epoch: &str,
    epoch_generation: u8,
) -> Result<(String, String, u8), WireError> {
    let source_id = ConnectionKeyV1::new(source_id)?.0;
    Ok((source_id, checked_epoch(epoch)?, epoch_generation))
}

impl ClockSourceKeyV1 {
    pub fn new(source_id: &str, subject: ContributorKeyV1) -> Result<Self, WireError> {
        Ok(Self {
            source_id: ConnectionKeyV1::new(source_id)?.0,
            subject,
        })
    }
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
    pub fn subject(&self) -> &ContributorKeyV1 {
        &self.subject
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
    pub fn key(&self) -> &ClockSourceKeyV1 {
        &self.key
    }
    pub fn epoch(&self) -> &str {
        &self.epoch
    }
    pub fn epoch_generation(&self) -> u8 {
        self.epoch_generation
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
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
    pub fn subject(&self) -> &ContributorKeyV1 {
        &self.subject
    }
    pub fn family(&self) -> FamilyV1 {
        self.family
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
    pub fn key(&self) -> &CoverageSourceKeyV1 {
        &self.key
    }
    pub fn epoch(&self) -> &str {
        &self.epoch
    }
    pub fn epoch_generation(&self) -> u8 {
        self.epoch_generation
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClockKeyWire {
    source_id: String,
    subject: ContributorKeyV1,
}
impl<'de> Deserialize<'de> for ClockSourceKeyV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = ClockKeyWire::deserialize(d)?;
        Self::new(&w.source_id, w.subject).map_err(serde::de::Error::custom)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClockSourceWire {
    key: ClockSourceKeyV1,
    epoch: String,
    epoch_generation: u8,
}
impl<'de> Deserialize<'de> for ClockSourceV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = ClockSourceWire::deserialize(d)?;
        Self::new(w.key, &w.epoch, w.epoch_generation).map_err(serde::de::Error::custom)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageKeyWire {
    source_id: String,
    subject: ContributorKeyV1,
    family: FamilyV1,
}
impl<'de> Deserialize<'de> for CoverageSourceKeyV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = CoverageKeyWire::deserialize(d)?;
        Self::new(&w.source_id, w.subject, w.family).map_err(serde::de::Error::custom)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageSourceWire {
    key: CoverageSourceKeyV1,
    epoch: String,
    epoch_generation: u8,
}
impl<'de> Deserialize<'de> for CoverageSourceV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = CoverageSourceWire::deserialize(d)?;
        Self::new(w.key, &w.epoch, w.epoch_generation).map_err(serde::de::Error::custom)
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
            (&scope_kind, &configured_target_key.0),
            (
                FaultScopeKindV1::Contributor,
                ConfiguredTargetInner::Contributor(_)
            ) | (
                FaultScopeKindV1::ConnectionEpoch,
                ConfiguredTargetInner::Connection(_)
            ) | (
                FaultScopeKindV1::Processor,
                ConfiguredTargetInner::Processor(_)
            )
        );
        if !target_matches {
            return Err(WireError::Identity);
        }
        if let ConfiguredTargetInner::Processor(id) = &configured_target_key.0 {
            bounded_identity(id)?;
        }
        Ok(Self {
            source_id: ConnectionKeyV1::new(source_id)?.0,
            scope_kind,
            configured_target_key,
            cursor_mode,
        })
    }
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
    pub fn scope_kind(&self) -> FaultScopeKindV1 {
        self.scope_kind
    }
    pub fn configured_target_key(&self) -> &ConfiguredTargetKeyV1 {
        &self.configured_target_key
    }
    pub fn cursor_mode(&self) -> CursorModeV1 {
        self.cursor_mode
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemKeyWire {
    source_id: String,
    scope_kind: FaultScopeKindV1,
    configured_target_key: ConfiguredTargetKeyV1,
    cursor_mode: CursorModeV1,
}
impl<'de> Deserialize<'de> for SystemSourceKeyV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = SystemKeyWire::deserialize(d)?;
        Self::new(
            &w.source_id,
            w.scope_kind,
            w.configured_target_key,
            w.cursor_mode,
        )
        .map_err(serde::de::Error::custom)
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
    pub fn key(&self) -> &SystemSourceKeyV1 {
        &self.key
    }
    pub fn epoch(&self) -> &str {
        &self.epoch
    }
    pub fn epoch_generation(&self) -> u8 {
        self.epoch_generation
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemSourceWire {
    key: SystemSourceKeyV1,
    epoch: String,
    epoch_generation: u8,
}
impl<'de> Deserialize<'de> for SystemSourceV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = SystemSourceWire::deserialize(d)?;
        Self::new(w.key, &w.epoch, w.epoch_generation).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClockStateV1 {
    Synchronized,
    Degraded,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClockQualityV1 {
    Validated,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VenueCatalogEntryV1 {
    venue: String,
    source_id: String,
}
impl VenueCatalogEntryV1 {
    pub fn new(venue: &str, source_id_value: &str) -> Result<Self, WireError> {
        if !matches!(venue, "BINANCE" | "HYPERLIQUID") {
            return Err(WireError::Identity);
        }
        Ok(Self {
            venue: venue.into(),
            source_id: ConnectionKeyV1::new(source_id_value)?.0,
        })
    }
    pub fn venue(&self) -> &str {
        &self.venue
    }
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VenueWire {
    venue: String,
    source_id: String,
}
impl<'de> Deserialize<'de> for VenueCatalogEntryV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = VenueWire::deserialize(d)?;
        Self::new(&w.venue, &w.source_id).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayEpochEntryV1 {
    connection_id: u64,
    session_id: u64,
    connection_epoch: String,
    epoch_generation: u8,
}
impl ReplayEpochEntryV1 {
    pub fn new(
        connection_id: u64,
        session_id: u64,
        connection_epoch: &str,
        epoch_generation: u8,
    ) -> Result<Self, WireError> {
        Ok(Self {
            connection_id,
            session_id,
            connection_epoch: checked_epoch(connection_epoch)?,
            epoch_generation,
        })
    }
    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
    pub fn connection_epoch(&self) -> &str {
        &self.connection_epoch
    }
    pub fn epoch_generation(&self) -> u8 {
        self.epoch_generation
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EpochWire {
    connection_id: u64,
    session_id: u64,
    connection_epoch: String,
    epoch_generation: u8,
}
impl<'de> Deserialize<'de> for ReplayEpochEntryV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = EpochWire::deserialize(d)?;
        Self::new(
            w.connection_id,
            w.session_id,
            &w.connection_epoch,
            w.epoch_generation,
        )
        .map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "encoding", rename_all = "SCREAMING_SNAKE_CASE")]
enum OiEncodingInner {
    Contracts,
    Base {
        contracts_per_base: CanonicalDecimal,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct OpenInterestEncodingV1(OiEncodingInner);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenInterestEncodingRefV1<'a> {
    Contracts,
    Base {
        contracts_per_base: &'a CanonicalDecimal,
    },
}
impl OpenInterestEncodingV1 {
    pub fn contracts() -> Self {
        Self(OiEncodingInner::Contracts)
    }
    pub fn base(value: &str) -> Result<Self, WireError> {
        let value = CanonicalDecimal::parse(value, 18, 8)?;
        if value.as_str().starts_with('-')
            || value
                .as_str()
                .bytes()
                .all(|byte| matches!(byte, b'0' | b'.'))
        {
            return Err(WireError::Decimal);
        }
        Ok(Self(OiEncodingInner::Base {
            contracts_per_base: value,
        }))
    }
    pub fn view(&self) -> OpenInterestEncodingRefV1<'_> {
        match &self.0 {
            OiEncodingInner::Contracts => OpenInterestEncodingRefV1::Contracts,
            OiEncodingInner::Base { contracts_per_base } => {
                OpenInterestEncodingRefV1::Base { contracts_per_base }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayCatalogV1 {
    venue_sources: BTreeMap<u16, VenueCatalogEntryV1>,
    instruments: BTreeMap<u32, InstrumentIdentityV1>,
    connection_epochs: Vec<ReplayEpochEntryV1>,
    open_interest: BTreeMap<u32, OpenInterestEncodingV1>,
}
impl ReplayCatalogV1 {
    pub fn new(
        venue_sources: BTreeMap<u16, VenueCatalogEntryV1>,
        instruments: BTreeMap<u32, InstrumentIdentityV1>,
        connection_epochs: Vec<ReplayEpochEntryV1>,
        open_interest: BTreeMap<u32, OpenInterestEncodingV1>,
    ) -> Result<Self, WireError> {
        if venue_sources.is_empty()
            || venue_sources.len() > 32
            || instruments.is_empty()
            || instruments.len() > 32
            || connection_epochs.is_empty()
            || connection_epochs.len() > 32
            || open_interest.keys().any(|id| !instruments.contains_key(id))
        {
            return Err(WireError::Identity);
        }
        if instruments
            .iter()
            .any(|(_, i)| !venue_sources.values().any(|v| v.venue == i.venue))
        {
            return Err(WireError::Identity);
        }
        let epochs = connection_epochs
            .iter()
            .map(|e| (e.connection_id, e.session_id))
            .collect::<std::collections::BTreeSet<_>>();
        if epochs.len() != connection_epochs.len() {
            return Err(WireError::Identity);
        }
        Ok(Self {
            venue_sources,
            instruments,
            connection_epochs,
            open_interest,
        })
    }
    pub fn validate(&self) -> Result<(), WireError> {
        Self::new(
            self.venue_sources.clone(),
            self.instruments.clone(),
            self.connection_epochs.clone(),
            self.open_interest.clone(),
        )
        .map(|_| ())
    }
    pub fn venue_source(&self, id: u16) -> Option<&VenueCatalogEntryV1> {
        self.venue_sources.get(&id)
    }
    pub fn instrument(&self, id: u32) -> Option<&InstrumentIdentityV1> {
        self.instruments.get(&id)
    }
    pub fn connection_epochs(&self) -> &[ReplayEpochEntryV1] {
        &self.connection_epochs
    }
    pub fn open_interest_encoding(&self, id: u32) -> Option<&OpenInterestEncodingV1> {
        self.open_interest.get(&id)
    }
    fn contains_envelope(&self, envelope: &marketfeed_model::EventEnvelope) -> bool {
        let Some(venue) = self.venue_sources.get(&envelope.venue.0) else {
            return false;
        };
        let instrument = match envelope.instrument {
            Some(id) => match self.instruments.get(&id.0) {
                Some(instrument) if instrument.venue == venue.venue => Some(id),
                _ => return false,
            },
            None => None,
        };
        let timestamp_valid = envelope
            .exchange_ts
            .is_some_and(|exchange| exchange <= envelope.receive_ts);
        let sequence_valid = envelope
            .source_sequence
            .is_none_or(|sequence| sequence.first <= sequence.last && sequence.last <= MAX_I64_U64);
        let oi_valid = !matches!(
            envelope.payload,
            marketfeed_model::MarketEvent::OpenInterest(_)
        ) || instrument.is_some_and(|id| self.open_interest.contains_key(&id.0));
        envelope.schema_version == 1
            && timestamp_valid
            && sequence_valid
            && oi_valid
            && self.connection_epochs.iter().any(|epoch| {
                epoch.connection_id == envelope.connection.0
                    && epoch.session_id == envelope.session.0
            })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogWire {
    venue_sources: BTreeMap<String, VenueCatalogEntryV1>,
    instruments: BTreeMap<String, InstrumentIdentityV1>,
    connection_epochs: Vec<ReplayEpochEntryV1>,
    open_interest: BTreeMap<String, serde_json::Value>,
}

fn parse_catalog_key<T>(key: &str) -> Result<T, WireError>
where
    T: std::str::FromStr + ToString,
{
    let parsed = key.parse::<T>().map_err(|_| WireError::Identity)?;
    if parsed.to_string() != key {
        return Err(WireError::Identity);
    }
    Ok(parsed)
}

impl<'de> Deserialize<'de> for ReplayCatalogV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = CatalogWire::deserialize(d)?;
        let venue_sources = w
            .venue_sources
            .into_iter()
            .map(|(id, value)| {
                parse_catalog_key::<u16>(&id)
                    .map(|id| (id, value))
                    .map_err(serde::de::Error::custom)
            })
            .collect::<Result<BTreeMap<_, _>, D::Error>>()?;
        let instruments = w
            .instruments
            .into_iter()
            .map(|(id, value)| {
                parse_catalog_key::<u32>(&id)
                    .map(|id| (id, value))
                    .map_err(serde::de::Error::custom)
            })
            .collect::<Result<BTreeMap<_, _>, D::Error>>()?;
        let mut oi = BTreeMap::new();
        for (id, v) in w.open_interest {
            let id = parse_catalog_key::<u32>(&id).map_err(serde::de::Error::custom)?;
            let object = v
                .as_object()
                .ok_or_else(|| serde::de::Error::custom("OI object"))?;
            let enc = object
                .get("encoding")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| serde::de::Error::custom("OI encoding"))?;
            let parsed = match enc {
                "CONTRACTS" if object.len() == 1 => OpenInterestEncodingV1::contracts(),
                "BASE" if object.len() == 2 => OpenInterestEncodingV1::base(
                    object
                        .get("contracts_per_base")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| serde::de::Error::custom("contracts_per_base"))?,
                )
                .map_err(serde::de::Error::custom)?,
                _ => return Err(serde::de::Error::custom("OI fields")),
            };
            oi.insert(id, parsed);
        }
        Self::new(venue_sources, instruments, w.connection_epochs, oi)
            .map_err(serde::de::Error::custom)
    }
}

fn validate_market_mapping(
    catalog: &ReplayCatalogV1,
    envelope: &marketfeed_model::EventEnvelope,
    action_index: u32,
) -> Result<(), WireError> {
    if action_index > 65_534 || !catalog.contains_envelope(envelope) {
        return Err(WireError::Identity);
    }
    match envelope.source_sequence {
        Some(sequence) => CursorV1::native(sequence.first, sequence.last),
        None => CursorV1::derived(
            envelope.frame_seq,
            action_index,
            u32::from(envelope.event_index),
        ),
    }
    .map(|_| ())
}

struct UniqueJsonSeed;

impl<'de> DeserializeSeed<'de> for UniqueJsonSeed {
    type Value = serde_json::Value;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> serde::de::Visitor<'de> for UniqueJsonVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("JSON with unique object keys")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(value))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value.to_owned()))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value))
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }
    fn visit_some<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        UniqueJsonSeed.deserialize(deserializer)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut sequence: A,
    ) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(UniqueJsonSeed)? {
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(serde::de::Error::custom("duplicate JSON object key"));
            }
            let value = map.next_value_seed(UniqueJsonSeed)?;
            values.insert(key, value);
        }
        Ok(serde_json::Value::Object(values))
    }
}

fn parse_unique_json(bytes: &[u8]) -> Result<serde_json::Value, WireError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = UniqueJsonSeed
        .deserialize(&mut deserializer)
        .map_err(|_| WireError::Identity)?;
    deserializer.end().map_err(|_| WireError::Identity)?;
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
enum MechanicsInputInner {
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

#[derive(Debug, Clone, Copy)]
pub enum MechanicsInputRefV1<'a> {
    Market {
        envelope: &'a marketfeed_model::EventEnvelope,
        action_index: u32,
        catalog: &'a ReplayCatalogV1,
        payload_hash: &'a str,
    },
    System {
        system_source: &'a SystemSourceV1,
        scope: &'a FaultScopeV1,
        occurred_at: &'a Rfc3339Time,
        available_at: &'a Rfc3339Time,
        system_cursor: &'a SystemCursorV1,
        fault: &'a SystemFaultV1,
        predecessor_system_chain_hash: Option<&'a str>,
        payload_hash: &'a str,
    },
    Coverage {
        contributor: &'a ContributorV1,
        coverage_source: &'a CoverageSourceV1,
        family: FamilyV1,
        covered_from: &'a Rfc3339Time,
        covered_through: &'a Rfc3339Time,
        available_at: &'a Rfc3339Time,
        coverage_cursor: &'a CoverageCursorV1,
        payload_hash: &'a str,
    },
    Clock {
        contributor: &'a ContributorV1,
        clock_source: &'a ClockSourceV1,
        observed_at: &'a Rfc3339Time,
        available_at: &'a Rfc3339Time,
        clock_cursor: &'a ClockCursorV1,
        clock_state: ClockStateV1,
        observed_skew_ms: &'a CanonicalDecimal,
        freshness_limit_ms: u64,
        quality_state: ClockQualityV1,
        reason_code: &'a str,
        payload_hash: &'a str,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MechanicsInputV1(MechanicsInputInner);

impl Serialize for MechanicsInputV1 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_json::to_value(&self.0)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl MechanicsInputV1 {
    fn inner_mut_hash(inner: &mut MechanicsInputInner) -> &mut String {
        match inner {
            MechanicsInputInner::Market { payload_hash, .. }
            | MechanicsInputInner::System { payload_hash, .. }
            | MechanicsInputInner::Coverage { payload_hash, .. }
            | MechanicsInputInner::Clock { payload_hash, .. } => payload_hash,
        }
    }
    fn authored(mut inner: MechanicsInputInner) -> Result<Self, WireError> {
        let mut this = Self(inner.clone());
        let hash = this.expected_payload_hash()?;
        *Self::inner_mut_hash(&mut inner) = hash;
        this.0 = inner;
        if serde_json::to_vec(&this)
            .map_err(|_| WireError::Identity)?
            .len()
            > MAX_INPUT_BYTES
        {
            return Err(WireError::Identity);
        }
        Ok(this)
    }
    fn verified(inner: MechanicsInputInner) -> Result<Self, WireError> {
        let this = Self(inner);
        if this.expected_payload_hash()? != this.payload_hash() {
            return Err(WireError::Identity);
        }
        this.validate_static()?;
        Ok(this)
    }
    pub fn payload_hash(&self) -> &str {
        match &self.0 {
            MechanicsInputInner::Market { payload_hash, .. }
            | MechanicsInputInner::System { payload_hash, .. }
            | MechanicsInputInner::Coverage { payload_hash, .. }
            | MechanicsInputInner::Clock { payload_hash, .. } => payload_hash,
        }
    }
    pub fn view(&self) -> MechanicsInputRefV1<'_> {
        match &self.0 {
            MechanicsInputInner::Market {
                envelope,
                action_index,
                catalog,
                payload_hash,
            } => MechanicsInputRefV1::Market {
                envelope,
                action_index: *action_index,
                catalog,
                payload_hash,
            },
            MechanicsInputInner::System {
                system_source,
                scope,
                occurred_at,
                available_at,
                system_cursor,
                fault,
                predecessor_system_chain_hash,
                payload_hash,
            } => MechanicsInputRefV1::System {
                system_source,
                scope,
                occurred_at,
                available_at,
                system_cursor,
                fault,
                predecessor_system_chain_hash: predecessor_system_chain_hash.as_deref(),
                payload_hash,
            },
            MechanicsInputInner::Coverage {
                contributor,
                coverage_source,
                family,
                covered_from,
                covered_through,
                available_at,
                coverage_cursor,
                payload_hash,
            } => MechanicsInputRefV1::Coverage {
                contributor,
                coverage_source,
                family: *family,
                covered_from,
                covered_through,
                available_at,
                coverage_cursor,
                payload_hash,
            },
            MechanicsInputInner::Clock {
                contributor,
                clock_source,
                observed_at,
                available_at,
                clock_cursor,
                clock_state,
                observed_skew_ms,
                freshness_limit_ms,
                quality_state,
                reason_code,
                payload_hash,
            } => MechanicsInputRefV1::Clock {
                contributor,
                clock_source,
                observed_at,
                available_at,
                clock_cursor,
                clock_state: *clock_state,
                observed_skew_ms,
                freshness_limit_ms: *freshness_limit_ms,
                quality_state: *quality_state,
                reason_code,
                payload_hash,
            },
        }
    }
    pub fn expected_payload_hash(&self) -> Result<String, WireError> {
        let mut value = serde_json::to_value(self).map_err(|_| WireError::Identity)?;
        value
            .as_object_mut()
            .ok_or(WireError::Identity)?
            .remove("payload_hash");
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&value).map_err(|_| WireError::Identity)?)
        ))
    }
    pub fn market(
        envelope: marketfeed_model::EventEnvelope,
        action_index: u32,
        catalog: ReplayCatalogV1,
    ) -> Result<Self, WireError> {
        validate_market_mapping(&catalog, &envelope, action_index)?;
        Self::authored(MechanicsInputInner::Market {
            envelope,
            action_index,
            catalog,
            payload_hash: String::new(),
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn system(
        system_source: SystemSourceV1,
        scope: FaultScopeV1,
        occurred_at: Rfc3339Time,
        available_at: Rfc3339Time,
        system_cursor: SystemCursorV1,
        fault: SystemFaultV1,
        predecessor_system_chain_hash: Option<String>,
    ) -> Result<Self, WireError> {
        if occurred_at > available_at {
            return Err(WireError::Time);
        }
        if let Some(h) = &predecessor_system_chain_hash {
            SystemChainPreimage::first(h)?;
        }
        validate_system_binding(&system_source, &scope, &system_cursor, &fault)?;
        Self::authored(MechanicsInputInner::System {
            system_source,
            scope,
            occurred_at,
            available_at,
            system_cursor,
            fault,
            predecessor_system_chain_hash,
            payload_hash: String::new(),
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn coverage(
        contributor: ContributorV1,
        coverage_source: CoverageSourceV1,
        family: FamilyV1,
        covered_from: Rfc3339Time,
        covered_through: Rfc3339Time,
        available_at: Rfc3339Time,
        coverage_cursor: CoverageCursorV1,
    ) -> Result<Self, WireError> {
        if covered_from > covered_through
            || covered_through > available_at
            || coverage_source.key.subject != contributor.key
            || coverage_source.key.family != family
        {
            return Err(WireError::Identity);
        }
        Self::authored(MechanicsInputInner::Coverage {
            contributor,
            coverage_source,
            family,
            covered_from,
            covered_through,
            available_at,
            coverage_cursor,
            payload_hash: String::new(),
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn clock(
        contributor: ContributorV1,
        clock_source: ClockSourceV1,
        observed_at: Rfc3339Time,
        available_at: Rfc3339Time,
        clock_cursor: ClockCursorV1,
        clock_state: ClockStateV1,
        observed_skew_ms: CanonicalDecimal,
        freshness_limit_ms: u64,
        quality_state: ClockQualityV1,
        reason_code: &str,
    ) -> Result<Self, WireError> {
        if observed_at > available_at
            || freshness_limit_ms == 0
            || clock_source.key.subject != contributor.key
            || reason_code.is_empty()
            || !reason_code
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(WireError::Identity);
        }
        Self::authored(MechanicsInputInner::Clock {
            contributor,
            clock_source,
            observed_at,
            available_at,
            clock_cursor,
            clock_state,
            observed_skew_ms,
            freshness_limit_ms,
            quality_state,
            reason_code: reason_code.into(),
            payload_hash: String::new(),
        })
    }
    pub fn from_epin_json(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(WireError::Identity);
        }
        let value = parse_unique_json(bytes)?;
        let input: Self = serde_json::from_value(value).map_err(|_| WireError::Identity)?;
        if serde_json::to_vec(&input).map_err(|_| WireError::Identity)? != bytes {
            return Err(WireError::Identity);
        }
        Ok(input)
    }
    pub fn validate_static(&self) -> Result<(), WireError> {
        if self.expected_payload_hash()? != self.payload_hash() {
            return Err(WireError::Identity);
        }
        let hash = |value: &str| SystemChainPreimage::first(value).map(|_| ());
        match &self.0 {
            MechanicsInputInner::Market {
                catalog,
                payload_hash,
                envelope,
                action_index,
            } => {
                catalog.validate()?;
                validate_market_mapping(catalog, envelope, *action_index)?;
                hash(payload_hash)
            }
            MechanicsInputInner::System {
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
            MechanicsInputInner::Coverage {
                contributor,
                coverage_source,
                family,
                covered_from,
                covered_through,
                available_at,
                coverage_cursor,
                payload_hash,
            } => {
                if covered_from > covered_through
                    || covered_through > available_at
                    || coverage_source.key.subject != contributor.key
                    || coverage_source.key.family != *family
                {
                    return Err(WireError::Identity);
                }
                coverage_cursor.validate_static()?;
                hash(payload_hash)
            }
            MechanicsInputInner::Clock {
                contributor,
                clock_source,
                observed_at,
                available_at,
                clock_cursor,
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
                clock_cursor.validate_static()?;
                hash(payload_hash)
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
enum MechanicsInputWire {
    Market {
        envelope: serde_json::Value,
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
impl<'de> Deserialize<'de> for MechanicsInputV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let inner = match MechanicsInputWire::deserialize(d)? {
            MechanicsInputWire::Market {
                envelope,
                action_index,
                catalog,
                payload_hash,
            } => {
                let decoded: marketfeed_model::EventEnvelope =
                    serde_json::from_value(envelope.clone()).map_err(serde::de::Error::custom)?;
                if serde_json::to_value(&decoded).map_err(serde::de::Error::custom)? != envelope {
                    return Err(serde::de::Error::custom("non-exact envelope fields"));
                }
                MechanicsInputInner::Market {
                    envelope: decoded,
                    action_index,
                    catalog,
                    payload_hash,
                }
            }
            MechanicsInputWire::System {
                system_source,
                scope,
                occurred_at,
                available_at,
                system_cursor,
                fault,
                predecessor_system_chain_hash,
                payload_hash,
            } => MechanicsInputInner::System {
                system_source,
                scope,
                occurred_at,
                available_at,
                system_cursor,
                fault,
                predecessor_system_chain_hash,
                payload_hash,
            },
            MechanicsInputWire::Coverage {
                contributor,
                coverage_source,
                family,
                covered_from,
                covered_through,
                available_at,
                coverage_cursor,
                payload_hash,
            } => MechanicsInputInner::Coverage {
                contributor,
                coverage_source,
                family,
                covered_from,
                covered_through,
                available_at,
                coverage_cursor,
                payload_hash,
            },
            MechanicsInputWire::Clock {
                contributor,
                clock_source,
                observed_at,
                available_at,
                clock_cursor,
                clock_state,
                observed_skew_ms,
                freshness_limit_ms,
                quality_state,
                reason_code,
                payload_hash,
            } => MechanicsInputInner::Clock {
                contributor,
                clock_source,
                observed_at,
                available_at,
                clock_cursor,
                clock_state,
                observed_skew_ms,
                freshness_limit_ms,
                quality_state,
                reason_code,
                payload_hash,
            },
        };
        Self::verified(inner).map_err(serde::de::Error::custom)
    }
}

fn validate_system_binding(
    source: &SystemSourceV1,
    scope: &FaultScopeV1,
    cursor: &SystemCursorV1,
    fault: &SystemFaultV1,
) -> Result<(), WireError> {
    let mode_matches = match source.key.cursor_mode {
        CursorModeV1::Native => cursor.native_range().is_some(),
        CursorModeV1::Derived => cursor.derived_coordinate().is_some(),
    };
    let scope_matches = match (&source.key.configured_target_key.0, &scope.0) {
        (
            ConfiguredTargetInner::Contributor(expected),
            FaultScopeInner::Contributor {
                contributor: actual,
            },
        ) => expected == actual.key(),
        (
            ConfiguredTargetInner::Connection(expected),
            FaultScopeInner::ConnectionEpoch {
                connection_key: actual,
                ..
            },
        ) => expected == actual,
        (
            ConfiguredTargetInner::Processor(expected),
            FaultScopeInner::Processor {
                processor_id: actual,
            },
        ) => expected == actual,
        _ => false,
    };
    let scope_kind_matches = matches!(
        (source.key.scope_kind, &scope.0),
        (
            FaultScopeKindV1::Contributor,
            FaultScopeInner::Contributor { .. }
        ) | (
            FaultScopeKindV1::ConnectionEpoch,
            FaultScopeInner::ConnectionEpoch { .. }
        ) | (
            FaultScopeKindV1::Processor,
            FaultScopeInner::Processor { .. }
        )
    );
    let fault_matches = matches!(
        (&fault.0, &scope.0),
        (
            SystemFaultInner::SequenceGap { .. }
                | SystemFaultInner::ChecksumMismatch
                | SystemFaultInner::BookInvalidated
                | SystemFaultInner::BookResynchronized,
            FaultScopeInner::Contributor { .. }
        ) | (
            SystemFaultInner::Disconnected,
            FaultScopeInner::ConnectionEpoch { .. }
        ) | (
            SystemFaultInner::EventsDropped { .. } | SystemFaultInner::ClockJump { .. },
            FaultScopeInner::Processor { .. }
        )
    );
    let reserved_matches = match (cursor.derived_coordinate(), &fault.0) {
        (Some((_, action_index, item_index)), SystemFaultInner::EventsDropped { category, .. })
            if action_index == u16::MAX as u32 =>
        {
            item_index
                == match category {
                    DropCategoryV1::ActionBuffer => 0,
                    DropCategoryV1::MarketDispatch => 1,
                    DropCategoryV1::SystemDispatch => 2,
                }
        }
        (Some((_, action_index, _)), _) => action_index != u16::MAX as u32,
        _ => true,
    };
    if mode_matches && scope_matches && scope_kind_matches && fault_matches && reserved_matches {
        Ok(())
    } else {
        Err(WireError::Cursor)
    }
}
