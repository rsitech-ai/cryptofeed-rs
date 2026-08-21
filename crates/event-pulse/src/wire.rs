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
        if bytes.is_empty() || bytes.len() > max_integer + max_fraction + 2 {
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Rfc3339Time {
    original: String,
    utc_micros: i128,
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
        let normalized = if let Some(dot) = body.rfind('.') {
            let (head, fraction) = body.split_at(dot);
            let digits = &fraction[1..];
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(WireError::Time);
            }
            format!("{head}.{:0<6}{offset}", &digits[..digits.len().min(6)])
        } else {
            format!("{body}{offset}")
        };
        let parsed = OffsetDateTime::parse(&normalized, &Rfc3339).map_err(|_| WireError::Time)?;
        let nanos = parsed.unix_timestamp_nanos();
        let micros = nanos / 1_000;
        Ok(Self {
            original: value.into(),
            utc_micros: micros,
        })
    }
    pub fn utc_micros(&self) -> i128 {
        self.utc_micros
    }
    pub fn as_str(&self) -> &str {
        &self.original
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
        if action_index == u16::MAX as u32 || item_index >= u16::MAX as u32 {
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
        Ok(Self::DerivedAction {
            frame_ordinal,
            action_index: u16::MAX as u32,
            item_index,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionKeyV1(String);
impl ConnectionKeyV1 {
    pub fn new(source_id: &str) -> Result<Self, WireError> {
        if source_id.is_empty()
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
