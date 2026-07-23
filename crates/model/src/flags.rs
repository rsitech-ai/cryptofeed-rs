//! Quality / provenance flags on normalized events.

use serde::{Deserialize, Serialize};

/// Event quality and provenance bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct EventFlags(pub u32);

impl EventFlags {
    pub const SNAPSHOT: Self = Self(1 << 0);
    pub const DELTA: Self = Self(1 << 1);
    pub const REPLAY: Self = Self(1 << 2);
    pub const RECOVERED: Self = Self(1 << 3);
    pub const STALE: Self = Self(1 << 4);
    pub const OUT_OF_ORDER_SOURCE: Self = Self(1 << 5);
    pub const SOURCE_TIMESTAMP_MISSING: Self = Self(1 << 6);
    pub const SYNTHETIC: Self = Self(1 << 7);
    pub const RAW_REFERENCE_AVAILABLE: Self = Self(1 << 8);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}
