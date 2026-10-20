//! Negotiation and structural routing for the opt-in engine causal journal.
//! Payload meaning, bootstrap digests and actual engine replay belong to engine.
use super::{FeedRecord, MAX_RECORD, err};
use crate::RecordingError;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The expected format must be selected before reading records or creating an
/// engine. Default APIs retain the legacy profile and reject causal journals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedProfile {
    Legacy,
    EngineCausalV1,
    /// Explicit owner-history negotiation. Journal ordinals include owner
    /// observations as well as native operations; payload semantics are engine-owned.
    EngineCausalV2,
    /// Runtime owner history with explicit execution permits and late completion.
    EngineCausalV3,
    /// Native observations retained atomically with canonical owner outputs.
    EngineCausalV4,
}
impl FeedProfile {
    pub(super) fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Legacy => b"MFSF1\0\0\0",
            Self::EngineCausalV1 => b"MFCJ1\0\0\0",
            Self::EngineCausalV2 => b"MFCJ2\0\0\0",
            Self::EngineCausalV3 => b"MFCJ3\0\0\0",
            Self::EngineCausalV4 => b"MFCJ4\0\0\0",
        }
    }
}

/// Historical routes are retained for the entire feed; no eviction or rebinding.
pub const MAX_CAUSAL_ROUTES: usize = 4096;
/// Reserve room for routing, JSON and framing under the existing record bound.
pub const MAX_CAUSAL_ENCODED_PAYLOAD: usize = MAX_RECORD - 4096;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CausalPhase {
    Bootstrap,
    Operation,
    Owner,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CausalRoute {
    pub venue: u16,
    pub session: u64,
    pub ordinal: u64,
    pub generation: u64,
    /// Lowercase SHA-256 identifier. Engine must verify it over bootstrap bytes.
    pub bootstrap_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CausalEnvelope {
    pub schema_version: u16,
    pub phase: CausalPhase,
    pub route: CausalRoute,
    /// Opaque bytes: recording never deserializes engine or financial values.
    pub payload_base64: String,
}
impl CausalEnvelope {
    pub fn new(
        phase: CausalPhase,
        route: CausalRoute,
        payload: &[u8],
    ) -> Result<Self, RecordingError> {
        Self::for_profile(FeedProfile::EngineCausalV1, phase, route, payload)
    }

    pub fn for_profile(
        profile: FeedProfile,
        phase: CausalPhase,
        route: CausalRoute,
        payload: &[u8],
    ) -> Result<Self, RecordingError> {
        let schema_version = match profile {
            FeedProfile::EngineCausalV1 => 1,
            FeedProfile::EngineCausalV2 => 2,
            FeedProfile::EngineCausalV3 => 3,
            FeedProfile::EngineCausalV4 => 4,
            FeedProfile::Legacy => return Err(err("causal envelope requires causal profile")),
        };
        if payload.is_empty() || payload.len() > MAX_CAUSAL_ENCODED_PAYLOAD / 4 * 3 {
            return Err(err("causal payload byte budget"));
        }
        let envelope = Self {
            schema_version,
            phase,
            route,
            payload_base64: STANDARD.encode(payload),
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Bounded decoding only; this does not establish payload semantics or hash.
    pub fn payload(&self) -> Result<Vec<u8>, RecordingError> {
        self.validate_fields()?;
        STANDARD.decode(&self.payload_base64).map_err(err)
    }

    pub(crate) fn validate(&self) -> Result<(), RecordingError> {
        self.validate_fields()?;
        // Validate without allocating another payload-sized buffer during
        // preparation, writer admission or reader routing. Padding is final.
        let mut buffer = [0u8; 3072];
        for (index, chunk) in self.payload_base64.as_bytes().chunks(4096).enumerate() {
            let written = STANDARD.decode_slice(chunk, &mut buffer).map_err(err)?;
            if index * 4096 + chunk.len() < self.payload_base64.len() && written != buffer.len() {
                return Err(err("non-final causal base64 padding"));
            }
        }
        Ok(())
    }

    fn validate_fields(&self) -> Result<(), RecordingError> {
        if !matches!(self.schema_version, 1..=4) {
            return Err(RecordingError::UnsupportedVersion(self.schema_version));
        }
        if self.phase == CausalPhase::Owner && self.schema_version < 2 {
            return Err(err("owner history requires causal V2 or later"));
        }
        let id = self.route.bootstrap_id.as_bytes();
        if self.route.session == 0
            || id.len() != 64
            || !id
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Err(err("invalid causal route identity"));
        }
        if self.payload_base64.is_empty() || self.payload_base64.len() > MAX_CAUSAL_ENCODED_PAYLOAD
        {
            return Err(err("causal payload byte budget"));
        }
        if (self.phase == CausalPhase::Bootstrap
            && (self.route.ordinal != 0 || self.route.generation != 0))
            || (self.phase != CausalPhase::Bootstrap && self.route.ordinal == 0)
        {
            return Err(err("invalid causal phase ordinal or generation"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct CausalRoutes(pub(super) BTreeMap<u64, CausalRoute>);
impl CausalRoutes {
    /// Stage only changed routes. Avoid cloning all historical sessions for
    /// every record; install the delta only after the whole validation/commit.
    pub(super) fn prepare<'a>(
        &self,
        profile: FeedProfile,
        records: impl IntoIterator<Item = &'a FeedRecord>,
    ) -> Result<Self, RecordingError> {
        let mut staged = Self::default();
        let mut new_routes = 0usize;
        for record in records {
            staged.accept(self, &mut new_routes, profile, record)?;
        }
        Ok(staged)
    }

    pub(super) fn install(&mut self, changes: Self) {
        self.0.extend(changes.0);
    }

    fn accept(
        &mut self,
        base: &Self,
        new_routes: &mut usize,
        profile: FeedProfile,
        record: &FeedRecord,
    ) -> Result<(), RecordingError> {
        match (profile, record) {
            (FeedProfile::Legacy, FeedRecord::Causal(_)) => {
                Err(err("causal record in legacy feed"))
            }
            (FeedProfile::Legacy, _) | (_, FeedRecord::Seal { .. }) => Ok(()),
            (
                FeedProfile::EngineCausalV1
                | FeedProfile::EngineCausalV2
                | FeedProfile::EngineCausalV3
                | FeedProfile::EngineCausalV4,
                FeedRecord::Causal(envelope),
            ) => {
                envelope.validate()?;
                if envelope.schema_version
                    != match profile {
                        FeedProfile::EngineCausalV1 => 1,
                        FeedProfile::EngineCausalV2 => 2,
                        FeedProfile::EngineCausalV3 => 3,
                        FeedProfile::EngineCausalV4 => 4,
                        FeedProfile::Legacy => unreachable!(),
                    }
                {
                    return Err(err("causal envelope/profile version mismatch"));
                }
                let route = &envelope.route;
                match envelope.phase {
                    CausalPhase::Bootstrap => {
                        if self.0.contains_key(&route.session)
                            || base.0.contains_key(&route.session)
                            || base.0.len() + *new_routes >= MAX_CAUSAL_ROUTES
                        {
                            return Err(err("duplicate causal session or route budget"));
                        }
                        *new_routes += 1;
                    }
                    CausalPhase::Operation | CausalPhase::Owner => {
                        let previous = self
                            .0
                            .get(&route.session)
                            .or_else(|| base.0.get(&route.session))
                            .ok_or_else(|| err("causal operation before bootstrap"))?;
                        if route.venue != previous.venue
                            || route.bootstrap_id != previous.bootstrap_id
                            || Some(route.ordinal) != previous.ordinal.checked_add(1)
                            || (route.generation != previous.generation
                                && Some(route.generation) != previous.generation.checked_add(1))
                            || (envelope.phase == CausalPhase::Owner
                                && route.generation != previous.generation)
                        {
                            return Err(err("causal route discontinuity"));
                        }
                    }
                }
                self.0.insert(route.session, route.clone());
                Ok(())
            }
            (
                FeedProfile::EngineCausalV1
                | FeedProfile::EngineCausalV2
                | FeedProfile::EngineCausalV3
                | FeedProfile::EngineCausalV4,
                _,
            ) => Err(err("legacy record in causal feed")),
        }
    }
}
