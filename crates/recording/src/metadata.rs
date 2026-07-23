//! Reproduction metadata embedded in every production MFR1 segment.

use marketfeed_adapter_api::SessionSpec;
use marketfeed_model::{CatalogView, Fixed, SessionId, VenueId};
use serde::{Deserialize, Serialize};

use crate::RecordingError;

const METADATA_SCHEMA_VERSION: u16 = 1;
const MAX_METADATA_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataRecord {
    Build(BuildMetadata),
    Session(SessionRecordingMetadata),
}

impl MetadataRecord {
    pub fn current_build() -> Self {
        Self::Build(BuildMetadata::current())
    }

    pub fn session_id(&self) -> SessionId {
        match self {
            Self::Build(_) => SessionId(0),
            Self::Session(metadata) => SessionId(metadata.session_id),
        }
    }

    pub fn stable_key(&self) -> String {
        match self {
            Self::Build(_) => "build".into(),
            Self::Session(metadata) => format!("session:{}", metadata.session_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildMetadata {
    pub schema_version: u16,
    pub package_name: String,
    pub package_version: String,
    pub build_sha: Option<String>,
    pub target_os: String,
    pub target_arch: String,
}

impl BuildMetadata {
    pub fn current() -> Self {
        let build_sha = option_env!("MARKETFEED_BUILD_SHA")
            .or(option_env!("VERGEN_GIT_SHA"))
            .map(str::to_owned);
        Self {
            schema_version: METADATA_SCHEMA_VERSION,
            package_name: "marketfeed".into(),
            package_version: env!("CARGO_PKG_VERSION").into(),
            build_sha,
            target_os: std::env::consts::OS.into(),
            target_arch: std::env::consts::ARCH.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRecordingMetadata {
    pub schema_version: u16,
    pub session_id: u64,
    pub venue_id: u16,
    pub adapter: String,
    pub environment: String,
    pub endpoint: String,
    pub catalog_version: u64,
    pub catalog: Vec<CatalogInstrumentMetadata>,
    pub initial_subscriptions: Vec<SubscriptionMetadata>,
}

impl SessionRecordingMetadata {
    pub fn from_plan(
        session: SessionId,
        venue: VenueId,
        adapter: impl Into<String>,
        environment: impl Into<String>,
        spec: &SessionSpec,
        catalog: &CatalogView,
    ) -> Self {
        let catalog_entries = catalog
            .instruments
            .iter()
            .map(|instrument| CatalogInstrumentMetadata {
                instrument_id: instrument.id.0,
                native_symbol: instrument.key.native_symbol.clone(),
                kind: format!("{:?}", instrument.key.kind),
                base: instrument.base.0.clone(),
                quote: instrument.quote.0.clone(),
                settlement: instrument.settlement.as_ref().map(|asset| asset.0.clone()),
                price_scale: instrument.price_scale,
                quantity_scale: instrument.quantity_scale,
                price_increment: FixedMetadata::from(instrument.price_increment),
                quantity_increment: FixedMetadata::from(instrument.quantity_increment),
                min_quantity: instrument.min_quantity.map(FixedMetadata::from),
                max_quantity: instrument.max_quantity.map(FixedMetadata::from),
                min_notional: instrument.min_notional.map(FixedMetadata::from),
                contract_size: instrument.contract_size.map(FixedMetadata::from),
                expiry_ns: instrument.expiry_ns,
                status: format!("{:?}", instrument.status),
                inverse: instrument.inverse,
            })
            .collect();
        let initial_subscriptions = spec
            .subscriptions
            .items
            .iter()
            .map(|subscription| SubscriptionMetadata {
                instrument_id: subscription.instrument.0,
                channel: format!("{:?}", subscription.channel),
                emit_book_snapshots: subscription.delivery.emit_book_snapshots,
                emit_book_deltas: subscription.delivery.emit_book_deltas,
                emit_bbo: subscription.delivery.emit_bbo,
            })
            .collect();
        Self {
            schema_version: METADATA_SCHEMA_VERSION,
            session_id: session.0,
            venue_id: venue.0,
            adapter: adapter.into(),
            environment: environment.into(),
            endpoint: sanitize_endpoint(&spec.endpoint_name),
            catalog_version: catalog.version.0,
            catalog: catalog_entries,
            initial_subscriptions,
        }
    }
}

fn sanitize_endpoint(endpoint: &str) -> String {
    let (base, query) = endpoint
        .split_once('?')
        .map_or((endpoint, None), |(base, query)| (base, Some(query)));
    let base = if let Some((scheme, authority_and_path)) = base.split_once("://") {
        let authority_end = authority_and_path
            .find('/')
            .unwrap_or(authority_and_path.len());
        let (authority, path) = authority_and_path.split_at(authority_end);
        if let Some((_, host)) = authority.rsplit_once('@') {
            format!("{scheme}://[REDACTED]@{host}{path}")
        } else {
            base.to_string()
        }
    } else {
        base.to_string()
    };
    let Some(query) = query else {
        return base;
    };
    let query = query
        .split('&')
        .map(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            if is_sensitive_query_name(name) {
                format!("{name}=[REDACTED]")
            } else if value.is_empty() {
                name.to_string()
            } else {
                format!("{name}={value}")
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{query}")
}

fn is_sensitive_query_name(name: &str) -> bool {
    let normalized = name.trim().to_ascii_lowercase().replace('-', "_");
    [
        "api_key",
        "apikey",
        "key",
        "signature",
        "sig",
        "token",
        "secret",
        "passphrase",
        "authorization",
        "auth",
    ]
    .iter()
    .any(|candidate| normalized == *candidate || normalized.ends_with(&format!("_{candidate}")))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogInstrumentMetadata {
    pub instrument_id: u32,
    pub native_symbol: String,
    pub kind: String,
    pub base: String,
    pub quote: String,
    pub settlement: Option<String>,
    pub price_scale: u8,
    pub quantity_scale: u8,
    pub price_increment: FixedMetadata,
    pub quantity_increment: FixedMetadata,
    pub min_quantity: Option<FixedMetadata>,
    pub max_quantity: Option<FixedMetadata>,
    pub min_notional: Option<FixedMetadata>,
    pub contract_size: Option<FixedMetadata>,
    pub expiry_ns: Option<i64>,
    pub status: String,
    pub inverse: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixedMetadata {
    pub coefficient: String,
    pub scale: u8,
}

impl From<Fixed> for FixedMetadata {
    fn from(value: Fixed) -> Self {
        Self {
            coefficient: value.coefficient.to_string(),
            scale: value.scale,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionMetadata {
    pub instrument_id: u32,
    pub channel: String,
    pub emit_book_snapshots: bool,
    pub emit_book_deltas: bool,
    pub emit_bbo: bool,
}

pub fn encode_metadata(metadata: &MetadataRecord) -> Result<Vec<u8>, RecordingError> {
    let bytes = serde_json::to_vec(metadata).map_err(|_| RecordingError::InvalidHeader)?;
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(RecordingError::RecordTooLarge {
            record_len: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
            max: MAX_METADATA_BYTES as u32,
        });
    }
    Ok(bytes)
}

pub fn decode_metadata(payload: &[u8]) -> Result<MetadataRecord, RecordingError> {
    if payload.len() > MAX_METADATA_BYTES {
        return Err(RecordingError::RecordTooLarge {
            record_len: u32::try_from(payload.len()).unwrap_or(u32::MAX),
            max: MAX_METADATA_BYTES as u32,
        });
    }
    serde_json::from_slice(payload).map_err(|_| RecordingError::InvalidHeader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
    use marketfeed_model::{CatalogVersion, CatalogView};

    #[test]
    fn build_metadata_roundtrips_without_secret_environment_access() {
        let metadata = MetadataRecord::current_build();
        let decoded = decode_metadata(&encode_metadata(&metadata).unwrap()).unwrap();
        assert_eq!(decoded, metadata);
        let MetadataRecord::Build(build) = decoded else {
            panic!("build metadata");
        };
        assert_eq!(build.schema_version, 1);
        assert_eq!(build.package_name, "marketfeed");
    }

    #[test]
    fn session_metadata_preserves_reproduction_identity() {
        let spec = SessionSpec {
            endpoint_name: "wss://example.invalid/ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        };
        let catalog = CatalogView::new(VenueId(9), CatalogVersion(7));
        let metadata = MetadataRecord::Session(SessionRecordingMetadata::from_plan(
            SessionId(42),
            VenueId(9),
            "example",
            "test",
            &spec,
            &catalog,
        ));
        let decoded = decode_metadata(&encode_metadata(&metadata).unwrap()).unwrap();
        assert_eq!(decoded, metadata);
        let MetadataRecord::Session(session) = decoded else {
            panic!("session metadata");
        };
        assert_eq!(session.catalog_version, 7);
        assert_eq!(session.endpoint, "wss://example.invalid/ws");
    }

    #[test]
    fn session_metadata_redacts_endpoint_userinfo_and_secret_query_values() {
        let spec = SessionSpec {
            endpoint_name:
                "wss://user:password@example.invalid/ws?symbol=BTCUSD&api_key=secret&signature=sig"
                    .into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        };
        let catalog = CatalogView::new(VenueId(9), CatalogVersion(7));
        let metadata = SessionRecordingMetadata::from_plan(
            SessionId(42),
            VenueId(9),
            "example",
            "test",
            &spec,
            &catalog,
        );
        assert_eq!(
            metadata.endpoint,
            "wss://[REDACTED]@example.invalid/ws?symbol=BTCUSD&api_key=[REDACTED]&signature=[REDACTED]"
        );
        assert!(!metadata.endpoint.contains("password"));
        assert!(!metadata.endpoint.contains("secret"));
    }
}
