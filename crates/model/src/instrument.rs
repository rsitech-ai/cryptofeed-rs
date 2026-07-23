//! Instrument identity and catalog metadata.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{CatalogVersion, Fixed, InstrumentId, VenueId};

/// Stable external instrument key (not a single inferred string).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstrumentKey {
    pub venue: VenueCode,
    /// ponytail: String until CompactString dep is justified; ceiling = hot-path alloc; upgrade = compact_str.
    pub native_symbol: String,
    pub kind: InstrumentKind,
    pub settlement: Option<AssetCode>,
    pub expiry_ns: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VenueCode(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssetCode(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstrumentKind {
    Spot,
    PerpetualLinear,
    PerpetualInverse,
    FutureLinear,
    FutureInverse,
    Option,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstrumentStatus {
    Active,
    Suspended,
    Expired,
    Delisted,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    pub id: InstrumentId,
    pub key: InstrumentKey,
    pub base: AssetCode,
    pub quote: AssetCode,
    pub settlement: Option<AssetCode>,
    pub price_scale: u8,
    pub quantity_scale: u8,
    pub price_increment: Fixed,
    pub quantity_increment: Fixed,
    pub min_quantity: Option<Fixed>,
    pub max_quantity: Option<Fixed>,
    pub min_notional: Option<Fixed>,
    pub contract_size: Option<Fixed>,
    pub expiry_ns: Option<i64>,
    pub status: InstrumentStatus,
    pub inverse: bool,
    pub catalog_version: CatalogVersion,
}

/// Adapter-facing instrument definition before catalog ID assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentDefinition {
    pub key: InstrumentKey,
    pub base: AssetCode,
    pub quote: AssetCode,
    pub settlement: Option<AssetCode>,
    pub price_scale: u8,
    pub quantity_scale: u8,
    pub price_increment: Fixed,
    pub quantity_increment: Fixed,
    pub min_quantity: Option<Fixed>,
    pub max_quantity: Option<Fixed>,
    pub min_notional: Option<Fixed>,
    pub contract_size: Option<Fixed>,
    pub expiry_ns: Option<i64>,
    pub status: InstrumentStatus,
    pub inverse: bool,
}

impl InstrumentDefinition {
    pub fn into_instrument(self, id: InstrumentId, catalog_version: CatalogVersion) -> Instrument {
        Instrument {
            id,
            key: self.key,
            base: self.base,
            quote: self.quote,
            settlement: self.settlement,
            price_scale: self.price_scale,
            quantity_scale: self.quantity_scale,
            price_increment: self.price_increment,
            quantity_increment: self.quantity_increment,
            min_quantity: self.min_quantity,
            max_quantity: self.max_quantity,
            min_notional: self.min_notional,
            contract_size: self.contract_size,
            expiry_ns: self.expiry_ns,
            status: self.status,
            inverse: self.inverse,
            catalog_version,
        }
    }
}

/// Snapshot of assigned instruments visible to adapters for one venue version.
///
/// # ponytail
/// Arc slice until a catalog manager owns lookups; empty means caller supplies
/// scales via session config. Upgrade: process-wide catalog with versioned views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogView {
    pub venue: VenueId,
    pub version: CatalogVersion,
    pub instruments: Arc<[Instrument]>,
}

impl CatalogView {
    pub fn new(venue: VenueId, version: CatalogVersion) -> Self {
        Self {
            venue,
            version,
            instruments: Arc::from([]),
        }
    }

    pub fn with_instruments(
        venue: VenueId,
        version: CatalogVersion,
        instruments: impl Into<Arc<[Instrument]>>,
    ) -> Self {
        Self {
            venue,
            version,
            instruments: instruments.into(),
        }
    }

    pub fn find_by_native(&self, symbol: &str) -> Option<&Instrument> {
        self.instruments
            .iter()
            .find(|i| i.key.native_symbol == symbol)
    }
}
