use super::*;

/// Full decimal domain, including i128 extrema and every native u8 scale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixedDto {
    #[serde(deserialize_with = "coefficient")]
    pub coefficient: String,
    pub scale: u8,
}
fn coefficient<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let text = String::deserialize(d)?;
    let value = text.parse::<i128>().map_err(serde::de::Error::custom)?;
    if value.to_string() != text {
        return Err(serde::de::Error::custom("noncanonical decimal coefficient"));
    }
    Ok(text)
}
impl From<Fixed> for FixedDto {
    fn from(value: Fixed) -> Self {
        Self {
            coefficient: value.coefficient.to_string(),
            scale: value.scale,
        }
    }
}
impl FixedDto {
    pub fn fixed(&self) -> Result<Fixed, CodecError> {
        let coefficient = self
            .coefficient
            .parse::<i128>()
            .map_err(|_| invalid("invalid coefficient"))?;
        if coefficient.to_string() != self.coefficient {
            return Err(invalid("noncanonical coefficient"));
        }
        Ok(Fixed {
            coefficient,
            scale: self.scale,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKindDto {
    Spot,
    PerpetualLinear,
    PerpetualInverse,
    FutureLinear,
    FutureInverse,
    Option,
}
impl From<InstrumentKind> for InstrumentKindDto {
    fn from(value: InstrumentKind) -> Self {
        match value {
            InstrumentKind::Spot => Self::Spot,
            InstrumentKind::PerpetualLinear => Self::PerpetualLinear,
            InstrumentKind::PerpetualInverse => Self::PerpetualInverse,
            InstrumentKind::FutureLinear => Self::FutureLinear,
            InstrumentKind::FutureInverse => Self::FutureInverse,
            InstrumentKind::Option => Self::Option,
        }
    }
}
impl From<InstrumentKindDto> for InstrumentKind {
    fn from(value: InstrumentKindDto) -> Self {
        match value {
            InstrumentKindDto::Spot => Self::Spot,
            InstrumentKindDto::PerpetualLinear => Self::PerpetualLinear,
            InstrumentKindDto::PerpetualInverse => Self::PerpetualInverse,
            InstrumentKindDto::FutureLinear => Self::FutureLinear,
            InstrumentKindDto::FutureInverse => Self::FutureInverse,
            InstrumentKindDto::Option => Self::Option,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentKeyDto {
    pub venue: String,
    pub native_symbol: String,
    pub kind: InstrumentKindDto,
    pub settlement: Option<String>,
    pub expiry_ns: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentDto {
    pub id: u32,
    pub key: InstrumentKeyDto,
    pub base: String,
    pub quote: String,
    pub settlement: Option<String>,
    pub price_scale: u8,
    pub quantity_scale: u8,
    pub price_increment: FixedDto,
    pub quantity_increment: FixedDto,
    pub min_quantity: Option<FixedDto>,
    pub max_quantity: Option<FixedDto>,
    pub min_notional: Option<FixedDto>,
    pub contract_size: Option<FixedDto>,
    pub expiry_ns: Option<i64>,
    pub status: InstrumentStatus,
    pub inverse: bool,
    pub catalog_version: u64,
}
impl From<&Instrument> for InstrumentDto {
    fn from(value: &Instrument) -> Self {
        Self {
            id: value.id.0,
            key: InstrumentKeyDto {
                venue: value.key.venue.0.clone(),
                native_symbol: value.key.native_symbol.clone(),
                kind: value.key.kind.into(),
                settlement: value.key.settlement.as_ref().map(|s| s.0.clone()),
                expiry_ns: value.key.expiry_ns,
            },
            base: value.base.0.clone(),
            quote: value.quote.0.clone(),
            settlement: value.settlement.as_ref().map(|s| s.0.clone()),
            price_scale: value.price_scale,
            quantity_scale: value.quantity_scale,
            price_increment: value.price_increment.into(),
            quantity_increment: value.quantity_increment.into(),
            min_quantity: value.min_quantity.map(Into::into),
            max_quantity: value.max_quantity.map(Into::into),
            min_notional: value.min_notional.map(Into::into),
            contract_size: value.contract_size.map(Into::into),
            expiry_ns: value.expiry_ns,
            status: value.status,
            inverse: value.inverse,
            catalog_version: value.catalog_version.0,
        }
    }
}
impl InstrumentDto {
    pub fn instrument(&self) -> Result<Instrument, CodecError> {
        Ok(Instrument {
            id: InstrumentId(self.id),
            key: InstrumentKey {
                venue: VenueCode(self.key.venue.clone()),
                native_symbol: self.key.native_symbol.clone(),
                kind: self.key.kind.into(),
                settlement: self.key.settlement.clone().map(AssetCode),
                expiry_ns: self.key.expiry_ns,
            },
            base: AssetCode(self.base.clone()),
            quote: AssetCode(self.quote.clone()),
            settlement: self.settlement.clone().map(AssetCode),
            price_scale: self.price_scale,
            quantity_scale: self.quantity_scale,
            price_increment: self.price_increment.fixed()?,
            quantity_increment: self.quantity_increment.fixed()?,
            min_quantity: self
                .min_quantity
                .as_ref()
                .map(FixedDto::fixed)
                .transpose()?,
            max_quantity: self
                .max_quantity
                .as_ref()
                .map(FixedDto::fixed)
                .transpose()?,
            min_notional: self
                .min_notional
                .as_ref()
                .map(FixedDto::fixed)
                .transpose()?,
            contract_size: self
                .contract_size
                .as_ref()
                .map(FixedDto::fixed)
                .transpose()?,
            expiry_ns: self.expiry_ns,
            status: self.status,
            inverse: self.inverse,
            catalog_version: CatalogVersion(self.catalog_version),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDto {
    pub venue: u16,
    pub version: u64,
    pub instruments: Vec<InstrumentDto>,
}
impl From<&CatalogView> for CatalogDto {
    fn from(value: &CatalogView) -> Self {
        Self {
            venue: value.venue.0,
            version: value.version.0,
            instruments: value.instruments.iter().map(Into::into).collect(),
        }
    }
}
impl CatalogDto {
    pub fn catalog(&self) -> Result<CatalogView, CodecError> {
        Ok(CatalogView::with_instruments(
            VenueId(self.venue),
            CatalogVersion(self.version),
            self.instruments
                .iter()
                .map(InstrumentDto::instrument)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
}
