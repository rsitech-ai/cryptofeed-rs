use std::cmp::Ordering;

use marketfeed_model::{Fixed, Price, Quantity, RoundingMode};
use serde::{Deserialize, Serialize};

use crate::{AnalyticsError, invalid_config, overflow};

/// Integer index of one configured price bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PriceBucket(pub i128);

/// Integer index of one exact configured price tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PriceTick(pub i128);

/// Quantity represented at the grid's canonical quantity scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QuantityUnits(pub i128);

/// Signed quantity represented at the grid's canonical quantity scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SignedQuantityUnits(pub i128);

/// Validated exact conversion between model decimals and analytics integer units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GridSpec {
    pub price_scale: u8,
    pub quantity_scale: u8,
    pub tick_size: Fixed,
    pub levels_per_bucket: u32,
}

pub(crate) fn compare_price(left: Price, right: Price) -> Ordering {
    debug_assert_eq!(left.0.scale, right.0.scale);
    left.0.coefficient.cmp(&right.0.coefficient)
}

pub(crate) fn min_price(left: Price, right: Price) -> Price {
    if compare_price(left, right).is_gt() {
        right
    } else {
        left
    }
}

pub(crate) fn max_price(left: Price, right: Price) -> Price {
    if compare_price(left, right).is_lt() {
        right
    } else {
        left
    }
}

impl GridSpec {
    pub fn new(
        price_scale: u8,
        quantity_scale: u8,
        tick_size: Fixed,
        levels_per_bucket: u32,
    ) -> Result<Self, AnalyticsError> {
        if levels_per_bucket == 0 {
            return Err(invalid_config(
                "levels_per_bucket",
                "must be greater than zero",
            ));
        }
        let tick_size = tick_size
            .rescale(price_scale, RoundingMode::ExactOnly)
            .map_err(|source| AnalyticsError::Fixed {
                field: "tick_size",
                source,
            })?;
        if tick_size.coefficient <= 0 {
            return Err(AnalyticsError::NonPositive { field: "tick_size" });
        }
        tick_size
            .coefficient
            .checked_mul(i128::from(levels_per_bucket))
            .ok_or_else(|| overflow("calculating bucket size"))?;
        Ok(Self {
            price_scale,
            quantity_scale,
            tick_size,
            levels_per_bucket,
        })
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(
            self.price_scale,
            self.quantity_scale,
            self.tick_size,
            self.levels_per_bucket,
        )
        .map(|_| ())
    }

    pub fn bucket_size_coefficient(&self) -> Result<i128, AnalyticsError> {
        self.tick_size
            .coefficient
            .checked_mul(i128::from(self.levels_per_bucket))
            .ok_or_else(|| overflow("calculating bucket size"))
    }

    pub fn price_bucket(&self, price: Price) -> Result<PriceBucket, AnalyticsError> {
        let tick = self.price_tick(price)?;
        Ok(PriceBucket(
            tick.0.div_euclid(i128::from(self.levels_per_bucket)),
        ))
    }

    pub fn price_tick(&self, price: Price) -> Result<PriceTick, AnalyticsError> {
        let price = price
            .0
            .rescale(self.price_scale, RoundingMode::ExactOnly)
            .map_err(|source| AnalyticsError::Fixed {
                field: "price",
                source,
            })?;
        if price.coefficient <= 0 {
            return Err(AnalyticsError::NonPositive { field: "price" });
        }
        if price.coefficient % self.tick_size.coefficient != 0 {
            return Err(AnalyticsError::MisalignedPrice);
        }
        Ok(PriceTick(price.coefficient / self.tick_size.coefficient))
    }

    pub fn price_at_tick(&self, tick: PriceTick) -> Result<Price, AnalyticsError> {
        if tick.0 <= 0 {
            return Err(AnalyticsError::NonPositive {
                field: "price tick",
            });
        }
        let coefficient = tick
            .0
            .checked_mul(self.tick_size.coefficient)
            .ok_or_else(|| overflow("converting a price tick"))?;
        Ok(Price(Fixed::new(coefficient, self.price_scale)))
    }

    pub fn price_at(&self, bucket: PriceBucket) -> Result<Price, AnalyticsError> {
        if bucket.0 <= 0 {
            return Err(AnalyticsError::NonPositive {
                field: "price bucket",
            });
        }
        let coefficient = bucket
            .0
            .checked_mul(self.bucket_size_coefficient()?)
            .ok_or_else(|| overflow("converting a price bucket"))?;
        Ok(Price(Fixed::new(coefficient, self.price_scale)))
    }

    pub fn quantity_units(&self, quantity: Quantity) -> Result<QuantityUnits, AnalyticsError> {
        let units = self.non_negative_quantity_units(quantity)?;
        if units.0 == 0 {
            return Err(AnalyticsError::NonPositive { field: "quantity" });
        }
        Ok(units)
    }

    pub(crate) fn non_negative_quantity_units(
        &self,
        quantity: Quantity,
    ) -> Result<QuantityUnits, AnalyticsError> {
        let quantity = quantity
            .0
            .rescale(self.quantity_scale, RoundingMode::ExactOnly)
            .map_err(|source| AnalyticsError::Fixed {
                field: "quantity",
                source,
            })?;
        if quantity.coefficient < 0 {
            return Err(AnalyticsError::NonPositive { field: "quantity" });
        }
        Ok(QuantityUnits(quantity.coefficient))
    }

    pub fn quantity_at(&self, units: QuantityUnits) -> Result<Quantity, AnalyticsError> {
        if units.0 < 0 {
            return Err(invalid_config("quantity units", "must not be negative"));
        }
        Ok(Quantity(Fixed::new(units.0, self.quantity_scale)))
    }

    pub fn signed_quantity_at(&self, units: SignedQuantityUnits) -> Fixed {
        Fixed::new(units.0, self.quantity_scale)
    }
}
