//! L2 book storage with explicit invariants.

use std::collections::{BTreeMap, HashSet};

use marketfeed_model::{BookChange, BookLevel, BookOperation, BookSide, Fixed, Price, Quantity};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BookError {
    #[error("book is not valid for mutation/query: {0:?}")]
    NotValid(BookValidity),
    #[error("non-positive quantity on upsert")]
    NonPositiveQuantity,
    #[error("crossed book: best bid >= best ask")]
    Crossed,
    #[error("delete missing quantity is required to be absent or zero")]
    InvalidDelete,
    #[error("price/qty scale does not match catalog exactly")]
    ScaleMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookValidity {
    Valid,
    Synchronizing,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookMode {
    Disabled,
    RawDeltas,
    ValidatedDeltas,
    Maintain { depth: Option<u32> },
}

/// Internal L2 book. Uses BTreeMap (safe stdlib) with cached BBO.
///
/// Prices are stored as scaled i128 coefficients at a fixed catalog scale so
/// ordering does not depend on heterogeneous `Fixed.scale` values.
#[derive(Debug, Clone)]
pub struct OrderBook {
    price_scale: u8,
    qty_scale: u8,
    /// Bid prices descending via reverse iteration; key = price coefficient.
    bids: BTreeMap<i128, i128>,
    asks: BTreeMap<i128, i128>,
    validity: BookValidity,
    depth: Option<u32>,
    sequence: Option<u64>,
}

impl OrderBook {
    pub fn new(price_scale: u8, qty_scale: u8, depth: Option<u32>) -> Self {
        Self {
            price_scale,
            qty_scale,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            validity: BookValidity::Synchronizing,
            depth,
            sequence: None,
        }
    }

    pub fn validity(&self) -> BookValidity {
        self.validity
    }

    pub fn set_validity(&mut self, validity: BookValidity) {
        self.validity = validity;
    }

    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }

    pub fn set_sequence(&mut self, seq: u64) {
        self.sequence = Some(seq);
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.sequence = None;
    }

    pub fn apply_snapshot(
        &mut self,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        sequence: Option<u64>,
    ) -> Result<(), BookError> {
        let mut next = Self::new(self.price_scale, self.qty_scale, self.depth);
        next.validity = BookValidity::Synchronizing;
        for &(price, qty) in bids {
            next.upsert(BookSide::Bid, price, qty)?;
        }
        for &(price, qty) in asks {
            next.upsert(BookSide::Ask, price, qty)?;
        }
        next.trim_depth();
        next.check_not_crossed()?;
        next.validity = BookValidity::Valid;
        next.sequence = sequence;
        // Atomic ownership transfer after validation.
        *self = next;
        Ok(())
    }

    pub fn apply_change(
        &mut self,
        side: BookSide,
        operation: BookOperation,
        price: Price,
        quantity: Option<Quantity>,
    ) -> Result<(), BookError> {
        if self.validity != BookValidity::Valid {
            return Err(BookError::NotValid(self.validity));
        }
        match operation {
            BookOperation::Delete => {
                self.delete(side, price);
                Ok(())
            }
            BookOperation::Upsert => {
                let qty = quantity.ok_or(BookError::InvalidDelete)?;
                // Zero quantity normalizes to delete (spec §8.10).
                if qty.0.coefficient == 0 {
                    self.delete(side, price);
                    return Ok(());
                }
                self.upsert(side, price, qty)?;
                self.trim_depth();
                if let Err(err) = self.check_not_crossed() {
                    self.validity = BookValidity::Invalid;
                    return Err(err);
                }
                Ok(())
            }
        }
    }

    /// Apply one wire message's level changes as a single transaction.
    ///
    /// Venue updates may contain an ordering of deletes and upserts whose
    /// intermediate states are crossed even though the completed message is
    /// valid. Mutate a candidate book, validate the final state once, and only
    /// then commit it.
    pub fn apply_changes_atomic(&mut self, changes: &[BookChange]) -> Result<(), BookError> {
        if self.validity != BookValidity::Valid {
            return Err(BookError::NotValid(self.validity));
        }

        // Normalize and validate every fallible field before touching the book.
        // `None` is a delete; invalid-scale deletes preserve `delete`'s no-op
        // semantics and are omitted.
        let mut prepared = Vec::with_capacity(changes.len());
        for change in changes {
            let mutation = match change.operation {
                BookOperation::Delete => self
                    .price_key(change.price)
                    .ok()
                    .map(|price| (change.side, price, None)),
                BookOperation::Upsert => {
                    let quantity = change.quantity.ok_or(BookError::InvalidDelete)?;
                    if quantity.0.coefficient == 0 {
                        self.price_key(change.price)
                            .ok()
                            .map(|price| (change.side, price, None))
                    } else {
                        if quantity.0.coefficient < 0 {
                            return Err(BookError::NonPositiveQuantity);
                        }
                        let price = self.price_key(change.price)?;
                        let quantity = scale_coeff(quantity.0, self.qty_scale)?;
                        Some((change.side, price, Some(quantity)))
                    }
                }
            };
            if let Some(mutation) = mutation {
                prepared.push(mutation);
            }
        }

        // Record each touched level once so a failed final invariant check can
        // restore the exact pre-message state without cloning the whole book.
        let mut undo = Vec::with_capacity(prepared.len());
        let mut recorded = HashSet::with_capacity(prepared.len());
        for &(side, price, quantity) in &prepared {
            if recorded.insert((side, price)) {
                let previous = match side {
                    BookSide::Bid => self.bids.get(&price).copied(),
                    BookSide::Ask => self.asks.get(&price).copied(),
                };
                undo.push((side, price, previous));
            }
            let levels = match side {
                BookSide::Bid => &mut self.bids,
                BookSide::Ask => &mut self.asks,
            };
            if let Some(quantity) = quantity {
                levels.insert(price, quantity);
            } else {
                levels.remove(&price);
            }
        }

        if let Err(error) = self.check_not_crossed() {
            for (side, price, previous) in undo {
                let levels = match side {
                    BookSide::Bid => &mut self.bids,
                    BookSide::Ask => &mut self.asks,
                };
                if let Some(quantity) = previous {
                    levels.insert(price, quantity);
                } else {
                    levels.remove(&price);
                }
            }
            return Err(error);
        }

        self.trim_depth();
        Ok(())
    }

    pub fn best_bid(&self) -> Option<(Price, Quantity)> {
        if self.validity != BookValidity::Valid {
            return None;
        }
        self.bids
            .iter()
            .next_back()
            .map(|(&p, &q)| (self.price(p), self.qty(q)))
    }

    pub fn best_ask(&self) -> Option<(Price, Quantity)> {
        if self.validity != BookValidity::Valid {
            return None;
        }
        self.asks
            .iter()
            .next()
            .map(|(&p, &q)| (self.price(p), self.qty(q)))
    }

    pub fn snapshot_levels(&self) -> Option<(Vec<BookLevel>, Vec<BookLevel>)> {
        if self.validity != BookValidity::Valid {
            return None;
        }
        let bids: Vec<BookLevel> = self
            .bids
            .iter()
            .rev()
            .map(|(&p, &q)| BookLevel {
                price: self.price(p),
                quantity: self.qty(q),
            })
            .collect();
        let asks: Vec<BookLevel> = self
            .asks
            .iter()
            .map(|(&p, &q)| BookLevel {
                price: self.price(p),
                quantity: self.qty(q),
            })
            .collect();
        Some((bids, asks))
    }

    fn upsert(&mut self, side: BookSide, price: Price, qty: Quantity) -> Result<(), BookError> {
        if qty.0.coefficient <= 0 {
            return Err(BookError::NonPositiveQuantity);
        }
        let pk = self.price_key(price)?;
        let qk = scale_coeff(qty.0, self.qty_scale)?;
        match side {
            BookSide::Bid => {
                self.bids.insert(pk, qk);
            }
            BookSide::Ask => {
                self.asks.insert(pk, qk);
            }
        }
        Ok(())
    }

    fn delete(&mut self, side: BookSide, price: Price) {
        // Invalid scale on delete: treat as no-op level miss (do not panic).
        let Ok(pk) = self.price_key(price) else {
            return;
        };
        match side {
            BookSide::Bid => {
                self.bids.remove(&pk);
            }
            BookSide::Ask => {
                self.asks.remove(&pk);
            }
        }
    }

    fn trim_depth(&mut self) {
        let Some(depth) = self.depth else {
            return;
        };
        let depth = depth as usize;
        while self.bids.len() > depth {
            // Drop worst bid (lowest price).
            if let Some((&k, _)) = self.bids.iter().next() {
                self.bids.remove(&k);
            } else {
                break;
            }
        }
        while self.asks.len() > depth {
            // Drop worst ask (highest price).
            if let Some((&k, _)) = self.asks.iter().next_back() {
                self.asks.remove(&k);
            } else {
                break;
            }
        }
    }

    fn check_not_crossed(&self) -> Result<(), BookError> {
        let Some((&bid, _)) = self.bids.iter().next_back() else {
            return Ok(());
        };
        let Some((&ask, _)) = self.asks.iter().next() else {
            return Ok(());
        };
        if bid >= ask {
            return Err(BookError::Crossed);
        }
        Ok(())
    }

    fn price_key(&self, price: Price) -> Result<i128, BookError> {
        scale_coeff(price.0, self.price_scale)
    }

    fn price(&self, coeff: i128) -> Price {
        Price(Fixed::new(coeff, self.price_scale))
    }

    fn qty(&self, coeff: i128) -> Quantity {
        Quantity(Fixed::new(coeff, self.qty_scale))
    }
}

fn scale_coeff(value: Fixed, target_scale: u8) -> Result<i128, BookError> {
    // Exact rescale only — book levels must match catalog scales.
    value
        .rescale(target_scale, marketfeed_model::RoundingMode::ExactOnly)
        .map(|v| v.coefficient)
        .map_err(|_| BookError::ScaleMismatch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::Fixed;

    fn px(c: i128) -> Price {
        Price(Fixed::new(c, 2))
    }
    fn qty(c: i128) -> Quantity {
        Quantity(Fixed::new(c, 3))
    }

    #[test]
    fn snapshot_and_delta_maintain_invariants() {
        let mut book = OrderBook::new(2, 3, Some(2));
        book.apply_snapshot(
            &[
                (px(100_00), qty(1_000)),
                (px(99_00), qty(2_000)),
                (px(98_00), qty(3_000)),
            ],
            &[(px(101_00), qty(1_500)), (px(102_00), qty(2_500))],
            Some(1),
        )
        .unwrap();
        assert_eq!(book.validity(), BookValidity::Valid);
        assert_eq!(book.best_bid().unwrap().0, px(100_00));
        assert_eq!(book.best_ask().unwrap().0, px(101_00));
        // depth=2 drops 98.00 bid
        let (bids, _) = book.snapshot_levels().unwrap();
        assert_eq!(bids.len(), 2);

        book.apply_change(
            BookSide::Bid,
            BookOperation::Upsert,
            px(100_50),
            Some(qty(500)),
        )
        .unwrap();
        assert_eq!(book.best_bid().unwrap().0, px(100_50));

        book.apply_change(BookSide::Ask, BookOperation::Delete, px(101_00), None)
            .unwrap();
        assert_eq!(book.best_ask().unwrap().0, px(102_00));
    }

    #[test]
    fn zero_qty_is_delete_and_cross_invalidates_apply() {
        let mut book = OrderBook::new(2, 3, None);
        book.apply_snapshot(&[(px(100_00), qty(1))], &[(px(101_00), qty(1))], Some(1))
            .unwrap();
        book.apply_change(
            BookSide::Bid,
            BookOperation::Upsert,
            px(100_00),
            Some(qty(0)),
        )
        .unwrap();
        assert!(book.best_bid().is_none());

        let err = book
            .apply_change(
                BookSide::Bid,
                BookOperation::Upsert,
                px(102_00),
                Some(qty(1)),
            )
            .unwrap_err();
        assert!(matches!(err, BookError::Crossed));
        assert_eq!(book.validity(), BookValidity::Invalid);
        assert!(book.best_bid().is_none());
        assert!(book.best_ask().is_none());
        assert!(book.snapshot_levels().is_none());
    }

    #[test]
    fn atomic_changes_validate_only_the_final_book() {
        let mut book = OrderBook::new(2, 3, None);
        book.apply_snapshot(
            &[(px(100_00), qty(1_000))],
            &[(px(101_00), qty(1_000))],
            Some(1),
        )
        .unwrap();

        book.apply_changes_atomic(&[
            BookChange {
                side: BookSide::Bid,
                operation: BookOperation::Upsert,
                price: px(102_00),
                quantity: Some(qty(1_000)),
            },
            BookChange {
                side: BookSide::Ask,
                operation: BookOperation::Delete,
                price: px(101_00),
                quantity: None,
            },
            BookChange {
                side: BookSide::Ask,
                operation: BookOperation::Upsert,
                price: px(103_00),
                quantity: Some(qty(1_000)),
            },
        ])
        .unwrap();

        assert_eq!(book.best_bid().unwrap().0, px(102_00));
        assert_eq!(book.best_ask().unwrap().0, px(103_00));
    }

    #[test]
    fn atomic_changes_reject_a_crossed_final_book_without_partial_commit() {
        let mut book = OrderBook::new(2, 3, None);
        book.apply_snapshot(
            &[(px(100_00), qty(1_000))],
            &[(px(101_00), qty(1_000))],
            Some(1),
        )
        .unwrap();

        let error = book
            .apply_changes_atomic(&[BookChange {
                side: BookSide::Bid,
                operation: BookOperation::Upsert,
                price: px(102_00),
                quantity: Some(qty(1_000)),
            }])
            .unwrap_err();

        assert_eq!(error, BookError::Crossed);
        assert_eq!(book.validity(), BookValidity::Valid);
        assert_eq!(book.best_bid().unwrap().0, px(100_00));
        assert_eq!(book.best_ask().unwrap().0, px(101_00));
    }

    #[test]
    fn atomic_changes_restore_original_level_after_duplicate_mutations_fail() {
        let mut book = OrderBook::new(2, 3, None);
        book.apply_snapshot(
            &[(px(100_00), qty(1_000))],
            &[(px(101_00), qty(1_000))],
            Some(1),
        )
        .unwrap();

        let error = book
            .apply_changes_atomic(&[
                BookChange {
                    side: BookSide::Bid,
                    operation: BookOperation::Upsert,
                    price: px(100_00),
                    quantity: Some(qty(2_000)),
                },
                BookChange {
                    side: BookSide::Bid,
                    operation: BookOperation::Upsert,
                    price: px(100_00),
                    quantity: Some(qty(3_000)),
                },
                BookChange {
                    side: BookSide::Bid,
                    operation: BookOperation::Upsert,
                    price: px(102_00),
                    quantity: Some(qty(1_000)),
                },
            ])
            .unwrap_err();

        assert_eq!(error, BookError::Crossed);
        let (bids, _) = book.snapshot_levels().unwrap();
        assert_eq!(bids[0].price, px(100_00));
        assert_eq!(bids[0].quantity, qty(1_000));
        assert_eq!(bids.len(), 1);
    }

    /// Lightweight no-panic corpus for CI; full coverage lives in `fuzz/book_transition`.
    #[test]
    fn book_transition_fuzz_smoke_no_panic() {
        let mut state: u64 = 0xB00C_u64;
        for _ in 0..256 {
            let depth = if state & 1 == 0 {
                Some(((state >> 8) % 8 + 1) as u32)
            } else {
                None
            };
            let mut book = OrderBook::new(2, 3, depth);
            let _ = book.apply_snapshot(
                &[(px(100_00), qty(1_000)), (px(99_00), qty(2_000))],
                &[(px(101_00), qty(1_500)), (px(102_00), qty(2_500))],
                Some(1),
            );
            for _ in 0..16 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let side = if state & 1 == 0 {
                    BookSide::Bid
                } else {
                    BookSide::Ask
                };
                let op = if (state >> 1) & 1 == 0 {
                    BookOperation::Upsert
                } else {
                    BookOperation::Delete
                };
                let price = px(((state >> 8) % 200) as i128 * 100);
                let quantity = if op == BookOperation::Upsert {
                    Some(qty(((state >> 16) % 50) as i128))
                } else {
                    None
                };
                let _ = book.apply_change(side, op, price, quantity);
                let _ = book.best_bid();
                let _ = book.best_ask();
                let _ = book.snapshot_levels();
            }
        }
    }
}
