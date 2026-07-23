//! Fuzz L2 book transitions (snapshot + deltas) for no-panic / invariant errors only.
#![no_main]

use libfuzzer_sys::fuzz_target;
use marketfeed_book::{BookOperation, BookSide, OrderBook};
use marketfeed_model::{Fixed, Price, Quantity};

fn price(coeff: i128) -> Price {
    Price(Fixed::new(coeff, 2))
}

fn qty(coeff: i128) -> Quantity {
    Quantity(Fixed::new(coeff, 3))
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let depth = if data[0] & 1 == 0 {
        Some(u32::from(data[0] % 8) + 1)
    } else {
        None
    };
    let mut book = OrderBook::new(2, 3, depth);
    let mut i = 1usize;
    if data.len() >= 1 + 16 {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for k in 0..2 {
            let off = 1 + k * 4;
            let bp = i128::from(data[off]) * 100;
            let bq = i128::from(data[off + 1]).max(1);
            let ap = i128::from(data[off + 2]) * 100 + 10_000;
            let aq = i128::from(data[off + 3]).max(1);
            bids.push((price(bp), qty(bq)));
            asks.push((price(ap), qty(aq)));
        }
        let _ = book.apply_snapshot(&bids, &asks, Some(1));
        i = 1 + 16;
    } else {
        let _ = book.apply_snapshot(&[(price(100_00), qty(1))], &[(price(101_00), qty(1))], Some(1));
    }

    while i + 3 < data.len() {
        let side = if data[i] & 1 == 0 {
            BookSide::Bid
        } else {
            BookSide::Ask
        };
        let op = if data[i + 1] & 1 == 0 {
            BookOperation::Upsert
        } else {
            BookOperation::Delete
        };
        let px = price(i128::from(data[i + 2]) * 100);
        let q = if op == BookOperation::Upsert {
            Some(qty(i128::from(data[i + 3])))
        } else {
            None
        };
        let _ = book.apply_change(side, op, px, q);
        let _ = book.best_bid();
        let _ = book.best_ask();
        let _ = book.snapshot_levels();
        i += 4;
    }
});
