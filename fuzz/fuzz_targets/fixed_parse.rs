//! Fuzz `Fixed::parse_decimal` for no-panic + error-or-ok only.
#![no_main]

use libfuzzer_sys::fuzz_target;
use marketfeed_model::Fixed;

fuzz_target!(|data: &[u8]| {
    let _ = Fixed::parse_decimal(data);
});
