//! Minimal C ABI surface for embedding / language bindings.
//!
//! Exposes crate version and [`marketfeed_model::Fixed`] decimal parse only —
//! not sessions, books, or the full event API.
//!
//! Hand-written header: [`include/marketfeed.h`](../include/marketfeed.h).
//! # ponytail
//! No `cbindgen` build-dep; update the header by hand if signatures change.

#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_int};
use std::ptr;

use marketfeed_model::{Fixed, FixedError};

/// C layout of [`Fixed`]: `i128` coefficient as little-endian limbs + scale.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MfFixed {
    pub coefficient_lo: u64,
    pub coefficient_hi: i64,
    pub scale: u8,
}

impl From<Fixed> for MfFixed {
    fn from(value: Fixed) -> Self {
        Self {
            coefficient_lo: value.coefficient as u64,
            coefficient_hi: (value.coefficient >> 64) as i64,
            scale: value.scale,
        }
    }
}

impl MfFixed {
    /// Reassemble the Rust [`Fixed`] (for tests / round-trip checks).
    pub fn to_fixed(self) -> Fixed {
        let coefficient = (i128::from(self.coefficient_hi) << 64) | i128::from(self.coefficient_lo);
        Fixed::new(coefficient, self.scale)
    }
}

/// Success.
pub const MF_OK: c_int = 0;
/// Null pointer argument.
pub const MF_ERR_NULL: c_int = 1;
/// Empty decimal input.
pub const MF_ERR_EMPTY: c_int = 2;
/// Invalid decimal syntax.
pub const MF_ERR_SYNTAX: c_int = 3;
/// Coefficient overflow.
pub const MF_ERR_OVERFLOW: c_int = 4;
/// Scale overflow.
pub const MF_ERR_SCALE: c_int = 5;
/// Inexact rescale (unused by parse; reserved).
pub const MF_ERR_INEXACT: c_int = 6;

fn map_fixed_error(err: FixedError) -> c_int {
    match err {
        FixedError::Empty => MF_ERR_EMPTY,
        FixedError::InvalidSyntax => MF_ERR_SYNTAX,
        FixedError::Overflow => MF_ERR_OVERFLOW,
        FixedError::ScaleOverflow => MF_ERR_SCALE,
        FixedError::InexactRescale => MF_ERR_INEXACT,
    }
}

/// NUL-terminated crate version (`CARGO_PKG_VERSION`).
#[unsafe(no_mangle)]
pub extern "C" fn marketfeed_version() -> *const c_char {
    static VERSION: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
    VERSION.as_ptr().cast::<c_char>()
}

/// Parse ASCII decimal bytes into `out`.
///
/// `ptr` may be null only when `len == 0` (returns [`MF_ERR_EMPTY`]).
///
/// # Safety
///
/// - When `len > 0`, `ptr` must be valid for `len` bytes.
/// - `out` must be non-null and writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marketfeed_fixed_parse(
    ptr: *const c_char,
    len: usize,
    out: *mut MfFixed,
) -> c_int {
    if out.is_null() {
        return MF_ERR_NULL;
    }
    if ptr.is_null() {
        return if len == 0 { MF_ERR_EMPTY } else { MF_ERR_NULL };
    }
    // SAFETY: caller guarantees `ptr` is valid for `len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
    match Fixed::parse_decimal(bytes) {
        Ok(fixed) => {
            // SAFETY: caller guarantees `out` is writable.
            unsafe {
                ptr::write(out, MfFixed::from(fixed));
            }
            MF_OK
        }
        Err(err) => map_fixed_error(err),
    }
}

/// Parse a NUL-terminated C string into `out`.
///
/// # Safety
///
/// - `s` must be a valid NUL-terminated C string (or null → [`MF_ERR_NULL`]).
/// - `out` must be non-null and writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marketfeed_fixed_parse_cstr(s: *const c_char, out: *mut MfFixed) -> c_int {
    if s.is_null() || out.is_null() {
        return MF_ERR_NULL;
    }
    // SAFETY: caller guarantees a valid C string at `s`.
    let cstr = unsafe { CStr::from_ptr(s) };
    match Fixed::parse_decimal(cstr.to_bytes()) {
        Ok(fixed) => {
            // SAFETY: caller guarantees `out` is writable.
            unsafe {
                ptr::write(out, MfFixed::from(fixed));
            }
            MF_OK
        }
        Err(err) => map_fixed_error(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_nonempty_cstr() {
        let p = marketfeed_version();
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn fixed_layout_matches_public_c_header() {
        assert_eq!(std::mem::size_of::<MfFixed>(), 24);
        assert_eq!(std::mem::align_of::<MfFixed>(), 8);
        assert_eq!(std::mem::offset_of!(MfFixed, coefficient_lo), 0);
        assert_eq!(std::mem::offset_of!(MfFixed, coefficient_hi), 8);
        assert_eq!(std::mem::offset_of!(MfFixed, scale), 16);
    }

    #[test]
    fn parse_ok_and_roundtrip() {
        let bytes = b"123.45";
        let mut out = MfFixed::default();
        let rc = unsafe {
            marketfeed_fixed_parse(bytes.as_ptr().cast::<c_char>(), bytes.len(), &mut out)
        };
        assert_eq!(rc, MF_OK);
        assert_eq!(out.scale, 2);
        assert_eq!(out.coefficient_lo, 12345);
        assert_eq!(out.coefficient_hi, 0);
        assert_eq!(out.to_fixed(), Fixed::new(12345, 2));
    }

    #[test]
    fn parse_negative_cstr() {
        let s = c"-0.001";
        let mut out = MfFixed::default();
        let rc = unsafe { marketfeed_fixed_parse_cstr(s.as_ptr(), &mut out) };
        assert_eq!(rc, MF_OK);
        assert_eq!(out.to_fixed(), Fixed::new(-1, 3));
    }

    #[test]
    fn parse_errors() {
        let mut out = MfFixed::default();
        assert_eq!(
            unsafe { marketfeed_fixed_parse(ptr::null(), 0, &mut out) },
            MF_ERR_EMPTY
        );
        assert_eq!(
            unsafe { marketfeed_fixed_parse(ptr::null(), 1, &mut out) },
            MF_ERR_NULL
        );
        assert_eq!(
            unsafe { marketfeed_fixed_parse(b"1".as_ptr().cast(), 1, ptr::null_mut()) },
            MF_ERR_NULL
        );
        assert_eq!(
            unsafe { marketfeed_fixed_parse(b"1e3".as_ptr().cast(), 3, &mut out) },
            MF_ERR_SYNTAX
        );
        assert_eq!(
            unsafe { marketfeed_fixed_parse_cstr(ptr::null(), &mut out) },
            MF_ERR_NULL
        );
    }
}
