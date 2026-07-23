//! Exact fixed-point decimal arithmetic.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical exact decimal: `value = coefficient * 10^(-scale)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Fixed {
    pub coefficient: i128,
    pub scale: u8,
}

/// Newtype wrappers keep semantic domains distinct at the type level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Price(pub Fixed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Quantity(pub Fixed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Rate(pub Fixed);

/// Explicit rounding when rescaling; silent rounding is forbidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoundingMode {
    ExactOnly,
    TowardZero,
    AwayFromZero,
    TowardNegInfinity,
    TowardPosInfinity,
    HalfAwayFromZero,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FixedError {
    #[error("empty decimal input")]
    Empty,
    #[error("invalid decimal syntax")]
    InvalidSyntax,
    #[error("decimal overflow")]
    Overflow,
    #[error("inexact rescale under ExactOnly")]
    InexactRescale,
    #[error("scale overflow")]
    ScaleOverflow,
}

impl Fixed {
    pub const ZERO: Self = Self {
        coefficient: 0,
        scale: 0,
    };

    #[inline]
    pub const fn new(coefficient: i128, scale: u8) -> Self {
        Self { coefficient, scale }
    }

    /// Parse ASCII decimal bytes (`[-]?[0-9]+(\.[0-9]+)?`).
    ///
    /// Scientific notation is rejected here; enable only when a venue emits it
    /// and conversion remains exact.
    pub fn parse_decimal(bytes: &[u8]) -> Result<Self, FixedError> {
        if bytes.is_empty() {
            return Err(FixedError::Empty);
        }

        let (neg, rest) = match bytes[0] {
            b'-' => (true, &bytes[1..]),
            b'+' => (false, &bytes[1..]),
            _ => (false, bytes),
        };
        if rest.is_empty() {
            return Err(FixedError::InvalidSyntax);
        }

        let mut int_digits: &[u8] = rest;
        let mut frac_digits: &[u8] = &[];
        if let Some(dot) = rest.iter().position(|&b| b == b'.') {
            int_digits = &rest[..dot];
            frac_digits = &rest[dot + 1..];
            if int_digits.is_empty() && frac_digits.is_empty() {
                return Err(FixedError::InvalidSyntax);
            }
            if frac_digits.contains(&b'.') {
                return Err(FixedError::InvalidSyntax);
            }
        }

        if int_digits.is_empty() {
            int_digits = b"0";
        }
        if !int_digits.iter().all(u8::is_ascii_digit) || !frac_digits.iter().all(u8::is_ascii_digit)
        {
            return Err(FixedError::InvalidSyntax);
        }

        // Strip leading zeros from integer part (keep one zero if all zeros).
        let int_digits = trim_leading_zeros(int_digits);
        let scale = u8::try_from(frac_digits.len()).map_err(|_| FixedError::ScaleOverflow)?;

        let mut coeff: i128 = 0;
        for &d in int_digits.iter().chain(frac_digits.iter()) {
            let digit = (d - b'0') as i128;
            coeff = coeff
                .checked_mul(10)
                .and_then(|c| c.checked_add(digit))
                .ok_or(FixedError::Overflow)?;
        }
        if neg {
            coeff = coeff.checked_neg().ok_or(FixedError::Overflow)?;
        }

        Ok(Self {
            coefficient: coeff,
            scale,
        })
    }

    /// Parse an exact decimal, including JSON scientific notation.
    ///
    /// Exponents are applied with checked integer arithmetic; no binary
    /// floating-point conversion or rounding is involved.
    pub fn parse_str(s: &str) -> Result<Self, FixedError> {
        let Some(exponent_at) = s.as_bytes().iter().position(|b| matches!(b, b'e' | b'E')) else {
            return Self::parse_decimal(s.as_bytes());
        };
        if s.as_bytes()[exponent_at + 1..]
            .iter()
            .any(|b| matches!(b, b'e' | b'E'))
        {
            return Err(FixedError::InvalidSyntax);
        }

        let mantissa = Self::parse_decimal(&s.as_bytes()[..exponent_at])?;
        let exponent_bytes = &s.as_bytes()[exponent_at + 1..];
        if exponent_bytes.is_empty() {
            return Err(FixedError::InvalidSyntax);
        }
        let (negative_exponent, digits) = match exponent_bytes[0] {
            b'-' => (true, &exponent_bytes[1..]),
            b'+' => (false, &exponent_bytes[1..]),
            _ => (false, exponent_bytes),
        };
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(FixedError::InvalidSyntax);
        }
        let mut exponent = 0u32;
        for &digit in digits {
            exponent = exponent
                .checked_mul(10)
                .and_then(|value| value.checked_add(u32::from(digit - b'0')))
                .ok_or(FixedError::Overflow)?;
        }
        if mantissa.coefficient == 0 {
            return Ok(Self::ZERO);
        }

        if negative_exponent {
            let exponent = u8::try_from(exponent).map_err(|_| FixedError::ScaleOverflow)?;
            let scale = mantissa
                .scale
                .checked_add(exponent)
                .ok_or(FixedError::ScaleOverflow)?;
            return Ok(Self {
                coefficient: mantissa.coefficient,
                scale,
            });
        }

        let scale = u32::from(mantissa.scale);
        if exponent <= scale {
            return Ok(Self {
                coefficient: mantissa.coefficient,
                scale: u8::try_from(scale - exponent).expect("difference remains within u8"),
            });
        }
        let coefficient = mantissa
            .coefficient
            .checked_mul(pow10_i128(exponent - scale)?)
            .ok_or(FixedError::Overflow)?;
        Ok(Self {
            coefficient,
            scale: 0,
        })
    }

    /// Rescale with an explicit rounding mode. Never rounds silently.
    pub fn rescale(self, new_scale: u8, mode: RoundingMode) -> Result<Self, FixedError> {
        if new_scale == self.scale {
            return Ok(self);
        }
        if new_scale > self.scale {
            let delta = (new_scale - self.scale) as u32;
            let factor = pow10_i128(delta)?;
            let coefficient = self
                .coefficient
                .checked_mul(factor)
                .ok_or(FixedError::Overflow)?;
            return Ok(Self {
                coefficient,
                scale: new_scale,
            });
        }

        let delta = (self.scale - new_scale) as u32;
        let divisor = pow10_i128(delta)?;
        let q = self.coefficient / divisor;
        let r = self.coefficient % divisor;
        if r == 0 {
            return Ok(Self {
                coefficient: q,
                scale: new_scale,
            });
        }

        let coefficient = match mode {
            RoundingMode::ExactOnly => return Err(FixedError::InexactRescale),
            RoundingMode::TowardZero => q,
            RoundingMode::AwayFromZero => {
                if self.coefficient > 0 {
                    q.checked_add(1).ok_or(FixedError::Overflow)?
                } else {
                    q.checked_sub(1).ok_or(FixedError::Overflow)?
                }
            }
            RoundingMode::TowardNegInfinity => {
                if self.coefficient < 0 {
                    q.checked_sub(1).ok_or(FixedError::Overflow)?
                } else {
                    q
                }
            }
            RoundingMode::TowardPosInfinity => {
                if self.coefficient > 0 {
                    q.checked_add(1).ok_or(FixedError::Overflow)?
                } else {
                    q
                }
            }
            RoundingMode::HalfAwayFromZero => {
                let half = divisor / 2;
                let abs_r = r.unsigned_abs();
                if abs_r * 2 < divisor.unsigned_abs() {
                    q
                } else if abs_r > half.unsigned_abs() || abs_r * 2 == divisor.unsigned_abs() {
                    if self.coefficient > 0 {
                        q.checked_add(1).ok_or(FixedError::Overflow)?
                    } else {
                        q.checked_sub(1).ok_or(FixedError::Overflow)?
                    }
                } else {
                    q
                }
            }
        };

        Ok(Self {
            coefficient,
            scale: new_scale,
        })
    }

    /// Convenience only — never use as canonical storage.
    pub fn to_f64_lossy(self) -> f64 {
        let mut v = self.coefficient as f64;
        for _ in 0..self.scale {
            v /= 10.0;
        }
        v
    }
}

fn trim_leading_zeros(digits: &[u8]) -> &[u8] {
    let first_nonzero = digits
        .iter()
        .position(|&b| b != b'0')
        .unwrap_or(digits.len());
    if first_nonzero == digits.len() {
        &digits[digits.len().saturating_sub(1)..]
    } else {
        &digits[first_nonzero..]
    }
}

fn pow10_i128(exp: u32) -> Result<i128, FixedError> {
    10i128.checked_pow(exp).ok_or(FixedError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_decimals() {
        assert_eq!(Fixed::parse_str("123.45").unwrap(), Fixed::new(12345, 2));
        assert_eq!(Fixed::parse_str("-0.001").unwrap(), Fixed::new(-1, 3));
        assert_eq!(Fixed::parse_str("42").unwrap(), Fixed::new(42, 0));
        assert_eq!(Fixed::parse_str(".5").unwrap(), Fixed::new(5, 1));
        assert_eq!(Fixed::parse_str("0").unwrap(), Fixed::new(0, 0));
    }

    #[test]
    fn parse_scientific_decimals_exactly() {
        assert_eq!(Fixed::parse_str("1e3").unwrap(), Fixed::new(1_000, 0));
        assert_eq!(Fixed::parse_str("1.0e4").unwrap(), Fixed::new(10_000, 0));
        assert_eq!(Fixed::parse_str("9.74e-6").unwrap(), Fixed::new(974, 8));
        assert_eq!(Fixed::parse_str("1e-8").unwrap(), Fixed::new(1, 8));
        assert_eq!(Fixed::parse_str("0e-255").unwrap(), Fixed::ZERO);
        assert_eq!(
            Fixed::parse_str("-8713e4").unwrap(),
            Fixed::new(-87_130_000, 0)
        );
        assert_eq!(Fixed::parse_str("1.25E+2").unwrap(), Fixed::new(125, 0));
    }

    #[test]
    fn reject_bad_syntax_and_overflow() {
        assert!(matches!(Fixed::parse_str(""), Err(FixedError::Empty)));
        assert!(matches!(
            Fixed::parse_str("1.2.3"),
            Err(FixedError::InvalidSyntax)
        ));
        assert!(matches!(
            Fixed::parse_str("1e"),
            Err(FixedError::InvalidSyntax)
        ));
        assert!(matches!(
            Fixed::parse_str("1e999999999999999999999"),
            Err(FixedError::Overflow)
        ));
        // 39 nines overflow i128 when parsed as integer*scale combined.
        let too_big = "9".repeat(39);
        assert!(matches!(
            Fixed::parse_str(&too_big),
            Err(FixedError::Overflow)
        ));
    }

    #[test]
    fn rescale_exact_and_rounding() {
        let v = Fixed::new(12345, 2);
        assert_eq!(
            v.rescale(4, RoundingMode::ExactOnly).unwrap(),
            Fixed::new(1_234_500, 4)
        );
        assert!(matches!(
            v.rescale(1, RoundingMode::ExactOnly),
            Err(FixedError::InexactRescale)
        ));
        assert_eq!(
            v.rescale(1, RoundingMode::TowardZero).unwrap(),
            Fixed::new(1234, 1)
        );
        assert_eq!(
            Fixed::new(-15, 1)
                .rescale(0, RoundingMode::TowardNegInfinity)
                .unwrap(),
            Fixed::new(-2, 0)
        );
    }

    // Property: exact upscale then downscale preserves the original Fixed.
    // Cases kept small so `cargo test --workspace` CI stays light.
    proptest::proptest! {
        #[test]
        fn exact_rescale_roundtrip(
            coeff in -1000i128..=1000,
            scale in 0u8..=6,
            delta in 1u8..=4,
        ) {
            let f = Fixed::new(coeff, scale);
            let new_scale = scale + delta;
            let Ok(up) = f.rescale(new_scale, RoundingMode::ExactOnly) else {
                return Ok(());
            };
            let down = up.rescale(scale, RoundingMode::ExactOnly).unwrap();
            proptest::prop_assert_eq!(down, f);
        }
    }

    /// Lightweight no-panic corpus for CI; full coverage lives in `fuzz/fixed_parse`.
    #[test]
    fn parse_decimal_fuzz_smoke_no_panic() {
        let seeds: &[&[u8]] = &[
            b"",
            b"-",
            b"+",
            b".",
            b"1e3",
            b"1.2.3",
            b"0000123.4500",
            b"-0.000",
            b"999999999999999999999999999999999999999",
            &[0xff, 0x00, b'1', b'.', b'2'],
            b"1.\\02",
        ];
        for s in seeds {
            let _ = Fixed::parse_decimal(s);
        }
        // Deterministic LCG byte sequences (ponytail: not libFuzzer; upgrade = cargo fuzz).
        let mut state: u64 = 0xC0FFEE_u64;
        let mut buf = [0u8; 32];
        for _ in 0..2_048 {
            for b in &mut buf {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                *b = (state >> 33) as u8;
            }
            let len = (state as usize % buf.len()) + 1;
            let _ = Fixed::parse_decimal(&buf[..len]);
        }
    }
}
