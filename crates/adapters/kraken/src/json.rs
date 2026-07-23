//! Shared JSON deserialize helpers for Kraken Spot WS v2 hot-path decode.
//!
//! # ponytail
//! Default backend is `serde_json` (no SIMD, no mutable scratch). Optional
//! crate feature `simd-json` switches [`from_slice`] / [`value_from_slice`] to
//! `simd-json` (mutates a scratch `Vec`, dependency contains `unsafe`).
//!
//! **Enable criteria:** only after a latency / parse histogram profile shows
//! JSON parse as a bottleneck under realistic load
//! ([`docs/ops/latency_runtime.md`](../../../../docs/ops/latency_runtime.md)).
//! Do not enable in portable public binaries by default.
//!
//! **Ceiling:** optional parse path only — no latency profiles / bench gate.
//! Upgrade = evidence-backed enable criteria.

use serde::de::DeserializeOwned;
use serde_json::Value;

/// Deserialize `bytes` with the **active** backend (serde default, simd when featured).
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    #[cfg(feature = "simd-json")]
    {
        from_slice_simd(bytes)
    }
    #[cfg(not(feature = "simd-json"))]
    {
        from_slice_serde(bytes)
    }
}

/// Always `serde_json` — reference path for parity tests.
pub fn from_slice_serde<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}

/// `simd-json` Serde path (feature-gated). Mutates a scratch copy of `bytes`.
#[cfg(feature = "simd-json")]
pub fn from_slice_simd<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let mut buf = bytes.to_vec();
    simd_json::serde::from_slice(&mut buf).map_err(|e| e.to_string())
}

/// Parse into `serde_json::Value` with the active backend.
pub fn value_from_slice(bytes: &[u8]) -> Result<Value, String> {
    from_slice(bytes)
}

/// Always-serde `Value` parse (parity reference).
pub fn value_from_slice_serde(bytes: &[u8]) -> Result<Value, String> {
    from_slice_serde(bytes)
}

#[cfg(feature = "simd-json")]
pub fn value_from_slice_simd(bytes: &[u8]) -> Result<Value, String> {
    from_slice_simd(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Tiny {
        channel: String,
    }

    #[test]
    fn serde_backend_roundtrip() {
        let raw = br#"{"channel":"trade"}"#;
        let v: Tiny = from_slice_serde(raw).unwrap();
        assert_eq!(
            v,
            Tiny {
                channel: "trade".into(),
            }
        );
    }

    #[cfg(feature = "simd-json")]
    #[test]
    fn serde_simd_value_and_typed_parity() {
        let raw = br#"{"channel":"trade","nested":{"x":1}}"#;
        let a: Value = value_from_slice_serde(raw).unwrap();
        let b: Value = value_from_slice_simd(raw).unwrap();
        assert_eq!(a, b);
        let ta: Tiny = from_slice_serde(raw).unwrap();
        let tb: Tiny = from_slice_simd(raw).unwrap();
        assert_eq!(ta, tb);
    }
}
