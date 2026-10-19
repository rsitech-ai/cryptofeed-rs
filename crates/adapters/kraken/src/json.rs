//! Lexical financial JSON DOM for Kraken Spot/Futures. Decimal tokens
//! always use the shared RawValue boundary, including builds with simd-json.
//! This optional feature no longer selects a float-based financial DOM.
use serde_json::Value;
pub fn value_from_slice(bytes: &[u8]) -> Result<Value, String> {
    marketfeed_adapter_api::exact_value_from_slice(bytes)
}
pub fn value_from_slice_serde(bytes: &[u8]) -> Result<Value, String> {
    marketfeed_adapter_api::exact_value_from_slice(bytes)
}
#[cfg(feature = "simd-json")]
pub fn value_from_slice_simd(bytes: &[u8]) -> Result<Value, String> {
    marketfeed_adapter_api::exact_value_from_slice(bytes)
}
