//! Bounded lexical JSON DOM for Bybit. Native financial values remain exact;
//! duplicate keys and excessive bytes/structure/work fail before decoding.
//! The simd-json feature does not select a float financial DOM. Backend parity
//! uses this same boundary; explicit native expectations provide the oracle.
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
