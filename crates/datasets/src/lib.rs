//! Offline exact storage. Publication never promotes uncertain source coverage.
#![recursion_limit = "512"]
pub mod export;
mod page_bounds;
pub mod parquet;
pub mod store;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub fn fail(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message).into()
}
pub fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}
