//! Public Hyperliquid spot/perpetual protocols. Pure parsing; networking belongs
//! to the collector. Prices and quantities never pass through binary floats.
mod factory;
pub mod instruments;
pub mod protocol;
mod session;
mod specification;
pub use factory::HyperliquidFactory;
pub use session::HyperliquidSession;
pub use specification::*;

/// This package currently declares no Cargo features. Keep the selected
/// adapter explicit in causal build bindings, including that empty set.
pub fn compiled_features() -> Vec<String> {
    Vec::new()
}
