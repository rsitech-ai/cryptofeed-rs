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
