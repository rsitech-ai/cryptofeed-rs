//! Coinbase International (INTX) authenticated market-data protocol.

mod credentials;
mod factory;
mod instruments;
mod messages;
mod session;
mod specification;

pub use credentials::{CoinbaseIntlCredentials, CredentialsError, SubscribeAuth};
pub use factory::{CoinbaseIntlFactory, session_config_from_catalog};
pub use instruments::parse_instruments;
pub use messages::{
    BookLevelChange, BookSideWire, DecodedEvent, TradeRow, decode_text, ns_to_ts, trade_id_source,
};
pub use session::{CoinbaseIntlSession, CoinbaseIntlSessionConfig};
pub use specification::{COINBASE_INTL_SPEC, COINBASE_INTL_VENUE_ID, REST_BASE, WS_URL, ws_url};
