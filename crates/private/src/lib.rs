//! Private account streams — Phase 6 alpha (fixture SM + optional live wire).
//!
//! Fixture-driven [`PrivateSessionMachine`] for Binance Spot, OKX Spot, and
//! Bybit Spot account streams. Live paths (feature `live`) drive those SMs over
//! engine transports; credentials such as [`BinanceApiCredentials`] load only
//! at the private library boundary. Daemon enablement is fail-closed until it
//! has durable account-event delivery and private-session supervision.
//! No order placement.

#![forbid(unsafe_code)]

mod account;
mod binance_spot;
mod bybit;
mod credentials;
mod error;
#[cfg(feature = "live")]
mod live;
mod okx;
mod session;

pub use account::{
    AccountEvent, AccountEventSink, Balance, BalanceDelta, Fill, OrderUpdate, Position,
    PrivateStream,
};
pub use binance_spot::{
    BINANCE_SPOT_VENUE_ID, BinanceSpotUserDataConfig, BinanceSpotUserDataSession,
};
pub use bybit::{
    BYBIT_PRIVATE_WS_URL, BYBIT_SPOT_VENUE_ID, BybitPrivateConfig, BybitPrivateSession,
    FIXTURE_AUTH_PAYLOAD,
};
pub use credentials::{
    BinanceApiCredentials, BybitApiCredentials, CredentialsError, OkxApiCredentials,
};
pub use error::PrivateError;
#[cfg(feature = "live")]
pub use live::{
    NullAccountSink, PrivateLiveStats, binance_spot_session_from_env, bybit_session_from_env,
    okx_session_from_env, run_binance_spot_user_data_live, run_binance_spot_user_data_live_until,
    run_bybit_private_live, run_bybit_private_live_until, run_okx_private_live,
    run_okx_private_live_until,
};
pub use okx::{
    FIXTURE_LOGIN_PAYLOAD, OKX_PRIVATE_WS_URL, OKX_SPOT_VENUE_ID, OkxPrivateConfig,
    OkxPrivateSession,
};
pub use session::{
    DEFAULT_PRIVATE_ACTION_BUFFER_CAPACITY, PrivateActionBuffer, PrivateSessionAction,
    PrivateSessionMachine,
};
