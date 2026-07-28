//! Optional production daemon library (config, health, metrics, venue wiring).

#![forbid(unsafe_code)]

pub mod catalog_discover;
pub mod cli;
pub mod config;
pub mod http;
pub mod private;
pub mod reload;
pub mod run;
pub mod sinks;
pub mod state;
pub(crate) mod subscriptions;
#[cfg(feature = "ui-api")]
pub mod view;

pub use config::{ConfigError, DaemonConfig};
pub use http::serve;
pub use private::spawn_private_sessions;
pub use reload::{ReloadPlan, ReloadableConfig, classify_reload, plan_reload_from_path};
pub use run::spawn_venues;
pub use sinks::DaemonSinks;
pub use state::{DaemonState, evaluate_readiness};
#[cfg(feature = "ui-api")]
pub use view::serve_view;
