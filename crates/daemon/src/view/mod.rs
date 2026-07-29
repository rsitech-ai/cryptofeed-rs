//! Loopback view plane: aggregated books + bounded trade/quote tape for the UI.
//!
//! Venue tasks do not share a single [`marketfeed_engine::EngineControl`] handle
//! today (see `reload` ponytail). Books are therefore maintained from the same
//! normalized `BookSnapshot` / `BookDelta` events the engine would expose via
//! `book_snapshot`, with an API that mirrors that control surface. When a shared
//! control plane exists, prefer wiring `EngineControl::book_snapshot` directly;
//! until then event-derived books are the production path for the view panel.

pub mod http;
mod plane;
mod replay;

pub use http::{handle_view_conn, handle_view_conn_with_prefix, respond_view_request, serve_view};
pub use plane::{
    SharedViewPlane, TapeEntry, ViewBookSnapshot, ViewPlane, ViewPlaneConfig, ViewStatus,
};
