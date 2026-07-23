//! Private user-data daemon gate.
//!
//! The `marketfeed-private` library supports explicit caller-owned account
//! sinks, but the daemon does not yet have a bounded durable account-event sink,
//! private readiness/liveness tracking, or reconnect supervision. Enabling a
//! private session in the daemon therefore fails closed instead of authenticating
//! and silently discarding account events.

use tokio::sync::watch;

use crate::config::DaemonConfig;

/// Return fail-closed tasks for programmatic configurations that bypass
/// [`DaemonConfig::validate`](crate::config::DaemonConfig::validate).
///
/// Parsed daemon configs reject these enable flags before runtime resources are
/// opened. This second gate prevents direct struct mutation from restoring the
/// former null-drain behavior.
pub fn spawn_private_sessions(
    config: &DaemonConfig,
    _shutdown: watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<Result<(), String>>> {
    let mut handles = Vec::new();
    for (enabled, name, reason) in [
        (
            config.private.binance_spot.enabled,
            "binance_spot",
            "retired listen-key protocol must be replaced by authenticated WebSocket API subscriptions",
        ),
        (
            config.private.okx_spot.enabled,
            "okx_spot",
            "bounded durable account-event sink, readiness tracking, and reconnect supervision are not implemented",
        ),
        (
            config.private.bybit_spot.enabled,
            "bybit_spot",
            "bounded durable account-event sink, readiness tracking, and reconnect supervision are not implemented",
        ),
    ] {
        if enabled {
            let error = format!("private {name} daemon session unavailable: {reason}");
            handles.push(tokio::spawn(async move {
                tracing::error!(session = name, %error, "private session rejected");
                Err(error)
            }));
        }
    }
    handles
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn programmatic_private_enablement_fails_closed() {
        let mut config =
            DaemonConfig::from_toml_str(include_str!("../config.offline.toml")).expect("config");
        config.private.binance_spot.enabled = true;
        config.private.okx_spot.enabled = true;
        config.private.bybit_spot.enabled = true;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let handles = spawn_private_sessions(&config, shutdown_rx);
        assert_eq!(handles.len(), 3);
        let mut errors = Vec::new();
        for handle in handles {
            errors.push(
                handle
                    .await
                    .expect("rejection task must not panic")
                    .expect_err("private daemon integration must fail closed"),
            );
        }
        assert!(errors.iter().any(|error| error.contains("listen-key")));
        assert_eq!(
            errors
                .iter()
                .filter(|error| error.contains("account-event sink"))
                .count(),
            2
        );
    }
}
