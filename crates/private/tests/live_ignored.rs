//! Live private user-data smokes (ignored by default — needs API keys; keep CI offline).
//!
//! **No order placement.** Authentication + idle WS only.
//!
//! Env vars (never print values):
//! - OKX: `OKX_API_KEY` / `OKX_API_SECRET` / `OKX_API_PASSPHRASE`
//! - Bybit: `BYBIT_API_KEY` / `BYBIT_API_SECRET`
//! - Duration: `PRIVATE_LIVE_SECS` (default 5), `PRIVATE_LIVE_EXTENDED_SECS` (default 30)
//!
//! ```bash
//! # from repo root, with env set (or `.env` — never commit `.env`)
//! set -a && source .env && set +a
//! cargo test -p marketfeed-private --features live --test live_ignored -- --ignored --nocapture
//!
//! # laptop archive helper (skips venues whose keys are missing):
//! ./scripts/laptop_private_canary.sh
//! ```

use std::env;
use std::time::Duration;

use marketfeed_private::{
    bybit_session_from_env, okx_session_from_env, run_bybit_private_live, run_okx_private_live,
};
use marketfeed_transport::TungsteniteWebSocket;

fn env_secs(name: &str, default: u64) -> Duration {
    let secs = env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
        .max(1);
    Duration::from_secs(secs)
}

fn live_secs() -> Duration {
    env_secs("PRIVATE_LIVE_SECS", 5)
}

fn extended_secs() -> Duration {
    env_secs("PRIVATE_LIVE_EXTENDED_SECS", 30)
}

#[tokio::test]
#[ignore = "live private: needs OKX_API_KEY/SECRET/PASSPHRASE; cargo test -p marketfeed-private --features live --test live_ignored -- --ignored"]
async fn live_okx_private_login_and_ws() {
    let mut session = match okx_session_from_env() {
        Ok(s) => s,
        Err(_) => {
            eprintln!(
                "skip: set OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE to run this live smoke"
            );
            return;
        }
    };

    let mut ws = TungsteniteWebSocket::new();
    let dur = live_secs();
    let stats = run_okx_private_live(&mut session, &mut ws, dur)
        .await
        .expect("live okx private");

    assert!(
        stats.marked_live && session.is_live() && session.is_authed(),
        "expected MarkLive after OKX login"
    );
    assert!(stats.ws_writes >= 1, "expected login SendText write");
    eprintln!(
        "live okx private smoke ok: secs={} marked_live={} ws_writes={} text_frames={} account_events={}",
        dur.as_secs(),
        stats.marked_live,
        stats.ws_writes,
        stats.text_frames,
        stats.account_events
    );
}

#[tokio::test]
#[ignore = "live private: needs BYBIT_API_KEY/SECRET; cargo test -p marketfeed-private --features live --test live_ignored -- --ignored"]
async fn live_bybit_private_auth_and_ws() {
    let mut session = match bybit_session_from_env() {
        Ok(s) => s,
        Err(_) => {
            eprintln!("skip: set BYBIT_API_KEY and BYBIT_API_SECRET to run this live smoke");
            return;
        }
    };

    let mut ws = TungsteniteWebSocket::new();
    let dur = live_secs();
    let stats = run_bybit_private_live(&mut session, &mut ws, dur)
        .await
        .expect("live bybit private");

    assert!(
        stats.marked_live && session.is_live() && session.is_authed(),
        "expected MarkLive after Bybit auth"
    );
    assert!(stats.ws_writes >= 1, "expected auth SendText write");
    eprintln!(
        "live bybit private smoke ok: secs={} marked_live={} ws_writes={} text_frames={} account_events={}",
        dur.as_secs(),
        stats.marked_live,
        stats.ws_writes,
        stats.text_frames,
        stats.account_events
    );
}

/// Longer idle after MarkLive — multi-event surface when the account is active.
/// Idle accounts may still report `account_events=0`; that is OK.
#[tokio::test]
#[ignore = "live private extended: needs OKX_*; PRIVATE_LIVE_EXTENDED_SECS"]
async fn live_okx_private_extended() {
    let mut session = match okx_session_from_env() {
        Ok(s) => s,
        Err(_) => {
            eprintln!("skip: set OKX_* for extended private smoke");
            return;
        }
    };
    let mut ws = TungsteniteWebSocket::new();
    let dur = extended_secs();
    let stats = run_okx_private_live(&mut session, &mut ws, dur)
        .await
        .expect("extended okx private");
    assert!(
        stats.marked_live && session.is_authed(),
        "expected MarkLive"
    );
    eprintln!(
        "live okx private extended ok: secs={} text_frames={} account_events={} ws_writes={}",
        dur.as_secs(),
        stats.text_frames,
        stats.account_events,
        stats.ws_writes
    );
}

#[tokio::test]
#[ignore = "live private extended: needs BYBIT_*; PRIVATE_LIVE_EXTENDED_SECS"]
async fn live_bybit_private_extended() {
    let mut session = match bybit_session_from_env() {
        Ok(s) => s,
        Err(_) => {
            eprintln!("skip: set BYBIT_* for extended private smoke");
            return;
        }
    };
    let mut ws = TungsteniteWebSocket::new();
    let dur = extended_secs();
    let stats = run_bybit_private_live(&mut session, &mut ws, dur)
        .await
        .expect("extended bybit private");
    assert!(
        stats.marked_live && session.is_authed(),
        "expected MarkLive"
    );
    eprintln!(
        "live bybit private extended ok: secs={} text_frames={} account_events={} ws_writes={}",
        dur.as_secs(),
        stats.text_frames,
        stats.account_events,
        stats.ws_writes
    );
}

/// Re-bootstrap after close: MarkLive → close → fresh session → MarkLive.
/// Not an engine-level kill-switch reconnect probe (public canary covers that).
#[tokio::test]
#[ignore = "live private reauth: needs OKX_*"]
async fn live_okx_private_reauth_probe() {
    let dur = Duration::from_secs(3);

    let mut session1 = match okx_session_from_env() {
        Ok(s) => s,
        Err(_) => {
            eprintln!("skip: set OKX_* for reauth probe");
            return;
        }
    };
    let mut ws1 = TungsteniteWebSocket::new();
    let s1 = run_okx_private_live(&mut session1, &mut ws1, dur)
        .await
        .expect("okx private pass1");
    assert!(s1.marked_live, "pass1 MarkLive");

    let mut session2 = okx_session_from_env().expect("OKX_* still set");
    let mut ws2 = TungsteniteWebSocket::new();
    let s2 = run_okx_private_live(&mut session2, &mut ws2, dur)
        .await
        .expect("okx private pass2");
    assert!(s2.marked_live, "pass2 MarkLive after reauth");
    eprintln!(
        "live okx private reauth ok: pass1_ws_writes={} pass2_ws_writes={}",
        s1.ws_writes, s2.ws_writes
    );
}

#[tokio::test]
#[ignore = "live private reauth: needs BYBIT_*"]
async fn live_bybit_private_reauth_probe() {
    let dur = Duration::from_secs(3);

    let mut session1 = match bybit_session_from_env() {
        Ok(s) => s,
        Err(_) => {
            eprintln!("skip: set BYBIT_* for reauth probe");
            return;
        }
    };
    let mut ws1 = TungsteniteWebSocket::new();
    let s1 = run_bybit_private_live(&mut session1, &mut ws1, dur)
        .await
        .expect("bybit private pass1");
    assert!(s1.marked_live, "pass1 MarkLive");

    let mut session2 = bybit_session_from_env().expect("BYBIT_* still set");
    let mut ws2 = TungsteniteWebSocket::new();
    let s2 = run_bybit_private_live(&mut session2, &mut ws2, dur)
        .await
        .expect("bybit private pass2");
    assert!(s2.marked_live, "pass2 MarkLive after reauth");
    eprintln!(
        "live bybit private reauth ok: pass1_ws_writes={} pass2_ws_writes={}",
        s1.ws_writes, s2.ws_writes
    );
}
