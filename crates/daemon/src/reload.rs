//! §21.4 config hot reload — validate + safe apply; rest needs restart.
//!
//! # ponytail
//! Ceiling: daemon venue loops own private `EngineSupervisor`s; there is no
//! shared `EngineControl` handle to apply subscription/sink patches in place.
//! Upgrade path: hold supervisors behind a control plane, then map TOML venue
//! symbol diffs → `SubscriptionPatch` / sink rebuild. Until then SIGHUP is
//! validate-only for unsafe keys (venues, sinks, recording, private, bind,
//! runtime_profile, log_format) and applies only log filter + readiness.

use crate::config::{DaemonConfig, ReadinessConfig};

/// In-process knobs that §21.4 MAY change without restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadableConfig {
    pub log_level: String,
    pub readiness: ReadinessConfig,
}

impl ReloadableConfig {
    pub fn from_daemon(cfg: &DaemonConfig) -> Self {
        Self {
            log_level: cfg.telemetry.log_level.clone(),
            readiness: cfg.readiness.clone(),
        }
    }
}

/// Outcome of comparing a re-validated TOML against the running process.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReloadPlan {
    /// New filter string when `telemetry.log_level` changed.
    pub apply_log_level: Option<String>,
    /// New readiness policy when `[readiness]` changed.
    pub apply_readiness: Option<ReadinessConfig>,
    /// Keys that validated but cannot be applied without process restart.
    pub restart_required: Vec<&'static str>,
}

impl ReloadPlan {
    pub fn is_noop(&self) -> bool {
        self.apply_log_level.is_none()
            && self.apply_readiness.is_none()
            && self.restart_required.is_empty()
    }
}

/// Classify a newly loaded (already validated) config against process state.
///
/// `base` is the start-of-process config (venues/sinks/recording frozen).
/// `applied` tracks reloadable knobs that may already differ from `base`.
pub fn classify_reload(
    base: &DaemonConfig,
    applied: &ReloadableConfig,
    new: &DaemonConfig,
) -> ReloadPlan {
    let mut plan = ReloadPlan::default();

    if new.telemetry.log_level != applied.log_level {
        plan.apply_log_level = Some(new.telemetry.log_level.clone());
    }
    if new.readiness != applied.readiness {
        plan.apply_readiness = Some(new.readiness.clone());
    }

    if new.engine.runtime_profile != base.engine.runtime_profile {
        plan.restart_required.push("engine.runtime_profile");
    }
    if new.engine.shutdown_deadline_secs != base.engine.shutdown_deadline_secs {
        plan.restart_required.push("engine.shutdown_deadline_secs");
    }
    if new.telemetry.log_format != base.telemetry.log_format {
        plan.restart_required.push("telemetry.log_format");
    }
    if new.telemetry.bind != base.telemetry.bind {
        plan.restart_required.push("telemetry.bind");
    }
    if new.telemetry.ui_bind != base.telemetry.ui_bind
        || new.telemetry.ui_tape_capacity != base.telemetry.ui_tape_capacity
        || new.telemetry.ui_tape_max_per_sec != base.telemetry.ui_tape_max_per_sec
        || new.telemetry.ui_static_dir != base.telemetry.ui_static_dir
    {
        plan.restart_required.push("telemetry.ui");
    }
    if new.recording != base.recording {
        plan.restart_required.push("recording");
    }
    if new.private != base.private {
        plan.restart_required.push("private");
    }
    if new.venues != base.venues {
        // ponytail: subscription-safe in spec via EngineControl — not wired here
        plan.restart_required.push("venues");
    }
    if new.sinks != base.sinks {
        plan.restart_required.push("sinks");
    }

    plan
}

/// Load + validate TOML and classify against running state.
pub fn plan_reload_from_path(
    path: &std::path::Path,
    base: &DaemonConfig,
    applied: &ReloadableConfig,
) -> Result<ReloadPlan, crate::config::ConfigError> {
    let new = DaemonConfig::load_path(path)?;
    Ok(classify_reload(base, applied, &new))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DaemonConfig {
        DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            log_level = "info"
            [[venues]]
            id = "syn"
            adapter = "synthetic"
            required = true
        "#,
        )
        .unwrap()
    }

    #[test]
    fn noop_when_identical() {
        let cfg = sample();
        let applied = ReloadableConfig::from_daemon(&cfg);
        assert!(classify_reload(&cfg, &applied, &cfg).is_noop());
    }

    #[test]
    fn applies_log_level_and_readiness() {
        let base = sample();
        let applied = ReloadableConfig::from_daemon(&base);
        let mut new = base.clone();
        new.telemetry.log_level = "debug".into();
        new.readiness.min_live_sessions = 2;
        let plan = classify_reload(&base, &applied, &new);
        assert_eq!(plan.apply_log_level.as_deref(), Some("debug"));
        assert_eq!(plan.apply_readiness.unwrap().min_live_sessions, 2);
        assert!(plan.restart_required.is_empty());
    }

    #[test]
    fn venues_require_restart() {
        let base = sample();
        let applied = ReloadableConfig::from_daemon(&base);
        let mut new = base.clone();
        new.venues[0].id = "other".into();
        let plan = classify_reload(&base, &applied, &new);
        assert!(plan.restart_required.contains(&"venues"));
        assert!(plan.apply_log_level.is_none());
    }

    #[test]
    fn runtime_profile_requires_restart() {
        let base = sample();
        let applied = ReloadableConfig::from_daemon(&base);
        let mut new = base.clone();
        new.engine.runtime_profile = "latency".into();
        let plan = classify_reload(&base, &applied, &new);
        assert_eq!(plan.restart_required, vec!["engine.runtime_profile"]);
    }

    #[test]
    fn mixed_applies_safe_and_flags_unsafe() {
        let base = sample();
        let applied = ReloadableConfig::from_daemon(&base);
        let mut new = base.clone();
        new.telemetry.log_level = "warn".into();
        new.telemetry.bind = "127.0.0.1:9999".into();
        let plan = classify_reload(&base, &applied, &new);
        assert_eq!(plan.apply_log_level.as_deref(), Some("warn"));
        assert!(plan.restart_required.contains(&"telemetry.bind"));
    }
}
