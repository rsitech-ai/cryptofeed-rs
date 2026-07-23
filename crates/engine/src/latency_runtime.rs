//! Latency runtime profile (spec §13.2).
//!
//! Default portable profile is unchanged. Hooks here MUST NOT alter normalized
//! decode/book results — only scheduling / affinity hints for Linux operators.
//!
//! Feature `latency-runtime` is an intent flag (like sinks `kafka`/`nats`): it
//! does not pull extra deps.

/// Runtime profile selected by daemon `engine.runtime_profile`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeProfile {
    /// Tokio multi-thread, no affinity (default; all platforms).
    Portable,
    /// Linux latency intent (pinned shards later). Results must match portable.
    Latency,
}

impl RuntimeProfile {
    /// Parse config string (`portable` / `latency`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "portable" => Some(Self::Portable),
            "latency" => Some(Self::Latency),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Portable => "portable",
            Self::Latency => "latency",
        }
    }
}

/// Errors from latency-runtime hooks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencyRuntimeError(&'static str);

impl std::fmt::Display for LatencyRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for LatencyRuntimeError {}

/// Apply profile-specific runtime hooks at process start.
///
/// Portable: no-op. Latency: reserves the affinity hook surface (no default
/// core mapping yet — workers may call [`pin_worker_to_core`] explicitly).
/// Does not change decode paths or Tokio builder.
pub fn apply_runtime_profile(profile: RuntimeProfile) {
    match profile {
        RuntimeProfile::Portable => {}
        RuntimeProfile::Latency => {
            // ponytail: no auto core map; session = shard lane (see latency_runtime.md).
            // Upgrade = shared N current-thread shards only if profile proves need.
            let _ = cfg!(feature = "latency-runtime");
            let _ = pin_worker_to_core(None);
        }
    }
}

/// Request CPU affinity for the current worker thread.
///
/// - `None` = no preference (always `Ok(())`; leaves OS default).
/// - `Some(core)` on Linux: `sched_setaffinity` for the calling thread.
/// - `Some(core)` elsewhere: portable no-op `Ok(())`.
///
/// On Linux, OS / invalid-core failures return [`LatencyRuntimeError`] (safe
/// fallback — caller keeps running unpinned). Normalized results are unaffected.
pub fn pin_worker_to_core(core: Option<usize>) -> Result<(), LatencyRuntimeError> {
    match core {
        None => Ok(()),
        Some(core) => pin_worker_to_core_impl(core),
    }
}

#[cfg(target_os = "linux")]
fn pin_worker_to_core_impl(core: usize) -> Result<(), LatencyRuntimeError> {
    affinity::set_affinity(core)
}

#[cfg(not(target_os = "linux"))]
fn pin_worker_to_core_impl(_core: usize) -> Result<(), LatencyRuntimeError> {
    // Non-Linux: portable no-op (latency affinity is Linux-only).
    Ok(())
}

/// Isolated Linux affinity (only module that uses `unsafe`).
#[cfg(target_os = "linux")]
mod affinity {
    #![allow(unsafe_code)]

    use super::LatencyRuntimeError;
    use libc::{CPU_SET, CPU_SETSIZE, CPU_ZERO, cpu_set_t, sched_setaffinity};
    use std::mem::{MaybeUninit, size_of};

    pub(super) fn set_affinity(core: usize) -> Result<(), LatencyRuntimeError> {
        if core >= CPU_SETSIZE as usize {
            return Err(LatencyRuntimeError("cpu core index exceeds CPU_SETSIZE"));
        }

        // SAFETY: `cpu_set_t` is a C bitset zeroed then written via libc
        // CPU_* macros; `sched_setaffinity(0, …)` pins the calling thread with
        // a valid pointer and size. Failure is reported via errno / return code.
        unsafe {
            let mut set = MaybeUninit::<cpu_set_t>::zeroed();
            let set_ptr = set.as_mut_ptr();
            CPU_ZERO(set_ptr);
            CPU_SET(core, set_ptr);
            let set = set.assume_init();
            let rc = sched_setaffinity(0, size_of::<cpu_set_t>(), &set);
            if rc == 0 {
                Ok(())
            } else {
                Err(LatencyRuntimeError("sched_setaffinity failed"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_profiles() {
        assert_eq!(
            RuntimeProfile::parse("portable"),
            Some(RuntimeProfile::Portable)
        );
        assert_eq!(
            RuntimeProfile::parse("latency"),
            Some(RuntimeProfile::Latency)
        );
        assert_eq!(RuntimeProfile::parse("nope"), None);
    }

    /// Portable path: no preference + explicit core request must not panic and
    /// must succeed off-Linux (no-op). On Linux, core 0 is almost always valid.
    #[test]
    fn pin_worker_portable_path_ok() {
        assert!(pin_worker_to_core(None).is_ok());
        // Some(core): no-op on non-Linux; real affinity on Linux (core 0).
        assert!(
            pin_worker_to_core(Some(0)).is_ok(),
            "pin to core 0 must succeed (no-op off-Linux; affinity on Linux)"
        );
    }

    #[test]
    fn apply_profiles_do_not_panic() {
        apply_runtime_profile(RuntimeProfile::Portable);
        apply_runtime_profile(RuntimeProfile::Latency);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_rejects_core_beyond_setsize() {
        let err = pin_worker_to_core(Some(usize::MAX)).expect_err("out of range");
        assert!(err.to_string().contains("CPU_SETSIZE"));
    }
}
