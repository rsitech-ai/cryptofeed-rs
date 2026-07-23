//! Exponential backoff with full jitter (spec §12.4).

use marketfeed_adapter_api::ReconnectPolicy;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct StableLiveReset {
    live_since: Option<Instant>,
    applied: bool,
}

impl StableLiveReset {
    /// Observe whether the session is currently live.
    ///
    /// Returns `true` exactly once after one uninterrupted live interval reaches
    /// `threshold`. Leaving `Live` clears the interval so a later session must
    /// earn a fresh reset.
    pub fn observe(&mut self, is_live: bool, now: Instant, threshold: Duration) -> bool {
        if !is_live {
            self.live_since = None;
            self.applied = false;
            return false;
        }

        let live_since = *self.live_since.get_or_insert(now);
        if !self.applied && now.saturating_duration_since(live_since) >= threshold {
            self.applied = true;
            return true;
        }

        false
    }

    pub fn clear(&mut self) {
        self.live_since = None;
        self.applied = false;
    }
}

#[derive(Debug, Clone)]
pub struct BackoffState {
    pub policy: ReconnectPolicy,
    pub attempt: u32,
    seed: u64,
}

impl BackoffState {
    pub fn new(policy: ReconnectPolicy) -> Self {
        Self {
            policy,
            attempt: 0,
            seed: 0xC0FFEE,
        }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn next_delay(&mut self) -> Duration {
        let exp = self.attempt.min(16);
        let capped = self
            .policy
            .min_delay_ms
            .saturating_mul(1u64 << exp)
            .min(self.policy.max_delay_ms);
        self.attempt = self.attempt.saturating_add(1);
        // Full jitter: uniform in [0, capped].
        let delay_ms = xorshift(&mut self.seed) % (capped.saturating_add(1));
        Duration::from_millis(delay_ms)
    }
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_respects_max() {
        let mut b = BackoffState::new(ReconnectPolicy {
            min_delay_ms: 10,
            max_delay_ms: 100,
            reset_after_live_ms: 1_000,
        });
        for _ in 0..20 {
            assert!(b.next_delay() <= Duration::from_millis(100));
        }
    }

    #[test]
    fn stable_live_reset_waits_for_threshold_and_fires_once() {
        let start = Instant::now();
        let threshold = Duration::from_secs(5);
        let mut reset = StableLiveReset::default();

        assert!(!reset.observe(true, start, threshold));
        assert!(!reset.observe(true, start + Duration::from_secs(4), threshold));
        assert!(reset.observe(true, start + threshold, threshold));
        assert!(!reset.observe(true, start + Duration::from_secs(10), threshold));
    }

    #[test]
    fn leaving_live_clears_stability_window() {
        let start = Instant::now();
        let threshold = Duration::from_secs(5);
        let mut reset = StableLiveReset::default();

        assert!(!reset.observe(true, start, threshold));
        assert!(!reset.observe(false, start + Duration::from_secs(4), threshold));
        assert!(!reset.observe(true, start + Duration::from_secs(5), threshold));
        assert!(!reset.observe(true, start + Duration::from_secs(9), threshold));
        assert!(reset.observe(true, start + Duration::from_secs(10), threshold));
    }
}
