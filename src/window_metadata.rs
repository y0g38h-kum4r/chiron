use std::collections::HashMap;

/// Metadata for a time window, tracking query frequency (with decay)
/// and query velocity.
#[derive(Clone, Debug)]
pub struct WindowMeta {
    /// Decayed query frequency: freq = freq * exp(-λ * dt) + 1 on each query.
    pub freq: f64,
    /// Timestamp of the last query that touched this window.
    pub last_query_time: f64,
    /// Query velocity: d(freq)/dt, computed as EMA of inter-query deltas.
    pub velocity: f64,
    /// Previous query timestamp used to compute velocity.
    prev_query_time: f64,
    /// EMA smoothing factor for velocity.
    velocity_alpha: f64,
}

impl WindowMeta {
    pub fn new() -> Self {
        Self {
            freq: 0.0,
            last_query_time: 0.0,
            velocity: 0.0,
            prev_query_time: 0.0,
            velocity_alpha: 0.3,
        }
    }

    /// Record a query hit at `now`. Applies exponential decay to frequency
    /// and updates velocity via EMA.
    pub fn record_query(&mut self, now: f64, decay_lambda: f64) {
        // Decay existing frequency.
        if self.last_query_time > 0.0 {
            let dt = now - self.last_query_time;
            self.freq *= (-decay_lambda * dt).exp();

            // Update velocity: EMA of 1/dt (queries per second).
            if dt > 0.0 {
                let instant_rate = 1.0 / dt;
                self.velocity = self.velocity_alpha * instant_rate
                    + (1.0 - self.velocity_alpha) * self.velocity;
            }
        }

        self.freq += 1.0;
        self.prev_query_time = self.last_query_time;
        self.last_query_time = now;
    }

    /// Trigger score for predictive pre-positioning, based solely on velocity.
    pub fn trigger_score(&self) -> f64 {
        self.velocity
    }

    /// Compute the keep score used by eviction policy.
    /// Higher score = more important = evict last.
    pub fn keep_score(&self, now: f64, decay_lambda: f64) -> f64 {
        let dt = now - self.last_query_time;
        self.freq * (-decay_lambda * dt).exp()
    }
}

/// Map of time-window-start → metadata.
/// Time windows are fixed-width buckets (e.g., 10-second windows).
pub struct WindowMetadataMap {
    pub window_size: i64,
    pub windows: HashMap<i64, WindowMeta>,
    pub decay_lambda: f64,
}

impl WindowMetadataMap {
    pub fn new(window_size: i64, decay_lambda: f64) -> Self {
        Self {
            window_size,
            windows: HashMap::new(),
            decay_lambda,
        }
    }

    /// Which window does this timestamp belong to?
    pub fn window_key(&self, timestamp: i64) -> i64 {
        (timestamp / self.window_size) * self.window_size
    }

    /// Record a query touching the given timestamp range.
    pub fn record_query(&mut self, t1: i64, t2: i64, now: f64) {
        let start_window = self.window_key(t1);
        let end_window = self.window_key(t2);
        let mut w = start_window;
        while w <= end_window {
            self.windows
                .entry(w)
                .or_insert_with(WindowMeta::new)
                .record_query(now, self.decay_lambda);
            w += self.window_size;
        }
    }

    /// Get the keep_score for a window.
    pub fn keep_score(&self, window_key: i64, now: f64) -> f64 {
        self.windows
            .get(&window_key)
            .map(|m| m.keep_score(now, self.decay_lambda))
            .unwrap_or(0.0)
    }

    /// Remove metadata for windows that have been evicted.
    pub fn remove_window(&mut self, window_key: i64) {
        self.windows.remove(&window_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frequency_decay() {
        let mut meta = WindowMeta::new();
        meta.record_query(1.0, 0.1);
        assert!((meta.freq - 1.0).abs() < 1e-9);

        meta.record_query(2.0, 0.1);
        // freq = 1.0 * exp(-0.1 * 1.0) + 1.0 ≈ 1.905
        assert!((meta.freq - (1.0_f64 * (-0.1_f64).exp() + 1.0)).abs() < 1e-6);
    }

    #[test]
    fn trigger_score_is_velocity() {
        let mut meta = WindowMeta::new();
        assert_eq!(meta.trigger_score(), 0.0);

        meta.record_query(1.0, 0.1);
        meta.record_query(1.1, 0.1);
        assert!(meta.velocity > 0.0);
        assert_eq!(meta.trigger_score(), meta.velocity);
    }

    #[test]
    fn keep_score_is_decayed_freq() {
        let mut meta = WindowMeta::new();
        meta.record_query(1.0, 0.1);
        meta.record_query(2.0, 0.1);
        let score = meta.keep_score(3.0, 0.1);
        // Should be freq * exp(-0.1 * 1.0)
        let expected = meta.freq * (-0.1_f64).exp();
        assert!((score - expected).abs() < 1e-6);
    }
}
