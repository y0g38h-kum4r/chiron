/// Monotonic stack encoding eviction order via the causal horizon.
///
/// The stack tracks time-window keys ordered by their keep_score.
/// The causal horizon is at the peak: everything to the right (newer)
/// of the peak is protected. Eviction is left-only relative to the peak.
///
/// ```text
/// freq                peak (horizon)
///  ↑                      │
///  │                  ████│
///  │              ████████│████
///  │          ████████████│████████
///  └──────────────────────│────────→ time
///    evictable ←──────────┤──────→ protected
/// ```
pub struct MonotonicStack {
    /// (window_key, keep_score) pairs, ordered by window_key.
    entries: Vec<(i64, f64)>,
}

impl MonotonicStack {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Rebuild the stack from window metadata.
    /// Takes an iterator of (window_key, keep_score) sorted by window_key.
    pub fn rebuild(&mut self, windows: impl Iterator<Item = (i64, f64)>) {
        self.entries.clear();
        self.entries.extend(windows);
        self.entries.sort_by_key(|&(k, _)| k);
    }

    /// Find the causal horizon: the window key with the maximum keep_score.
    /// Everything to the right of (and including) this window is protected.
    pub fn horizon(&self) -> Option<i64> {
        self.entries
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|&(k, _)| k)
    }

    /// Return evictable windows: all windows to the LEFT of the horizon,
    /// sorted by keep_score ascending (lowest score = evict first).
    pub fn evictable(&self) -> Vec<i64> {
        let horizon = match self.horizon() {
            Some(h) => h,
            None => return vec![],
        };

        let mut candidates: Vec<(i64, f64)> = self
            .entries
            .iter()
            .filter(|&&(k, _)| k < horizon)
            .copied()
            .collect();

        // Sort by keep_score ascending — evict lowest first.
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.into_iter().map(|(k, _)| k).collect()
    }

    /// How many windows are in the stack?
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn horizon_is_max_score() {
        let mut stack = MonotonicStack::new();
        stack.rebuild(
            vec![(0, 1.0), (10, 5.0), (20, 3.0), (30, 2.0)]
                .into_iter(),
        );
        assert_eq!(stack.horizon(), Some(10));
    }

    #[test]
    fn evictable_is_left_of_horizon() {
        let mut stack = MonotonicStack::new();
        stack.rebuild(
            vec![(0, 1.0), (10, 0.5), (20, 8.0), (30, 3.0)]
                .into_iter(),
        );
        // Horizon is at 20 (score 8.0). Evictable: 0, 10 (left of 20).
        // Sorted by score: 10 (0.5) then 0 (1.0).
        let ev = stack.evictable();
        assert_eq!(ev, vec![10, 0]);
    }

    #[test]
    fn nothing_evictable_when_horizon_is_leftmost() {
        let mut stack = MonotonicStack::new();
        stack.rebuild(vec![(0, 10.0), (10, 2.0), (20, 1.0)].into_iter());
        // Horizon at 0 — nothing to the left.
        let ev = stack.evictable();
        assert!(ev.is_empty());
    }
}
