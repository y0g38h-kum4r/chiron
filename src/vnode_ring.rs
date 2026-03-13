use crate::core_shard::CoreShard;

/// A virtual node on the timestamp ring.
/// Maps a time range to a core shard.
#[derive(Clone, Debug)]
pub struct VNode {
    pub time_start: i64,
    pub time_end: i64,
    pub core_id: usize,
}

/// Virtual Node Ring: consistent hashing on the timestamp space.
/// Distributes time ranges across core shards.
/// Supports hot-core detection via EMA and three-phase vnode splitting.
pub struct VNodeRing {
    /// Sorted by time_start.
    pub vnodes: Vec<VNode>,
    /// The core shards.
    pub shards: Vec<CoreShard>,
    /// Per-shard ring buffer capacity.
    pub shard_capacity: usize,
    /// Hot-core threshold multiplier (e.g., 3.0 = 3x average).
    pub hot_threshold_multiplier: f64,
}

/// Result of a split operation.
pub struct SplitResult {
    pub old_vnode_idx: usize,
    pub new_core_id: usize,
    pub split_point: i64,
}

impl VNodeRing {
    /// Create a ring with `num_cores` shards, each owning an equal slice of [0, max_time).
    pub fn new(num_cores: usize, max_time: i64, shard_capacity: usize) -> Self {
        let slice = max_time / num_cores as i64;
        let mut vnodes = Vec::new();
        let mut shards = Vec::new();

        for i in 0..num_cores {
            let start = i as i64 * slice;
            let end = if i == num_cores - 1 {
                max_time
            } else {
                (i as i64 + 1) * slice
            };
            vnodes.push(VNode {
                time_start: start,
                time_end: end,
                core_id: i,
            });
            shards.push(CoreShard::new(i, start, end, shard_capacity));
        }

        Self {
            vnodes,
            shards,
            shard_capacity,
            hot_threshold_multiplier: 3.0,
        }
    }

    /// Find which vnode owns a timestamp via binary search.
    pub fn find_vnode(&self, timestamp: i64) -> Option<usize> {
        let idx = self
            .vnodes
            .partition_point(|v| v.time_start <= timestamp);
        if idx == 0 {
            return None;
        }
        let idx = idx - 1;
        if timestamp < self.vnodes[idx].time_end {
            Some(idx)
        } else {
            None
        }
    }

    /// Find all vnodes overlapping with [t1, t2].
    pub fn find_vnodes_in_range(&self, t1: i64, t2: i64) -> Vec<usize> {
        self.vnodes
            .iter()
            .enumerate()
            .filter(|(_, v)| t1 < v.time_end && t2 >= v.time_start)
            .map(|(i, _)| i)
            .collect()
    }

    /// Get the core shard for a vnode.
    pub fn shard_for_vnode(&self, vnode_idx: usize) -> &CoreShard {
        &self.shards[self.vnodes[vnode_idx].core_id]
    }

    pub fn shard_for_vnode_mut(&mut self, vnode_idx: usize) -> &mut CoreShard {
        let core_id = self.vnodes[vnode_idx].core_id;
        &mut self.shards[core_id]
    }

    /// Detect hot cores: any core with query_rate_ema > hot_threshold_multiplier × average.
    pub fn detect_hot_cores(&self) -> Vec<usize> {
        if self.shards.is_empty() {
            return vec![];
        }
        let avg: f64 =
            self.shards.iter().map(|s| s.query_rate_ema).sum::<f64>() / self.shards.len() as f64;
        let threshold = avg * self.hot_threshold_multiplier;

        self.shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.query_rate_ema > threshold)
            .map(|(i, _)| i)
            .collect()
    }

    /// Split a vnode: create a new core shard and divide the time range.
    /// The split point is the query-density weighted midpoint.
    ///
    /// This is Phase 1 (COPY) of the three-phase migration.
    /// Phase 2 (REDIRECT) and Phase 3 (CLEANUP) happen via the returned SplitResult.
    pub fn split_vnode(&mut self, vnode_idx: usize) -> Option<SplitResult> {
        let vnode = &self.vnodes[vnode_idx];
        let range = vnode.time_end - vnode.time_start;
        if range <= 1 {
            return None; // Can't split further.
        }

        let old_core_id = vnode.core_id;

        // Query-density weighted midpoint: use window metadata to find where
        // queries concentrate. For now, use geometric midpoint as fallback.
        let split_point = self.compute_split_point(old_core_id, vnode.time_start, vnode.time_end);

        // Phase 1: Create new shard.
        let new_core_id = self.shards.len();
        let new_shard = CoreShard::new(
            new_core_id,
            split_point,
            vnode.time_end,
            self.shard_capacity,
        );
        self.shards.push(new_shard);

        // Copy entries from old shard that belong to the new range.
        let entries_to_move: Vec<_> = self.shards[old_core_id]
            .ring_buffer
            .iter()
            .filter(|(_, e)| e.timestamp >= split_point)
            .map(|(_, e)| e.clone())
            .collect();

        for entry in entries_to_move {
            self.shards[new_core_id].ingest(entry);
        }

        // Phase 2: Update vnode ring (atomic swap in production — here, direct mutation).
        let old_end = self.vnodes[vnode_idx].time_end;
        self.vnodes[vnode_idx].time_end = split_point;
        self.shards[old_core_id].time_end = split_point;

        self.vnodes.insert(
            vnode_idx + 1,
            VNode {
                time_start: split_point,
                time_end: old_end,
                core_id: new_core_id,
            },
        );

        Some(SplitResult {
            old_vnode_idx: vnode_idx,
            new_core_id,
            split_point,
        })
    }

    /// Compute split point using query density from window metadata.
    /// Falls back to geometric midpoint if no metadata.
    fn compute_split_point(&self, core_id: usize, start: i64, end: i64) -> i64 {
        let shard = &self.shards[core_id];
        let window_size = shard.window_meta.window_size;
        let now = shard.last_query_count_time;

        // Accumulate weighted sum to find density midpoint.
        let mut total_weight = 0.0;
        let mut weighted_sum = 0.0;
        let mut w = start;
        while w < end {
            let score = shard.window_meta.keep_score(w, now);
            weighted_sum += score * w as f64;
            total_weight += score;
            w += window_size;
        }

        if total_weight > 0.0 {
            let density_midpoint = (weighted_sum / total_weight) as i64;
            // Clamp to valid range, ensure we actually split.
            density_midpoint.clamp(start + 1, end - 1)
        } else {
            // Geometric midpoint fallback.
            (start + end) / 2
        }
    }

    /// Update query rates on all shards. Call periodically.
    pub fn update_all_query_rates(&mut self, now: f64) {
        for shard in &mut self.shards {
            shard.update_query_rate(now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_entry::LogEntry;

    fn make_entry(ts: i64) -> LogEntry {
        LogEntry {
            timestamp: ts,
            service_name: "svc".into(),
            host_id: "h1".into(),
            message: format!("log@{}", ts),
            severity: 1,
        }
    }

    #[test]
    fn vnode_lookup() {
        let ring = VNodeRing::new(3, 300, 100);
        // Core 0: [0, 100), Core 1: [100, 200), Core 2: [200, 300)
        assert_eq!(ring.find_vnode(50), Some(0));
        assert_eq!(ring.find_vnode(150), Some(1));
        assert_eq!(ring.find_vnode(250), Some(2));
        assert_eq!(ring.find_vnode(300), None);
    }

    #[test]
    fn range_query_spans_vnodes() {
        let ring = VNodeRing::new(3, 300, 100);
        let vnodes = ring.find_vnodes_in_range(80, 150);
        assert_eq!(vnodes, vec![0, 1]);
    }

    #[test]
    fn split_vnode_creates_new_shard() {
        let mut ring = VNodeRing::new(2, 200, 100);
        // Ingest into shard 0 ([0, 100))
        for ts in (0..100).step_by(5) {
            ring.shards[0].ingest(make_entry(ts));
        }
        let result = ring.split_vnode(0).unwrap();
        assert_eq!(ring.vnodes.len(), 3);
        assert_eq!(ring.shards.len(), 3);
        assert!(result.split_point > 0 && result.split_point < 100);
    }
}
