use crate::inverted_index::InvertedIndex;
use crate::log_entry::LogEntry;
use crate::monotonic_stack::MonotonicStack;
use crate::ring_buffer::RingBuffer;
use crate::window_metadata::WindowMetadataMap;

/// Intersect two sorted offset slices from different indexes.
fn intersect_sorted(a: Option<&[u64]>, b: Option<&[u64]>) -> Vec<u64> {
    let a = match a {
        Some(v) => v,
        None => return vec![],
    };
    let b = match b {
        Some(v) => v,
        None => return vec![],
    };
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            result.push(a[i]);
            i += 1;
            j += 1;
        } else if a[i] < b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

/// A single core shard owning a time range.
/// Contains: primary ring buffer + inverted indexes + window metadata + eviction stack.
pub struct CoreShard {
    pub id: usize,
    /// The time range this shard owns: [time_start, time_end).
    pub time_start: i64,
    pub time_end: i64,
    /// Primary ring buffer holding actual log data.
    pub ring_buffer: RingBuffer,
    /// Inverted index for service_name dimension.
    pub service_index: InvertedIndex,
    /// Inverted index for host_id dimension.
    pub host_index: InvertedIndex,
    /// Window metadata for eviction intelligence.
    pub window_meta: WindowMetadataMap,
    /// Monotonic stack for eviction ordering.
    pub eviction_stack: MonotonicStack,
    /// EMA of query rate for hot-core detection.
    pub query_rate_ema: f64,
    pub last_query_count_time: f64,
    pub query_count_since_last: u64,
    pub ema_alpha: f64,
}

/// Query filter specifying which dimensions to match.
pub enum QueryFilter<'a> {
    /// All logs in time range.
    TimeOnly,
    /// Filter by service name.
    ByService(&'a str),
    /// Filter by host id.
    ByHost(&'a str),
    /// Filter by service + host.
    ByServiceAndHost(&'a str, &'a str),
}

impl CoreShard {
    pub fn new(id: usize, time_start: i64, time_end: i64, rb_capacity: usize) -> Self {
        Self {
            id,
            time_start,
            time_end,
            ring_buffer: RingBuffer::new(rb_capacity),
            service_index: InvertedIndex::new(),
            host_index: InvertedIndex::new(),
            window_meta: WindowMetadataMap::new(10, 0.01), // 10s windows, λ=0.01
            eviction_stack: MonotonicStack::new(),
            query_rate_ema: 0.0,
            last_query_count_time: 0.0,
            query_count_since_last: 0,
            ema_alpha: 0.3,
        }
    }

    /// Does this shard own the given timestamp?
    pub fn owns(&self, timestamp: i64) -> bool {
        timestamp >= self.time_start && timestamp < self.time_end
    }

    /// Does this shard overlap with the query range [t1, t2]?
    pub fn overlaps(&self, t1: i64, t2: i64) -> bool {
        t1 < self.time_end && t2 >= self.time_start
    }

    /// Ingest a log entry. Returns the global offset assigned.
    pub fn ingest(&mut self, entry: LogEntry) -> u64 {
        let offset = self.ring_buffer.push(entry.clone());
        self.service_index.insert(&entry.service_name, offset);
        self.host_index.insert(&entry.host_id, offset);
        offset
    }

    /// Query logs matching the filter within [t1, t2].
    pub fn query(&mut self, t1: i64, t2: i64, filter: &QueryFilter, now: f64) -> Vec<LogEntry> {
        // Record query for window metadata.
        self.window_meta.record_query(t1, t2, now);
        self.query_count_since_last += 1;

        // Clamp to our time range.
        let t1 = t1.max(self.time_start);
        let t2 = t2.min(self.time_end - 1);

        let offsets: Vec<u64> = match filter {
            QueryFilter::TimeOnly => {
                // Scan all live entries.
                self.ring_buffer
                    .iter()
                    .filter(|(_, e)| e.timestamp >= t1 && e.timestamp <= t2)
                    .map(|(o, _)| o)
                    .collect()
            }
            QueryFilter::ByService(svc) => {
                self.filter_offsets_by_time(self.service_index.get(svc), t1, t2)
            }
            QueryFilter::ByHost(host) => {
                self.filter_offsets_by_time(self.host_index.get(host), t1, t2)
            }
            QueryFilter::ByServiceAndHost(svc, host) => {
                let svc_offsets = self.service_index.get(svc);
                let host_offsets = self.host_index.get(host);
                let intersected = intersect_sorted(svc_offsets, host_offsets);
                self.filter_offsets_by_time(Some(&intersected), t1, t2)
            }
        };

        offsets
            .iter()
            .filter_map(|&o| self.ring_buffer.get(o).cloned())
            .collect()
    }

    fn filter_offsets_by_time(&self, offsets: Option<&[u64]>, t1: i64, t2: i64) -> Vec<u64> {
        match offsets {
            None => vec![],
            Some(offs) => offs
                .iter()
                .filter(|&&o| {
                    self.ring_buffer
                        .get(o)
                        .map(|e| e.timestamp >= t1 && e.timestamp <= t2)
                        .unwrap_or(false)
                })
                .copied()
                .collect(),
        }
    }

    /// Run eviction: rebuild the monotonic stack and evict the lowest-scored
    /// windows until we're below capacity threshold.
    pub fn run_eviction(&mut self, now: f64, target_free_pct: f64) {
        let target_len =
            (self.ring_buffer.capacity() as f64 * (1.0 - target_free_pct)) as usize;

        if self.ring_buffer.len() <= target_len {
            return;
        }

        // Rebuild eviction stack from current window metadata.
        let entries: Vec<(i64, f64)> = self
            .window_meta
            .windows
            .iter()
            .map(|(&k, _)| (k, self.window_meta.keep_score(k, now)))
            .collect();
        self.eviction_stack.rebuild(entries.into_iter());

        let evictable = self.eviction_stack.evictable();

        for window_key in evictable {
            if self.ring_buffer.len() <= target_len {
                break;
            }
            // Count entries in this window and evict from head.
            let window_end = window_key + self.window_meta.window_size;
            let count = self
                .ring_buffer
                .iter()
                .filter(|(_, e)| e.timestamp >= window_key && e.timestamp < window_end)
                .count();
            self.ring_buffer.evict_head(count);
            self.service_index
                .purge_below(self.ring_buffer.oldest_offset());
            self.host_index
                .purge_below(self.ring_buffer.oldest_offset());
            self.window_meta.remove_window(window_key);
        }
    }

    /// Update query rate EMA. Call periodically.
    pub fn update_query_rate(&mut self, now: f64) {
        if self.last_query_count_time > 0.0 {
            let dt = now - self.last_query_count_time;
            if dt > 0.0 {
                let rate = self.query_count_since_last as f64 / dt;
                self.query_rate_ema =
                    self.ema_alpha * rate + (1.0 - self.ema_alpha) * self.query_rate_ema;
            }
        }
        self.query_count_since_last = 0;
        self.last_query_count_time = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(ts: i64, svc: &str, host: &str) -> LogEntry {
        LogEntry {
            timestamp: ts,
            service_name: svc.to_string(),
            host_id: host.to_string(),
            message: format!("log@{}", ts),
            severity: 1,
        }
    }

    #[test]
    fn ingest_and_query_by_service() {
        let mut shard = CoreShard::new(0, 0, 1000, 100);
        shard.ingest(make_entry(10, "auth", "h1"));
        shard.ingest(make_entry(20, "payment", "h1"));
        shard.ingest(make_entry(30, "auth", "h2"));

        let results = shard.query(0, 100, &QueryFilter::ByService("auth"), 1.0);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.service_name == "auth"));
    }

    #[test]
    fn query_by_service_and_host() {
        let mut shard = CoreShard::new(0, 0, 1000, 100);
        shard.ingest(make_entry(10, "auth", "h1"));
        shard.ingest(make_entry(20, "auth", "h2"));
        shard.ingest(make_entry(30, "payment", "h1"));

        let results =
            shard.query(0, 100, &QueryFilter::ByServiceAndHost("auth", "h1"), 1.0);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].timestamp, 10);
    }

    #[test]
    fn query_respects_time_range() {
        let mut shard = CoreShard::new(0, 0, 1000, 100);
        for ts in [5, 10, 15, 20, 25] {
            shard.ingest(make_entry(ts, "svc", "h1"));
        }
        let results = shard.query(10, 20, &QueryFilter::ByService("svc"), 1.0);
        assert_eq!(results.len(), 3); // 10, 15, 20
    }
}
