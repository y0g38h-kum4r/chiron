use crate::core_shard::QueryFilter;
use crate::log_entry::LogEntry;
use crate::vnode_ring::VNodeRing;

/// Top-level ChironStore coordinator.
/// Routes ingestion to the correct shard, fans out queries across shards,
/// and manages hot-core splitting.
pub struct ChironStore {
    pub ring: VNodeRing,
}

/// Query result with metadata.
pub struct QueryResult {
    pub entries: Vec<LogEntry>,
    pub shards_queried: usize,
}

/// Errors from query execution.
#[derive(Debug)]
pub enum QueryError {
    NoShardsFound,
}

impl ChironStore {
    /// Create a new ChironStore with the given number of cores and time range.
    pub fn new(num_cores: usize, max_time: i64, shard_capacity: usize) -> Self {
        Self {
            ring: VNodeRing::new(num_cores, max_time, shard_capacity),
        }
    }

    /// Ingest a log entry. Routes to the correct shard based on timestamp.
    pub fn ingest(&mut self, entry: LogEntry) -> Option<u64> {
        let vnode_idx = self.ring.find_vnode(entry.timestamp)?;
        let shard = self.ring.shard_for_vnode_mut(vnode_idx);
        Some(shard.ingest(entry))
    }

    /// Query by service name in time range [t1, t2].
    pub fn query_by_service(
        &mut self,
        service: &str,
        t1: i64,
        t2: i64,
        now: f64,
    ) -> Result<QueryResult, QueryError> {
        self.execute_query(t1, t2, now, |shard, t1, t2, now| {
            shard.query(t1, t2, &QueryFilter::ByService(service), now)
        })
    }

    /// Query by host id in time range [t1, t2].
    pub fn query_by_host(
        &mut self,
        host: &str,
        t1: i64,
        t2: i64,
        now: f64,
    ) -> Result<QueryResult, QueryError> {
        self.execute_query(t1, t2, now, |shard, t1, t2, now| {
            shard.query(t1, t2, &QueryFilter::ByHost(host), now)
        })
    }

    /// Query by service + host in time range [t1, t2].
    pub fn query_by_service_and_host(
        &mut self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
        now: f64,
    ) -> Result<QueryResult, QueryError> {
        self.execute_query(t1, t2, now, |shard, t1, t2, now| {
            shard.query(
                t1,
                t2,
                &QueryFilter::ByServiceAndHost(service, host),
                now,
            )
        })
    }

    /// Internal: fan out a query across all overlapping shards.
    fn execute_query<F>(
        &mut self,
        t1: i64,
        t2: i64,
        now: f64,
        query_fn: F,
    ) -> Result<QueryResult, QueryError>
    where
        F: Fn(
            &mut crate::core_shard::CoreShard,
            i64,
            i64,
            f64,
        ) -> Vec<LogEntry>,
    {
        let vnode_indices = self.ring.find_vnodes_in_range(t1, t2);
        if vnode_indices.is_empty() {
            return Err(QueryError::NoShardsFound);
        }

        let shards_queried = vnode_indices.len();

        // Fan out: query each shard and merge results.
        let mut all_entries = Vec::new();
        for vnode_idx in vnode_indices {
            let core_id = self.ring.vnodes[vnode_idx].core_id;
            let shard = &mut self.ring.shards[core_id];
            let entries = query_fn(shard, t1, t2, now);
            all_entries.extend(entries);
        }

        // Sort merged results by timestamp.
        all_entries.sort_by_key(|e| e.timestamp);

        Ok(QueryResult {
            entries: all_entries,
            shards_queried,
        })
    }

    /// Set an algorithm signal for a specific timestamp (incident detection).
    pub fn set_algo_signal(&mut self, timestamp: i64, score: f64, ttl: f64, now: f64) {
        if let Some(vnode_idx) = self.ring.find_vnode(timestamp) {
            let shard = self.ring.shard_for_vnode_mut(vnode_idx);
            shard
                .window_meta
                .set_algo_signal(timestamp, score, ttl, now);
        }
    }

    /// Periodic maintenance: update query rates, detect hot cores, trigger splits.
    pub fn tick(&mut self, now: f64) {
        // Update query rates.
        self.ring.update_all_query_rates(now);

        // Detect hot cores and try splitting hot vnodes.
        let hot_cores = self.ring.detect_hot_cores();
        for core_id in hot_cores {
            if let Some(vnode_idx) = self
                .ring
                .vnodes
                .iter()
                .position(|v| v.core_id == core_id)
            {
                self.ring.split_vnode(vnode_idx);
            }
        }

        // Run eviction on all shards.
        for shard in &mut self.ring.shards {
            shard.run_eviction(now, 0.2); // Keep 20% free.
        }
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
    fn ingest_and_query_e2e() {
        let mut store = ChironStore::new(3, 300, 1000);

        for ts in (0..300).step_by(5) {
            let svc = if ts % 2 == 0 { "auth" } else { "payment" };
            let host = format!("h{}", ts % 3);
            store.ingest(make_entry(ts, svc, &host));
        }

        let result = store.query_by_service("auth", 50, 150, 1.0).unwrap();
        assert!(!result.entries.is_empty());
        assert!(result.entries.iter().all(|e| e.service_name == "auth"));
        assert!(result
            .entries
            .iter()
            .all(|e| e.timestamp >= 50 && e.timestamp <= 150));

        let result = store.query_by_host("h0", 0, 100, 1.0).unwrap();
        assert!(!result.entries.is_empty());
        assert!(result.entries.iter().all(|e| e.host_id == "h0"));

        let result = store
            .query_by_service_and_host("auth", "h0", 0, 300, 1.0)
            .unwrap();
        assert!(result
            .entries
            .iter()
            .all(|e| e.service_name == "auth" && e.host_id == "h0"));
    }

    #[test]
    fn cross_shard_query() {
        let mut store = ChironStore::new(3, 300, 1000);
        store.ingest(make_entry(50, "svc", "h1"));
        store.ingest(make_entry(150, "svc", "h1"));
        store.ingest(make_entry(250, "svc", "h1"));

        let result = store.query_by_service("svc", 0, 300, 1.0).unwrap();
        assert_eq!(result.entries.len(), 3);
        assert_eq!(result.shards_queried, 3);
        assert_eq!(result.entries[0].timestamp, 50);
        assert_eq!(result.entries[1].timestamp, 150);
        assert_eq!(result.entries[2].timestamp, 250);
    }
}
