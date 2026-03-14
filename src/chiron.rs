use std::io;
use std::path::Path;

use crate::inverted_index::InvertedIndex;
use crate::log_entry::LogEntry;
use crate::ring_buffer::RingBuffer;
use crate::snapshot::{KafkaOffsets, Snapshot};

/// Unified ChironStore: shared append-only log with async indexing.
///
/// Write path:  append to ring buffer (lock-free in production via atomic fetch_add).
/// Index path:  indexer trails behind write head, building service + host indexes.
/// Read path:   index lookup → offsets → ring buffer reads. No fan-out for any query type.
/// Eviction:    head-only (oldest first) — with monotonic timestamps, oldest = least relevant.
pub struct ChironStore {
    /// Single shared ring buffer holding all log data.
    ring_buffer: RingBuffer,

    // --- Indexer state ---
    /// How far the indexer has processed (trails behind ring_buffer.next_offset()).
    indexer_pos: u64,
    /// Inverted index: service_name → [offsets].
    service_index: InvertedIndex,
    /// Inverted index: host_id → [offsets].
    host_index: InvertedIndex,
}

pub struct QueryResult {
    pub entries: Vec<LogEntry>,
}

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

impl ChironStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring_buffer: RingBuffer::new(capacity),
            indexer_pos: 0,
            service_index: InvertedIndex::new(),
            host_index: InvertedIndex::new(),
        }
    }

    /// Ingest a log entry. Appends to the shared ring buffer.
    /// The entry is NOT queryable until `flush_indexer()` is called.
    /// In production, this would be an atomic fetch_add — zero contention between writers.
    pub fn ingest(&mut self, entry: LogEntry) -> u64 {
        self.ring_buffer.push(entry)
    }

    /// Advance the indexer: process all entries between indexer_pos and write head.
    /// Builds service and host indexes for newly ingested entries.
    /// In production, this runs on a dedicated thread trailing behind writers.
    pub fn flush_indexer(&mut self) {
        let write_head = self.ring_buffer.next_offset();
        while self.indexer_pos < write_head {
            if let Some(entry) = self.ring_buffer.get(self.indexer_pos) {
                self.service_index
                    .insert(&entry.service_name, self.indexer_pos);
                self.host_index.insert(&entry.host_id, self.indexer_pos);
            }
            self.indexer_pos += 1;
        }
    }

    /// How many entries are ingested but not yet indexed.
    pub fn indexer_lag(&self) -> u64 {
        self.ring_buffer.next_offset() - self.indexer_pos
    }

    /// Query by service name in time range [t1, t2].
    pub fn query_by_service(&self, service: &str, t1: i64, t2: i64) -> QueryResult {
        let offsets = self.service_index.get(service);
        QueryResult {
            entries: self.collect_entries(offsets, t1, t2),
        }
    }

    /// Query by host id in time range [t1, t2].
    pub fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> QueryResult {
        let offsets = self.host_index.get(host);
        QueryResult {
            entries: self.collect_entries(offsets, t1, t2),
        }
    }

    /// Query by service + host in time range [t1, t2].
    pub fn query_by_service_and_host(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> QueryResult {
        let svc_offsets = self.service_index.get(service);
        let host_offsets = self.host_index.get(host);
        let intersected = intersect_sorted(svc_offsets, host_offsets);
        QueryResult {
            entries: self.collect_entries(Some(&intersected), t1, t2),
        }
    }

    /// Resolve offsets to entries, filtering by time range.
    fn collect_entries(&self, offsets: Option<&[u64]>, t1: i64, t2: i64) -> Vec<LogEntry> {
        match offsets {
            None => vec![],
            Some(offs) => offs
                .iter()
                .filter_map(|&o| {
                    self.ring_buffer
                        .get(o)
                        .filter(|e| e.timestamp >= t1 && e.timestamp <= t2)
                        .cloned()
                })
                .collect(),
        }
    }

    /// Run eviction: evict oldest entries from the head until below capacity threshold.
    /// With monotonically increasing timestamps, the oldest entries are always
    /// the least relevant — no need for a separate eviction ordering structure.
    pub fn run_eviction(&mut self, target_free_pct: f64) {
        let target_len =
            (self.ring_buffer.capacity() as f64 * (1.0 - target_free_pct)) as usize;

        if self.ring_buffer.len() <= target_len {
            return;
        }

        let to_evict = self.ring_buffer.len() - target_len;
        self.ring_buffer.evict_head(to_evict);
        self.service_index
            .purge_below(self.ring_buffer.oldest_offset());
        self.host_index
            .purge_below(self.ring_buffer.oldest_offset());
    }

    /// Periodic maintenance: flush indexer + run eviction.
    pub fn tick(&mut self) {
        self.flush_indexer();
        self.run_eviction(0.2);
    }

    pub fn len(&self) -> usize {
        self.ring_buffer.len()
    }

    pub fn capacity(&self) -> usize {
        self.ring_buffer.capacity()
    }

    // --- Snapshot / Restore ---

    /// Take a snapshot of the current ring buffer + kafka offsets to disk.
    /// Writes atomically (tmp file + rename). Indexes are NOT included —
    /// they are rebuilt from the ring buffer on restore via flush_indexer().
    pub fn save_snapshot(
        &self,
        path: &Path,
        kafka_offsets: &KafkaOffsets,
    ) -> io::Result<()> {
        Snapshot::save(path, &self.ring_buffer, kafka_offsets)
    }

    /// Restore from a snapshot file. Returns (ChironStore, KafkaOffsets).
    /// The store's indexes are fully rebuilt from the ring buffer data.
    /// Resume Kafka consumers from the returned offsets.
    pub fn from_snapshot(path: &Path) -> io::Result<(Self, KafkaOffsets)> {
        let (ring_buffer, kafka_offsets) = Snapshot::load(path)?;

        let mut store = Self {
            ring_buffer,
            indexer_pos: 0,
            service_index: InvertedIndex::new(),
            host_index: InvertedIndex::new(),
        };

        // Rebuild indexes from restored ring buffer.
        store.flush_indexer();

        Ok((store, kafka_offsets))
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
    fn ingest_flush_and_query() {
        let mut store = ChironStore::new(1000);

        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "payment", "h1"));
        store.ingest(make_entry(30, "auth", "h2"));

        // Not queryable yet — indexer hasn't run.
        let result = store.query_by_service("auth", 0, 100);
        assert!(result.entries.is_empty());

        // Flush indexer.
        store.flush_indexer();

        let result = store.query_by_service("auth", 0, 100);
        assert_eq!(result.entries.len(), 2);
        assert!(result.entries.iter().all(|e| e.service_name == "auth"));
    }

    #[test]
    fn query_by_host() {
        let mut store = ChironStore::new(1000);
        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "auth", "h2"));
        store.ingest(make_entry(30, "payment", "h1"));
        store.flush_indexer();

        let result = store.query_by_host("h1", 0, 100);
        assert_eq!(result.entries.len(), 2);
        assert!(result.entries.iter().all(|e| e.host_id == "h1"));
    }

    #[test]
    fn query_by_service_and_host() {
        let mut store = ChironStore::new(1000);
        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "auth", "h2"));
        store.ingest(make_entry(30, "payment", "h1"));
        store.flush_indexer();

        let result = store.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].timestamp, 10);
    }

    #[test]
    fn query_respects_time_range() {
        let mut store = ChironStore::new(1000);
        for ts in [5, 10, 15, 20, 25] {
            store.ingest(make_entry(ts, "svc", "h1"));
        }
        store.flush_indexer();

        let result = store.query_by_service("svc", 10, 20);
        assert_eq!(result.entries.len(), 3); // 10, 15, 20
    }

    #[test]
    fn indexer_lag_tracks_unindexed() {
        let mut store = ChironStore::new(1000);
        assert_eq!(store.indexer_lag(), 0);

        store.ingest(make_entry(1, "a", "h1"));
        store.ingest(make_entry(2, "b", "h2"));
        assert_eq!(store.indexer_lag(), 2);

        store.flush_indexer();
        assert_eq!(store.indexer_lag(), 0);
    }

    #[test]
    fn tick_flushes_and_evicts() {
        let mut store = ChironStore::new(1000);
        for ts in 0..100 {
            store.ingest(make_entry(ts, "svc", "h1"));
        }
        store.tick();
        assert_eq!(store.indexer_lag(), 0);

        let result = store.query_by_service("svc", 0, 100);
        assert_eq!(result.entries.len(), 100);
    }

    #[test]
    fn snapshot_and_restore() {
        let dir = std::env::temp_dir().join("chiron_test_store_snapshot");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.snap");

        // Build store, ingest, flush.
        let mut store = ChironStore::new(1000);
        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "payment", "h2"));
        store.ingest(make_entry(30, "auth", "h1"));
        store.flush_indexer();

        // Set kafka offsets.
        let mut offsets = KafkaOffsets::new();
        offsets.set("logs", 0, 99999);
        offsets.set("logs", 1, 88888);

        // Save snapshot.
        store.save_snapshot(&path, &offsets).unwrap();

        // Restore from snapshot.
        let (restored, restored_offsets) = ChironStore::from_snapshot(&path).unwrap();

        // Indexes should be rebuilt — queries should work immediately.
        let result = restored.query_by_service("auth", 0, 100);
        assert_eq!(result.entries.len(), 2);

        let result = restored.query_by_host("h1", 0, 100);
        assert_eq!(result.entries.len(), 2);

        let result = restored.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(result.entries.len(), 2);

        // Kafka offsets restored.
        assert_eq!(restored_offsets.get("logs", 0), Some(99999));
        assert_eq!(restored_offsets.get("logs", 1), Some(88888));

        std::fs::remove_dir_all(&dir).ok();
    }
}
