use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::io;
use std::path::Path;

use crate::inverted_index::InvertedIndex;
use crate::log_entry::LogEntry;
use crate::snapshot::{KafkaOffsets, RestoredShard, Snapshot, SnapshotShard};

/// Partition-local Chiron store.
///
/// Each shard owns its own append buffer and indexes so Kafka partition traffic
/// can be routed into independent hot paths. The total number of live entries
/// is governed by a single global budget on the store, so hot partitions can
/// consume more of the available space without being capped by a fixed local
/// slice of the capacity.
pub struct ChironStore {
    shards: Vec<PartitionShard>,
    host_routes: HashMap<String, BTreeSet<u32>>,
    total_capacity: usize,
    live_len: usize,
}

struct PartitionShard {
    shard_id: u32,
    buffer: PartitionBuffer,
    indexer_pos: u64,
    service_index: InvertedIndex,
    host_index: InvertedIndex,
}

struct PartitionBuffer {
    entries: VecDeque<LogEntry>,
    oldest_offset: u64,
    next_offset: u64,
}

pub struct QueryResult {
    pub entries: Vec<LogEntry>,
}

impl PartitionBuffer {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            oldest_offset: 0,
            next_offset: 0,
        }
    }

    fn from_snapshot(next_offset: u64, entries: Vec<LogEntry>) -> io::Result<Self> {
        if next_offset < entries.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "next offset must be >= live entry count",
            ));
        }

        Ok(Self {
            oldest_offset: next_offset - entries.len() as u64,
            next_offset,
            entries: entries.into(),
        })
    }

    fn push(&mut self, entry: LogEntry) -> u64 {
        let offset = self.next_offset;
        self.entries.push_back(entry);
        self.next_offset += 1;
        offset
    }

    fn get(&self, offset: u64) -> Option<&LogEntry> {
        if offset < self.oldest_offset || offset >= self.next_offset {
            return None;
        }

        let idx = (offset - self.oldest_offset) as usize;
        self.entries.get(idx)
    }

    fn evict_head(&mut self, count: usize) -> usize {
        let to_evict = count.min(self.entries.len());
        for _ in 0..to_evict {
            self.entries.pop_front();
        }
        self.oldest_offset += to_evict as u64;
        to_evict
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn next_offset(&self) -> u64 {
        self.next_offset
    }

    fn oldest_offset(&self) -> u64 {
        self.oldest_offset
    }

    fn oldest_entry(&self) -> Option<&LogEntry> {
        self.entries.front()
    }

    fn entries_cloned(&self) -> Vec<LogEntry> {
        self.entries.iter().cloned().collect()
    }
}

impl PartitionShard {
    fn new(shard_id: u32) -> Self {
        Self {
            shard_id,
            buffer: PartitionBuffer::new(),
            indexer_pos: 0,
            service_index: InvertedIndex::new(),
            host_index: InvertedIndex::new(),
        }
    }

    fn from_restored(restored: RestoredShard) -> io::Result<Self> {
        Ok(Self {
            shard_id: restored.shard_id,
            buffer: PartitionBuffer::from_snapshot(restored.next_offset, restored.entries)?,
            indexer_pos: 0,
            service_index: InvertedIndex::new(),
            host_index: InvertedIndex::new(),
        })
    }

    fn ingest(&mut self, entry: LogEntry) -> u64 {
        self.buffer.push(entry)
    }

    fn flush_indexer(&mut self, host_routes: &mut HashMap<String, BTreeSet<u32>>) {
        let write_head = self.buffer.next_offset();
        while self.indexer_pos < write_head {
            if let Some(entry) = self.buffer.get(self.indexer_pos) {
                self.service_index
                    .insert(&entry.service_name, self.indexer_pos);
                self.host_index.insert(&entry.host_id, self.indexer_pos);
                host_routes
                    .entry(entry.host_id.clone())
                    .or_default()
                    .insert(self.shard_id);
            }
            self.indexer_pos += 1;
        }
    }

    fn indexer_lag(&self) -> u64 {
        self.buffer.next_offset() - self.indexer_pos
    }

    fn query_by_service(&self, service: &str, t1: i64, t2: i64) -> Vec<LogEntry> {
        self.collect_entries(self.service_index.get(service), t1, t2)
    }

    fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> Vec<LogEntry> {
        self.collect_entries(self.host_index.get(host), t1, t2)
    }

    fn query_by_service_and_host(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> Vec<LogEntry> {
        let Some(service_offsets) = self.service_index.get(service) else {
            return vec![];
        };
        let Some(host_offsets) = self.host_index.get(host) else {
            return vec![];
        };

        let mut entries = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < service_offsets.len() && j < host_offsets.len() {
            use std::cmp::Ordering;

            match service_offsets[i].cmp(&host_offsets[j]) {
                Ordering::Less => i += 1,
                Ordering::Greater => j += 1,
                Ordering::Equal => {
                    if let Some(entry) = self.buffer.get(service_offsets[i]) {
                        if entry.timestamp >= t1 && entry.timestamp <= t2 {
                            entries.push(entry.clone());
                        }
                    }
                    i += 1;
                    j += 1;
                }
            }
        }

        entries
    }

    fn collect_entries(&self, offsets: Option<&[u64]>, t1: i64, t2: i64) -> Vec<LogEntry> {
        match offsets {
            None => vec![],
            Some(offsets) => offsets
                .iter()
                .filter_map(|&offset| {
                    self.buffer
                        .get(offset)
                        .filter(|entry| entry.timestamp >= t1 && entry.timestamp <= t2)
                        .cloned()
                })
                .collect(),
        }
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn evict_head(&mut self, count: usize) -> usize {
        let evicted = self.buffer.evict_head(count);
        if evicted == 0 {
            return 0;
        }

        let oldest_offset = self.buffer.oldest_offset();
        self.service_index.purge_below(oldest_offset);
        self.host_index.purge_below(oldest_offset);
        evicted
    }

    fn oldest_timestamp(&self) -> Option<i64> {
        self.buffer.oldest_entry().map(|entry| entry.timestamp)
    }
}

impl ChironStore {
    pub fn new(capacity: usize) -> Self {
        Self::with_shards(capacity, 1)
    }

    pub fn with_shards(total_capacity: usize, shard_count: usize) -> Self {
        assert!(total_capacity > 0, "store capacity must be > 0");
        assert!(shard_count > 0, "shard count must be > 0");

        let shards = (0..shard_count)
            .map(|shard_id| PartitionShard::new(shard_id as u32))
            .collect();

        Self {
            shards,
            host_routes: HashMap::new(),
            total_capacity,
            live_len: 0,
        }
    }

    /// Append using the store's host-based sharding strategy.
    pub fn ingest(&mut self, entry: LogEntry) -> u64 {
        let shard_id = self.route_host_to_shard(&entry.host_id);
        self.evict_while_full();
        let offset = self.shards[shard_id].ingest(entry);
        self.live_len += 1;
        offset
    }

    /// Append directly into the shard that corresponds to a Kafka partition.
    pub fn ingest_partition(&mut self, partition_id: u32, entry: LogEntry) -> u64 {
        let position = self.ensure_shard(partition_id);
        self.evict_while_full();
        let offset = self.shards[position].ingest(entry);
        self.live_len += 1;
        offset
    }

    pub fn flush_indexer(&mut self) {
        let host_routes = &mut self.host_routes;
        for shard in &mut self.shards {
            shard.flush_indexer(host_routes);
        }
    }

    pub fn indexer_lag(&self) -> u64 {
        self.shards.iter().map(PartitionShard::indexer_lag).sum()
    }

    pub fn query_by_service(&self, service: &str, t1: i64, t2: i64) -> QueryResult {
        let mut entries = Vec::new();
        for shard in &self.shards {
            entries.extend(shard.query_by_service(service, t1, t2));
        }
        sort_entries(&mut entries);
        QueryResult { entries }
    }

    pub fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> QueryResult {
        let mut entries = Vec::new();
        for shard_idx in self.shard_positions_for_host(host) {
            entries.extend(self.shards[shard_idx].query_by_host(host, t1, t2));
        }
        sort_entries(&mut entries);
        QueryResult { entries }
    }

    pub fn query_by_service_and_host(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> QueryResult {
        let mut entries = Vec::new();
        for shard_idx in self.shard_positions_for_host(host) {
            entries.extend(
                self.shards[shard_idx].query_by_service_and_host(service, host, t1, t2),
            );
        }
        sort_entries(&mut entries);
        QueryResult { entries }
    }

    /// Global oldest-first eviction across shards.
    pub fn run_eviction(&mut self, target_free_pct: f64) {
        let target_len = (self.capacity() as f64 * (1.0 - target_free_pct)) as usize;
        while self.live_len > target_len {
            if self.evict_global_oldest(1) == 0 {
                break;
            }
        }
    }

    pub fn tick(&mut self) {
        self.flush_indexer();
        self.run_eviction(0.2);
    }

    pub fn len(&self) -> usize {
        self.live_len
    }

    pub fn capacity(&self) -> usize {
        self.total_capacity
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn save_snapshot(
        &self,
        path: &Path,
        kafka_offsets: &KafkaOffsets,
    ) -> io::Result<()> {
        let shards: Vec<_> = self
            .shards
            .iter()
            .map(|shard| SnapshotShard {
                shard_id: shard.shard_id,
                next_offset: shard.buffer.next_offset(),
                entries: shard.buffer.entries_cloned(),
            })
            .collect();

        Snapshot::save_sharded(path, self.total_capacity, &shards, kafka_offsets)
    }

    pub fn from_snapshot(path: &Path) -> io::Result<(Self, KafkaOffsets)> {
        let (total_capacity, restored_shards, kafka_offsets) = Snapshot::load_sharded(path)?;
        if restored_shards.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot must contain at least one shard",
            ));
        }

        let mut shards = Vec::with_capacity(restored_shards.len());
        for restored in restored_shards {
            shards.push(PartitionShard::from_restored(restored)?);
        }
        shards.sort_by_key(|shard| shard.shard_id);

        let mut store = Self {
            shards,
            host_routes: HashMap::new(),
            total_capacity: total_capacity.max(1),
            live_len: 0,
        };
        store.live_len = store.shards.iter().map(PartitionShard::len).sum();
        store.flush_indexer();

        Ok((store, kafka_offsets))
    }

    fn evict_while_full(&mut self) {
        while self.live_len >= self.total_capacity {
            if self.evict_global_oldest(1) == 0 {
                break;
            }
        }
    }

    fn evict_global_oldest(&mut self, count: usize) -> usize {
        let Some((shard_idx, _)) = self
            .shards
            .iter()
            .enumerate()
            .filter_map(|(idx, shard)| shard.oldest_timestamp().map(|ts| (idx, ts)))
            .min_by_key(|(_, ts)| *ts)
        else {
            return 0;
        };

        let evicted = self.shards[shard_idx].evict_head(count);
        self.live_len = self.live_len.saturating_sub(evicted);
        evicted
    }

    fn route_host_to_shard(&self, host: &str) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }

        let mut hasher = DefaultHasher::new();
        host.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn shard_positions_for_host(&self, host: &str) -> Vec<usize> {
        if let Some(routes) = self.host_routes.get(host) {
            let positions: Vec<_> = routes
                .iter()
                .filter_map(|shard_id| self.shard_position(*shard_id))
                .collect();
            if !positions.is_empty() {
                return positions;
            }
        }

        (0..self.shards.len()).collect()
    }

    fn shard_position(&self, shard_id: u32) -> Option<usize> {
        self.shards.iter().position(|shard| shard.shard_id == shard_id)
    }

    fn ensure_shard(&mut self, shard_id: u32) -> usize {
        if let Some(position) = self.shard_position(shard_id) {
            return position;
        }

        let start = self.shards.len() as u32;
        for id in start..=shard_id {
            self.shards.push(PartitionShard::new(id));
        }

        self.shards.len() - 1
    }
}

fn sort_entries(entries: &mut [LogEntry]) {
    entries.sort_by(|a, b| {
        a.timestamp
            .cmp(&b.timestamp)
            .then_with(|| a.host_id.cmp(&b.host_id))
            .then_with(|| a.service_name.cmp(&b.service_name))
            .then_with(|| a.message.cmp(&b.message))
    });
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

        let result = store.query_by_service("auth", 0, 100);
        assert!(result.entries.is_empty());

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
        assert_eq!(result.entries.len(), 3);
    }

    #[test]
    fn indexer_lag_tracks_unindexed() {
        let mut store = ChironStore::new(1000);
        assert_eq!(store.indexer_lag(), 0);
        assert_eq!(store.len(), 0);

        store.ingest(make_entry(1, "a", "h1"));
        store.ingest(make_entry(2, "b", "h2"));
        assert_eq!(store.indexer_lag(), 2);
        assert_eq!(store.len(), 2);

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
    fn sharded_queries_route_by_host() {
        let mut store = ChironStore::with_shards(12, 4);
        store.ingest_partition(0, make_entry(10, "auth", "h0"));
        store.ingest_partition(1, make_entry(20, "auth", "h1"));
        store.ingest_partition(2, make_entry(30, "payments", "h2"));
        store.ingest_partition(1, make_entry(40, "auth", "h1"));
        store.flush_indexer();

        let host_result = store.query_by_host("h1", 0, 100);
        assert_eq!(host_result.entries.len(), 2);
        assert!(host_result.entries.iter().all(|entry| entry.host_id == "h1"));

        let service_result = store.query_by_service("auth", 0, 100);
        assert_eq!(service_result.entries.len(), 3);

        let combined = store.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(combined.entries.len(), 2);
    }

    #[test]
    fn sharded_eviction_drops_global_oldest_entries() {
        let mut store = ChironStore::with_shards(4, 2);
        store.ingest_partition(0, make_entry(10, "svc", "h0"));
        store.ingest_partition(1, make_entry(20, "svc", "h1"));
        store.ingest_partition(0, make_entry(30, "svc", "h0"));
        store.ingest_partition(1, make_entry(40, "svc", "h1"));
        assert_eq!(store.len(), 4);
        store.ingest_partition(0, make_entry(50, "svc", "h0"));
        assert_eq!(store.len(), 4);
        store.flush_indexer();

        let result = store.query_by_service("svc", 0, 100);
        assert_eq!(result.entries.len(), 4);
        assert_eq!(result.entries[0].timestamp, 20);
    }

    #[test]
    fn global_capacity_budget_handles_hot_shard() {
        let mut store = ChironStore::with_shards(12, 4);
        for ts in 0..12 {
            store.ingest_partition(0, make_entry(ts, "svc", "hot-host"));
        }
        store.flush_indexer();

        let result = store.query_by_service("svc", 0, i64::MAX);
        assert_eq!(result.entries.len(), 12);

        store.tick();
        let result = store.query_by_service("svc", 0, i64::MAX);
        assert_eq!(result.entries.len(), 9);
        assert_eq!(result.entries.first().unwrap().timestamp, 3);
        assert_eq!(result.entries.last().unwrap().timestamp, 11);
    }

    #[test]
    fn snapshot_and_restore() {
        let dir = std::env::temp_dir().join("chiron_test_store_snapshot");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.snap");

        let mut store = ChironStore::with_shards(12, 3);
        store.ingest_partition(0, make_entry(10, "auth", "h1"));
        store.ingest_partition(1, make_entry(20, "payment", "h2"));
        store.ingest_partition(0, make_entry(30, "auth", "h1"));
        store.flush_indexer();

        let mut offsets = KafkaOffsets::new();
        offsets.set("logs", 0, 99999);
        offsets.set("logs", 1, 88888);

        store.save_snapshot(&path, &offsets).unwrap();

        let (restored, restored_offsets) = ChironStore::from_snapshot(&path).unwrap();
        assert_eq!(restored.len(), 3);

        let result = restored.query_by_service("auth", 0, 100);
        assert_eq!(result.entries.len(), 2);

        let result = restored.query_by_host("h1", 0, 100);
        assert_eq!(result.entries.len(), 2);

        let result = restored.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(result.entries.len(), 2);

        assert_eq!(restored.shard_count(), 3);
        assert_eq!(restored_offsets.get("logs", 0), Some(99999));
        assert_eq!(restored_offsets.get("logs", 1), Some(88888));

        std::fs::remove_dir_all(&dir).ok();
    }
}
