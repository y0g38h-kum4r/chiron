use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::partition_for_host;

use crate::inverted_index::InvertedIndex;
use crate::log_entry::{LogEntry, SharedLogEntry};
use crate::snapshot::{KafkaOffsets, RestoredShard, Snapshot, SnapshotShard};

/// Partition-local Chiron store.
///
/// Each shard owns its own append buffer and indexes so Kafka partition traffic
/// can be routed into independent hot paths. The total number of live entries
/// is still governed by a single global budget on the store, but when a hot
/// shard causes capacity pressure it preferentially sheds its own oldest data
/// before displacing quieter shards.
pub struct ChironStore {
    shards: RwLock<Vec<Arc<PartitionShard>>>,
    total_capacity: usize,
    live_len: AtomicUsize,
    shard_admin: Mutex<()>,
    capacity_admin: Mutex<()>,
}

struct PartitionShard {
    shard_id: u32,
    capacity: usize,
    inner: RwLock<PartitionShardState>,
}

struct PartitionShardState {
    buffer: PartitionBuffer,
    indexer_pos: u64,
    service_index: InvertedIndex,
    host_index: InvertedIndex,
}

struct PartitionBuffer {
    entries: VecDeque<SharedLogEntry>,
    oldest_offset: u64,
    next_offset: u64,
}

pub struct QueryResult {
    pub entries: Vec<SharedLogEntry>,
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
            entries: entries.into_iter().map(SharedLogEntry::from).collect(),
        })
    }

    fn push(&mut self, entry: SharedLogEntry) -> u64 {
        let offset = self.next_offset;
        self.entries.push_back(entry);
        self.next_offset += 1;
        offset
    }

    fn get(&self, offset: u64) -> Option<&SharedLogEntry> {
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

    fn entries_owned(&self) -> Vec<LogEntry> {
        self.entries.iter().map(LogEntry::from).collect()
    }
}

impl PartitionShardState {
    fn ingest(&mut self, entry: SharedLogEntry) -> u64 {
        let offset = self.buffer.push(entry);
        // Index inline while we already hold the write lock — avoids
        // accumulating indexer lag between background flush ticks.
        if let Some(e) = self.buffer.get(offset) {
            self.service_index.insert(&e.service_name, offset);
            self.host_index.insert(&e.host_id, offset);
        }
        self.indexer_pos = self.buffer.next_offset();
        offset
    }

    fn flush_indexer(&mut self) {
        let write_head = self.buffer.next_offset();
        while self.indexer_pos < write_head {
            if let Some(entry) = self.buffer.get(self.indexer_pos) {
                self.service_index
                    .insert(&entry.service_name, self.indexer_pos);
                self.host_index.insert(&entry.host_id, self.indexer_pos);
            }
            self.indexer_pos += 1;
        }
    }

    fn indexer_lag(&self) -> u64 {
        self.buffer.next_offset() - self.indexer_pos
    }

    fn query_by_service(&self, service: &str, t1: i64, t2: i64) -> Vec<SharedLogEntry> {
        self.collect_entries(self.service_index.get(service), t1, t2)
    }

    fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> Vec<SharedLogEntry> {
        self.collect_entries(self.host_index.get(host), t1, t2)
    }

    fn query_by_service_and_host(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> Vec<SharedLogEntry> {
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

    fn collect_entries(
        &self,
        offsets: Option<&[u64]>,
        t1: i64,
        t2: i64,
    ) -> Vec<SharedLogEntry> {
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
}

impl PartitionShard {
    fn new(shard_id: u32, capacity: usize) -> Self {
        Self {
            shard_id,
            capacity,
            inner: RwLock::new(PartitionShardState {
                buffer: PartitionBuffer::new(),
                indexer_pos: 0,
                service_index: InvertedIndex::new(),
                host_index: InvertedIndex::new(),
            }),
        }
    }

    fn from_restored(restored: RestoredShard) -> io::Result<Self> {
        Ok(Self {
            shard_id: restored.shard_id,
            capacity: restored.capacity,
            inner: RwLock::new(PartitionShardState {
                buffer: PartitionBuffer::from_snapshot(restored.next_offset, restored.entries)?,
                indexer_pos: 0,
                service_index: InvertedIndex::new(),
                host_index: InvertedIndex::new(),
            }),
        })
    }

    fn ingest(&self, entry: SharedLogEntry) -> u64 {
        self.inner.write().unwrap().ingest(entry)
    }

    fn ingest_batch(&self, entries: Vec<SharedLogEntry>) -> usize {
        let len = entries.len();
        if len == 0 {
            return 0;
        }

        let mut inner = self.inner.write().unwrap();
        for entry in entries {
            inner.ingest(entry);
        }

        len
    }

    fn flush_indexer(&self) {
        self.inner.write().unwrap().flush_indexer();
    }

    fn indexer_lag(&self) -> u64 {
        self.inner.read().unwrap().indexer_lag()
    }

    fn query_by_service(&self, service: &str, t1: i64, t2: i64) -> Vec<SharedLogEntry> {
        self.inner.read().unwrap().query_by_service(service, t1, t2)
    }

    fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> Vec<SharedLogEntry> {
        self.inner.read().unwrap().query_by_host(host, t1, t2)
    }

    fn query_by_service_and_host(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> Vec<SharedLogEntry> {
        self.inner
            .read()
            .unwrap()
            .query_by_service_and_host(service, host, t1, t2)
    }

    fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn evict_head(&self, count: usize) -> usize {
        self.inner.write().unwrap().evict_head(count)
    }

    fn entries_owned(&self) -> Vec<LogEntry> {
        self.inner.read().unwrap().buffer.entries_owned()
    }

}

impl ChironStore {
    pub fn new(capacity: usize) -> Self {
        Self::with_shards(capacity, 1)
    }

    pub fn with_shards(total_capacity: usize, shard_count: usize) -> Self {
        assert!(total_capacity > 0, "store capacity must be > 0");
        assert!(shard_count > 0, "shard count must be > 0");
        assert!(
            total_capacity.is_multiple_of(shard_count),
            "store capacity ({total_capacity}) must be divisible by shard count ({shard_count})"
        );

        let shard_capacity = total_capacity / shard_count;

        let shards = (0..shard_count)
            .map(|shard_id| Arc::new(PartitionShard::new(shard_id as u32, shard_capacity)))
            .collect();

        Self {
            shards: RwLock::new(shards),
            total_capacity,
            live_len: AtomicUsize::new(0),
            shard_admin: Mutex::new(()),
            capacity_admin: Mutex::new(()),
        }
    }

    /// Append using the store's host-based sharding strategy.
    pub fn ingest(&self, entry: LogEntry) -> u64 {
        let shard = self.shard_for_host(&entry.host_id);
        self.reserve_slot_for_shard(&shard);
        shard.ingest(entry.into())
    }

    /// Append directly into the shard that corresponds to a Kafka partition.
    pub fn ingest_partition(&self, partition_id: u32, entry: LogEntry) -> u64 {
        let shard = self.ensure_shard(partition_id);
        self.reserve_slot_for_shard(&shard);
        shard.ingest(entry.into())
    }

    /// Append a batch directly into the shard that corresponds to a Kafka partition.
    pub fn ingest_partition_batch(&self, partition_id: u32, entries: Vec<LogEntry>) -> usize {
        if entries.is_empty() {
            return 0;
        }

        let shard = self.ensure_shard(partition_id);
        let mut shared_entries = Vec::with_capacity(entries.len());

        for entry in entries {
            self.reserve_slot_for_shard(&shard);
            shared_entries.push(entry.into());
        }

        shard.ingest_batch(shared_entries)
    }

    pub fn flush_indexer(&self) {
        for shard in self.shard_handles() {
            shard.flush_indexer();
        }
    }

    pub fn flush_indexer_shard(&self, shard_id: u32) {
        if let Some(shard) = self.shard_by_id(shard_id) {
            shard.flush_indexer();
        }
    }

    pub fn indexer_lag(&self) -> u64 {
        self.shard_handles()
            .iter()
            .map(|shard| shard.indexer_lag())
            .sum()
    }

    pub fn query_by_service(&self, service: &str, t1: i64, t2: i64) -> QueryResult {
        let mut entries = Vec::new();
        for shard in self.shard_handles() {
            entries.extend(shard.query_by_service(service, t1, t2));
        }
        sort_entries(&mut entries);
        QueryResult { entries }
    }

    pub fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> QueryResult {
        let shard = self.shard_for_host(host);
        let mut entries = shard.query_by_host(host, t1, t2);
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
        let shard = self.shard_for_host(host);
        let mut entries = shard.query_by_service_and_host(service, host, t1, t2);
        sort_entries(&mut entries);
        QueryResult { entries }
    }

    pub fn len(&self) -> usize {
        self.live_len.load(Ordering::Acquire)
    }

    pub fn capacity(&self) -> usize {
        self.total_capacity
    }

    pub fn shard_count(&self) -> usize {
        self.shards.read().unwrap().len()
    }

    /// Returns the number of live entries in each shard, indexed by shard position.
    pub fn shard_lens(&self) -> Vec<usize> {
        self.shard_handles().iter().map(|s| s.len()).collect()
    }

    pub fn save_snapshot(&self, path: &Path, kafka_offsets: &KafkaOffsets) -> io::Result<()> {
        let shards: Vec<_> = self
            .shard_handles()
            .iter()
            .map(|shard| SnapshotShard {
                shard_id: shard.shard_id,
                capacity: shard.capacity(),
                next_offset: shard.inner.read().unwrap().buffer.next_offset(),
                entries: shard.entries_owned(),
            })
            .collect();

        Snapshot::save_sharded(path, &shards, kafka_offsets)
    }

    pub fn from_snapshot(path: &Path) -> io::Result<(Self, KafkaOffsets)> {
        let (restored_shards, kafka_offsets) = Snapshot::load_sharded(path)?;
        if restored_shards.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot must contain at least one shard",
            ));
        }

        let mut shards = Vec::with_capacity(restored_shards.len());
        for restored in restored_shards {
            shards.push(Arc::new(PartitionShard::from_restored(restored)?));
        }
        shards.sort_by_key(|shard| shard.shard_id);

        let live_len = shards.iter().map(|shard| shard.len()).sum();
        let total_capacity = shards.iter().map(|shard| shard.capacity()).sum();
        let store = Self {
            shards: RwLock::new(shards),
            total_capacity,
            live_len: AtomicUsize::new(live_len),
            shard_admin: Mutex::new(()),
            capacity_admin: Mutex::new(()),
        };
        store.flush_indexer();

        Ok((store, kafka_offsets))
    }

    fn reserve_slot_for_shard(&self, target_shard: &Arc<PartitionShard>) {
        loop {
            let current = self.live_len.load(Ordering::Acquire);
            if current < self.total_capacity {
                if self
                    .live_len
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return;
                }
                continue;
            }

            let _guard = self.capacity_admin.lock().unwrap();
            if self.live_len.load(Ordering::Acquire) < self.total_capacity {
                continue;
            }

            // TODO: Batch eviction — evict e.g. 1% of shard capacity at once
            // instead of 1 entry at a time. Each evict_head triggers a full
            // InvertedIndex::purge_below scan (O(K * log N) over all keys),
            // so batching amortizes that cost and avoids latency spikes under
            // high-cardinality dimensions (ephemeral hosts/services).
            let evicted = if target_shard.len() > 0 {
                target_shard.evict_head(1)
            } else {
                // If the target shard is empty, fall back to trimming the
                // fullest shard so the new write can still be admitted.
                self.evict_fullest_shard(1)
            };

            if evicted == 0 {
                self.live_len
                    .store(self.live_len_from_shards(), Ordering::Release);
                continue;
            }

            self.live_len.fetch_sub(evicted, Ordering::AcqRel);
        }
    }

    fn evict_fullest_shard(&self, count: usize) -> usize {
        let Some(shard) = self
            .shard_handles()
            .iter()
            .filter(|shard| shard.len() > 0)
            .max_by_key(|shard| shard.len())
            .cloned()
        else {
            return 0;
        };

        shard.evict_head(count)
    }

    /// Deterministic host→shard routing. Uses the shared `partition_for_host`
    /// function so producers and the store always agree on shard placement.
    fn shard_for_host(&self, host: &str) -> Arc<PartitionShard> {
        let pos = partition_for_host(host, self.shard_count());
        self.shards.read().unwrap()[pos].clone()
    }

    fn shard_handles(&self) -> Vec<Arc<PartitionShard>> {
        self.shards.read().unwrap().iter().cloned().collect()
    }

    fn shard_by_id(&self, shard_id: u32) -> Option<Arc<PartitionShard>> {
        self.shards
            .read()
            .unwrap()
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .cloned()
    }

    fn ensure_shard(&self, shard_id: u32) -> Arc<PartitionShard> {
        if let Some(shard) = self.shard_by_id(shard_id) {
            return shard;
        }

        let _guard = self.shard_admin.lock().unwrap();
        if let Some(shard) = self.shard_by_id(shard_id) {
            return shard;
        }

        let mut shards = self.shards.write().unwrap();
        let start = shards.len() as u32;
        for id in start..=shard_id {
            shards.push(Arc::new(PartitionShard::new(id, 0)));
        }

        shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .cloned()
            .expect("newly created shard must exist")
    }

    fn live_len_from_shards(&self) -> usize {
        self.shard_handles().iter().map(|shard| shard.len()).sum()
    }
}

fn sort_entries(entries: &mut [SharedLogEntry]) {
    entries.sort_unstable_by_key(|entry| entry.timestamp);
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
        }
    }

    fn assert_nondecreasing_timestamps(entries: &[SharedLogEntry]) {
        assert!(
            entries
                .windows(2)
                .all(|pair| pair[0].timestamp <= pair[1].timestamp),
            "query results must be ordered by nondecreasing timestamp"
        );
    }

    #[test]
    fn ingest_flush_and_query() {
        let store = ChironStore::new(1000);

        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "payment", "h1"));
        store.ingest(make_entry(30, "auth", "h2"));

        // Entries are queryable immediately after ingest (inline indexing).
        let result = store.query_by_service("auth", 0, 100);
        assert_eq!(result.entries.len(), 2);
        assert!(result.entries.iter().all(|e| &*e.service_name == "auth"));
        assert_nondecreasing_timestamps(&result.entries);

        // flush_indexer is still safe to call (idempotent).
        store.flush_indexer();

        let result = store.query_by_service("auth", 0, 100);
        assert_eq!(result.entries.len(), 2);
    }

    #[test]
    fn query_by_host() {
        let store = ChironStore::new(1000);
        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "auth", "h2"));
        store.ingest(make_entry(30, "payment", "h1"));
        store.flush_indexer();

        let result = store.query_by_host("h1", 0, 100);
        assert_eq!(result.entries.len(), 2);
        assert!(result.entries.iter().all(|e| &*e.host_id == "h1"));
        assert_nondecreasing_timestamps(&result.entries);
    }

    #[test]
    fn query_by_service_and_host() {
        let store = ChironStore::new(1000);
        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "auth", "h2"));
        store.ingest(make_entry(30, "payment", "h1"));
        store.flush_indexer();

        let result = store.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].timestamp, 10);
    }

    /// Helper: compute the correct partition for a host given a shard count,
    /// using the same deterministic hash the store uses.
    fn part(host: &str, shard_count: usize) -> u32 {
        partition_for_host(host, shard_count) as u32
    }

    #[test]
    fn shard_local_flush_only_indexes_that_shard() {
        let store = ChironStore::with_shards(12, 2);
        let p0 = part("host-0", 2);
        let p1 = part("host-1", 2);
        assert_ne!(p0, p1, "pick hosts that hash to different shards");

        store.ingest_partition(p0, make_entry(10, "auth", "host-0"));
        store.ingest_partition(p1, make_entry(20, "auth", "host-1"));

        // With inline indexing both shards are already indexed.
        let h0 = store.query_by_host("host-0", 0, 100);
        assert_eq!(h0.entries.len(), 1);
        assert_eq!(h0.entries[0].timestamp, 10);

        let h1 = store.query_by_host("host-1", 0, 100);
        assert_eq!(h1.entries.len(), 1);
        assert_eq!(h1.entries[0].timestamp, 20);
    }

    #[test]
    fn batch_ingest_partition_indexes_all_entries() {
        let store = ChironStore::with_shards(12, 2);
        let p = part("h1", 2);
        let ingested = store.ingest_partition_batch(
            p,
            vec![
                make_entry(10, "auth", "h1"),
                make_entry(20, "auth", "h1"),
            ],
        );
        assert_eq!(ingested, 2);
        assert_eq!(store.len(), 2);

        store.flush_indexer_shard(p);

        let host = store.query_by_host("h1", 0, 100);
        assert_eq!(host.entries.len(), 2);
        assert_nondecreasing_timestamps(&host.entries);

        let pair = store.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(pair.entries.len(), 2);
        assert_nondecreasing_timestamps(&pair.entries);
    }

    #[test]
    fn query_respects_time_range() {
        let store = ChironStore::new(1000);
        for ts in [5, 10, 15, 20, 25] {
            store.ingest(make_entry(ts, "svc", "h1"));
        }
        store.flush_indexer();

        let result = store.query_by_service("svc", 10, 20);
        assert_eq!(result.entries.len(), 3);
        assert_nondecreasing_timestamps(&result.entries);
    }

    #[test]
    fn indexer_lag_tracks_unindexed() {
        let store = ChironStore::new(1000);
        assert_eq!(store.indexer_lag(), 0);
        assert_eq!(store.len(), 0);

        store.ingest(make_entry(1, "a", "h1"));
        store.ingest(make_entry(2, "b", "h1"));
        // Inline indexing keeps lag at zero.
        assert_eq!(store.indexer_lag(), 0);
        assert_eq!(store.len(), 2);

        store.flush_indexer();
        assert_eq!(store.indexer_lag(), 0);
    }

    #[test]
    fn flush_indexer_makes_entries_queryable() {
        let store = ChironStore::new(1000);
        for ts in 0..100 {
            store.ingest(make_entry(ts, "svc", "h1"));
        }
        store.flush_indexer();
        assert_eq!(store.indexer_lag(), 0);

        let result = store.query_by_service("svc", 0, 100);
        assert_eq!(result.entries.len(), 100);
        assert_nondecreasing_timestamps(&result.entries);
    }

    #[test]
    fn sharded_queries_route_by_host() {
        let shard_count = 4;
        let store = ChironStore::with_shards(100, shard_count);

        // Use ingest() which routes via partition_for_host automatically.
        store.ingest(make_entry(10, "auth", "h0"));
        store.ingest(make_entry(20, "auth", "h1"));
        store.ingest(make_entry(30, "payments", "h2"));
        store.ingest(make_entry(40, "auth", "h1"));
        store.flush_indexer();

        let host_result = store.query_by_host("h1", 0, 100);
        assert_eq!(host_result.entries.len(), 2);
        assert!(host_result.entries.iter().all(|e| &*e.host_id == "h1"));

        let service_result = store.query_by_service("auth", 0, 100);
        assert_eq!(service_result.entries.len(), 3);
        assert_nondecreasing_timestamps(&service_result.entries);

        let combined = store.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(combined.entries.len(), 2);
        assert_nondecreasing_timestamps(&combined.entries);
    }

    #[test]
    fn sharded_eviction_prefers_the_writing_shard() {
        let store = ChironStore::with_shards(4, 2);
        let p0 = part("h0", 2);
        let p1 = part("h1", 2);
        assert_ne!(p0, p1, "pick hosts that hash to different shards");

        store.ingest_partition(p0, make_entry(30, "svc", "h0"));
        store.ingest_partition(p1, make_entry(10, "svc", "h1"));
        store.ingest_partition(p0, make_entry(40, "svc", "h0"));
        store.ingest_partition(p1, make_entry(20, "svc", "h1"));
        assert_eq!(store.len(), 4);
        store.ingest_partition(p0, make_entry(50, "svc", "h0"));
        assert_eq!(store.len(), 4);
        store.flush_indexer();

        let result = store.query_by_service("svc", 0, 100);
        assert_eq!(result.entries.len(), 4);
        let timestamps: Vec<_> = result.entries.iter().map(|e| e.timestamp).collect();
        assert_eq!(timestamps, vec![10, 20, 40, 50]);
        assert_nondecreasing_timestamps(&result.entries);
    }

    #[test]
    fn hot_shard_evicts_own_entries_under_pressure() {
        let store = ChironStore::with_shards(12, 4);
        let p = part("hot-host", 4);
        // Fill all 12 slots into one shard.
        for ts in 0..12 {
            store.ingest_partition(p, make_entry(ts, "svc", "hot-host"));
        }
        assert_eq!(store.len(), 12);

        // 13th entry forces eviction from the writing shard.
        store.ingest_partition(p, make_entry(12, "svc", "hot-host"));
        assert_eq!(store.len(), 12);
        store.flush_indexer();

        let result = store.query_by_service("svc", 0, i64::MAX);
        assert_eq!(result.entries.len(), 12);
        assert_eq!(result.entries.first().unwrap().timestamp, 1);
        assert_eq!(result.entries.last().unwrap().timestamp, 12);
        assert_nondecreasing_timestamps(&result.entries);
    }

    #[test]
    fn query_results_only_guarantee_timestamp_order() {
        // Use ingest() to let the store route each host deterministically.
        let store = ChironStore::with_shards(100, 4);
        store.ingest(make_entry(10, "svc", "h1"));
        store.ingest(make_entry(10, "svc", "h2"));
        store.ingest(make_entry(11, "svc", "h3"));
        store.flush_indexer();

        let result = store.query_by_service("svc", 0, i64::MAX);
        assert_eq!(result.entries.len(), 3);
        assert_nondecreasing_timestamps(&result.entries);

        let mut same_timestamp_hosts: Vec<_> = result
            .entries
            .iter()
            .filter(|entry| entry.timestamp == 10)
            .map(|entry| &*entry.host_id)
            .collect();
        same_timestamp_hosts.sort_unstable();
        assert_eq!(same_timestamp_hosts, vec!["h1", "h2"]);
    }

    #[test]
    fn snapshot_and_restore() {
        let dir = std::env::temp_dir().join("chiron_test_store_snapshot");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.snap");

        let store = ChironStore::with_shards(12, 3);
        // Use ingest() so hosts are routed to the correct shard.
        store.ingest(make_entry(10, "auth", "h1"));
        store.ingest(make_entry(20, "payment", "h2"));
        store.ingest(make_entry(30, "auth", "h1"));
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

    #[test]
    #[should_panic(expected = "must be divisible by shard count")]
    fn with_shards_requires_uniform_capacity() {
        let _ = ChironStore::with_shards(10, 3);
    }
}
