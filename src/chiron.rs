use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

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
    shards: Vec<PartitionShard>,
    host_routes: HashMap<String, u32>,
    total_capacity: usize,
    live_len: usize,
}

struct PartitionShard {
    shard_id: u32,
    capacity: usize,
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
    pub entries: Vec<LogEntry>,
}

pub struct SharedQueryResult {
    pub entries: Vec<SharedLogEntry>,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct HostQueryProfile {
    pub shards_touched: usize,
    pub hits: usize,
    pub posting_list_scan: Duration,
    pub materialize: Duration,
    pub sort: Duration,
    pub total: Duration,
}

impl HostQueryProfile {
    fn accumulate(&mut self, other: HostQueryProfile) {
        self.shards_touched += other.shards_touched;
        self.hits += other.hits;
        self.posting_list_scan += other.posting_list_scan;
        self.materialize += other.materialize;
        self.sort += other.sort;
        self.total += other.total;
    }
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

impl PartitionShard {
    fn new(shard_id: u32, capacity: usize) -> Self {
        Self {
            shard_id,
            capacity,
            buffer: PartitionBuffer::new(),
            indexer_pos: 0,
            service_index: InvertedIndex::new(),
            host_index: InvertedIndex::new(),
        }
    }

    fn from_restored(restored: RestoredShard) -> io::Result<Self> {
        Ok(Self {
            shard_id: restored.shard_id,
            capacity: restored.capacity,
            buffer: PartitionBuffer::from_snapshot(restored.next_offset, restored.entries)?,
            indexer_pos: 0,
            service_index: InvertedIndex::new(),
            host_index: InvertedIndex::new(),
        })
    }

    fn ingest(&mut self, entry: SharedLogEntry) -> u64 {
        self.buffer.push(entry)
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

    fn query_by_service(&self, service: &str, t1: i64, t2: i64) -> Vec<LogEntry> {
        self.collect_owned_entries(self.service_index.get(service), t1, t2)
    }

    fn query_by_service_shared(&self, service: &str, t1: i64, t2: i64) -> Vec<SharedLogEntry> {
        self.collect_shared_entries(self.service_index.get(service), t1, t2)
    }

    fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> Vec<LogEntry> {
        self.collect_owned_entries(self.host_index.get(host), t1, t2)
    }

    fn query_by_host_shared(&self, host: &str, t1: i64, t2: i64) -> Vec<SharedLogEntry> {
        self.collect_shared_entries(self.host_index.get(host), t1, t2)
    }

    fn query_by_host_profiled(
        &self,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> (Vec<LogEntry>, HostQueryProfile) {
        let (entries, profile) = self.query_by_host_shared_profiled(host, t1, t2);
        let materialize_started = Instant::now();
        let owned_entries = entries.iter().map(LogEntry::from).collect();
        let extra_materialize = materialize_started.elapsed();
        (
            owned_entries,
            HostQueryProfile {
                materialize: profile.materialize + extra_materialize,
                total: profile.total + extra_materialize,
                ..profile
            },
        )
    }

    fn query_by_host_shared_profiled(
        &self,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> (Vec<SharedLogEntry>, HostQueryProfile) {
        let total_started = Instant::now();
        let Some(offsets) = self.host_index.get(host) else {
            return (vec![], HostQueryProfile::default());
        };

        let scan_started = Instant::now();
        let mut entries = Vec::with_capacity(offsets.len());
        for &offset in offsets {
            if let Some(entry) = self.buffer.get(offset) {
                if entry.timestamp >= t1 && entry.timestamp <= t2 {
                    entries.push(entry.clone());
                }
            }
        }
        let posting_list_scan = scan_started.elapsed();

        let materialize = Duration::ZERO;

        let hits = entries.len();
        let total = total_started.elapsed();
        (
            entries,
            HostQueryProfile {
                shards_touched: 1,
                hits,
                posting_list_scan,
                materialize,
                sort: Duration::ZERO,
                total,
            },
        )
    }

    fn query_by_service_and_host(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> Vec<LogEntry> {
        self.query_by_service_and_host_shared(service, host, t1, t2)
            .iter()
            .map(LogEntry::from)
            .collect()
    }

    fn query_by_service_and_host_shared(
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

    fn collect_owned_entries(&self, offsets: Option<&[u64]>, t1: i64, t2: i64) -> Vec<LogEntry> {
        self.collect_shared_entries(offsets, t1, t2)
            .iter()
            .map(LogEntry::from)
            .collect()
    }

    fn collect_shared_entries(
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

    fn capacity(&self) -> usize {
        self.capacity
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

impl ChironStore {
    pub fn new(capacity: usize) -> Self {
        Self::with_shards(capacity, 1)
    }

    pub fn with_shards(total_capacity: usize, shard_count: usize) -> Self {
        assert!(total_capacity > 0, "store capacity must be > 0");
        assert!(shard_count > 0, "shard count must be > 0");

        let shards = (0..shard_count)
            .map(|shard_id| {
                PartitionShard::new(
                    shard_id as u32,
                    shard_capacity_for_position(total_capacity, shard_count, shard_id),
                )
            })
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
        let shard_route = self.shards[shard_id].shard_id;
        self.register_host_route(&entry.host_id, shard_route);
        self.evict_while_full_for_shard(shard_id);
        let offset = self.shards[shard_id].ingest(entry.into());
        self.live_len += 1;
        offset
    }

    /// Append directly into the shard that corresponds to a Kafka partition.
    pub fn ingest_partition(&mut self, partition_id: u32, entry: LogEntry) -> u64 {
        let position = self.ensure_shard(partition_id);
        let shard_route = self.shards[position].shard_id;
        self.register_host_route(&entry.host_id, shard_route);
        self.evict_while_full_for_shard(position);
        let offset = self.shards[position].ingest(entry.into());
        self.live_len += 1;
        offset
    }

    pub fn flush_indexer(&mut self) {
        for shard in &mut self.shards {
            shard.flush_indexer();
        }
    }

    pub fn flush_indexer_shard(&mut self, shard_id: u32) {
        if let Some(position) = self.shard_position(shard_id) {
            self.shards[position].flush_indexer();
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

    pub fn query_by_service_shared(&self, service: &str, t1: i64, t2: i64) -> SharedQueryResult {
        let mut entries = Vec::new();
        for shard in &self.shards {
            entries.extend(shard.query_by_service_shared(service, t1, t2));
        }
        sort_shared_entries(&mut entries);
        SharedQueryResult { entries }
    }

    pub fn query_by_host(&self, host: &str, t1: i64, t2: i64) -> QueryResult {
        let mut entries = Vec::new();
        if let Some(shard_idx) = self.shard_position_for_host(host) {
            entries.extend(self.shards[shard_idx].query_by_host(host, t1, t2));
        } else {
            for shard in &self.shards {
                entries.extend(shard.query_by_host(host, t1, t2));
            }
        }
        sort_entries(&mut entries);
        QueryResult { entries }
    }

    pub fn query_by_host_shared(&self, host: &str, t1: i64, t2: i64) -> SharedQueryResult {
        let mut entries = Vec::new();
        if let Some(shard_idx) = self.shard_position_for_host(host) {
            entries.extend(self.shards[shard_idx].query_by_host_shared(host, t1, t2));
        } else {
            for shard in &self.shards {
                entries.extend(shard.query_by_host_shared(host, t1, t2));
            }
        }
        sort_shared_entries(&mut entries);
        SharedQueryResult { entries }
    }

    pub fn query_by_host_profiled(
        &self,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> (QueryResult, HostQueryProfile) {
        let total_started = Instant::now();
        let mut entries = Vec::new();
        let mut profile = HostQueryProfile::default();

        if let Some(shard_idx) = self.shard_position_for_host(host) {
            let (shard_entries, shard_profile) =
                self.shards[shard_idx].query_by_host_profiled(host, t1, t2);
            entries.extend(shard_entries);
            profile.accumulate(shard_profile);
        } else {
            for shard in &self.shards {
                let (shard_entries, shard_profile) = shard.query_by_host_profiled(host, t1, t2);
                entries.extend(shard_entries);
                profile.accumulate(shard_profile);
            }
        }

        let sort_started = Instant::now();
        sort_entries(&mut entries);
        profile.sort = sort_started.elapsed();
        profile.hits = entries.len();
        profile.total = total_started.elapsed();

        (QueryResult { entries }, profile)
    }

    pub fn query_by_host_shared_profiled(
        &self,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> (SharedQueryResult, HostQueryProfile) {
        let total_started = Instant::now();
        let mut entries = Vec::new();
        let mut profile = HostQueryProfile::default();

        if let Some(shard_idx) = self.shard_position_for_host(host) {
            let (shard_entries, shard_profile) =
                self.shards[shard_idx].query_by_host_shared_profiled(host, t1, t2);
            entries.extend(shard_entries);
            profile.accumulate(shard_profile);
        } else {
            for shard in &self.shards {
                let (shard_entries, shard_profile) =
                    shard.query_by_host_shared_profiled(host, t1, t2);
                entries.extend(shard_entries);
                profile.accumulate(shard_profile);
            }
        }

        let sort_started = Instant::now();
        sort_shared_entries(&mut entries);
        profile.sort = sort_started.elapsed();
        profile.hits = entries.len();
        profile.total = total_started.elapsed();

        (SharedQueryResult { entries }, profile)
    }

    pub fn query_by_service_and_host(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> QueryResult {
        let mut entries = Vec::new();
        if let Some(shard_idx) = self.shard_position_for_host(host) {
            entries.extend(self.shards[shard_idx].query_by_service_and_host(service, host, t1, t2));
        } else {
            for shard in &self.shards {
                entries.extend(shard.query_by_service_and_host(service, host, t1, t2));
            }
        }
        sort_entries(&mut entries);
        QueryResult { entries }
    }

    pub fn query_by_service_and_host_shared(
        &self,
        service: &str,
        host: &str,
        t1: i64,
        t2: i64,
    ) -> SharedQueryResult {
        let mut entries = Vec::new();
        if let Some(shard_idx) = self.shard_position_for_host(host) {
            entries.extend(
                self.shards[shard_idx].query_by_service_and_host_shared(service, host, t1, t2),
            );
        } else {
            for shard in &self.shards {
                entries.extend(shard.query_by_service_and_host_shared(service, host, t1, t2));
            }
        }
        sort_shared_entries(&mut entries);
        SharedQueryResult { entries }
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

    pub fn save_snapshot(&self, path: &Path, kafka_offsets: &KafkaOffsets) -> io::Result<()> {
        let shards: Vec<_> = self
            .shards
            .iter()
            .map(|shard| SnapshotShard {
                shard_id: shard.shard_id,
                capacity: shard.capacity(),
                next_offset: shard.buffer.next_offset(),
                entries: shard.buffer.entries_owned(),
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
            shards.push(PartitionShard::from_restored(restored)?);
        }
        shards.sort_by_key(|shard| shard.shard_id);

        let live_len = shards.iter().map(PartitionShard::len).sum();
        let total_capacity = shards.iter().map(PartitionShard::capacity).sum();
        let mut store = Self {
            shards,
            host_routes: HashMap::new(),
            total_capacity,
            live_len,
        };
        store.rebuild_host_routes();
        store.flush_indexer();

        Ok((store, kafka_offsets))
    }

    fn evict_while_full_for_shard(&mut self, shard_idx: usize) {
        while self.len() >= self.capacity() {
            let evicted = if self.shards[shard_idx].len() > 0 {
                self.evict_from_shard(shard_idx, 1)
            } else {
                // If the target shard is empty, fall back to trimming the
                // fullest shard so the new write can still be admitted.
                self.evict_fullest_shard(1)
            };

            if evicted == 0 {
                break;
            }
        }
    }

    fn evict_fullest_shard(&mut self, count: usize) -> usize {
        let Some((shard_idx, _)) = self
            .shards
            .iter()
            .enumerate()
            .map(|(idx, shard)| (idx, shard.len()))
            .filter(|(_, len)| *len > 0)
            .max_by_key(|(_, len)| *len)
        else {
            return 0;
        };

        self.evict_from_shard(shard_idx, count)
    }

    fn evict_from_shard(&mut self, shard_idx: usize, count: usize) -> usize {
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

    fn shard_position_for_host(&self, host: &str) -> Option<usize> {
        if let Some(shard_id) = self.host_routes.get(host) {
            if let Some(position) = self.shard_position(*shard_id) {
                return Some(position);
            }
        }

        None
    }

    fn shard_position(&self, shard_id: u32) -> Option<usize> {
        self.shards
            .iter()
            .position(|shard| shard.shard_id == shard_id)
    }

    fn ensure_shard(&mut self, shard_id: u32) -> usize {
        if let Some(position) = self.shard_position(shard_id) {
            return position;
        }

        let start = self.shards.len() as u32;
        for id in start..=shard_id {
            // Dynamically added shards do not change the existing global budget.
            self.shards.push(PartitionShard::new(id, 0));
        }

        self.shards.len() - 1
    }

    fn register_host_route(&mut self, host: &str, shard_id: u32) {
        match self.host_routes.get(host) {
            Some(existing_shard_id) if *existing_shard_id != shard_id => {
                panic!(
                    "host {} was indexed into multiple shards: {} and {}",
                    host, existing_shard_id, shard_id
                );
            }
            Some(_) => {}
            None => {
                self.host_routes.insert(host.to_string(), shard_id);
            }
        }
    }

    fn rebuild_host_routes(&mut self) {
        self.host_routes.clear();
        let mut routes = Vec::new();

        for shard in &self.shards {
            for entry in &shard.buffer.entries {
                routes.push((entry.host_id.clone(), shard.shard_id));
            }
        }

        for (host, shard_id) in routes {
            self.register_host_route(&host, shard_id);
        }
    }
}

fn sort_entries(entries: &mut [LogEntry]) {
    entries.sort_unstable_by_key(|entry| entry.timestamp);
}

fn sort_shared_entries(entries: &mut [SharedLogEntry]) {
    entries.sort_unstable_by_key(|entry| entry.timestamp);
}

fn shard_capacity_for_position(
    total_capacity: usize,
    shard_count: usize,
    position: usize,
) -> usize {
    let base = total_capacity / shard_count;
    let remainder = total_capacity % shard_count;
    base + usize::from(position < remainder)
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

    fn assert_nondecreasing_timestamps(entries: &[LogEntry]) {
        assert!(
            entries
                .windows(2)
                .all(|pair| pair[0].timestamp <= pair[1].timestamp),
            "query results must be ordered by nondecreasing timestamp"
        );
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
        assert_nondecreasing_timestamps(&result.entries);
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
        assert_nondecreasing_timestamps(&result.entries);
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
    fn shard_local_flush_only_indexes_that_shard() {
        let mut store = ChironStore::with_shards(12, 2);
        store.ingest_partition(0, make_entry(10, "auth", "h0"));
        store.ingest_partition(1, make_entry(20, "auth", "h1"));

        store.flush_indexer_shard(1);

        let h0 = store.query_by_host("h0", 0, 100);
        assert!(h0.entries.is_empty());

        let h1 = store.query_by_host("h1", 0, 100);
        assert_eq!(h1.entries.len(), 1);
        assert_eq!(h1.entries[0].timestamp, 20);
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
        assert_nondecreasing_timestamps(&result.entries);
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
    fn flush_indexer_makes_entries_queryable() {
        let mut store = ChironStore::new(1000);
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
        let mut store = ChironStore::with_shards(12, 4);
        store.ingest_partition(0, make_entry(10, "auth", "h0"));
        store.ingest_partition(1, make_entry(20, "auth", "h1"));
        store.ingest_partition(2, make_entry(30, "payments", "h2"));
        store.ingest_partition(1, make_entry(40, "auth", "h1"));
        store.flush_indexer();

        let host_result = store.query_by_host("h1", 0, 100);
        assert_eq!(host_result.entries.len(), 2);
        assert!(
            host_result
                .entries
                .iter()
                .all(|entry| entry.host_id == "h1")
        );

        let service_result = store.query_by_service("auth", 0, 100);
        assert_eq!(service_result.entries.len(), 3);
        assert_nondecreasing_timestamps(&service_result.entries);

        let combined = store.query_by_service_and_host("auth", "h1", 0, 100);
        assert_eq!(combined.entries.len(), 2);
        assert_nondecreasing_timestamps(&combined.entries);
    }

    #[test]
    #[should_panic(expected = "was indexed into multiple shards")]
    fn indexing_panics_when_host_appears_in_multiple_shards() {
        let mut store = ChironStore::with_shards(12, 4);
        store.ingest_partition(0, make_entry(10, "auth", "h1"));
        store.ingest_partition(1, make_entry(20, "auth", "h1"));
    }

    #[test]
    fn sharded_eviction_prefers_the_writing_shard() {
        let mut store = ChironStore::with_shards(4, 2);
        store.ingest_partition(0, make_entry(30, "svc", "h0"));
        store.ingest_partition(1, make_entry(10, "svc", "h1"));
        store.ingest_partition(0, make_entry(40, "svc", "h0"));
        store.ingest_partition(1, make_entry(20, "svc", "h1"));
        assert_eq!(store.len(), 4);
        store.ingest_partition(0, make_entry(50, "svc", "h0"));
        assert_eq!(store.len(), 4);
        store.flush_indexer();

        let result = store.query_by_service("svc", 0, 100);
        assert_eq!(result.entries.len(), 4);
        let timestamps: Vec<_> = result.entries.iter().map(|entry| entry.timestamp).collect();
        assert_eq!(timestamps, vec![10, 20, 40, 50]);
        assert_nondecreasing_timestamps(&result.entries);
    }

    #[test]
    fn hot_shard_evicts_own_entries_under_pressure() {
        let mut store = ChironStore::with_shards(12, 4);
        // Fill all 12 slots into shard 0.
        for ts in 0..12 {
            store.ingest_partition(0, make_entry(ts, "svc", "hot-host"));
        }
        assert_eq!(store.len(), 12);

        // 13th entry forces eviction from shard 0 (the writing shard).
        store.ingest_partition(0, make_entry(12, "svc", "hot-host"));
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
        let mut store = ChironStore::with_shards(8, 2);
        store.ingest_partition(0, make_entry(10, "svc", "h1"));
        store.ingest_partition(1, make_entry(10, "svc", "h2"));
        store.ingest_partition(0, make_entry(11, "svc", "h3"));
        store.flush_indexer();

        let result = store.query_by_service("svc", 0, i64::MAX);
        assert_eq!(result.entries.len(), 3);
        assert_nondecreasing_timestamps(&result.entries);

        let mut same_timestamp_hosts: Vec<_> = result
            .entries
            .iter()
            .filter(|entry| entry.timestamp == 10)
            .map(|entry| entry.host_id.as_str())
            .collect();
        same_timestamp_hosts.sort_unstable();
        assert_eq!(same_timestamp_hosts, vec!["h1", "h2"]);
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
