pub mod chiron;
pub mod inverted_index;
pub mod kafka;
pub mod log_entry;
pub mod pipeline;
pub mod snapshot;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::{env, fmt::Display};

use crate::chiron::ChironStore;
use crate::log_entry::LogEntry;

/// Deterministic host→partition routing shared by producers and the store.
///
/// Both the Kafka producer and `ChironStore` must agree on which partition
/// owns a given host. This single function is the source of truth.
pub fn partition_for_host(host_id: &str, num_partitions: usize) -> usize {
    if num_partitions <= 1 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    host_id.hash(&mut hasher);
    (hasher.finish() as usize) % num_partitions
}

pub fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

pub fn env_u32(name: &str, default: u32) -> u32 {
    env_parsed(name).unwrap_or(default)
}

pub fn env_usize(name: &str, default: usize) -> usize {
    env_parsed(name).unwrap_or(default)
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    env_parsed(name).unwrap_or(default)
}

fn env_parsed<T>(name: &str) -> Option<T>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    match env::var(name) {
        Ok(raw) => match raw.parse::<T>() {
            Ok(value) => Some(value),
            Err(err) => {
                eprintln!(
                    "invalid value for {}: {:?} ({}) - falling back to default",
                    name, raw, err
                );
                None
            }
        },
        Err(_) => None,
    }
}

/// Sort a batch of entries by partition, preserving the original order within
/// each partition, then ingest each partition's entries in a single batch call.
pub fn ingest_batch_by_partition(store: &ChironStore, batch: &mut Vec<(LogEntry, u32)>) {
    if batch.is_empty() {
        return;
    }

    batch.sort_by_key(|(_, partition)| *partition);

    let mut current_partition = batch[0].1;
    let mut partition_entries = Vec::new();

    for (entry, partition) in batch.drain(..) {
        if partition != current_partition && !partition_entries.is_empty() {
            store.ingest_partition_batch(current_partition, std::mem::take(&mut partition_entries));
            current_partition = partition;
        }
        partition_entries.push(entry);
    }

    if !partition_entries.is_empty() {
        store.ingest_partition_batch(current_partition, partition_entries);
    }
}
