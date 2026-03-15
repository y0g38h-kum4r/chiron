pub mod chiron;
pub mod inverted_index;
pub mod kafka;
pub mod log_entry;
pub mod pipeline;
pub mod snapshot;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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
