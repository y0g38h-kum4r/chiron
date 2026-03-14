use std::collections::HashMap;

/// Inverted index mapping dimension values (service name, host id) to
/// global offsets in the primary ring buffer.
///
/// Each dimension key maps to a sorted list of offsets.
/// Queries intersect offset lists across dimensions and filter by timestamp range.
pub struct InvertedIndex {
    index: HashMap<String, Vec<u64>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Record that `global_offset` contains an entry with the given dimension `key`.
    pub fn insert(&mut self, key: &str, global_offset: u64) {
        self.index
            .entry(key.to_string())
            .or_default()
            .push(global_offset);
    }

    /// Get all offsets for a given dimension key.
    pub fn get(&self, key: &str) -> Option<&[u64]> {
        self.index.get(key).map(|v| v.as_slice())
    }

    /// Purge all offsets below `min_offset` (they've been evicted from the ring buffer).
    pub fn purge_below(&mut self, min_offset: u64) {
        for offsets in self.index.values_mut() {
            // Offsets are sorted, find first >= min_offset via binary search.
            let pos = offsets.partition_point(|&o| o < min_offset);
            if pos > 0 {
                offsets.drain(..pos);
            }
        }
        // Remove empty keys.
        self.index.retain(|_, v| !v.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut idx = InvertedIndex::new();
        idx.insert("svcA", 0);
        idx.insert("svcA", 3);
        idx.insert("svcB", 1);
        assert_eq!(idx.get("svcA"), Some([0u64, 3].as_slice()));
        assert_eq!(idx.get("svcB"), Some([1u64].as_slice()));
        assert_eq!(idx.get("svcC"), None);
    }

    #[test]
    fn purge_below() {
        let mut idx = InvertedIndex::new();
        for o in 0..10 {
            idx.insert("svc", o);
        }
        idx.purge_below(5);
        assert_eq!(idx.get("svc"), Some([5u64, 6, 7, 8, 9].as_slice()));
    }
}
