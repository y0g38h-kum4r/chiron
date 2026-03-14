use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::log_entry::LogEntry;
use crate::ring_buffer::RingBuffer;

/// Kafka consumer offsets at the time of snapshot.
/// Maps (topic, partition) → offset.
#[derive(Clone, Debug, PartialEq)]
pub struct KafkaOffsets {
    /// topic → (partition → offset)
    pub offsets: HashMap<String, HashMap<u32, u64>>,
}

impl KafkaOffsets {
    pub fn new() -> Self {
        Self {
            offsets: HashMap::new(),
        }
    }

    pub fn set(&mut self, topic: &str, partition: u32, offset: u64) {
        self.offsets
            .entry(topic.to_string())
            .or_default()
            .insert(partition, offset);
    }

    pub fn get(&self, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.get(topic)?.get(&partition).copied()
    }

    /// Iterate over all (topic, partition→offset) entries.
    pub fn inner(&self) -> &HashMap<String, HashMap<u32, u64>> {
        &self.offsets
    }
}

// --- Binary serialization (no external deps) ---

const SNAPSHOT_MAGIC: &[u8; 8] = b"CHIRON01";
const SHARDED_SNAPSHOT_MAGIC: &[u8; 8] = b"CHIRON03";

pub struct SnapshotShard {
    pub shard_id: u32,
    pub next_offset: u64,
    pub entries: Vec<LogEntry>,
}

pub struct RestoredShard {
    pub shard_id: u32,
    pub next_offset: u64,
    pub entries: Vec<LogEntry>,
}

/// Snapshot: ring buffer state + kafka offsets.
/// Written atomically (write to tmp, then rename).
pub struct Snapshot;

impl Snapshot {
    /// Save ring buffer + kafka offsets to disk.
    /// Uses atomic write: write to .tmp, then rename.
    pub fn save(
        path: &Path,
        ring_buffer: &RingBuffer,
        kafka_offsets: &KafkaOffsets,
    ) -> io::Result<()> {
        let mut buf: Vec<u8> = Vec::new();

        // Header
        buf.extend_from_slice(SNAPSHOT_MAGIC);

        // Ring buffer metadata
        write_ring_buffer(&mut buf, ring_buffer);
        write_kafka_offsets(&mut buf, kafka_offsets);

        durable_replace(path, &buf)
    }

    /// Load ring buffer + kafka offsets from a snapshot file.
    /// Returns (RingBuffer, KafkaOffsets).
    /// The caller should rebuild indexes from the ring buffer via flush_indexer().
    pub fn load(path: &Path) -> io::Result<(RingBuffer, KafkaOffsets)> {
        let data = fs::read(path)?;
        let mut cursor = &data[..];

        // Header
        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic)?;
        if &magic != SNAPSHOT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid snapshot magic",
            ));
        }

        // Ring buffer metadata
        let rb = read_ring_buffer(&mut cursor)?;
        let kafka_offsets = read_kafka_offsets(&mut cursor)?;

        Ok((rb, kafka_offsets))
    }

    pub fn save_sharded(
        path: &Path,
        total_capacity: usize,
        shards: &[SnapshotShard],
        kafka_offsets: &KafkaOffsets,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(SHARDED_SNAPSHOT_MAGIC);
        write_u64(&mut buf, total_capacity as u64);
        write_u32(&mut buf, shards.len() as u32);
        for shard in shards {
            write_u32(&mut buf, shard.shard_id);
            write_u64(&mut buf, shard.next_offset);
            write_u64(&mut buf, shard.entries.len() as u64);
            for entry in &shard.entries {
                write_entry(&mut buf, entry);
            }
        }
        write_kafka_offsets(&mut buf, kafka_offsets);

        durable_replace(path, &buf)
    }

    pub fn load_sharded(path: &Path) -> io::Result<(usize, Vec<RestoredShard>, KafkaOffsets)> {
        let data = fs::read(path)?;
        let mut cursor = &data[..];

        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic)?;
        if &magic != SHARDED_SNAPSHOT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid sharded snapshot magic",
            ));
        }

        let total_capacity = read_u64(&mut cursor)? as usize;
        let shard_count = read_u32(&mut cursor)? as usize;
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let shard_id = read_u32(&mut cursor)?;
            let next_offset = read_u64(&mut cursor)?;
            let entry_count = read_u64(&mut cursor)? as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                entries.push(read_entry(&mut cursor)?);
            }
            shards.push(RestoredShard {
                shard_id,
                next_offset,
                entries,
            });
        }

        let kafka_offsets = read_kafka_offsets(&mut cursor)?;
        Ok((total_capacity, shards, kafka_offsets))
    }
}

// --- Binary encoding helpers ---

fn durable_replace(path: &Path, buf: &[u8]) -> io::Result<()> {
    let tmp_path = path.with_extension("tmp");

    // Write the temp file durably before rename so power loss does not
    // leave us with a renamed but not-yet-persisted snapshot payload.
    let mut tmp_file = File::create(&tmp_path)?;
    tmp_file.write_all(buf)?;
    tmp_file.sync_all()?;
    drop(tmp_file);

    fs::rename(&tmp_path, path)?;
    sync_parent_dir(path)?;

    Ok(())
}

fn write_u64(buf: &mut Vec<u8>, val: u64) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn write_u32(buf: &mut Vec<u8>, val: u32) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

fn write_entry(buf: &mut Vec<u8>, entry: &LogEntry) {
    write_u64(buf, entry.timestamp as u64);
    write_string(buf, &entry.service_name);
    write_string(buf, &entry.host_id);
    write_string(buf, &entry.message);
    buf.push(entry.severity);
}

fn write_ring_buffer(buf: &mut Vec<u8>, ring_buffer: &RingBuffer) {
    write_u64(buf, ring_buffer.capacity() as u64);
    write_u64(buf, ring_buffer.next_offset());
    write_u64(buf, ring_buffer.len() as u64);

    let entries: Vec<_> = ring_buffer.iter().collect();
    write_u64(buf, entries.len() as u64);
    for (_offset, entry) in &entries {
        write_entry(buf, entry);
    }
}

fn write_kafka_offsets(buf: &mut Vec<u8>, kafka_offsets: &KafkaOffsets) {
    write_u32(buf, kafka_offsets.offsets.len() as u32);
    for (topic, partitions) in &kafka_offsets.offsets {
        write_string(buf, topic);
        write_u32(buf, partitions.len() as u32);
        for (&partition, &offset) in partitions {
            write_u32(buf, partition);
            write_u64(buf, offset);
        }
    }
}

fn read_u64(cursor: &mut &[u8]) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    cursor.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32(cursor: &mut &[u8]) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    cursor.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_string(cursor: &mut &[u8]) -> io::Result<String> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    String::from_utf8(bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_entry(cursor: &mut &[u8]) -> io::Result<LogEntry> {
    let timestamp = read_u64(cursor)? as i64;
    let service_name = read_string(cursor)?;
    let host_id = read_string(cursor)?;
    let message = read_string(cursor)?;
    let mut sev = [0u8; 1];
    cursor.read_exact(&mut sev)?;
    Ok(LogEntry {
        timestamp,
        service_name,
        host_id,
        message,
        severity: sev[0],
    })
}

fn read_ring_buffer(cursor: &mut &[u8]) -> io::Result<RingBuffer> {
    let capacity = read_u64(cursor)? as usize;
    let global_offset = read_u64(cursor)?;
    let len = read_u64(cursor)? as usize;

    let entry_count = read_u64(cursor)? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(read_entry(cursor)?);
    }

    RingBuffer::try_restore(capacity, global_offset, len, entries)
}

fn read_kafka_offsets(cursor: &mut &[u8]) -> io::Result<KafkaOffsets> {
    let topic_count = read_u32(cursor)? as usize;
    let mut kafka_offsets = KafkaOffsets::new();
    for _ in 0..topic_count {
        let topic = read_string(cursor)?;
        let partition_count = read_u32(cursor)? as usize;
        for _ in 0..partition_count {
            let partition = read_u32(cursor)?;
            let offset = read_u64(cursor)?;
            kafka_offsets.set(&topic, partition, offset);
        }
    }
    Ok(kafka_offsets)
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn make_entry(ts: i64, svc: &str, host: &str) -> LogEntry {
        LogEntry {
            timestamp: ts,
            service_name: svc.to_string(),
            host_id: host.to_string(),
            message: format!("msg@{}", ts),
            severity: 2,
        }
    }

    #[test]
    fn roundtrip_snapshot() {
        let dir = std::env::temp_dir().join("chiron_test_snapshot");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.snap");

        // Build ring buffer with some data.
        let mut rb = RingBuffer::new(100);
        rb.push(make_entry(100, "auth", "h1"));
        rb.push(make_entry(200, "payment", "h2"));
        rb.push(make_entry(300, "auth", "h3"));

        // Build kafka offsets.
        let mut offsets = KafkaOffsets::new();
        offsets.set("logs", 0, 45230);
        offsets.set("logs", 1, 38901);
        offsets.set("metrics", 0, 12000);

        // Save.
        Snapshot::save(&path, &rb, &offsets).unwrap();

        // Load.
        let (rb_loaded, offsets_loaded) = Snapshot::load(&path).unwrap();

        // Verify ring buffer.
        assert_eq!(rb_loaded.capacity(), 100);
        assert_eq!(rb_loaded.len(), 3);
        assert_eq!(rb_loaded.next_offset(), rb.next_offset());
        assert_eq!(rb_loaded.get(0).unwrap().timestamp, 100);
        assert_eq!(rb_loaded.get(1).unwrap().service_name, "payment");
        assert_eq!(rb_loaded.get(2).unwrap().host_id, "h3");

        // Verify kafka offsets.
        assert_eq!(offsets_loaded.get("logs", 0), Some(45230));
        assert_eq!(offsets_loaded.get("logs", 1), Some(38901));
        assert_eq!(offsets_loaded.get("metrics", 0), Some(12000));
        assert_eq!(offsets_loaded.get("metrics", 1), None);

        // Cleanup.
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn snapshot_with_wrapped_ring_buffer() {
        let dir = std::env::temp_dir().join("chiron_test_snapshot_wrap");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wrapped.snap");

        // Capacity 3, push 5 entries → wraps around, oldest 2 evicted.
        let mut rb = RingBuffer::new(3);
        rb.push(make_entry(1, "a", "h1")); // offset 0 — will be evicted
        rb.push(make_entry(2, "b", "h1")); // offset 1 — will be evicted
        rb.push(make_entry(3, "c", "h1")); // offset 2
        rb.push(make_entry(4, "d", "h1")); // offset 3
        rb.push(make_entry(5, "e", "h1")); // offset 4

        assert_eq!(rb.len(), 3);
        assert!(rb.get(0).is_none()); // evicted
        assert_eq!(rb.get(2).unwrap().timestamp, 3);

        let offsets = KafkaOffsets::new();
        Snapshot::save(&path, &rb, &offsets).unwrap();
        let (rb_loaded, _) = Snapshot::load(&path).unwrap();

        assert_eq!(rb_loaded.len(), 3);
        assert_eq!(rb_loaded.capacity(), 3);
        assert_eq!(rb_loaded.next_offset(), 5);
        assert!(rb_loaded.get(0).is_none());
        assert!(rb_loaded.get(1).is_none());
        assert_eq!(rb_loaded.get(2).unwrap().timestamp, 3);
        assert_eq!(rb_loaded.get(3).unwrap().timestamp, 4);
        assert_eq!(rb_loaded.get(4).unwrap().timestamp, 5);

        fs::remove_dir_all(&dir).ok();
    }
}
