use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::log_entry::LogEntry;

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

const SHARDED_SNAPSHOT_MAGIC: &[u8; 8] = b"CHIRON04";

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
    String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_entry(cursor: &mut &[u8]) -> io::Result<LogEntry> {
    let timestamp = read_u64(cursor)? as i64;
    let service_name = read_string(cursor)?;
    let host_id = read_string(cursor)?;
    let message = read_string(cursor)?;
    Ok(LogEntry {
        timestamp,
        service_name,
        host_id,
        message,
    })
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
        }
    }

    #[test]
    fn sharded_snapshot_roundtrip() {
        let dir = std::env::temp_dir().join("chiron_test_sharded_snapshot");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("sharded.snap");

        let shards = vec![
            SnapshotShard {
                shard_id: 0,
                next_offset: 3,
                entries: vec![
                    make_entry(100, "auth", "h0"),
                    make_entry(200, "auth", "h0"),
                    make_entry(300, "payment", "h0"),
                ],
            },
            SnapshotShard {
                shard_id: 1,
                next_offset: 2,
                entries: vec![
                    make_entry(150, "auth", "h1"),
                    make_entry(250, "payment", "h1"),
                ],
            },
        ];

        let mut offsets = KafkaOffsets::new();
        offsets.set("logs", 0, 45230);
        offsets.set("logs", 1, 38901);

        Snapshot::save_sharded(&path, 500, &shards, &offsets).unwrap();

        let (capacity, restored, offsets_loaded) = Snapshot::load_sharded(&path).unwrap();

        assert_eq!(capacity, 500);
        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0].shard_id, 0);
        assert_eq!(restored[0].entries.len(), 3);
        assert_eq!(restored[0].next_offset, 3);
        assert_eq!(restored[0].entries[0].timestamp, 100);
        assert_eq!(restored[1].shard_id, 1);
        assert_eq!(restored[1].entries.len(), 2);
        assert_eq!(restored[1].next_offset, 2);
        assert_eq!(restored[1].entries[0].service_name, "auth");

        assert_eq!(offsets_loaded.get("logs", 0), Some(45230));
        assert_eq!(offsets_loaded.get("logs", 1), Some(38901));
        assert_eq!(offsets_loaded.get("logs", 2), None);

        fs::remove_dir_all(&dir).ok();
    }
}
