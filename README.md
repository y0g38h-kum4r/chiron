# ChironVision Log Buffer

An in-memory log storage system for incident detection in streaming observability systems.

## Assumptions

1. **Timestamps are monotonically non-decreasing.** Logs arrive roughly in order (small jitter acceptable). This is standard for streaming observability pipelines — you won't see a log from `t=500` arrive after hours of ingesting at `t=10000`.

2. **ByService and ByHost queries have equal importance.** During incident investigation, engineers use both interchangeably as primary lenses. ByServiceAndHost is a lower-frequency confirmation query.

3. **Oldest data is least relevant.** With monotonically increasing timestamps, the oldest entries in the buffer are always the eviction candidates. No per-window scoring needed — head eviction is the correct policy.

4. **Indexing lag (microseconds to low milliseconds) is acceptable.** For an observability system querying minute-level windows, the delay between write and queryability is invisible.

## Architecture: Shared Log + Async Indexing

The key insight: **separate the write path from the read path.** Instead of sharding (which forces a tradeoff where one query type always requires fan-out), use a single shared append-only log with independently built indexes.

### Why not shard?

| Shard by | Write throughput | ByService | ByHost | ByServiceAndHost |
|----------|-----------------|-----------|--------|------------------|
| time-range | 1 core (all writes hit "now") | 1 shard | 1 shard | 1 shard |
| hash(service) | N cores | 1 shard | N shards (fan-out) | 1 shard |
| hash(host) | N cores | N shards (fan-out) | 1 shard | 1 shard |
| hash(svc\|host) | N cores | N shards (fan-out) | N shards (fan-out) | 1 shard |

Every sharding strategy makes at least one of the two primary query types expensive. The shared log approach avoids this tradeoff entirely.

### Write Path

```
Writer 0 ──┐
Writer 1 ──┼──► Shared Ring Buffer (atomic fetch_add on write_pos)
Writer 2 ──┤    [log0][log1][log2][log3][log4][log5]...
Writer 3 ──┘
            Zero contention between writers.
            Write throughput = N cores × memory bandwidth.
```

Writers only touch the ring buffer — no index updates, no routing decisions. In production, `write_pos` is an `AtomicU64` and each writer reserves a unique slot via `fetch_add`.

### Index Path (Async)

```
Ring Buffer: [log0][log1][log2][log3][log4][log5][log6]...
                                              ▲         ▲
                                        indexer_pos   write_pos

Indexer thread reads from indexer_pos → write_pos:
  → Inserts into service_index (service_name → [offsets])
  → Inserts into host_index    (host_id → [offsets])
```

The gap between `indexer_pos` and `write_pos` is the indexing lag. Entries in this gap are written but not yet queryable.

### Read Path

All three query types are single-index lookups — no fan-out:

```
ByService("auth", t1, t2)          → service_index["auth"]     → offsets → filter by time
ByHost("h1", t1, t2)               → host_index["h1"]          → offsets → filter by time
ByServiceAndHost("auth","h1",t1,t2) → intersect(service, host) → offsets → filter by time
```

### Eviction

With monotonically increasing timestamps, eviction is simply **head eviction** — drop the oldest entries first. No scoring, no decay functions, no ordering structures needed. The ring buffer naturally overwrites from the head when full.

## Durability: Hourly Snapshots + Kafka Replay

The in-memory ring buffer is volatile. Durability is achieved via periodic checkpointing combined with Kafka replay:

```
Normal operation:

  Kafka ──► Consumer threads ──► RingBuffer ──► Indexer ──► Indexes
                 │
                 tracks consumer offsets per topic/partition

Every hour:

  1. Pause consumers
  2. Snapshot = ring buffer entries + Kafka consumer offsets
  3. Write to disk (atomic: write to .tmp, then rename)
  4. Resume consumers
```

### Recovery after crash

```
1. Load latest snapshot from disk
   → ring buffer restored to hour-ago state
   → Kafka consumer offsets restored

2. Resume Kafka consumers from recorded offsets
   → replays the last ≤1 hour of logs
   → ring buffer catches up to present

3. Indexes are rebuilt from the ring buffer automatically
   → flush_indexer() over the full buffer
   → queries work immediately
```

### Key invariants

- **Zero data loss**: Kafka retains the full stream. The snapshot saves replay time, not data.
- **Atomic writes**: snapshot is written to a `.tmp` file first, then atomically renamed. A crash mid-write leaves the previous good snapshot intact.
- **Indexes are not snapshotted**: they are rebuilt from the ring buffer on restore. Re-indexing 1M entries takes milliseconds.
- **Kafka retention must exceed snapshot interval**: if you snapshot every hour, Kafka must retain at least 2 hours (safety margin).

### Snapshot format

```
┌─────────────────────────────────┐
│ magic "CHIRON01"        8 bytes │
│ ring buffer capacity    8 bytes │
│ global_offset           8 bytes │
│ entry count             8 bytes │
│ entries[]           variable    │
│ kafka topic count       4 bytes │
│ kafka offsets[]     variable    │
└─────────────────────────────────┘
```

## File Structure

```
src/
├── lib.rs               # Module declarations
├── main.rs              # Demo (ingest, query, snapshot/restore)
├── log_entry.rs         # LogEntry struct
├── ring_buffer.rs       # Shared append-only circular buffer
├── inverted_index.rs    # Per-dimension offset indexes (service, host)
├── snapshot.rs          # Binary snapshot serialization + KafkaOffsets
└── chiron.rs            # ChironStore: shared log + indexer + eviction + snapshots
```

## Usage

```bash
cargo run     # Run demo
cargo test    # Run all tests
```

### Configuration

The local demo reads these environment variables so you do not have to hardcode sizing in Rust:

- `CHIRON_NUM_PARTITIONS`: Kafka topic partition count and the number of consumer threads to spawn. Default: `4`
- `CHIRON_RING_BUFFER_CAPACITY`: in-memory ring buffer capacity. Default: `100000`

Example:

```bash
export CHIRON_NUM_PARTITIONS=8
export CHIRON_RING_BUFFER_CAPACITY=250000
docker compose up -d
cargo run
```

`docker-compose.yml` also uses `CHIRON_NUM_PARTITIONS` for Kafka's `KAFKA_NUM_PARTITIONS`, so setting the env var keeps the broker default and the app config aligned.
