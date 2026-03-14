# ChironVision Log Buffer

An in-memory, Kafka-backed log store for incident-style queries over recent observability data.

## Assumptions

1. **Timestamps are roughly monotonic.** Logs arrive mostly in order, with only small jitter.
2. **`ByService` and `ByHost` are both primary query paths.** `ByServiceAndHost` is a narrower confirmation query built on top of them.
3. **Oldest data is least relevant.** Eviction should prefer the oldest entries first.
4. **Small indexing lag is acceptable.** Newly ingested records may become queryable a short time after they are accepted.

## Architecture: Partition-Local Shards

The store is organized as **partition-local shards**. Each shard owns:

- its own append buffer
- its own service index
- its own host index
- its own indexer position

In the Kafka pipeline, the store is created with one shard per Kafka partition. Consumed records are routed into the shard for the **actual Kafka partition returned by the broker**.

This keeps the storage model aligned with Kafka's partitioning and makes host-routed queries cheap when `host_id` is the partitioning key.

### Current Implementation Caveat

The store is shard-aware internally, but the current pipeline still wraps the top-level `ChironStore` in one `Mutex`. So:

- records are routed to partition-local shards
- indexes are partition-local
- queries can avoid unnecessary fanout

but ingestion is still coordinated through one top-level lock in the current prototype.

## Write Path

```text
Producer threads
  -> Kafka topic (key = host_id)
  -> consumer threads in one group
  -> batch records
  -> route each record by Kafka partition
  -> append into that shard's local buffer
  -> commit Kafka offsets after local ingest succeeds
```

Two write APIs exist in the store:

- `ingest(entry)`: local host-hash routing, useful for tests and in-memory use
- `ingest_partition(partition_id, entry)`: explicit shard routing, used by the Kafka pipeline

## Index Path

Each shard maintains local inverted indexes:

- `service_name -> [local shard offsets]`
- `host_id -> [local shard offsets]`

The store also maintains a `host_routes` map while indexing so host-aware queries can usually go straight to the shard or shards that actually contain that host.

The current pipeline runs a background indexer thread that periodically calls `flush_indexer()` across all shards. Query freshness therefore depends on indexer lag.

### Commit vs. Searchability

In the Kafka pipeline, a consumer appends records into the in-memory shard buffer and only then commits the Kafka offset. The commit therefore means "accepted into the in-memory store," not "already visible in the indexes."

That creates a small window where:

- a record has been committed in Kafka
- the record is present in the shard buffer
- but `ByService` / `ByHost` / `ByServiceAndHost` may not return it yet because the background indexer has not flushed that shard

On a clean run, the background indexer catches up before the pipeline exits. During live ingestion, though, queries are only as fresh as the current indexer lag.

## Read Path

### `ByService(service, t1, t2)`

- fan out across shards
- query each shard's local service index
- merge and sort matching entries by timestamp

### `ByHost(host, t1, t2)`

- use `host_routes` when available to query only known shard(s) for that host
- fall back to fanout if the host has not been indexed yet

### `ByServiceAndHost(service, host, t1, t2)`

- use `host_routes` to narrow to the relevant shard(s)
- intersect the local service and host posting lists inside each shard
- merge the results

This means the current architecture accepts bounded fanout for `ByService`, while keeping `ByHost` and `ByServiceAndHost` narrow when routing information is available.

## Why Dynamic Buffers Per Shard?

Sharding and local buffers solve different problems:

- **Sharding** gives partition-local ownership and bounded fanout.
- **Dynamic shard-local buffers** keep append order and local offsets while letting hot shards borrow more of the global live-entry budget.

The current store does **not** slice capacity evenly across shards anymore. Instead:

- the store has one global live-entry budget
- hot shards can temporarily hold more entries than cold shards
- eviction still happens globally by oldest timestamp
- partition-local indexes stay intact

## Eviction

Eviction remains **oldest-first**, but it is now computed across shards.

The store compares the oldest live timestamp in each shard and repeatedly evicts from the globally oldest shard until the target capacity is reached. This preserves the "oldest data is least relevant" policy even though data is spread across multiple shard-local buffers.

## Durability: Kafka Replay + Sharded Snapshots

Kafka remains the durable source of truth. The in-memory store is a fast query cache for recent data.

### Snapshot Contents

`ChironStore::save_snapshot` writes a single **sharded snapshot file** containing:

- snapshot magic `CHIRON02`
- shard count
- for each shard:
  - shard id
  - next local offset
  - live entries in oldest-to-newest order
- Kafka offsets supplied by the caller

The snapshot write path is durable:

- write temp file
- `fsync` temp file
- rename into place
- `fsync` the parent directory

### Recovery

Recovery does the following:

1. Load every shard from the snapshot file.
2. Rebuild shard-local indexes by calling `flush_indexer()`.
3. Restore Kafka offsets from the snapshot.
4. Resume consumers from those offsets and replay forward.

### Important Snapshot Limitation

The current code does **not** coordinate a live snapshot barrier with the consumers. `save_snapshot` serializes the current in-memory shard state together with the Kafka offsets provided by the caller, but it does not pause ingestion or atomically capture "buffer state + offsets" from one globally synchronized instant.

That means:

- snapshots are fully durable on disk once written
- restore works correctly for the serialized state
- Kafka commits can get ahead of both index visibility and snapshot durability
- but a live snapshot is only as consistent as the caller's offset capture strategy

For offline snapshots taken after ingestion stops, this is fine. For live streaming snapshots, this is still an area for future improvement.

## File Structure

```text
src/
├── lib.rs               # Module declarations
├── main.rs              # Demo entrypoint
├── log_entry.rs         # LogEntry struct
├── ring_buffer.rs       # Fixed-capacity buffer used by low-level helpers/tests
├── inverted_index.rs    # Local service/host posting lists
├── kafka.rs             # Kafka producer/consumer wrappers
├── pipeline.rs          # Kafka pipeline and background indexer
├── snapshot.rs          # Snapshot encoding/decoding
└── chiron.rs            # Shard-aware ChironStore
```

## Usage

```bash
cargo run
cargo test
```

Kafka integration tests are ignored by default:

```bash
cargo test --test e2e -- --ignored --nocapture
```

If no broker is reachable at `localhost:9092`, the E2E tests skip with a message instead of failing noisily.

## Configuration

The local demo reads these environment variables:

- `CHIRON_NUM_PARTITIONS`: Kafka topic partition count and shard count used by the pipeline. Default: `4`
- `CHIRON_RING_BUFFER_CAPACITY`: total in-memory capacity distributed across shards. Default: `100000`
- `CHIRON_INDEX_FLUSH_INTERVAL_MS`: background indexer flush interval in milliseconds. Default: `50`

Example:

```bash
export CHIRON_NUM_PARTITIONS=8
export CHIRON_RING_BUFFER_CAPACITY=250000
export CHIRON_INDEX_FLUSH_INTERVAL_MS=25
docker compose up -d
cargo run
```

`docker-compose.yml` also uses `CHIRON_NUM_PARTITIONS` for Kafka's `KAFKA_NUM_PARTITIONS`, so keeping that env var aligned makes the demo behavior more predictable.
