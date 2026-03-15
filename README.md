# ChironVision Log Buffer

An in-memory, Kafka-backed log store for incident-style queries over recent observability data

## Assumptions

1. **Timestamps are roughly monotonic.** Logs arrive mostly in order, with only small jitter.
2. **Host-scoped queries are the primary query paths.** `ByHost` is the hottest path, `ByServiceAndHost` is the next most common narrowing query, and `ByService` is a broader fleet-wide query that can afford fanout.
3. **Abnormally noisy hosts are expected to be handled upstream.** If a host starts emitting an unusual amount of data, we assume an external alerting or signal-management service will detect and manage that condition. Locally, retention still prefers to shed that shard's own older data instead of pushing out quieter shards.
4. **Freshness on the live path matters.** Newly ingested records are indexed inline so accepted records are queryable immediately instead of waiting on a background flush tick.

## Architecture: Partition-Local Shards

The store is organized as **partition-local shards**. Each shard owns:

- its own append buffer
- its own service index
- its own host index
- its own indexer position
- its own persisted capacity share for snapshot/restore

In the Kafka pipeline, the store is created with one shard per Kafka partition. Consumed records are routed into the shard for the **actual Kafka partition returned by the broker**.

This keeps the storage model aligned with Kafka's partitioning and makes the common host-routed queries cheap when `host_id` is the partitioning key.

### Current Implementation Caveat

The store is shard-aware end-to-end now, but a few control-plane pieces are still coordinated centrally. Today:

- records are routed to partition-local shards
- shard buffers and indexes sit behind per-shard locks
- host-scoped queries use deterministic `partition_for_host(...)` routing instead of a mutable routing table
- eviction is shard-local in effect, but still triggered against a store-level occupancy budget
- snapshot orchestration and some capacity bookkeeping remain store-scoped

So the hot data path is shard-local, while a small amount of lifecycle and capacity coordination still lives at the store level.

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

The intended invariant is strict: a given `host_id` must never appear in multiple shards. If ingest or recovery observes the same host in different shards, the store treats that as a bug and fails fast.

Host-scoped queries do not need a mutable routing table anymore. They compute the owning shard directly with `partition_for_host(host_id, shard_count)`, so routing stays aligned with the producer-side Kafka partitioning rule.

Each shard indexes accepted records inline while already holding its write lock, so the common ingest path does not accumulate index lag. The store still exposes `flush_indexer()` and `flush_indexer_shard(shard_id)`, but they are now mostly compatibility and recovery helpers.

`e2e_bench` still carries a background indexer thread for benchmark instrumentation. The main pipeline path no longer depends on that loop.

### Commit vs. Searchability

In the Kafka pipeline, a consumer appends records into the in-memory shard buffer, indexes them inline, and only then commits the Kafka offset. The commit therefore means "accepted into the in-memory store and visible to queries on the live path."

`flush_indexer()` still matters for restore flows because snapshots persist buffered entries, not materialized indexes. On the live ingest path, though, query freshness no longer depends on a background flush cadence.

## Read Path

### `ByHost(host, t1, t2)`

- compute the owning shard directly with `partition_for_host(host, shard_count)`
- query only that shard's local host index

### `ByServiceAndHost(service, host, t1, t2)`

- compute the owning shard directly with `partition_for_host(host, shard_count)`
- intersect the local service and host posting lists inside that shard

### `ByService(service, t1, t2)`

- fan out across shards
- query each shard's local service index
- merge and sort matching entries by timestamp

This means the current architecture is intentionally optimized for `ByHost` first, then `ByServiceAndHost`, while accepting bounded fanout for broader `ByService` queries.

All query APIs guarantee only nondecreasing timestamp order. Entries that share
the same timestamp may appear in any relative order.

### Query Result Shape

`QueryResult` returns `Vec<SharedLogEntry>` on purpose. That keeps the hot path zero-copy and avoids re-allocating `service_name`, `host_id`, and `message` strings for every match.

That result shape is best treated as an internal-performance API. If you want a stable external contract, convert to owned `LogEntry` values or protobuf/gRPC response messages at the service boundary instead of freezing `SharedLogEntry` into the public surface area.

## Capacity Model

The current store still admits writes against one store-level budget, but it carries shard-level capacity metadata too.

- `with_shards(total_capacity, shard_count)` requires uniform per-shard capacity, so `total_capacity` must be divisible by `shard_count`
- those shard capacities are persisted in snapshots and reconstructed on restore
- runtime admission is still checked against the store-wide occupancy, not against a hard per-shard quota
- that means hot shards can temporarily consume more of the live dataset until eviction runs

This is why the code still has both shard-local state and a top-level notion of total occupancy: the layout is shard-based, but admission control has not been fully pushed down to independent shard budgets.

## Eviction

Eviction remains **oldest-first within a shard**, but it is now biased toward the shard creating the pressure.

When a write arrives and the store is already full, the target shard evicts its own oldest entry first. That means a noisy host mostly overwrites its own older history instead of displacing quieter hosts.

If the target shard cannot shed data, the store falls back to trimming the fullest shard. Background maintenance follows the same bias: it repeatedly trims the fullest shard first instead of comparing timestamps globally across all shards.

This retention policy is intentionally different from "keep the newest cluster-wide data at all costs." It favors host isolation over perfect global recency.

## Durability: Kafka Replay + Sharded Snapshots

Kafka remains the durable source of truth. The in-memory store is a fast query cache for recent data.

### Snapshot Contents

`ChironStore::save_snapshot` writes a single **sharded snapshot file** containing:

- snapshot magic `CHIRON05`
- shard count
- for each shard:
  - shard id
  - shard capacity
  - next local offset
  - live entries in oldest-to-newest order
- Kafka offsets supplied by the caller

The loader still accepts older `CHIRON04` snapshots and reconstructs shard capacities from the legacy total-capacity header.

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
├── main.rs              # Env-driven pipeline entrypoint
├── log_entry.rs         # Owned API LogEntry + shared-string store entry type
├── inverted_index.rs    # Local service/host posting lists
├── kafka.rs             # Kafka producer/consumer wrappers
├── pipeline.rs          # Env-driven Kafka consume/ingest startup
├── snapshot.rs          # Snapshot encoding/decoding
└── chiron.rs            # Shard-aware ChironStore
```

## Usage

```bash
cargo run
cargo test
```

Kafka-backed end-to-end benchmark:

```bash
cargo run --release --bin e2e_bench
```

Kafka integration tests are ignored by default:

```bash
cargo test --test e2e -- --ignored --nocapture
```

Use the remaining ignored Kafka E2E tests for:

- Kafka producer/consumer wiring
- partition routing into the store
- snapshot/restore round-trips
- long-run correctness across the real Kafka integration path

Those tests are useful for integration coverage.

If no broker is reachable at `localhost:9092`, the E2E tests skip with a message instead of failing noisily.

## Configuration

The local pipeline entrypoint reads these environment variables:

- `CHIRON_BROKERS`: Kafka bootstrap servers. Default: `localhost:9092`
- `CHIRON_TOPIC`: Kafka topic to consume. Default: `chiron-logs`
- `CHIRON_PARTITIONS`: Kafka partition count and shard count. Default: `4`
- `CHIRON_CAPACITY`: total in-memory capacity distributed uniformly across shards. Must be divisible by `CHIRON_PARTITIONS`. Default: `100000`
- `CHIRON_CONSUMER_GROUP`: consumer group id. Default: `chiron-pipeline`
- `CHIRON_CONSUMER_THREADS`: number of consumer threads to start. Default: `CHIRON_PARTITIONS`
- `CHIRON_CONSUMER_BATCH_SIZE`: max messages to ingest per batch. Default: `256`
- `CHIRON_CONSUMER_POLL_MS`: poll timeout in milliseconds. Default: `200`
- `CHIRON_CONSUMER_IDLE_MS`: idle timeout before the pipeline exits. Default: `5000`

Example:

```bash
export CHIRON_BROKERS=localhost:9092
export CHIRON_TOPIC=chiron-logs
export CHIRON_PARTITIONS=8
export CHIRON_CAPACITY=250000
export CHIRON_CONSUMER_GROUP=chiron-pipeline
export CHIRON_CONSUMER_THREADS=8
docker compose up -d
cargo run
```

`docker-compose.yml` also uses `CHIRON_NUM_PARTITIONS` for Kafka's `KAFKA_NUM_PARTITIONS`, and the pipeline still accepts `CHIRON_NUM_PARTITIONS` / `CHIRON_RING_BUFFER_CAPACITY` as compatibility fallbacks if you already have those env vars set.
