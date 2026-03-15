# ChironVision Log Buffer

An in-memory, Kafka-backed log store for incident-style queries over recent observability data, plus a small Go benchmark used for side-by-side query-path comparisons.

## Assumptions

1. **Timestamps are roughly monotonic.** Logs arrive mostly in order, with only small jitter.
2. **Host-scoped queries are the primary query paths.** `ByHost` is the hottest path, `ByServiceAndHost` is the next most common narrowing query, and `ByService` is a broader fleet-wide query that can afford fanout.
3. **Noisy hosts should mostly pay their own retention cost.** When one shard gets much hotter than the rest, it should preferentially shed its own older data instead of pushing out quieter shards.
4. **Small indexing lag is acceptable.** Newly ingested records may become queryable a short time after they are accepted.

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

The store is shard-aware internally, but the implementation is still a hybrid rather than a fully independent per-shard system. Today:

- records are routed to partition-local shards
- indexes are partition-local
- queries can avoid unnecessary fanout
- eviction is shard-local in effect, but triggered against a store-level occupancy budget
- the pipeline still wraps the top-level `ChironStore` in one `Mutex`

So the data path is shard-aware, but the control path is still coordinated centrally in the current prototype.

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

The store also maintains a `host_routes` map while indexing so host-scoped queries can go straight to the single shard that owns that host.

The intended invariant is strict: a given `host_id` must never appear in multiple shards. If indexing observes the same host in different shards, the store treats that as a bug and fails fast.

The current pipeline runs a background indexer thread that periodically calls `flush_indexer()` across all shards. Query freshness therefore depends on indexer lag.

### Commit vs. Searchability

In the Kafka pipeline, a consumer appends records into the in-memory shard buffer and only then commits the Kafka offset. The commit therefore means "accepted into the in-memory store," not "already visible in the indexes."

That creates a small window where:

- a record has been committed in Kafka
- the record is present in the shard buffer
- but `ByService` / `ByHost` / `ByServiceAndHost` may not return it yet because the background indexer has not flushed that shard

On a clean run, the background indexer catches up before the pipeline exits. During live ingestion, though, queries are only as fresh as the current indexer lag.

## Read Path

### `ByHost(host, t1, t2)`

- use `host_routes` when available to query only the known shard for that host
- fall back to fanout if the host has not been indexed yet

### `ByServiceAndHost(service, host, t1, t2)`

- use `host_routes` to narrow to the relevant host shard first
- intersect the local service and host posting lists inside that shard

### `ByService(service, t1, t2)`

- fan out across shards
- query each shard's local service index
- merge and sort matching entries by timestamp

This means the current architecture is intentionally optimized for `ByHost` first, then `ByServiceAndHost`, while accepting bounded fanout for broader `ByService` queries.

All query APIs guarantee only nondecreasing timestamp order. Entries that share
the same timestamp may appear in any relative order.

## Capacity Model

The current store still admits writes against one store-level budget, but it carries shard-level capacity metadata too.

- `with_shards(total_capacity, shard_count)` divides the configured capacity into per-shard shares
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
├── main.rs              # Demo entrypoint
├── log_entry.rs         # LogEntry struct
├── inverted_index.rs    # Local service/host posting lists
├── kafka.rs             # Kafka producer/consumer wrappers
├── pipeline.rs          # Kafka pipeline and background indexer
├── snapshot.rs          # Snapshot encoding/decoding
└── chiron.rs            # Shard-aware ChironStore
go_three_maps/
├── go.mod               # Standalone Go module for the benchmark
└── main.go              # In-memory Go benchmark with matching query semantics
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

```bash
cargo test --release --test e2e kafka_1m_ingest_and_10k_queries -- --ignored --nocapture
```

Go benchmark:

```bash
cd go_three_maps
go run .
```

The Go benchmark now mirrors the streaming-style Rust load test: it builds rows on
the fly, ingests into a live store, issues concurrent queries during ingestion,
then runs one final exact verification pass after the stream completes.

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
