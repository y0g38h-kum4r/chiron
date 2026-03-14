//! True end-to-end integration tests for ChironVision Log Buffer.
//!
//! Full path: LogEntry → Kafka produce → Kafka consume → ChironStore ingest
//!            → flush indexer → query → snapshot → restore
//!
//! Requires a running Kafka broker (docker compose up -d).
//! Run with:  cargo test --test e2e -- --ignored

use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chiron::chiron::ChironStore;
use chiron::kafka::{ChironConsumer, ChironProducer};
use chiron::log_entry::LogEntry;
use chiron::snapshot::KafkaOffsets;

const BROKERS: &str = "localhost:9092";

fn kafka_available() -> bool {
    match BROKERS.to_socket_addrs() {
        Ok(addrs) => addrs
            .into_iter()
            .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(250)).is_ok()),
        Err(_) => false,
    }
}

fn require_kafka() -> bool {
    if kafka_available() {
        true
    } else {
        eprintln!(
            "skipping Kafka E2E test because no broker is reachable at {}",
            BROKERS
        );
        false
    }
}

/// Generate a unique topic name per test to avoid cross-test interference.
fn unique_topic(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{}", prefix, nanos)
}

/// Helper: create a LogEntry with the given parameters.
fn entry(ts: i64, svc: &str, host: &str, severity: u8) -> LogEntry {
    LogEntry {
        timestamp: ts,
        service_name: svc.to_string(),
        host_id: host.to_string(),
        message: format!("{} on {} at t={}", svc, host, ts),
        severity,
    }
}

/// Produce entries to Kafka, then consume them all into a ChironStore.
/// Returns the store and the number of entries consumed.
fn produce_and_consume(
    topic: &str,
    group: &str,
    entries: &[LogEntry],
    store_capacity: usize,
) -> Result<(ChironStore, u64), Box<dyn std::error::Error>> {
    // --- Produce ---
    let producer = ChironProducer::new(BROKERS, topic)?;
    for e in entries {
        producer.send(e)?;
    }
    producer.flush(Duration::from_secs(10))?;
    // Drop the producer — all messages are flushed.
    drop(producer);

    // --- Consume ---
    let consumer = ChironConsumer::new(BROKERS, topic, group)?;
    let mut store = ChironStore::new(store_capacity);
    let mut consumed = 0u64;

    let poll_timeout = Duration::from_millis(500);
    let idle_deadline = Duration::from_secs(10);
    let mut last_msg = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Ok(Some((entry, _partition, _offset))) => {
                store.ingest(entry);
                consumed += 1;
                last_msg = Instant::now();
            }
            Ok(None) => {
                if consumed >= entries.len() as u64 || last_msg.elapsed() >= idle_deadline {
                    break;
                }
            }
            Err(err) => return Err(Box::new(err)),
        }
    }

    consumer.commit()?;
    Ok((store, consumed))
}

/// Produce entries to Kafka, then consume them into a sharded ChironStore using
/// the Kafka partition returned by the broker as the shard route.
fn produce_and_consume_sharded(
    topic: &str,
    group: &str,
    entries: &[LogEntry],
    store_capacity: usize,
    shard_count: usize,
) -> Result<(ChironStore, u64), Box<dyn std::error::Error>> {
    let producer = ChironProducer::new(BROKERS, topic)?;
    for e in entries {
        producer.send(e)?;
    }
    producer.flush(Duration::from_secs(10))?;
    drop(producer);

    let consumer = ChironConsumer::new(BROKERS, topic, group)?;
    let mut store = ChironStore::with_shards(store_capacity, shard_count);
    let mut consumed = 0u64;

    let poll_timeout = Duration::from_millis(500);
    let idle_deadline = Duration::from_secs(10);
    let mut last_msg = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Ok(Some((entry, partition, _offset))) => {
                store.ingest_partition(partition as u32, entry);
                consumed += 1;
                last_msg = Instant::now();
            }
            Ok(None) => {
                if consumed >= entries.len() as u64 || last_msg.elapsed() >= idle_deadline {
                    break;
                }
            }
            Err(err) => return Err(Box::new(err)),
        }
    }

    consumer.commit()?;
    Ok((store, consumed))
}

const LOAD_SERVICE_COUNT: usize = 100;
const LOAD_HOST_COUNT: usize = 100;
const LOAD_ROWS_PER_PAIR: usize = 100;
const LOAD_TOTAL_ROWS: usize = LOAD_SERVICE_COUNT * LOAD_HOST_COUNT * LOAD_ROWS_PER_PAIR;
const LOAD_QUERY_COUNT: usize = 10_000;
const LOAD_INITIAL_SHARDS: usize = 8;
const LOAD_PRODUCER_FLUSH_EVERY: usize = 25_000;

const LOAD_FULL_RANGE_START: i64 = 0;
const LOAD_FULL_RANGE_END: i64 = LOAD_ROWS_PER_PAIR as i64 - 1;
const LOAD_MID_RANGE_START: i64 = 2;
const LOAD_MID_RANGE_END: i64 = 6;
const LOAD_MID_RANGE_COUNT: usize = (LOAD_MID_RANGE_END - LOAD_MID_RANGE_START + 1) as usize;
const LOAD_NARROW_RANGE_START: i64 = 1;
const LOAD_NARROW_RANGE_END: i64 = 3;
const LOAD_NARROW_RANGE_COUNT: usize =
    (LOAD_NARROW_RANGE_END - LOAD_NARROW_RANGE_START + 1) as usize;

fn build_load_workload() -> (Vec<String>, Vec<String>, Vec<LogEntry>) {
    let services: Vec<String> = (0..LOAD_SERVICE_COUNT)
        .map(|idx| format!("svc-{idx:03}"))
        .collect();
    let hosts: Vec<String> = (0..LOAD_HOST_COUNT)
        .map(|idx| format!("host-{idx:03}"))
        .collect();

    let mut entries = Vec::with_capacity(LOAD_TOTAL_ROWS);
    for service in &services {
        for host in &hosts {
            for ts in 0..LOAD_ROWS_PER_PAIR {
                entries.push(LogEntry {
                    timestamp: ts as i64,
                    service_name: service.clone(),
                    host_id: host.clone(),
                    message: "m".to_string(),
                    severity: (ts % 8) as u8,
                });
            }
        }
    }

    (services, hosts, entries)
}

fn run_load_queries(store: &ChironStore, services: &[String], hosts: &[String]) -> usize {
    let service_full_hits = LOAD_HOST_COUNT * LOAD_ROWS_PER_PAIR;
    let service_mid_hits = LOAD_HOST_COUNT * LOAD_MID_RANGE_COUNT;
    let host_full_hits = LOAD_SERVICE_COUNT * LOAD_ROWS_PER_PAIR;
    let host_mid_hits = LOAD_SERVICE_COUNT * LOAD_MID_RANGE_COUNT;
    let pair_full_hits = LOAD_ROWS_PER_PAIR;
    let pair_narrow_hits = LOAD_NARROW_RANGE_COUNT;

    let mut total_hits = 0usize;
    let mut expected_total_hits = 0usize;

    for query_idx in 0..LOAD_QUERY_COUNT {
        match query_idx % 6 {
            0 => {
                let service = &services[query_idx % LOAD_SERVICE_COUNT];
                let result =
                    store.query_by_service(service, LOAD_FULL_RANGE_START, LOAD_FULL_RANGE_END);
                assert_eq!(result.entries.len(), service_full_hits);
                total_hits += result.entries.len();
                expected_total_hits += service_full_hits;
            }
            1 => {
                let service = &services[query_idx % LOAD_SERVICE_COUNT];
                let result =
                    store.query_by_service(service, LOAD_MID_RANGE_START, LOAD_MID_RANGE_END);
                assert_eq!(result.entries.len(), service_mid_hits);
                total_hits += result.entries.len();
                expected_total_hits += service_mid_hits;
            }
            2 => {
                let host = &hosts[query_idx % LOAD_HOST_COUNT];
                let result = store.query_by_host(host, LOAD_FULL_RANGE_START, LOAD_FULL_RANGE_END);
                assert_eq!(result.entries.len(), host_full_hits);
                total_hits += result.entries.len();
                expected_total_hits += host_full_hits;
            }
            3 => {
                let host = &hosts[query_idx % LOAD_HOST_COUNT];
                let result = store.query_by_host(host, LOAD_MID_RANGE_START, LOAD_MID_RANGE_END);
                assert_eq!(result.entries.len(), host_mid_hits);
                total_hits += result.entries.len();
                expected_total_hits += host_mid_hits;
            }
            4 => {
                let service = &services[query_idx % LOAD_SERVICE_COUNT];
                let host = &hosts[(query_idx * 7) % LOAD_HOST_COUNT];
                let result = store.query_by_service_and_host(
                    service,
                    host,
                    LOAD_FULL_RANGE_START,
                    LOAD_FULL_RANGE_END,
                );
                assert_eq!(result.entries.len(), pair_full_hits);
                total_hits += result.entries.len();
                expected_total_hits += pair_full_hits;
            }
            _ => {
                let service = &services[query_idx % LOAD_SERVICE_COUNT];
                let host = &hosts[(query_idx * 7) % LOAD_HOST_COUNT];
                let result = store.query_by_service_and_host(
                    service,
                    host,
                    LOAD_NARROW_RANGE_START,
                    LOAD_NARROW_RANGE_END,
                );
                assert_eq!(result.entries.len(), pair_narrow_hits);
                total_hits += result.entries.len();
                expected_total_hits += pair_narrow_hits;
            }
        }
    }

    assert_eq!(total_hits, expected_total_hits);
    total_hits
}

// ---------------------------------------------------------------------------
// Test 1: Full produce → consume → index → query lifecycle
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_full_lifecycle() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-lifecycle");
    let group = format!("{}-group", topic);

    let services = ["auth", "payment", "orders"];
    let hosts = ["h0", "h1", "h2"];
    let entries_per_pair = 10;

    // Build all entries.
    let mut all_entries = Vec::new();
    for svc in &services {
        for host in &hosts {
            for i in 0..entries_per_pair {
                all_entries.push(entry(i as i64 * 100, svc, host, (i % 5) as u8));
            }
        }
    }
    let total = all_entries.len(); // 3 × 3 × 10 = 90

    // Produce → Kafka → Consume → ChironStore.
    let (mut store, consumed) = produce_and_consume(&topic, &group, &all_entries, 10_000).unwrap();

    assert_eq!(
        consumed, total as u64,
        "all entries should be consumed from Kafka"
    );

    // Before flushing — queries return nothing.
    assert!(
        store
            .query_by_service("auth", 0, i64::MAX)
            .entries
            .is_empty()
    );

    // Flush indexer.
    store.flush_indexer();
    assert_eq!(store.indexer_lag(), 0);

    // Query by service: "auth" → 3 hosts × 10 = 30.
    let result = store.query_by_service("auth", 0, i64::MAX);
    assert_eq!(result.entries.len(), 30);
    assert!(result.entries.iter().all(|e| e.service_name == "auth"));

    // Query by host: "h1" → 3 services × 10 = 30.
    let result = store.query_by_host("h1", 0, i64::MAX);
    assert_eq!(result.entries.len(), 30);
    assert!(result.entries.iter().all(|e| e.host_id == "h1"));

    // Query by service + host: "payment" on "h2" = 10.
    let result = store.query_by_service_and_host("payment", "h2", 0, i64::MAX);
    assert_eq!(result.entries.len(), 10);
    assert!(
        result
            .entries
            .iter()
            .all(|e| e.service_name == "payment" && e.host_id == "h2")
    );
}

// ---------------------------------------------------------------------------
// Test 2: Time range filtering through Kafka
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_time_range_filtering() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-timerange");
    let group = format!("{}-group", topic);

    // auth: timestamps 100, 200, 300, 400, 500
    // payment: timestamps 150, 250, 350, 450, 550
    let mut entries = Vec::new();
    for i in 1..=5 {
        entries.push(entry(i * 100, "auth", "h0", 1));
        entries.push(entry(i * 100 + 50, "payment", "h0", 2));
    }

    let (mut store, consumed) = produce_and_consume(&topic, &group, &entries, 1000).unwrap();
    assert_eq!(consumed, 10);
    store.flush_indexer();

    // auth in [200, 400] → 200, 300, 400.
    let result = store.query_by_service("auth", 200, 400);
    assert_eq!(result.entries.len(), 3);
    assert!(
        result
            .entries
            .iter()
            .all(|e| e.timestamp >= 200 && e.timestamp <= 400)
    );

    // payment in [200, 400] → 250, 350.
    let result = store.query_by_service("payment", 200, 400);
    assert_eq!(result.entries.len(), 2);

    // host h0 in [300, 350] → auth:300, payment:350 = 2.
    let result = store.query_by_host("h0", 300, 350);
    assert_eq!(result.entries.len(), 2);

    // Outside data range → empty.
    let result = store.query_by_service("auth", 600, 1000);
    assert!(result.entries.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: Eviction after Kafka consumption
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_eviction_purges_indexes() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-eviction");
    let group = format!("{}-group", topic);

    // Produce 50 entries, but store capacity is only 20.
    let entries: Vec<LogEntry> = (0..50).map(|i| entry(i, "svc-a", "h0", 1)).collect();

    let (mut store, consumed) = produce_and_consume(&topic, &group, &entries, 20).unwrap();
    assert_eq!(consumed, 50);
    // Ring buffer wrapped — only latest 20 survive in the buffer.
    assert_eq!(store.len(), 20);

    // tick() flushes + evicts (20% free → keeps 16).
    store.tick();

    let remaining = store.len();
    assert!(
        remaining <= 16,
        "eviction should free ≥20%: got {}",
        remaining
    );

    // Queries only return surviving entries.
    let result = store.query_by_service("svc-a", 0, i64::MAX);
    assert_eq!(result.entries.len(), remaining);

    // Oldest surviving entry should be recent.
    let min_ts = result.entries.iter().map(|e| e.timestamp).min().unwrap();
    assert!(min_ts >= 30, "oldest surviving t={}, expected ≥30", min_ts);

    // Evicted range is empty.
    let result = store.query_by_service("svc-a", 0, 10);
    assert!(result.entries.is_empty());
}

// ---------------------------------------------------------------------------
// Test 4: Snapshot roundtrip with Kafka-sourced data
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_snapshot_roundtrip() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-snapshot");
    let group = format!("{}-group", topic);
    let dir = std::env::temp_dir().join("chiron_e2e_kafka_snapshot");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("e2e.snap");

    // Produce entries from multiple services × hosts.
    let services = ["auth", "payment"];
    let hosts = ["h0", "h1", "h2"];
    let mut entries = Vec::new();
    for svc in &services {
        for host in &hosts {
            for i in 0..20 {
                entries.push(entry(1000 + i, svc, host, (i % 3) as u8));
            }
        }
    }
    let total = entries.len(); // 2 × 3 × 20 = 120

    let (mut store, consumed) = produce_and_consume(&topic, &group, &entries, 500).unwrap();
    assert_eq!(consumed, total as u64);
    store.flush_indexer();

    // Record pre-snapshot query results.
    let pre_auth = store.query_by_service("auth", 0, i64::MAX).entries.len();
    let pre_h1 = store.query_by_host("h1", 0, i64::MAX).entries.len();
    let pre_auth_h0 = store
        .query_by_service_and_host("auth", "h0", 0, i64::MAX)
        .entries
        .len();
    let pre_len = store.len();

    // Build kafka offsets (simulating what the pipeline tracks).
    let mut offsets = KafkaOffsets::new();
    offsets.set(&topic, 0, 12345);
    offsets.set(&topic, 1, 67890);
    offsets.set(&topic, 2, 11111);
    offsets.set(&topic, 3, 22222);

    // Save snapshot.
    store.save_snapshot(&path, &offsets).unwrap();

    // Restore from snapshot.
    let (restored, restored_offsets) = ChironStore::from_snapshot(&path).unwrap();

    assert_eq!(restored.len(), pre_len);
    assert_eq!(restored.indexer_lag(), 0);

    // All three query types match pre-snapshot.
    assert_eq!(
        restored.query_by_service("auth", 0, i64::MAX).entries.len(),
        pre_auth
    );
    assert_eq!(
        restored.query_by_host("h1", 0, i64::MAX).entries.len(),
        pre_h1
    );
    assert_eq!(
        restored
            .query_by_service_and_host("auth", "h0", 0, i64::MAX)
            .entries
            .len(),
        pre_auth_h0
    );

    // Kafka offsets preserved.
    assert_eq!(restored_offsets.get(&topic, 0), Some(12345));
    assert_eq!(restored_offsets.get(&topic, 1), Some(67890));
    assert_eq!(restored_offsets.get(&topic, 2), Some(11111));
    assert_eq!(restored_offsets.get(&topic, 3), Some(22222));

    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Test 5: Snapshot after eviction with Kafka-sourced data
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_snapshot_after_eviction() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-snap-evict");
    let group = format!("{}-group", topic);
    let dir = std::env::temp_dir().join("chiron_e2e_kafka_snap_evict");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("evicted.snap");

    // Produce 60 entries, store capacity 30.
    let mut entries = Vec::new();
    for i in 0..30 {
        entries.push(entry(i, "alpha", "h0", 1));
    }
    for i in 30..60 {
        entries.push(entry(i, "beta", "h0", 2));
    }

    let (mut store, consumed) = produce_and_consume(&topic, &group, &entries, 30).unwrap();
    assert_eq!(consumed, 60);

    // tick() → flush + evict.
    store.tick();
    let surviving = store.len();
    assert!(surviving > 0 && surviving < 30);

    // Snapshot post-eviction state.
    let offsets = KafkaOffsets::new();
    store.save_snapshot(&path, &offsets).unwrap();

    // Restore and verify.
    let (restored, _) = ChironStore::from_snapshot(&path).unwrap();
    assert_eq!(restored.len(), surviving);

    let alpha = restored
        .query_by_service("alpha", 0, i64::MAX)
        .entries
        .len();
    let beta = restored.query_by_service("beta", 0, i64::MAX).entries.len();
    assert_eq!(alpha + beta, surviving);

    let h0 = restored.query_by_host("h0", 0, i64::MAX).entries.len();
    assert_eq!(h0, surviving);

    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Test 6: Concurrent producers → Kafka → consumer → store
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_concurrent_producers() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-concurrent");
    let group = format!("{}-group", topic);

    let num_threads = 4;
    let entries_per_thread = 50;
    let total_expected = (num_threads * entries_per_thread) as u64;

    // Spawn producer threads — each creates its own ChironProducer.
    let mut handles = Vec::new();
    for t in 0..num_threads {
        let topic_clone = topic.clone();
        handles.push(thread::spawn(move || {
            let producer = ChironProducer::new(BROKERS, &topic_clone).unwrap();
            let svc = format!("svc-{}", t % 2);
            let host = format!("host-{}", t % 2);
            for i in 0..entries_per_thread {
                let ts = (t as i64 * 10_000) + i as i64;
                producer
                    .send(&entry(ts, &svc, &host, (i % 5) as u8))
                    .unwrap();
            }
            producer.flush(Duration::from_secs(10)).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Consume all messages into ChironStore.
    let consumer = ChironConsumer::new(BROKERS, &topic, &group).unwrap();
    let mut store = ChironStore::new(100_000);
    let mut consumed = 0u64;

    let poll_timeout = Duration::from_millis(500);
    let idle_deadline = Duration::from_secs(10);
    let mut last_msg = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Ok(Some((e, _, _))) => {
                store.ingest(e);
                consumed += 1;
                last_msg = Instant::now();
            }
            Ok(None) => {
                if consumed >= total_expected || last_msg.elapsed() >= idle_deadline {
                    break;
                }
            }
            Err(err) => panic!("consumer poll failed: {err}"),
        }
    }
    consumer.commit().unwrap();

    assert_eq!(consumed, total_expected, "all entries consumed from Kafka");

    store.flush_indexer();

    // svc-0 from threads 0, 2 → 2 × 50 = 100.
    let result = store.query_by_service("svc-0", 0, i64::MAX);
    assert_eq!(result.entries.len(), 100);

    // host-0 from threads 0, 2 → 100.
    let result = store.query_by_host("host-0", 0, i64::MAX);
    assert_eq!(result.entries.len(), 100);

    // svc-1 on host-1 from threads 1, 3 → 100.
    let result = store.query_by_service_and_host("svc-1", "host-1", 0, i64::MAX);
    assert_eq!(result.entries.len(), 100);
}

// ---------------------------------------------------------------------------
// Test 7: Kafka message serialization fidelity
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_serialization_fidelity() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-serde");
    let group = format!("{}-group", topic);

    // Produce entries with edge-case data.
    let entries = vec![
        entry(0, "svc-with-dashes", "host.with.dots", 0),
        entry(i64::MAX, "UPPERCASE", "MiXeD", 255),
        LogEntry {
            timestamp: -1,
            service_name: "negative-ts".to_string(),
            host_id: "h0".to_string(),
            message: "special chars: !@#$%^&*(){}[]|\\;:'\",.<>?/`~".to_string(),
            severity: 128,
        },
        LogEntry {
            timestamp: 42,
            service_name: "unicode".to_string(),
            host_id: "日本語ホスト".to_string(),
            message: "emoji 🔥 and ñ and ü".to_string(),
            severity: 3,
        },
    ];

    let (mut store, consumed) = produce_and_consume(&topic, &group, &entries, 100).unwrap();
    assert_eq!(consumed, 4);
    store.flush_indexer();

    // Verify each entry survived Kafka JSON serialization/deserialization intact.
    let r = store.query_by_service("svc-with-dashes", 0, 0);
    assert_eq!(r.entries.len(), 1);
    assert_eq!(r.entries[0].host_id, "host.with.dots");

    let r = store.query_by_service("UPPERCASE", 0, i64::MAX);
    assert_eq!(r.entries.len(), 1);
    assert_eq!(r.entries[0].timestamp, i64::MAX);
    assert_eq!(r.entries[0].severity, 255);

    let r = store.query_by_service("negative-ts", i64::MIN, 0);
    assert_eq!(r.entries.len(), 1);
    assert_eq!(r.entries[0].timestamp, -1);
    assert!(r.entries[0].message.contains("!@#$%^&*()"));

    let r = store.query_by_host("日本語ホスト", 0, i64::MAX);
    assert_eq!(r.entries.len(), 1);
    assert!(r.entries[0].message.contains("🔥"));
}

// ---------------------------------------------------------------------------
// Test 8: Consumer offset tracking through Kafka
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_consumer_offset_tracking() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-offsets");
    let group = format!("{}-group", topic);

    // Produce 20 entries.
    let entries: Vec<LogEntry> = (0..20).map(|i| entry(i, "svc", "h0", 1)).collect();

    let producer = ChironProducer::new(BROKERS, &topic).unwrap();
    for e in &entries {
        producer.send(e).unwrap();
    }
    producer.flush(Duration::from_secs(10)).unwrap();
    drop(producer);

    // Consume and commit.
    let consumer = ChironConsumer::new(BROKERS, &topic, &group).unwrap();
    let mut consumed = 0u64;

    let poll_timeout = Duration::from_millis(500);
    let idle_deadline = Duration::from_secs(10);
    let mut last_msg = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Ok(Some(_)) => {
                consumed += 1;
                last_msg = Instant::now();
            }
            Ok(None) => {
                if consumed >= 20 || last_msg.elapsed() >= idle_deadline {
                    break;
                }
            }
            Err(err) => panic!("consumer poll failed: {err}"),
        }
    }
    consumer.commit().unwrap();

    assert_eq!(consumed, 20);

    // Verify committed offsets are non-zero.
    let offsets = consumer.committed_offsets(Duration::from_secs(5));
    let total_committed: i64 = offsets
        .iter()
        .filter(|(_, o)| *o > 0)
        .map(|(_, o)| *o)
        .sum();
    assert_eq!(
        total_committed, 20,
        "committed offsets should sum to 20, got {}",
        total_committed
    );

    // Build KafkaOffsets and snapshot them.
    let mut kafka_offsets = KafkaOffsets::new();
    for (partition, offset) in &offsets {
        if *offset >= 0 {
            kafka_offsets.set(&topic, *partition as u32, *offset as u64);
        }
    }

    let dir = std::env::temp_dir().join("chiron_e2e_kafka_offsets");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("offsets.snap");

    // Create a minimal store just for snapshot.
    let store = ChironStore::new(100);
    store.save_snapshot(&path, &kafka_offsets).unwrap();

    // Restore and verify offsets survived.
    let (_, restored_offsets) = ChironStore::from_snapshot(&path).unwrap();
    for (partition, offset) in &offsets {
        if *offset >= 0 {
            assert_eq!(
                restored_offsets.get(&topic, *partition as u32),
                Some(*offset as u64)
            );
        }
    }

    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Test 9: Sharded store query + snapshot lifecycle through Kafka
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_sharded_store_roundtrip() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-sharded-roundtrip");
    let group = format!("{}-group", topic);
    let dir = std::env::temp_dir().join("chiron_e2e_sharded_roundtrip");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sharded.snap");

    let services = ["auth", "payment"];
    let hosts = ["h0", "h1", "h2", "h3"];
    let mut entries = Vec::new();
    for svc in &services {
        for host in &hosts {
            for i in 0..8 {
                entries.push(entry(
                    1_000 + i + (entries.len() as i64),
                    svc,
                    host,
                    (i % 4) as u8,
                ));
            }
        }
    }

    let (mut store, consumed) =
        produce_and_consume_sharded(&topic, &group, &entries, 256, 4).unwrap();
    assert_eq!(consumed, entries.len() as u64);
    assert!(store.shard_count() >= 4);

    store.flush_indexer();
    assert_eq!(store.indexer_lag(), 0);

    let auth = store.query_by_service("auth", 0, i64::MAX);
    assert_eq!(auth.entries.len(), hosts.len() * 8);

    let h2 = store.query_by_host("h2", 0, i64::MAX);
    assert_eq!(h2.entries.len(), services.len() * 8);
    assert!(h2.entries.iter().all(|e| e.host_id == "h2"));

    let payment_h1 = store.query_by_service_and_host("payment", "h1", 0, i64::MAX);
    assert_eq!(payment_h1.entries.len(), 8);
    assert!(
        payment_h1
            .entries
            .iter()
            .all(|e| e.service_name == "payment" && e.host_id == "h1")
    );

    let mut offsets = KafkaOffsets::new();
    offsets.set(&topic, 0, 111);
    offsets.set(&topic, 1, 222);
    offsets.set(&topic, 2, 333);
    offsets.set(&topic, 3, 444);

    store.save_snapshot(&path, &offsets).unwrap();
    let (restored, restored_offsets) = ChironStore::from_snapshot(&path).unwrap();

    assert_eq!(restored.shard_count(), store.shard_count());
    assert_eq!(
        restored.query_by_service("auth", 0, i64::MAX).entries.len(),
        auth.entries.len()
    );
    assert_eq!(
        restored.query_by_host("h2", 0, i64::MAX).entries.len(),
        h2.entries.len()
    );
    assert_eq!(
        restored
            .query_by_service_and_host("payment", "h1", 0, i64::MAX)
            .entries
            .len(),
        payment_h1.entries.len()
    );

    assert_eq!(restored_offsets.get(&topic, 0), Some(111));
    assert_eq!(restored_offsets.get(&topic, 1), Some(222));
    assert_eq!(restored_offsets.get(&topic, 2), Some(333));
    assert_eq!(restored_offsets.get(&topic, 3), Some(444));

    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Test 10: Sharded eviction keeps the newest global data
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn kafka_sharded_eviction_keeps_newest_global_entries() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-sharded-evict");
    let group = format!("{}-group", topic);

    // Four hosts spread the writes across keyed Kafka partitions while timestamps
    // stay globally ordered, which lets us verify global oldest-first eviction.
    let mut entries = Vec::new();
    for i in 0..12 {
        let host = format!("h{}", i % 4);
        entries.push(entry(i, "svc", &host, 1));
    }

    let (mut store, consumed) =
        produce_and_consume_sharded(&topic, &group, &entries, 12, 4).unwrap();
    assert_eq!(consumed, 12);

    store.tick();
    let result = store.query_by_service("svc", 0, i64::MAX);

    // Capacity 12 with 20% free target keeps 9 newest entries.
    assert_eq!(result.entries.len(), 9);
    let min_ts = result.entries.iter().map(|e| e.timestamp).min().unwrap();
    let max_ts = result.entries.iter().map(|e| e.timestamp).max().unwrap();
    assert_eq!(min_ts, 3);
    assert_eq!(max_ts, 11);

    let evicted = store.query_by_service("svc", 0, 2);
    assert!(evicted.entries.is_empty());
}

// ---------------------------------------------------------------------------
// Test 11: Kafka-backed load test with 1M ingests and 10k queries
// ---------------------------------------------------------------------------

#[test]
#[ignore = "expensive load-style test"]
fn kafka_1m_ingest_and_10k_queries() {
    if !require_kafka() {
        return;
    }

    let topic = unique_topic("e2e-kafka-load");
    let group = format!("{}-group", topic);
    let (services, hosts, entries) = build_load_workload();

    let mut store = ChironStore::with_shards(LOAD_TOTAL_ROWS, LOAD_INITIAL_SHARDS);
    let total_start = Instant::now();

    let producer = ChironProducer::new(BROKERS, &topic).unwrap();
    let produce_start = Instant::now();
    for (idx, entry) in entries.iter().enumerate() {
        producer.send(entry).unwrap();
        if (idx + 1) % LOAD_PRODUCER_FLUSH_EVERY == 0 {
            producer.flush(Duration::from_secs(30)).unwrap();
        }
    }
    producer.flush(Duration::from_secs(30)).unwrap();
    let produce_elapsed = produce_start.elapsed();
    drop(producer);

    let consumer = ChironConsumer::new(BROKERS, &topic, &group).unwrap();
    let consume_start = Instant::now();
    let mut consumed = 0u64;
    let poll_timeout = Duration::from_millis(500);
    let idle_deadline = Duration::from_secs(30);
    let mut last_msg = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Ok(Some((entry, partition, _offset))) => {
                store.ingest_partition(partition as u32, entry);
                consumed += 1;
                last_msg = Instant::now();
                if consumed == LOAD_TOTAL_ROWS as u64 {
                    break;
                }
            }
            Ok(None) => {
                if consumed >= LOAD_TOTAL_ROWS as u64 || last_msg.elapsed() >= idle_deadline {
                    break;
                }
            }
            Err(err) => panic!("consumer poll failed: {err}"),
        }
    }
    consumer.commit().unwrap();
    let consume_elapsed = consume_start.elapsed();

    assert_eq!(consumed, LOAD_TOTAL_ROWS as u64);
    assert_eq!(store.len(), LOAD_TOTAL_ROWS);

    let flush_start = Instant::now();
    store.flush_indexer();
    let flush_elapsed = flush_start.elapsed();

    assert_eq!(store.indexer_lag(), 0);

    let query_start = Instant::now();
    let total_hits = run_load_queries(&store, &services, &hosts);
    let query_elapsed = query_start.elapsed();
    let total_elapsed = total_start.elapsed();

    eprintln!(
        "kafka_load_e2e: rows={LOAD_TOTAL_ROWS}, queries={LOAD_QUERY_COUNT}, store_shards={}, \
produce={:.3}s ({:.0} rows/s), consume_ingest={:.3}s ({:.0} rows/s), flush={:.3}s, queries={:.3}s ({:.0} q/s), total={:.3}s, total_hits={total_hits}",
        store.shard_count(),
        produce_elapsed.as_secs_f64(),
        LOAD_TOTAL_ROWS as f64 / produce_elapsed.as_secs_f64(),
        consume_elapsed.as_secs_f64(),
        LOAD_TOTAL_ROWS as f64 / consume_elapsed.as_secs_f64(),
        flush_elapsed.as_secs_f64(),
        query_elapsed.as_secs_f64(),
        LOAD_QUERY_COUNT as f64 / query_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64(),
    );
}
