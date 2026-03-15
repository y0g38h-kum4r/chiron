/// End-to-end Kafka streaming benchmark.
///
/// All components run concurrently, just like the real pipeline:
///   - Producer threads stream entries into Kafka (one per host, client-side partitioning)
///   - Consumer threads poll Kafka and ingest into the sharded ChironStore
///   - Background indexer flushes indexes periodically
///   - Reader threads fire queries (60/30/10 mix) throughout the run
///
/// Partitioning: producers use `partition_for_host(host_id, num_partitions)` to
/// pick the Kafka partition, guaranteeing 1:1 alignment with store shards.
///
/// Tuning knobs (env vars):
///   CHIRON_BROKERS             Kafka bootstrap servers (default localhost:9092)
///   CHIRON_TOPIC               Kafka topic prefix (default chiron-e2e-bench)
///   CHIRON_SERVICES            number of services (default 10)
///   CHIRON_HOSTS               number of hosts (default 10)
///   CHIRON_PARTITIONS          Kafka partitions / store shards (default 4)
///   CHIRON_CAPACITY            store capacity (default 10_000_000)
///   CHIRON_READERS             number of reader threads (default 2)
///   CHIRON_DURATION_SECS       run duration in seconds (default 30)
///   CHIRON_FLUSH_INTERVAL_MS   indexer flush interval in ms (default 50)
use std::hint::black_box;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chiron::chiron::ChironStore;
use chiron::kafka::{ChironConsumer, ChironProducer};
use chiron::log_entry::LogEntry;

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn kafka_available(brokers: &str) -> bool {
    match brokers.to_socket_addrs() {
        Ok(addrs) => addrs
            .into_iter()
            .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()),
        Err(_) => false,
    }
}

fn unique_topic(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn percentile(sorted: &[u64], permille: u16) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) * permille as usize) / 1000;
    sorted[idx]
}

fn main() {
    let brokers = env_str("CHIRON_BROKERS", "localhost:9092");
    let topic_prefix = env_str("CHIRON_TOPIC", "chiron-e2e-bench");
    let service_count = env_usize("CHIRON_SERVICES", 10);
    let host_count = env_usize("CHIRON_HOSTS", 10);
    let num_partitions = env_usize("CHIRON_PARTITIONS", 4);
    let capacity = env_usize("CHIRON_CAPACITY", 10_000_000);
    let reader_threads = env_usize("CHIRON_READERS", 2);
    let duration_secs = env_u64("CHIRON_DURATION_SECS", 30);
    let flush_interval_ms = env_u64("CHIRON_FLUSH_INTERVAL_MS", 50);

    let topic = unique_topic(&topic_prefix);
    let group = format!("{topic}-group");

    if !kafka_available(&brokers) {
        eprintln!(
            "e2e_bench: no Kafka broker reachable at {brokers} — \
             start one with `docker compose up -d`"
        );
        std::process::exit(1);
    }

    let services: Vec<String> = (0..service_count).map(|i| format!("svc-{i:02}")).collect();
    let hosts: Vec<String> = (0..host_count).map(|i| format!("host-{i:02}")).collect();

    println!(
        "e2e_bench: brokers={brokers} topic={topic} services={service_count} \
         hosts={host_count} partitions={num_partitions} \
         capacity={capacity} readers={reader_threads} duration={duration_secs}s"
    );

    let store = Arc::new(ChironStore::with_shards(capacity, num_partitions));
    let stop = Arc::new(AtomicBool::new(false));
    let writes_total = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));
    let reads_total = Arc::new(AtomicU64::new(0));
    let read_hits_total = Arc::new(AtomicU64::new(0));
    let empty_reads_total = Arc::new(AtomicU64::new(0));
    let max_indexer_lag = Arc::new(AtomicU64::new(0));
    let query_counts: Arc<[AtomicU64; 3]> = Arc::new([
        AtomicU64::new(0), // by_host
        AtomicU64::new(0), // by_svc_host
        AtomicU64::new(0), // by_svc
    ]);
    let empty_query_counts: Arc<[AtomicU64; 3]> = Arc::new([
        AtomicU64::new(0), // by_host
        AtomicU64::new(0), // by_svc_host
        AtomicU64::new(0), // by_svc
    ]);
    let latency_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let bench_start = Instant::now();
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

    // -----------------------------------------------------------------------
    // Producer threads: one per host (client-side partitioning)
    //
    // Each host maps to exactly one partition via hash(host_id) % num_partitions.
    // The producer round-robins through services, so all (service, host) pairs
    // are covered with only `host_count` threads instead of `service * host`.
    // -----------------------------------------------------------------------
    for host in &hosts {
        let brokers = brokers.clone();
        let topic = topic.clone();
        let host = host.clone();
        let services = services.clone();
        let stop = Arc::clone(&stop);
        let writes_total = Arc::clone(&writes_total);

        handles.push(thread::spawn(move || {
            let producer = ChironProducer::new(&brokers, &topic, num_partitions)
                .expect("failed to create producer");
            let mut ts: i64 = 1_000_000;
            let mut local_writes = 0u64;
            let mut svc_idx = 0usize;

            while !stop.load(Ordering::Relaxed) {
                let svc = &services[svc_idx % services.len()];
                let entry = LogEntry {
                    timestamp: ts,
                    service_name: svc.clone(),
                    host_id: host.clone(),
                    message: format!("{svc} on {host} event {ts}"),
                };
                if producer.send(&entry).is_err() {
                    break;
                }
                ts += 1;
                local_writes += 1;
                svc_idx += 1;

                if local_writes % 100 == 0 {
                    let _ = producer.flush(Duration::from_secs(5));
                }
            }

            let _ = producer.flush(Duration::from_secs(10));
            writes_total.fetch_add(local_writes, Ordering::Relaxed);
        }));
    }

    // -----------------------------------------------------------------------
    // Consumer threads: one per partition, polling Kafka and ingesting
    // -----------------------------------------------------------------------
    for _ in 0..num_partitions {
        let brokers = brokers.clone();
        let topic = topic.clone();
        let group = group.clone();
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let consumed_total = Arc::clone(&consumed_total);

        handles.push(thread::spawn(move || {
            let consumer =
                ChironConsumer::new(&brokers, &topic, &group).expect("failed to create consumer");
            let mut batch: Vec<(LogEntry, u32)> = Vec::with_capacity(256);

            while !stop.load(Ordering::Relaxed) {
                match consumer.poll(Duration::from_millis(100)) {
                    Ok(Some((entry, partition, _offset))) => {
                        batch.push((entry, partition as u32));

                        while batch.len() < 256 {
                            match consumer.poll(Duration::from_millis(5)) {
                                Ok(Some((entry, partition, _))) => {
                                    batch.push((entry, partition as u32));
                                }
                                _ => break,
                            }
                        }

                        let batch_len = batch.len() as u64;
                        ingest_batch_by_partition(&store, &mut batch);
                        consumed_total.fetch_add(batch_len, Ordering::Relaxed);
                        let _ = consumer.commit();
                    }
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
        }));
    }

    // -----------------------------------------------------------------------
    // Background indexer thread
    // -----------------------------------------------------------------------
    {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let max_lag = Arc::clone(&max_indexer_lag);
        let flush_interval = Duration::from_millis(flush_interval_ms);

        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                store.flush_indexer();
                let lag = store.indexer_lag();
                max_lag.fetch_max(lag, Ordering::Relaxed);
                thread::sleep(flush_interval);
            }
            store.flush_indexer();
        }));
    }

    // -----------------------------------------------------------------------
    // Reader threads: 60/30/10 query mix running throughout
    // -----------------------------------------------------------------------
    for reader_id in 0..reader_threads {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let reads_total = Arc::clone(&reads_total);
        let read_hits_total = Arc::clone(&read_hits_total);
        let empty_reads_total = Arc::clone(&empty_reads_total);
        let query_counts = Arc::clone(&query_counts);
        let empty_query_counts = Arc::clone(&empty_query_counts);
        let latency_samples = Arc::clone(&latency_samples);
        let services = services.clone();
        let hosts: Vec<String> = hosts.clone();

        handles.push(thread::spawn(move || {
            let mut local_reads = 0u64;
            let mut local_hits = 0u64;
            let mut local_empty = 0u64;
            let mut local_latencies = Vec::new();
            let mut local_by_host = 0u64;
            let mut local_by_svc_host = 0u64;
            let mut local_by_svc = 0u64;
            let mut local_empty_by_host = 0u64;
            let mut local_empty_by_svc_host = 0u64;
            let mut local_empty_by_svc = 0u64;
            let mut q: usize = reader_id;

            // Wait until the store has some data before measuring queries.
            while !stop.load(Ordering::Relaxed) {
                if store.len() > 0 {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }

            while !stop.load(Ordering::Relaxed) {
                let bucket = q % 10;
                let started = Instant::now();

                let hits = match bucket {
                    // 60% ByHost
                    0..=5 => {
                        let host = &hosts[q % host_count];
                        let result = store.query_by_host(host, i64::MIN, i64::MAX);
                        local_by_host += 1;
                        black_box(result.entries.len())
                    }
                    // 30% ByServiceAndHost
                    6..=8 => {
                        let svc = &services[q % service_count];
                        let host = &hosts[(q * 3) % host_count];
                        let result =
                            store.query_by_service_and_host(svc, host, i64::MIN, i64::MAX);
                        local_by_svc_host += 1;
                        black_box(result.entries.len())
                    }
                    // 10% ByService
                    _ => {
                        let svc = &services[q % service_count];
                        let result = store.query_by_service(svc, i64::MIN, i64::MAX);
                        local_by_svc += 1;
                        black_box(result.entries.len())
                    }
                };

                let elapsed_us = started.elapsed().as_micros() as u64;


                if hits > 0 {
                    local_latencies.push(elapsed_us);
                    local_hits += hits as u64;
                    local_reads += 1;
                } else {
                    local_empty += 1;
                    match bucket {
                        0..=5 => local_empty_by_host += 1,
                        6..=8 => local_empty_by_svc_host += 1,
                        _ => local_empty_by_svc += 1,
                    }
                }

                q = q.wrapping_add(reader_threads);
            }

            reads_total.fetch_add(local_reads, Ordering::Relaxed);
            read_hits_total.fetch_add(local_hits, Ordering::Relaxed);
            empty_reads_total.fetch_add(local_empty, Ordering::Relaxed);
            query_counts[0].fetch_add(local_by_host, Ordering::Relaxed);
            query_counts[1].fetch_add(local_by_svc_host, Ordering::Relaxed);
            query_counts[2].fetch_add(local_by_svc, Ordering::Relaxed);
            empty_query_counts[0].fetch_add(local_empty_by_host, Ordering::Relaxed);
            empty_query_counts[1].fetch_add(local_empty_by_svc_host, Ordering::Relaxed);
            empty_query_counts[2].fetch_add(local_empty_by_svc, Ordering::Relaxed);
            latency_samples
                .lock()
                .unwrap()
                .extend_from_slice(&local_latencies);
        }));
    }

    // -----------------------------------------------------------------------
    // Let everything run, then stop
    // -----------------------------------------------------------------------
    thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::SeqCst);

    for h in handles {
        h.join().expect("thread panicked");
    }

    // -----------------------------------------------------------------------
    // Report
    // -----------------------------------------------------------------------
    let elapsed = bench_start.elapsed();
    let writes = writes_total.load(Ordering::Relaxed);
    let consumed = consumed_total.load(Ordering::Relaxed);
    let reads = reads_total.load(Ordering::Relaxed);
    let empty_reads = empty_reads_total.load(Ordering::Relaxed);
    let hits = read_hits_total.load(Ordering::Relaxed);
    let by_host = query_counts[0].load(Ordering::Relaxed);
    let by_svc_host = query_counts[1].load(Ordering::Relaxed);
    let by_svc = query_counts[2].load(Ordering::Relaxed);
    let empty_by_host = empty_query_counts[0].load(Ordering::Relaxed);
    let empty_by_svc_host = empty_query_counts[1].load(Ordering::Relaxed);
    let empty_by_svc = empty_query_counts[2].load(Ordering::Relaxed);

    let mut latencies = latency_samples.lock().unwrap();
    latencies.sort_unstable();
    let p50 = percentile(&latencies, 500);
    let p95 = percentile(&latencies, 950);
    let p99 = percentile(&latencies, 990);
    let p999 = percentile(&latencies, 999);

    println!(
        "e2e_bench: elapsed={:.3}s writes={writes} write_rate={:.0}/s \
         consumed={consumed} consume_rate={:.0}/s",
        elapsed.as_secs_f64(),
        writes as f64 / elapsed.as_secs_f64(),
        consumed as f64 / elapsed.as_secs_f64(),
    );
    println!(
        "e2e_bench: queries_with_data={reads} query_rate={:.0}/s hits={hits} empty_reads={empty_reads}",
        reads as f64 / elapsed.as_secs_f64(),
    );
    println!("e2e_bench: query_mix by_host={by_host} by_svc_host={by_svc_host} by_svc={by_svc}");
    println!(
        "e2e_bench: empty_reads_by_type by_host={empty_by_host} by_svc_host={empty_by_svc_host} by_svc={empty_by_svc}"
    );
    println!("e2e_bench: query_latency_us p50={p50} p95={p95} p99={p99} p99.9={p999}");
    println!(
        "e2e_bench: store_len={} capacity={capacity} shards={} max_indexer_lag={}",
        store.len(),
        store.shard_count(),
        max_indexer_lag.load(Ordering::Relaxed),
    );
}

fn ingest_batch_by_partition(store: &ChironStore, batch: &mut Vec<(LogEntry, u32)>) {
    if batch.is_empty() {
        return;
    }

    batch.sort_unstable_by_key(|(_, partition)| *partition);

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
