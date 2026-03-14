use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::{env, fmt::Display};

use crate::chiron::ChironStore;
use crate::kafka::{ChironConsumer, ChironKafkaError, ChironProducer};
use crate::log_entry::LogEntry;
use crate::snapshot::KafkaOffsets;

/// Configuration for the pipeline.
pub struct PipelineConfig {
    pub brokers: String,
    pub topic: String,
    pub num_partitions: u32,
    pub ring_buffer_capacity: usize,
    pub services: Vec<String>,
    pub hosts: Vec<String>,
    pub logs_per_producer: u64,
    pub consumer_group: String,
    pub index_flush_interval: Duration,
}

impl PipelineConfig {
    /// Convenience constructor with sensible defaults for local Docker Kafka.
    pub fn local(services: Vec<String>, hosts: Vec<String>, logs_per_producer: u64) -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "chiron-logs".to_string(),
            num_partitions: env_u32("CHIRON_NUM_PARTITIONS", 4),
            ring_buffer_capacity: env_usize("CHIRON_RING_BUFFER_CAPACITY", 100_000),
            services,
            hosts,
            logs_per_producer,
            consumer_group: "chiron-pipeline".to_string(),
            index_flush_interval: Duration::from_millis(env_u64(
                "CHIRON_INDEX_FLUSH_INTERVAL_MS",
                50,
            )),
        }
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    env_parsed(name).unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env_parsed(name).unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env_parsed(name).unwrap_or(default)
}

fn env_parsed<T>(name: &str) -> Option<T>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    match env::var(name) {
        Ok(raw) => match raw.parse::<T>() {
            Ok(value) => Some(value),
            Err(err) => {
                eprintln!(
                    "invalid value for {}: {:?} ({}) - falling back to default",
                    name, raw, err
                );
                None
            }
        },
        Err(_) => None,
    }
}

/// Stats collected from the pipeline run.
pub struct PipelineStats {
    pub total_produced: u64,
    pub total_consumed: u64,
    pub produce_duration: Duration,
    pub consume_duration: Duration,
    pub pipeline_duration: Duration,
    pub kafka_offsets: KafkaOffsets,
    pub index_flushes: u64,
    pub max_indexer_lag: u64,
}

/// A dummy log-producing service running on a host.
/// Each producer generates logs for a specific (service, host) pair.
struct DummyService {
    service_name: String,
    host_id: String,
    producer: ChironProducer,
    logs_to_produce: u64,
}

impl DummyService {
    fn run(self) -> Result<u64, ChironKafkaError> {
        let start_ts = 1_000_000i64; // base timestamp
        for i in 0..self.logs_to_produce {
            let entry = LogEntry {
                timestamp: start_ts + i as i64,
                service_name: self.service_name.clone(),
                host_id: self.host_id.clone(),
                message: format!("{} on {} event {}", self.service_name, self.host_id, i),
                severity: (i % 5) as u8,
            };
            self.producer.send(&entry)?;
        }
        self.producer.flush(Duration::from_secs(10))?;
        Ok(self.logs_to_produce)
    }
}

/// Run the full pipeline:
/// 1. Spawn producer threads (one per service×host pair)
/// 2. Wait for all producers to finish
/// 3. Spawn consumer threads (one per Kafka partition)
/// 4. Each consumer ingests into the shared ChironStore
/// 5. Wait for all consumers
/// 6. Flush indexer, return stats
pub fn run_pipeline(
    config: PipelineConfig,
) -> Result<(Arc<Mutex<ChironStore>>, PipelineStats), ChironKafkaError> {
    let store = Arc::new(Mutex::new(ChironStore::with_shards(
        config.ring_buffer_capacity,
        config.num_partitions as usize,
    )));
    let producers_done = Arc::new(AtomicBool::new(false));
    let consumers_done = Arc::new(AtomicBool::new(false));
    let max_indexer_lag = Arc::new(AtomicU64::new(0));

    let pipeline_start = Instant::now();

    // --- Spawn consumers first so ingestion happens live while producers are active ---
    let consume_start = Instant::now();
    let mut consumer_threads = Vec::new();

    for _partition in 0..config.num_partitions {
        let store_clone = Arc::clone(&store);
        let producers_done = Arc::clone(&producers_done);
        let brokers = config.brokers.clone();
        let topic = config.topic.clone();
        let group = config.consumer_group.clone();

        consumer_threads.push(thread::spawn(move || {
            consume_partition(&brokers, &topic, &group, store_clone, producers_done)
        }));
    }

    // --- Spawn a background indexer to keep queries warm during ingestion ---
    let index_store = Arc::clone(&store);
    let index_done = Arc::clone(&consumers_done);
    let index_flush_interval = config.index_flush_interval;
    let index_lag_metric = Arc::clone(&max_indexer_lag);
    let indexer_thread = thread::spawn(move || {
        let mut flushes = 0u64;

        loop {
            {
                let mut s = index_store.lock().unwrap();
                s.flush_indexer();
                flushes += 1;
                update_max_atomic(&index_lag_metric, s.indexer_lag());
            }

            if index_done.load(Ordering::SeqCst) {
                let lag = index_store.lock().unwrap().indexer_lag();
                update_max_atomic(&index_lag_metric, lag);
                if lag == 0 {
                    break;
                }
            }

            thread::sleep(index_flush_interval);
        }

        flushes
    });

    // --- Spawn producers ---
    let produce_start = Instant::now();
    let mut producer_threads = Vec::new();

    for svc in &config.services {
        for host in &config.hosts {
            let svc_name = svc.clone();
            let host_id = host.clone();
            let brokers = config.brokers.clone();
            let topic = config.topic.clone();
            let logs = config.logs_per_producer;

            producer_threads.push(thread::spawn(move || {
                let producer = ChironProducer::new(&brokers, &topic)?;
                let dummy = DummyService {
                    service_name: svc_name,
                    host_id,
                    producer,
                    logs_to_produce: logs,
                };
                dummy.run()
            }));
        }
    }

    // Wait for all producers to finish.
    let mut total_produced = 0u64;
    for t in producer_threads {
        total_produced += join_thread_result(t)?;
    }
    let produce_duration = produce_start.elapsed();
    producers_done.store(true, Ordering::SeqCst);

    // Wait for all consumers.
    let mut total_consumed = 0u64;
    let mut kafka_offsets = KafkaOffsets::new();
    for t in consumer_threads {
        let (count, offsets) = join_thread_result(t)?;
        total_consumed += count;
        // Merge offsets from each consumer thread.
        for (topic_name, partitions) in offsets.inner() {
            for (&partition, &offset) in partitions {
                kafka_offsets.set(&topic_name, partition, offset);
            }
        }
    }
    consumers_done.store(true, Ordering::SeqCst);
    let consume_duration = consume_start.elapsed();
    let index_flushes = indexer_thread
        .join()
        .map_err(|_| ChironKafkaError::ThreadPanic("indexer thread panicked"))?;
    let pipeline_duration = pipeline_start.elapsed();

    let stats = PipelineStats {
        total_produced,
        total_consumed,
        produce_duration,
        consume_duration,
        pipeline_duration,
        kafka_offsets,
        index_flushes,
        max_indexer_lag: max_indexer_lag.load(Ordering::SeqCst),
    };

    Ok((store, stats))
}

/// Consumer loop for one consumer in the group.
/// Records are routed into partition-local shards based on the partition id
/// returned by Kafka rather than by the thread that happened to read them.
fn consume_partition(
    brokers: &str,
    topic: &str,
    group: &str,
    store: Arc<Mutex<ChironStore>>,
    producers_done: Arc<AtomicBool>,
) -> Result<(u64, KafkaOffsets), ChironKafkaError> {
    let consumer = ChironConsumer::new(brokers, topic, group)?;
    let mut count = 0u64;
    let mut batch: Vec<(LogEntry, u32)> = Vec::with_capacity(256);

    let poll_timeout = Duration::from_millis(200);
    let idle_deadline = Duration::from_secs(5);
    let mut last_message_time = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Ok(Some((entry, partition, _offset))) => {
                batch.push((entry, partition as u32));
                count += 1;
                last_message_time = Instant::now();

                // Try to batch more.
                while batch.len() < 256 {
                    match consumer.poll(Duration::from_millis(10)) {
                        Ok(Some((entry, partition, _))) => {
                            batch.push((entry, partition as u32));
                            count += 1;
                            last_message_time = Instant::now();
                        }
                        Ok(None) => break,
                        Err(err) => return Err(err),
                    }
                }

                // Ingest batch into the partition-local shards first so a crash
                // cannot acknowledge records that were never accepted locally.
                {
                    let mut s = store.lock().unwrap();
                    for (entry, partition) in batch.drain(..) {
                        s.ingest_partition(partition, entry);
                    }
                }

                consumer.commit()?;
            }
            Ok(None) => {
                // No message — quit once producers are done and the stream has been idle.
                if producers_done.load(Ordering::SeqCst)
                    && last_message_time.elapsed() >= idle_deadline
                {
                    break;
                }
            }
            Err(err) => return Err(err),
        }
    }

    // Collect committed offsets.
    let mut offsets = KafkaOffsets::new();
    for (partition, offset) in consumer.committed_offsets(Duration::from_secs(5)) {
        if offset >= 0 {
            offsets.set(topic, partition as u32, offset as u64);
        }
    }

    Ok((count, offsets))
}

fn update_max_atomic(target: &AtomicU64, candidate: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while candidate > current {
        match target.compare_exchange(current, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn join_thread_result<T>(
    handle: thread::JoinHandle<Result<T, ChironKafkaError>>,
) -> Result<T, ChironKafkaError> {
    handle
        .join()
        .map_err(|_| ChironKafkaError::ThreadPanic("worker thread panicked"))?
}
