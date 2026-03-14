use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::{env, fmt::Display};

use crate::chiron::ChironStore;
use crate::kafka::{ChironConsumer, ChironProducer};
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
}

impl PipelineConfig {
    /// Convenience constructor with sensible defaults for local Docker Kafka.
    pub fn local(
        services: Vec<String>,
        hosts: Vec<String>,
        logs_per_producer: u64,
    ) -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "chiron-logs".to_string(),
            num_partitions: env_u32("CHIRON_NUM_PARTITIONS", 4),
            ring_buffer_capacity: env_usize("CHIRON_RING_BUFFER_CAPACITY", 100_000),
            services,
            hosts,
            logs_per_producer,
            consumer_group: "chiron-pipeline".to_string(),
        }
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    env_parsed(name).unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
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
    pub kafka_offsets: KafkaOffsets,
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
    fn run(self) -> u64 {
        let start_ts = 1_000_000i64; // base timestamp
        for i in 0..self.logs_to_produce {
            let entry = LogEntry {
                timestamp: start_ts + i as i64,
                service_name: self.service_name.clone(),
                host_id: self.host_id.clone(),
                message: format!(
                    "{} on {} event {}",
                    self.service_name, self.host_id, i
                ),
                severity: (i % 5) as u8,
            };
            self.producer.send(&entry);
        }
        self.producer.flush(Duration::from_secs(10));
        self.logs_to_produce
    }
}

/// Run the full pipeline:
/// 1. Spawn producer threads (one per service×host pair)
/// 2. Wait for all producers to finish
/// 3. Spawn consumer threads (one per Kafka partition)
/// 4. Each consumer ingests into the shared ChironStore
/// 5. Wait for all consumers
/// 6. Flush indexer, return stats
pub fn run_pipeline(config: PipelineConfig) -> (Arc<Mutex<ChironStore>>, PipelineStats) {
    let store = Arc::new(Mutex::new(ChironStore::new(config.ring_buffer_capacity)));

    let total_expected = config.services.len() as u64
        * config.hosts.len() as u64
        * config.logs_per_producer;

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
                let producer = ChironProducer::new(&brokers, &topic);
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
        total_produced += t.join().unwrap();
    }
    let produce_duration = produce_start.elapsed();

    // --- Spawn consumers ---
    let consume_start = Instant::now();
    let mut consumer_threads = Vec::new();

    for _partition in 0..config.num_partitions {
        let store_clone = Arc::clone(&store);
        let brokers = config.brokers.clone();
        let topic = config.topic.clone();
        let group = config.consumer_group.clone();
        let expected = total_expected;

        consumer_threads.push(thread::spawn(move || {
            consume_partition(&brokers, &topic, &group, store_clone, expected)
        }));
    }

    // Wait for all consumers.
    let mut total_consumed = 0u64;
    let mut kafka_offsets = KafkaOffsets::new();
    for t in consumer_threads {
        let (count, offsets) = t.join().unwrap();
        total_consumed += count;
        // Merge offsets from each consumer thread.
        for (topic_name, partitions) in offsets.inner() {
            for (&partition, &offset) in partitions {
                kafka_offsets.set(&topic_name, partition, offset);
            }
        }
    }
    let consume_duration = consume_start.elapsed();

    // --- Flush indexer ---
    {
        let mut s = store.lock().unwrap();
        s.flush_indexer();
    }

    let stats = PipelineStats {
        total_produced,
        total_consumed,
        produce_duration,
        consume_duration,
        kafka_offsets,
    };

    (store, stats)
}

/// Consumer loop for one consumer in the group.
/// Polls records from Kafka, ingests into ChironStore, tracks offsets.
/// Terminates when no new messages arrive for a sustained timeout period,
/// indicating all producers are done.
fn consume_partition(
    brokers: &str,
    topic: &str,
    group: &str,
    store: Arc<Mutex<ChironStore>>,
    _total_expected: u64,
) -> (u64, KafkaOffsets) {
    let consumer = ChironConsumer::new(brokers, topic, group);
    let mut count = 0u64;
    let mut batch = Vec::with_capacity(256);

    let poll_timeout = Duration::from_millis(200);
    let idle_deadline = Duration::from_secs(5);
    let mut last_message_time = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Some((entry, _partition, _offset)) => {
                batch.push(entry);
                count += 1;
                last_message_time = Instant::now();

                // Try to batch more.
                while batch.len() < 256 {
                    match consumer.poll(Duration::from_millis(10)) {
                        Some((entry, _, _)) => {
                            batch.push(entry);
                            count += 1;
                            last_message_time = Instant::now();
                        }
                        None => break,
                    }
                }

                // Commit offsets.
                consumer.commit();

                // Ingest batch into store.
                {
                    let mut s = store.lock().unwrap();
                    for entry in batch.drain(..) {
                        s.ingest(entry);
                    }
                }
            }
            None => {
                // No message — check if we've been idle long enough to quit.
                if last_message_time.elapsed() >= idle_deadline {
                    break;
                }
            }
        }
    }

    // Collect committed offsets.
    let mut offsets = KafkaOffsets::new();
    for (partition, offset) in consumer.committed_offsets(Duration::from_secs(5)) {
        if offset >= 0 {
            offsets.set(topic, partition as u32, offset as u64);
        }
    }

    (count, offsets)
}
