use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::{env, fmt::Display};

use crate::chiron::ChironStore;
use crate::kafka::{ChironConsumer, ChironKafkaError};
use crate::log_entry::LogEntry;
use crate::snapshot::KafkaOffsets;

/// Configuration for the Kafka -> Chiron ingest pipeline.
pub struct PipelineConfig {
    pub brokers: String,
    pub topic: String,
    pub num_partitions: u32,
    pub capacity: usize,
    pub consumer_group: String,
    pub consumer_threads: usize,
    pub consumer_batch_size: usize,
    pub consumer_poll_timeout: Duration,
    pub consumer_idle_timeout: Duration,
}

impl PipelineConfig {
    /// Read the pipeline configuration from environment variables.
    pub fn from_env() -> Self {
        let num_partitions = env_u32("CHIRON_PARTITIONS", env_u32("CHIRON_NUM_PARTITIONS", 4));
        let capacity = env_usize(
            "CHIRON_CAPACITY",
            env_usize("CHIRON_RING_BUFFER_CAPACITY", 100_000),
        );

        Self {
            brokers: env_string("CHIRON_BROKERS", "localhost:9092"),
            topic: env_string("CHIRON_TOPIC", "chiron-logs"),
            num_partitions,
            capacity,
            consumer_group: env_string("CHIRON_CONSUMER_GROUP", "chiron-pipeline"),
            consumer_threads: env_usize("CHIRON_CONSUMER_THREADS", num_partitions as usize).max(1),
            consumer_batch_size: env_usize("CHIRON_CONSUMER_BATCH_SIZE", 256).max(1),
            consumer_poll_timeout: Duration::from_millis(
                env_u64("CHIRON_CONSUMER_POLL_MS", 200).max(1),
            ),
            consumer_idle_timeout: Duration::from_millis(
                env_u64("CHIRON_CONSUMER_IDLE_MS", 5_000).max(1),
            ),
        }
    }
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
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

/// Stats collected from a pipeline run.
pub struct PipelineStats {
    pub total_consumed: u64,
    pub consume_duration: Duration,
    pub pipeline_duration: Duration,
    pub kafka_offsets: KafkaOffsets,
}

/// Start the Kafka -> store ingest pipeline and drain until the consumer group
/// has been idle for `consumer_idle_timeout`.
pub fn run_pipeline(
    config: PipelineConfig,
) -> Result<(Arc<ChironStore>, PipelineStats), ChironKafkaError> {
    let store = Arc::new(ChironStore::with_shards(
        config.capacity,
        config.num_partitions as usize,
    ));

    let pipeline_start = Instant::now();
    let consume_start = Instant::now();
    let mut consumer_threads = Vec::new();

    for _ in 0..config.consumer_threads {
        let store_clone = Arc::clone(&store);
        let brokers = config.brokers.clone();
        let topic = config.topic.clone();
        let group = config.consumer_group.clone();
        let batch_size = config.consumer_batch_size;
        let poll_timeout = config.consumer_poll_timeout;
        let idle_timeout = config.consumer_idle_timeout;

        consumer_threads.push(thread::spawn(move || {
            consume_partition(
                &brokers,
                &topic,
                &group,
                store_clone,
                batch_size,
                poll_timeout,
                idle_timeout,
            )
        }));
    }

    let mut total_consumed = 0u64;
    let mut kafka_offsets = KafkaOffsets::new();
    for thread_handle in consumer_threads {
        let (count, offsets) = join_thread_result(thread_handle)?;
        total_consumed += count;
        for (topic_name, partitions) in offsets.inner() {
            for (&partition, &offset) in partitions {
                kafka_offsets.set(topic_name, partition, offset);
            }
        }
    }

    let stats = PipelineStats {
        total_consumed,
        consume_duration: consume_start.elapsed(),
        pipeline_duration: pipeline_start.elapsed(),
        kafka_offsets,
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
    store: Arc<ChironStore>,
    batch_size: usize,
    poll_timeout: Duration,
    idle_timeout: Duration,
) -> Result<(u64, KafkaOffsets), ChironKafkaError> {
    let consumer = ChironConsumer::new(brokers, topic, group)?;
    let mut count = 0u64;
    let mut batch: Vec<(LogEntry, u32)> = Vec::with_capacity(batch_size);
    let mut last_message_time = Instant::now();

    loop {
        match consumer.poll(poll_timeout) {
            Ok(Some((entry, partition, _offset))) => {
                batch.push((entry, partition as u32));
                count += 1;
                last_message_time = Instant::now();

                while batch.len() < batch_size {
                    match consumer.poll(Duration::ZERO) {
                        Ok(Some((entry, partition, _))) => {
                            batch.push((entry, partition as u32));
                            count += 1;
                            last_message_time = Instant::now();
                        }
                        Ok(None) => break,
                        Err(err) => return Err(err),
                    }
                }

                ingest_batch_by_partition(&store, &mut batch);
                consumer.commit()?;
            }
            Ok(None) => {
                if last_message_time.elapsed() >= idle_timeout {
                    break;
                }
            }
            Err(err) => return Err(err),
        }
    }

    let mut offsets = KafkaOffsets::new();
    for (partition, offset) in consumer.committed_offsets(Duration::from_secs(5)) {
        if offset >= 0 {
            offsets.set(topic, partition as u32, offset as u64);
        }
    }

    Ok((count, offsets))
}

fn join_thread_result<T>(
    handle: thread::JoinHandle<Result<T, ChironKafkaError>>,
) -> Result<T, ChironKafkaError> {
    handle
        .join()
        .map_err(|_| ChironKafkaError::ThreadPanic("worker thread panicked"))?
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
