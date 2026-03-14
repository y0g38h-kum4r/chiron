use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::Offset;

use crate::log_entry::LogEntry;

#[derive(Debug)]
pub enum ChironKafkaError {
    Kafka(KafkaError),
    Serialize(serde_json::Error),
    Deserialize(serde_json::Error),
    Utf8(std::str::Utf8Error),
    MissingPayload,
    ThreadPanic(&'static str),
}

impl std::fmt::Display for ChironKafkaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kafka(err) => write!(f, "kafka error: {err}"),
            Self::Serialize(err) => write!(f, "failed to serialize log entry: {err}"),
            Self::Deserialize(err) => write!(f, "failed to deserialize log entry: {err}"),
            Self::Utf8(err) => write!(f, "invalid UTF-8 payload: {err}"),
            Self::MissingPayload => write!(f, "kafka message had no payload"),
            Self::ThreadPanic(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ChironKafkaError {}

impl From<KafkaError> for ChironKafkaError {
    fn from(value: KafkaError) -> Self {
        Self::Kafka(value)
    }
}

/// Kafka producer that sends LogEntry records as JSON to a topic.
/// Partitions by host_id (key).
pub struct ChironProducer {
    producer: BaseProducer,
    topic: String,
}

impl ChironProducer {
    pub fn new(brokers: &str, topic: &str) -> Result<Self, ChironKafkaError> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(ChironKafkaError::Kafka)?;

        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    /// Send a log entry. The host_id is used as the partition key
    /// so all logs from the same host land on the same partition (ordering guarantee).
    pub fn send(&self, entry: &LogEntry) -> Result<(), ChironKafkaError> {
        let payload =
            serde_json::to_string(entry).map_err(ChironKafkaError::Serialize)?;

        self.producer
            .send(
                BaseRecord::to(&self.topic)
                    .key(&entry.host_id)
                    .payload(&payload),
            )
            .map_err(|(err, _)| ChironKafkaError::Kafka(err))?;

        Ok(())
    }

    /// Flush all pending messages. Call after producing a batch.
    pub fn flush(&self, timeout: Duration) -> Result<(), ChironKafkaError> {
        self.producer.flush(timeout).map_err(ChironKafkaError::Kafka)
    }
}

/// Kafka consumer that reads LogEntry records from assigned partitions.
pub struct ChironConsumer {
    consumer: BaseConsumer,
    pub topic: String,
}

impl ChironConsumer {
    /// Create a consumer subscribed to the given topic.
    /// `group_id` is the Kafka consumer group.
    pub fn new(
        brokers: &str,
        topic: &str,
        group_id: &str,
    ) -> Result<Self, ChironKafkaError> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .map_err(ChironKafkaError::Kafka)?;

        consumer
            .subscribe(&[topic])
            .map_err(ChironKafkaError::Kafka)?;

        Ok(Self {
            consumer,
            topic: topic.to_string(),
        })
    }

    /// Poll for the next message. Returns `Ok(None)` on timeout.
    pub fn poll(
        &self,
        timeout: Duration,
    ) -> Result<Option<(LogEntry, i32, i64)>, ChironKafkaError> {
        match self.consumer.poll(timeout) {
            Some(Ok(msg)) => {
                let entry = deserialize_message(&msg)?;
                let partition = msg.partition();
                let offset = msg.offset();
                Ok(Some((entry, partition, offset)))
            }
            Some(Err(e)) => Err(ChironKafkaError::Kafka(e)),
            None => Ok(None),
        }
    }

    /// Commit the current offsets synchronously.
    pub fn commit(&self) -> Result<(), ChironKafkaError> {
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
            .map_err(ChironKafkaError::Kafka)
    }

    /// Get committed offsets for all assigned partitions.
    pub fn committed_offsets(&self, timeout: Duration) -> Vec<(i32, i64)> {
        let assignment = match self.consumer.assignment() {
            Ok(a) => a,
            Err(_) => return vec![],
        };

        let committed = match self.consumer.committed_offsets(assignment, timeout) {
            Ok(c) => c,
            Err(_) => return vec![],
        };

        committed
            .elements()
            .iter()
            .map(|elem| {
                let off = match elem.offset() {
                    Offset::Offset(o) => o,
                    _ => -1,
                };
                (elem.partition(), off)
            })
            .collect()
    }
}

fn deserialize_message(msg: &BorrowedMessage) -> Result<LogEntry, ChironKafkaError> {
    let payload = msg.payload().ok_or(ChironKafkaError::MissingPayload)?;
    let text = std::str::from_utf8(payload).map_err(ChironKafkaError::Utf8)?;
    serde_json::from_str(text).map_err(ChironKafkaError::Deserialize)
}
