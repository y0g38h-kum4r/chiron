use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::Offset;
use crate::log_entry::LogEntry;

/// Kafka producer that sends LogEntry records as JSON to a topic.
/// Partitions by host_id (key).
pub struct ChironProducer {
    producer: BaseProducer,
    topic: String,
}

impl ChironProducer {
    pub fn new(brokers: &str, topic: &str) -> Self {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("failed to create kafka producer");

        Self {
            producer,
            topic: topic.to_string(),
        }
    }

    /// Send a log entry. The host_id is used as the partition key
    /// so all logs from the same host land on the same partition (ordering guarantee).
    pub fn send(&self, entry: &LogEntry) {
        let payload = serde_json::to_string(entry).expect("failed to serialize log entry");

        self.producer
            .send(
                BaseRecord::to(&self.topic)
                    .key(&entry.host_id)
                    .payload(&payload),
            )
            .expect("failed to enqueue message");
    }

    /// Flush all pending messages. Call after producing a batch.
    pub fn flush(&self, timeout: Duration) {
        self.producer.flush(timeout).expect("flush failed");
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
    pub fn new(brokers: &str, topic: &str, group_id: &str) -> Self {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("failed to create kafka consumer");

        consumer
            .subscribe(&[topic])
            .expect("failed to subscribe to topic");

        Self {
            consumer,
            topic: topic.to_string(),
        }
    }

    /// Poll for the next message. Returns None on timeout.
    pub fn poll(&self, timeout: Duration) -> Option<(LogEntry, i32, i64)> {
        match self.consumer.poll(timeout) {
            Some(Ok(msg)) => {
                let entry = deserialize_message(&msg)?;
                let partition = msg.partition();
                let offset = msg.offset();
                Some((entry, partition, offset))
            }
            Some(Err(e)) => {
                eprintln!("kafka consumer error: {}", e);
                None
            }
            None => None,
        }
    }

    /// Commit the current offsets synchronously.
    pub fn commit(&self) {
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
            .ok();
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

fn deserialize_message(msg: &BorrowedMessage) -> Option<LogEntry> {
    let payload = msg.payload()?;
    let text = std::str::from_utf8(payload).ok()?;
    serde_json::from_str(text).ok()
}
