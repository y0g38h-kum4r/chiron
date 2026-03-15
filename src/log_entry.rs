use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// A single log event stored in the ring buffer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: i64,
    pub service_name: String,
    pub host_id: String,
    pub message: String,
}

/// Shared-string variant used inside the store and benchmark hot path.
#[derive(Clone, Debug)]
pub struct SharedLogEntry {
    pub timestamp: i64,
    pub service_name: Arc<str>,
    pub host_id: Arc<str>,
    pub message: Arc<str>,
}

impl From<LogEntry> for SharedLogEntry {
    fn from(entry: LogEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            service_name: Arc::from(entry.service_name),
            host_id: Arc::from(entry.host_id),
            message: Arc::from(entry.message),
        }
    }
}

impl From<&SharedLogEntry> for LogEntry {
    fn from(entry: &SharedLogEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            service_name: entry.service_name.to_string(),
            host_id: entry.host_id.to_string(),
            message: entry.message.to_string(),
        }
    }
}
