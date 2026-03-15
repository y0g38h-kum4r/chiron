use serde::{Deserialize, Serialize};

/// A single log event stored in the ring buffer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: i64,
    pub service_name: String,
    pub host_id: String,
    pub message: String,
}
