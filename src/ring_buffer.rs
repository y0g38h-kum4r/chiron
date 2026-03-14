use crate::log_entry::LogEntry;

/// Fixed-capacity circular buffer for log entries.
/// Append-only, cache-friendly (contiguous memory), evicts from head.
pub struct RingBuffer {
    buf: Vec<Option<LogEntry>>,
    capacity: usize,
    /// Next write position (modular).
    write_pos: usize,
    /// Number of entries currently stored.
    len: usize,
    /// Global monotonic counter — each inserted entry gets a unique offset.
    /// This is what the inverted index references.
    global_offset: u64,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        let mut buf = Vec::with_capacity(capacity);
        buf.resize_with(capacity, || None);
        Self {
            buf,
            capacity,
            write_pos: 0,
            len: 0,
            global_offset: 0,
        }
    }

    /// Append a log entry. Returns the global offset assigned to this entry.
    /// If the buffer is full, the oldest entry is overwritten (head eviction).
    pub fn push(&mut self, entry: LogEntry) -> u64 {
        let offset = self.global_offset;
        self.buf[self.write_pos] = Some(entry);
        self.write_pos = (self.write_pos + 1) % self.capacity;
        if self.len < self.capacity {
            self.len += 1;
        }
        self.global_offset += 1;
        offset
    }

    /// Read entry by global offset. Returns None if the offset has been evicted.
    pub fn get(&self, global_offset: u64) -> Option<&LogEntry> {
        let oldest_offset = self.global_offset.saturating_sub(self.len as u64);
        if global_offset < oldest_offset || global_offset >= self.global_offset {
            return None;
        }
        let ring_idx = (global_offset % self.capacity as u64) as usize;
        self.buf[ring_idx].as_ref()
    }

    /// Evict `count` entries from the head (oldest).
    /// Returns the number of entries actually evicted.
    pub fn evict_head(&mut self, count: usize) -> usize {
        let to_evict = count.min(self.len);
        let oldest_offset = self.global_offset.saturating_sub(self.len as u64);
        for i in 0..to_evict {
            let ring_idx = ((oldest_offset + i as u64) % self.capacity as u64) as usize;
            self.buf[ring_idx] = None;
        }
        self.len -= to_evict;
        to_evict
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The oldest global offset still present in the buffer.
    pub fn oldest_offset(&self) -> u64 {
        self.global_offset.saturating_sub(self.len as u64)
    }

    /// The next global offset that will be assigned.
    pub fn next_offset(&self) -> u64 {
        self.global_offset
    }

    /// Restore a ring buffer from a snapshot.
    /// `entries` must be in order oldest→newest, and there must be exactly `len` of them.
    pub fn restore(
        capacity: usize,
        global_offset: u64,
        len: usize,
        entries: Vec<LogEntry>,
    ) -> Self {
        assert!(entries.len() == len, "entry count must match len");
        assert!(len <= capacity, "len must not exceed capacity");

        let mut buf = Vec::with_capacity(capacity);
        buf.resize_with(capacity, || None);

        // The oldest entry has global offset = global_offset - len.
        // Place each entry at its correct ring position.
        let oldest = global_offset - len as u64;
        for (i, entry) in entries.into_iter().enumerate() {
            let ring_idx = ((oldest + i as u64) % capacity as u64) as usize;
            buf[ring_idx] = Some(entry);
        }

        let write_pos = (global_offset % capacity as u64) as usize;

        Self {
            buf,
            capacity,
            write_pos,
            len,
            global_offset,
        }
    }

    /// Iterate over all live entries in order (oldest to newest).
    pub fn iter(&self) -> RingBufferIter<'_> {
        RingBufferIter {
            rb: self,
            current: self.oldest_offset(),
            end: self.global_offset,
        }
    }
}

pub struct RingBufferIter<'a> {
    rb: &'a RingBuffer,
    current: u64,
    end: u64,
}

impl<'a> Iterator for RingBufferIter<'a> {
    type Item = (u64, &'a LogEntry);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current < self.end {
            let offset = self.current;
            self.current += 1;
            if let Some(entry) = self.rb.get(offset) {
                return Some((offset, entry));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(ts: i64) -> LogEntry {
        LogEntry {
            timestamp: ts,
            service_name: "svc".into(),
            host_id: "h1".into(),
            message: format!("msg@{}", ts),
            severity: 1,
        }
    }

    #[test]
    fn push_and_get() {
        let mut rb = RingBuffer::new(4);
        let o0 = rb.push(make_entry(100));
        let o1 = rb.push(make_entry(200));
        assert_eq!(o0, 0);
        assert_eq!(o1, 1);
        assert_eq!(rb.get(0).unwrap().timestamp, 100);
        assert_eq!(rb.get(1).unwrap().timestamp, 200);
        assert_eq!(rb.len(), 2);
    }

    #[test]
    fn wrap_around_evicts_oldest() {
        let mut rb = RingBuffer::new(3);
        rb.push(make_entry(1));
        rb.push(make_entry(2));
        rb.push(make_entry(3));
        assert!(rb.is_full());
        // This overwrites offset 0
        rb.push(make_entry(4));
        assert!(rb.get(0).is_none());
        assert_eq!(rb.get(3).unwrap().timestamp, 4);
        assert_eq!(rb.len(), 3);
    }

    #[test]
    fn evict_head() {
        let mut rb = RingBuffer::new(5);
        for i in 0..5 {
            rb.push(make_entry(i));
        }
        let evicted = rb.evict_head(2);
        assert_eq!(evicted, 2);
        assert!(rb.get(0).is_none());
        assert!(rb.get(1).is_none());
        assert_eq!(rb.get(2).unwrap().timestamp, 2);
        assert_eq!(rb.len(), 3);
    }
}
