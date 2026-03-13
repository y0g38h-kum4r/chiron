import threading
import bisect
from typing import List, Dict, Any

class LogEntry:
    def __init__(self, timestamp: int, service_name: str, host_id: str, message: str):
        self.timestamp = timestamp
        self.service_name = service_name
        self.host_id = host_id
        self.message = message

    def __lt__(self, other):
        # We sort by timestamp
        return self.timestamp < other.timestamp

    def __repr__(self):
        return f"[{self.timestamp}] {self.service_name} ({self.host_id}): {self.message}"

class LogPartition:
    def __init__(self):
        self.logs = []
        self.lock = threading.RLock()

    def insert(self, log: LogEntry):
        with self.lock:
            # Fast path for chronological insertion
            if not self.logs or self.logs[-1].timestamp <= log.timestamp:
                self.logs.append(log)
            else:
                bisect.insort(self.logs, log)

    def query(self, t1: int, t2: int) -> List[LogEntry]:
        with self.lock:
            # Dummy entries to find bounds
            dummy_start = LogEntry(t1, "", "", "")
            # We add 1 or use bisect_right for t2 conceptually. Wait, we want up to t2 inclusive.
            dummy_end = LogEntry(t2 + 1, "", "", "")
            
            start_idx = bisect.bisect_left(self.logs, dummy_start)
            end_idx = bisect.bisect_left(self.logs, dummy_end)
            
            # Return a copy to avoid concurrent modification issues during iteration
            return self.logs[start_idx:end_idx].copy()


class InMemoryLogStore:
    def __init__(self):
        self.by_service = {}
        self.by_host = {}
        self.by_service_host = {}
        
        self.service_lock = threading.Lock()
        self.host_lock = threading.Lock()
        self.service_host_lock = threading.Lock()

    def _get_partition(self, dictionary, key, lock) -> LogPartition:
        # Double checked locking structure not strictly needed in GIL Python but good practice
        if key not in dictionary:
            with lock:
                if key not in dictionary:
                    dictionary[key] = LogPartition()
        return dictionary[key]

    def ingest(self, timestamp: int, service_name: str, host_id: str, message: str):
        log = LogEntry(timestamp, service_name, host_id, message)
        
        # Insert into service partition
        part_service = self._get_partition(self.by_service, service_name, self.service_lock)
        part_service.insert(log)
        
        # Insert into host partition
        part_host = self._get_partition(self.by_host, host_id, self.host_lock)
        part_host.insert(log)
        
        # Insert into service+host partition
        key_sh = f"{service_name}|{host_id}"
        part_sh = self._get_partition(self.by_service_host, key_sh, self.service_host_lock)
        part_sh.insert(log)

    def get_logs_by_service(self, service_name: str, t1: int, t2: int) -> List[LogEntry]:
        if service_name not in self.by_service:
            return []
        return self.by_service[service_name].query(t1, t2)

    def get_logs_by_host(self, host_id: str, t1: int, t2: int) -> List[LogEntry]:
        if host_id not in self.by_host:
            return []
        return self.by_host[host_id].query(t1, t2)

    def get_logs_by_service_and_host(self, service_name: str, host_id: str, t1: int, t2: int) -> List[LogEntry]:
        key = f"{service_name}|{host_id}"
        if key not in self.by_service_host:
            return []
        return self.by_service_host[key].query(t1, t2)

if __name__ == "__main__":
    store = InMemoryLogStore()
    
    print("Ingesting test logs...")
    # Simulate some test data
    import time
    start_t = 1000
    
    threads = []
    def worker(i):
        ts = start_t + (i % 10)
        store.ingest(ts, "AuthService", f"Host-{i % 3}", f"Login attempt {i}")
        
    for i in range(50):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    print("Queries:")
    
    t1, t2 = start_t + 2, start_t + 5
    print(f"\n--- Logs for AuthService between {t1} and {t2} ---")
    res = store.get_logs_by_service("AuthService", t1, t2)
    for r in res: print(r)
    
    print(f"\n--- Logs for AuthService on Host-1 between {t1} and {t2} ---")
    res = store.get_logs_by_service_and_host("AuthService", "Host-1", t1, t2)
    for r in res: print(r)
    
    print(f"\n--- Logs for Host-2 between {t1} and {t2} ---")
    res = store.get_logs_by_host("Host-2", t1, t2)
    for r in res: print(r)
    
    print("\nAll good!")
