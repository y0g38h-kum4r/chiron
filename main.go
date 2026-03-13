package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// LogEntry represents a single log event.
type LogEntry struct {
	Timestamp   int64
	ServiceName string
	HostID      string
	LogMessage  string
}

// LogPartition holds a sorted slice of log entries and a mutex for concurrent access.
type LogPartition struct {
	mu   sync.RWMutex
	logs []LogEntry
}

// Insert adds a new log entry, keeping the slice sorted by Timestamp.
func (p *LogPartition) Insert(entry LogEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Fast path: if the slice flexes and the new entry is chronologically last, just append.
	if len(p.logs) == 0 || p.logs[len(p.logs)-1].Timestamp <= entry.Timestamp {
		p.logs = append(p.logs, entry)
		return
	}

	// Slower path: Insert at the correct sorted position.
	idx := sort.Search(len(p.logs), func(i int) bool {
		return p.logs[i].Timestamp > entry.Timestamp
	})

	// Insert into slice
	p.logs = append(p.logs, LogEntry{})
	copy(p.logs[idx+1:], p.logs[idx:])
	p.logs[idx] = entry
}

// Query returns logs between t1 and t2 (inclusive).
func (p *LogPartition) Query(t1, t2 int64) []LogEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Find starting index
	startIdx := sort.Search(len(p.logs), func(i int) bool {
		return p.logs[i].Timestamp >= t1
	})

	// Find ending index
	endIdx := sort.Search(len(p.logs), func(i int) bool {
		return p.logs[i].Timestamp > t2
	})

	if startIdx >= endIdx {
		return []LogEntry{}
	}

	// Copy the slice to prevent concurrent modification issues on the returned slice.
	result := make([]LogEntry, endIdx-startIdx)
	copy(result, p.logs[startIdx:endIdx])
	return result
}

// InMemoryLogStore is the main application coordinating log storage.
type InMemoryLogStore struct {
	// Mutexes for mapping creation (not for individual partition operations)
	muService     sync.RWMutex
	byService     map[string]*LogPartition

	muHost        sync.RWMutex
	byHost        map[string]*LogPartition

	muServiceHost sync.RWMutex
	byServiceHost map[string]*LogPartition
}

func NewInMemoryLogStore() *InMemoryLogStore {
	return &InMemoryLogStore{
		byService:     make(map[string]*LogPartition),
		byHost:        make(map[string]*LogPartition),
		byServiceHost: make(map[string]*LogPartition),
	}
}

// getOrCreatePartition helper to retrieve or initialize a partition
func getOrCreatePartition(mu *sync.RWMutex, m map[string]*LogPartition, key string) *LogPartition {
	mu.RLock()
	part, exists := m[key]
	mu.RUnlock()

	if exists {
		return part
	}

	mu.Lock()
	defer mu.Unlock()
	// Double-check after acquiring write lock
	if part, exists := m[key]; exists {
		return part
	}
	part = &LogPartition{
		logs: make([]LogEntry, 0),
	}
	m[key] = part
	return part
}

// Ingest asynchronously ingests a log into all relevant partitions
func (store *InMemoryLogStore) Ingest(log LogEntry) {
	// Create a combined key for service + host
	serviceHostKey := fmt.Sprintf("%s|%s", log.ServiceName, log.HostID)

	partService := getOrCreatePartition(&store.muService, store.byService, log.ServiceName)
	partHost := getOrCreatePartition(&store.muHost, store.byHost, log.HostID)
	partServiceHost := getOrCreatePartition(&store.muServiceHost, store.byServiceHost, serviceHostKey)

	// To handle concurrent inserts into multiple structures independently
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		partService.Insert(log)
	}()
	go func() {
		defer wg.Done()
		partHost.Insert(log)
	}()
	go func() {
		defer wg.Done()
		partServiceHost.Insert(log)
	}()

	wg.Wait()
}

// GetLogsByService: Logs of a given serviceName between time t1 and t2
func (store *InMemoryLogStore) GetLogsByService(serviceName string, t1, t2 int64) []LogEntry {
	store.muService.RLock()
	part, exists := store.byService[serviceName]
	store.muService.RUnlock()

	if !exists {
		return []LogEntry{}
	}
	return part.Query(t1, t2)
}

// GetLogsByServiceAndHost: Logs of a given serviceName from a given hostId between time t1 and t2
func (store *InMemoryLogStore) GetLogsByServiceAndHost(serviceName, hostID string, t1, t2 int64) []LogEntry {
	key := fmt.Sprintf("%s|%s", serviceName, hostID)
	store.muServiceHost.RLock()
	part, exists := store.byServiceHost[key]
	store.muServiceHost.RUnlock()

	if !exists {
		return []LogEntry{}
	}
	return part.Query(t1, t2)
}

// GetLogsByHost: Logs of a given hostId between time t1 and t2
func (store *InMemoryLogStore) GetLogsByHost(hostID string, t1, t2 int64) []LogEntry {
	store.muHost.RLock()
	part, exists := store.byHost[hostID]
	store.muHost.RUnlock()

	if !exists {
		return []LogEntry{}
	}
	return part.Query(t1, t2)
}

func main() {
	store := NewInMemoryLogStore()

	// Simulate concurrent log ingestion
	var wg sync.WaitGroup
	startT := time.Now().Unix()

	fmt.Println("Ingesting logs...")
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			timestamp := startT + int64(id%10) // spread across 10 seconds
			store.Ingest(LogEntry{
				Timestamp:   timestamp,
				ServiceName: "PaymentService",
				HostID:      fmt.Sprintf("Host-%d", id%3),
				LogMessage:  fmt.Sprintf("Processed transaction %d", id),
			})
		}(i)
	}
	wg.Wait()
	fmt.Println("Ingestion complete.")

	// Test queries
	t1 := startT + 2
	t2 := startT + 5

	fmt.Printf("\n--- Logs by Service (PaymentService) between %d and %d ---\n", t1, t2)
	logs := store.GetLogsByService("PaymentService", t1, t2)
	for _, l := range logs {
		fmt.Printf("[%d] %s (%s): %s\n", l.Timestamp, l.ServiceName, l.HostID, l.LogMessage)
	}

	fmt.Printf("\n--- Logs by Service (PaymentService) and Host (Host-1) between %d and %d ---\n", t1, t2)
	logs = store.GetLogsByServiceAndHost("PaymentService", "Host-1", t1, t2)
	for _, l := range logs {
		fmt.Printf("[%d] %s (%s): %s\n", l.Timestamp, l.ServiceName, l.HostID, l.LogMessage)
	}

	fmt.Printf("\n--- Logs by Host (Host-2) between %d and %d ---\n", t1, t2)
	logs = store.GetLogsByHost("Host-2", t1, t2)
	for _, l := range logs {
		fmt.Printf("[%d] %s (%s): %s\n", l.Timestamp, l.ServiceName, l.HostID, l.LogMessage)
	}
}
