package main

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// This benchmark keeps direct posting-list maps for service and host, then
// intersects those posting lists for service+host queries. The streaming run
// generates rows on one goroutine, ingests them into a shared store on another,
// and issues live queries while ingestion is still in flight.
const (
	serviceCount            = 100
	hostCount               = 100
	rowsPerPair             = 100
	totalRows               = serviceCount * hostCount * rowsPerPair
	queryCount              = 10_000
	liveQueryStartAfterRows = 50_000
	entryBufferSize         = 8_192

	fullRangeStart   int64 = 0
	fullRangeEnd     int64 = rowsPerPair - 1
	midRangeStart    int64 = 2
	midRangeEnd      int64 = 6
	narrowRangeStart int64 = 1
	narrowRangeEnd   int64 = 3
)

var errorMessages = [...]string{
	"error: connection timeout",
	"error: upstream returned 502",
	"error: request validation failed",
	"error: circuit breaker opened",
	"error: kafka publish failed",
	"error: database connection refused",
	"error: cache miss on hot key",
	"error: disk write latency spike",
	"error: auth token signature invalid",
	"error: dependency health check failed",
}

type Entry struct {
	Timestamp int64
	Service   string
	Host      string
	Message   string
}

type Store struct {
	entries   []Entry
	byService map[string][]int
	byHost    map[string][]int
}

type SafeStore struct {
	mu    sync.RWMutex
	store *Store
}

func NewStore(capacity int) *Store {
	return &Store{
		entries:   make([]Entry, 0, capacity),
		byService: make(map[string][]int, serviceCount),
		byHost:    make(map[string][]int, hostCount),
	}
}

func NewSafeStore(capacity int) *SafeStore {
	return &SafeStore{store: NewStore(capacity)}
}

func (s *SafeStore) Ingest(entry Entry) {
	s.mu.Lock()
	s.store.Ingest(entry)
	s.mu.Unlock()
}

func (s *SafeStore) QueryByService(service string, t1, t2 int64) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.QueryByService(service, t1, t2)
}

func (s *SafeStore) QueryByHost(host string, t1, t2 int64) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.QueryByHost(host, t1, t2)
}

func (s *SafeStore) QueryByServiceAndHost(service, host string, t1, t2 int64) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.QueryByServiceAndHost(service, host, t1, t2)
}

func (s *SafeStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.store.entries)
}

func (s *SafeStore) RunVerifiedQueries(services, hosts []string) (int, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return runQueries(s.store, services, hosts)
}

func (s *Store) Ingest(entry Entry) {
	idx := len(s.entries)
	s.entries = append(s.entries, entry)
	s.byService[entry.Service] = append(s.byService[entry.Service], idx)
	s.byHost[entry.Host] = append(s.byHost[entry.Host], idx)
}

func (s *Store) query(ids []int, t1, t2 int64) []Entry {
	results := make([]Entry, 0, len(ids))
	for _, idx := range ids {
		entry := s.entries[idx]
		if entry.Timestamp >= t1 && entry.Timestamp <= t2 {
			results = append(results, cloneEntry(entry))
		}
	}
	sortEntries(results)
	return results
}

func cloneEntry(entry Entry) Entry {
	return Entry{
		Timestamp: entry.Timestamp,
		Service:   strings.Clone(entry.Service),
		Host:      strings.Clone(entry.Host),
		Message:   strings.Clone(entry.Message),
	}
}

func sortEntries(entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int {
		switch {
		case a.Timestamp < b.Timestamp:
			return -1
		case a.Timestamp > b.Timestamp:
			return 1
		default:
			return 0
		}
	})
}

func assertNondecreasingTimestamps(entries []Entry) {
	for idx := 1; idx < len(entries); idx++ {
		if entries[idx-1].Timestamp > entries[idx].Timestamp {
			panic("query results must be ordered by nondecreasing timestamp")
		}
	}
}

func (s *Store) QueryByService(service string, t1, t2 int64) []Entry {
	return s.query(s.byService[service], t1, t2)
}

func (s *Store) QueryByHost(host string, t1, t2 int64) []Entry {
	return s.query(s.byHost[host], t1, t2)
}

func (s *Store) QueryByServiceAndHost(service, host string, t1, t2 int64) []Entry {
	serviceIDs := s.byService[service]
	hostIDs := s.byHost[host]
	results := make([]Entry, 0)
	i, j := 0, 0

	for i < len(serviceIDs) && j < len(hostIDs) {
		switch {
		case serviceIDs[i] < hostIDs[j]:
			i++
		case serviceIDs[i] > hostIDs[j]:
			j++
		default:
			entry := s.entries[serviceIDs[i]]
			if entry.Timestamp >= t1 && entry.Timestamp <= t2 {
				results = append(results, cloneEntry(entry))
			}
			i++
			j++
		}
	}

	sortEntries(results)
	return results
}

func buildDimensions() ([]string, []string) {
	services := make([]string, serviceCount)
	for idx := range services {
		services[idx] = fmt.Sprintf("svc-%03d", idx)
	}

	hosts := make([]string, hostCount)
	for idx := range hosts {
		hosts[idx] = fmt.Sprintf("host-%03d", idx)
	}

	return services, hosts
}

func makeLoadEntry(serviceIdx int, service string, hostIdx int, host string, ts int) Entry {
	msgIdx := (serviceIdx*31 + hostIdx*17 + ts) % len(errorMessages)
	return Entry{
		Timestamp: int64(ts),
		Service:   service,
		Host:      host,
		Message: fmt.Sprintf(
			"%s on svc %s host %s at ts %d with trace id abc-def-ghi-jkl-mno-pqr-stu",
			errorMessages[msgIdx],
			service,
			host,
			ts,
		),
	}
}

func mustEqual(label string, got, want int) {
	if got != want {
		panic(fmt.Sprintf("%s: got %d, want %d", label, got, want))
	}
}

func consumeEntries(entries []Entry) uint64 {
	var checksum uint64
	for _, entry := range entries {
		checksum += uint64(entry.Timestamp)
		checksum += uint64(len(entry.Service))
		checksum += uint64(len(entry.Host))
		checksum += uint64(len(entry.Message))
	}
	return checksum
}

func runQueries(store *Store, services, hosts []string) (int, uint64) {
	serviceFullHits := hostCount * rowsPerPair
	serviceMidHits := hostCount * int(midRangeEnd-midRangeStart+1)
	hostFullHits := serviceCount * rowsPerPair
	hostMidHits := serviceCount * int(midRangeEnd-midRangeStart+1)
	pairFullHits := rowsPerPair
	pairNarrowHits := int(narrowRangeEnd - narrowRangeStart + 1)

	totalHits := 0
	expectedTotalHits := 0
	var checksum uint64

	for queryIdx := 0; queryIdx < queryCount; queryIdx++ {
		switch queryIdx % 6 {
		case 0:
			service := services[queryIdx%serviceCount]
			result := store.QueryByService(service, fullRangeStart, fullRangeEnd)
			mustEqual("service full", len(result), serviceFullHits)
			assertNondecreasingTimestamps(result)
			totalHits += len(result)
			expectedTotalHits += serviceFullHits
			checksum += consumeEntries(result)
		case 1:
			service := services[queryIdx%serviceCount]
			result := store.QueryByService(service, midRangeStart, midRangeEnd)
			mustEqual("service mid", len(result), serviceMidHits)
			assertNondecreasingTimestamps(result)
			totalHits += len(result)
			expectedTotalHits += serviceMidHits
			checksum += consumeEntries(result)
		case 2:
			host := hosts[queryIdx%hostCount]
			result := store.QueryByHost(host, fullRangeStart, fullRangeEnd)
			mustEqual("host full", len(result), hostFullHits)
			assertNondecreasingTimestamps(result)
			totalHits += len(result)
			expectedTotalHits += hostFullHits
			checksum += consumeEntries(result)
		case 3:
			host := hosts[queryIdx%hostCount]
			result := store.QueryByHost(host, midRangeStart, midRangeEnd)
			mustEqual("host mid", len(result), hostMidHits)
			assertNondecreasingTimestamps(result)
			totalHits += len(result)
			expectedTotalHits += hostMidHits
			checksum += consumeEntries(result)
		case 4:
			serviceIdx := queryIdx % serviceCount
			hostIdx := (queryIdx * 7) % hostCount
			result := store.QueryByServiceAndHost(
				services[serviceIdx],
				hosts[hostIdx],
				fullRangeStart,
				fullRangeEnd,
			)
			mustEqual("pair full", len(result), pairFullHits)
			assertNondecreasingTimestamps(result)
			totalHits += len(result)
			expectedTotalHits += pairFullHits
			checksum += consumeEntries(result)
		default:
			serviceIdx := queryIdx % serviceCount
			hostIdx := (queryIdx * 7) % hostCount
			result := store.QueryByServiceAndHost(
				services[serviceIdx],
				hosts[hostIdx],
				narrowRangeStart,
				narrowRangeEnd,
			)
			mustEqual("pair narrow", len(result), pairNarrowHits)
			assertNondecreasingTimestamps(result)
			totalHits += len(result)
			expectedTotalHits += pairNarrowHits
			checksum += consumeEntries(result)
		}
	}

	mustEqual("total hits", totalHits, expectedTotalHits)
	return totalHits, checksum
}

func assertLiveQueryResult(entries []Entry, service string, checkService bool, host string, checkHost bool, t1, t2 int64, maxExpected int) (int, uint64) {
	if len(entries) > maxExpected {
		panic(fmt.Sprintf("live query returned too many rows: got %d, max %d", len(entries), maxExpected))
	}

	assertNondecreasingTimestamps(entries)

	for _, entry := range entries {
		if entry.Timestamp < t1 || entry.Timestamp > t2 {
			panic("live query returned an entry outside the requested time range")
		}
		if checkService && entry.Service != service {
			panic("live query returned an entry for the wrong service")
		}
		if checkHost && entry.Host != host {
			panic("live query returned an entry for the wrong host")
		}
	}

	return len(entries), consumeEntries(entries)
}

func runLiveQueries(store *SafeStore, services, hosts []string, ingested *atomic.Uint64, ingestDone *atomic.Bool) (int, uint64) {
	for ingested.Load() < liveQueryStartAfterRows && !ingestDone.Load() {
		time.Sleep(5 * time.Millisecond)
	}

	serviceFullHits := hostCount * rowsPerPair
	serviceMidHits := hostCount * int(midRangeEnd-midRangeStart+1)
	hostFullHits := serviceCount * rowsPerPair
	hostMidHits := serviceCount * int(midRangeEnd-midRangeStart+1)
	pairFullHits := rowsPerPair
	pairNarrowHits := int(narrowRangeEnd - narrowRangeStart + 1)

	totalHits := 0
	var checksum uint64

	for queryIdx := 0; queryIdx < queryCount; queryIdx++ {
		switch queryIdx % 6 {
		case 0:
			service := services[queryIdx%serviceCount]
			result := store.QueryByService(service, fullRangeStart, fullRangeEnd)
			hits, partial := assertLiveQueryResult(
				result,
				service,
				true,
				"",
				false,
				fullRangeStart,
				fullRangeEnd,
				serviceFullHits,
			)
			totalHits += hits
			checksum += partial
		case 1:
			service := services[queryIdx%serviceCount]
			result := store.QueryByService(service, midRangeStart, midRangeEnd)
			hits, partial := assertLiveQueryResult(
				result,
				service,
				true,
				"",
				false,
				midRangeStart,
				midRangeEnd,
				serviceMidHits,
			)
			totalHits += hits
			checksum += partial
		case 2:
			host := hosts[queryIdx%hostCount]
			result := store.QueryByHost(host, fullRangeStart, fullRangeEnd)
			hits, partial := assertLiveQueryResult(
				result,
				"",
				false,
				host,
				true,
				fullRangeStart,
				fullRangeEnd,
				hostFullHits,
			)
			totalHits += hits
			checksum += partial
		case 3:
			host := hosts[queryIdx%hostCount]
			result := store.QueryByHost(host, midRangeStart, midRangeEnd)
			hits, partial := assertLiveQueryResult(
				result,
				"",
				false,
				host,
				true,
				midRangeStart,
				midRangeEnd,
				hostMidHits,
			)
			totalHits += hits
			checksum += partial
		case 4:
			serviceIdx := queryIdx % serviceCount
			hostIdx := (queryIdx * 7) % hostCount
			result := store.QueryByServiceAndHost(
				services[serviceIdx],
				hosts[hostIdx],
				fullRangeStart,
				fullRangeEnd,
			)
			hits, partial := assertLiveQueryResult(
				result,
				services[serviceIdx],
				true,
				hosts[hostIdx],
				true,
				fullRangeStart,
				fullRangeEnd,
				pairFullHits,
			)
			totalHits += hits
			checksum += partial
		default:
			serviceIdx := queryIdx % serviceCount
			hostIdx := (queryIdx * 7) % hostCount
			result := store.QueryByServiceAndHost(
				services[serviceIdx],
				hosts[hostIdx],
				narrowRangeStart,
				narrowRangeEnd,
			)
			hits, partial := assertLiveQueryResult(
				result,
				services[serviceIdx],
				true,
				hosts[hostIdx],
				true,
				narrowRangeStart,
				narrowRangeEnd,
				pairNarrowHits,
			)
			totalHits += hits
			checksum += partial
		}
	}

	return totalHits, checksum
}

func main() {
	services, hosts := buildDimensions()
	store := NewSafeStore(totalRows)
	var ingested atomic.Uint64
	var ingestDone atomic.Bool
	entries := make(chan Entry, entryBufferSize)

	totalStart := time.Now()

	type durationResult struct {
		elapsed time.Duration
	}
	type queryResult struct {
		elapsed   time.Duration
		totalHits int
		checksum  uint64
	}

	generateResult := make(chan durationResult, 1)
	ingestResult := make(chan durationResult, 1)
	liveQueryResult := make(chan queryResult, 1)

	go func() {
		generateStart := time.Now()
		for ts := 0; ts < rowsPerPair; ts++ {
			for serviceIdx, service := range services {
				for hostIdx, host := range hosts {
					entries <- makeLoadEntry(serviceIdx, service, hostIdx, host, ts)
				}
			}
		}
		close(entries)
		generateResult <- durationResult{elapsed: time.Since(generateStart)}
	}()

	go func() {
		ingestStart := time.Now()
		for entry := range entries {
			store.Ingest(entry)
			ingested.Add(1)
		}
		ingestDone.Store(true)
		ingestResult <- durationResult{elapsed: time.Since(ingestStart)}
	}()

	go func() {
		queryStart := time.Now()
		totalHits, checksum := runLiveQueries(store, services, hosts, &ingested, &ingestDone)
		liveQueryResult <- queryResult{
			elapsed:   time.Since(queryStart),
			totalHits: totalHits,
			checksum:  checksum,
		}
	}()

	generateElapsed := (<-generateResult).elapsed
	ingestElapsed := (<-ingestResult).elapsed
	liveQuery := <-liveQueryResult

	totalElapsed := time.Since(totalStart)

	if got := int(ingested.Load()); got != totalRows {
		panic(fmt.Sprintf("ingested rows: got %d, want %d", got, totalRows))
	}
	if got := store.Len(); got != totalRows {
		panic(fmt.Sprintf("store rows: got %d, want %d", got, totalRows))
	}

	verifyStart := time.Now()
	totalHits, checksum := store.RunVerifiedQueries(services, hosts)
	verifyElapsed := time.Since(verifyStart)

	fmt.Printf(
		"go_three_maps_streaming: rows=%d, live_queries=%d, build=%.3fs (%.0f rows/s), ingest=%.3fs (%.0f rows/s), live_query=%.3fs (%.0f q/s), total_streaming=%.3fs, final_verify=%.3fs, live_hits=%d, total_hits=%d\n",
		totalRows,
		queryCount,
		generateElapsed.Seconds(),
		float64(totalRows)/generateElapsed.Seconds(),
		ingestElapsed.Seconds(),
		float64(totalRows)/ingestElapsed.Seconds(),
		liveQuery.elapsed.Seconds(),
		float64(queryCount)/liveQuery.elapsed.Seconds(),
		totalElapsed.Seconds(),
		verifyElapsed.Seconds(),
		liveQuery.totalHits,
		totalHits,
	)
	fmt.Printf("go_three_maps_checksum: live=%d final=%d\n", liveQuery.checksum, checksum)
}
