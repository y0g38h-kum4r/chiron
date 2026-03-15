package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// This benchmark keeps direct posting-list maps for service and host, then
// intersects those posting lists for service+host queries. The store-only run
// builds the full dataset eagerly, then executes the exact verified query mix
// used by the Rust benchmark binary.
const (
	serviceCount            = 100
	hostCount               = 100
	rowsPerPair             = 100
	totalRows               = serviceCount * hostCount * rowsPerPair
	queryCount              = 10_000
	queryRepeatsDefault     = 5
	queryPatternLen         = 20
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

type QueryBreakdown struct {
	Queries  int
	Hits     int
	Checksum uint64
	Elapsed  time.Duration
}

type QueryStats struct {
	TotalHits int
	Checksum  uint64
	Service   QueryBreakdown
	Host      QueryBreakdown
	Pair      QueryBreakdown
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
	stats := runQueriesDetailed(s.store, services, hosts)
	return stats.TotalHits, stats.Checksum
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
	// Go strings are immutable headers over shared backing bytes, so a plain
	// struct copy mirrors Rust's Arc-backed hot-path comparison more closely
	// than forcing a deep string clone here.
	return entry
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
	stats := runQueriesDetailed(store, services, hosts)
	return stats.TotalHits, stats.Checksum
}

func runQueriesDetailed(store *Store, services, hosts []string) QueryStats {
	serviceFullHits := hostCount * rowsPerPair
	serviceMidHits := hostCount * int(midRangeEnd-midRangeStart+1)
	hostFullHits := serviceCount * rowsPerPair
	hostMidHits := serviceCount * int(midRangeEnd-midRangeStart+1)
	pairFullHits := rowsPerPair
	pairNarrowHits := int(narrowRangeEnd - narrowRangeStart + 1)

	expectedTotalHits := 0
	var stats QueryStats

	for queryIdx := 0; queryIdx < queryCount; queryIdx++ {
		switch queryIdx % queryPatternLen {
		case 0:
			service := services[queryIdx%serviceCount]
			started := time.Now()
			result := store.QueryByService(service, fullRangeStart, fullRangeEnd)
			elapsed := time.Since(started)
			mustEqual("service full", len(result), serviceFullHits)
			assertNondecreasingTimestamps(result)
			expectedTotalHits += serviceFullHits
			checksum := consumeEntries(result)
			stats.TotalHits += len(result)
			stats.Checksum += checksum
			stats.Service = accumulateBreakdown(stats.Service, QueryBreakdown{
				Queries:  1,
				Hits:     len(result),
				Checksum: checksum,
				Elapsed:  elapsed,
			})
		case 1:
			service := services[queryIdx%serviceCount]
			started := time.Now()
			result := store.QueryByService(service, midRangeStart, midRangeEnd)
			elapsed := time.Since(started)
			mustEqual("service mid", len(result), serviceMidHits)
			assertNondecreasingTimestamps(result)
			expectedTotalHits += serviceMidHits
			checksum := consumeEntries(result)
			stats.TotalHits += len(result)
			stats.Checksum += checksum
			stats.Service = accumulateBreakdown(stats.Service, QueryBreakdown{
				Queries:  1,
				Hits:     len(result),
				Checksum: checksum,
				Elapsed:  elapsed,
			})
		case 2, 4, 6:
			serviceIdx := queryIdx % serviceCount
			hostIdx := (queryIdx * 7) % hostCount
			started := time.Now()
			result := store.QueryByServiceAndHost(
				services[serviceIdx],
				hosts[hostIdx],
				fullRangeStart,
				fullRangeEnd,
			)
			elapsed := time.Since(started)
			mustEqual("pair full", len(result), pairFullHits)
			assertNondecreasingTimestamps(result)
			expectedTotalHits += pairFullHits
			checksum := consumeEntries(result)
			stats.TotalHits += len(result)
			stats.Checksum += checksum
			stats.Pair = accumulateBreakdown(stats.Pair, QueryBreakdown{
				Queries:  1,
				Hits:     len(result),
				Checksum: checksum,
				Elapsed:  elapsed,
			})
		case 3, 5, 7:
			serviceIdx := queryIdx % serviceCount
			hostIdx := (queryIdx * 7) % hostCount
			started := time.Now()
			result := store.QueryByServiceAndHost(
				services[serviceIdx],
				hosts[hostIdx],
				narrowRangeStart,
				narrowRangeEnd,
			)
			elapsed := time.Since(started)
			mustEqual("pair narrow", len(result), pairNarrowHits)
			assertNondecreasingTimestamps(result)
			expectedTotalHits += pairNarrowHits
			checksum := consumeEntries(result)
			stats.TotalHits += len(result)
			stats.Checksum += checksum
			stats.Pair = accumulateBreakdown(stats.Pair, QueryBreakdown{
				Queries:  1,
				Hits:     len(result),
				Checksum: checksum,
				Elapsed:  elapsed,
			})
		case 8, 10, 12, 14, 16, 18:
			host := hosts[queryIdx%hostCount]
			started := time.Now()
			result := store.QueryByHost(host, fullRangeStart, fullRangeEnd)
			elapsed := time.Since(started)
			mustEqual("host full", len(result), hostFullHits)
			assertNondecreasingTimestamps(result)
			expectedTotalHits += hostFullHits
			checksum := consumeEntries(result)
			stats.TotalHits += len(result)
			stats.Checksum += checksum
			stats.Host = accumulateBreakdown(stats.Host, QueryBreakdown{
				Queries:  1,
				Hits:     len(result),
				Checksum: checksum,
				Elapsed:  elapsed,
			})
		default:
			host := hosts[queryIdx%hostCount]
			started := time.Now()
			result := store.QueryByHost(host, midRangeStart, midRangeEnd)
			elapsed := time.Since(started)
			mustEqual("host mid", len(result), hostMidHits)
			assertNondecreasingTimestamps(result)
			expectedTotalHits += hostMidHits
			checksum := consumeEntries(result)
			stats.TotalHits += len(result)
			stats.Checksum += checksum
			stats.Host = accumulateBreakdown(stats.Host, QueryBreakdown{
				Queries:  1,
				Hits:     len(result),
				Checksum: checksum,
				Elapsed:  elapsed,
			})
		}
	}

	mustEqual("total hits", stats.TotalHits, expectedTotalHits)
	return stats
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
	repeats := queryRepeatsDefault
	if raw := os.Getenv("CHIRON_BENCH_QUERY_REPEATS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			repeats = parsed
		}
	}

	services, hosts := buildDimensions()
	store := NewStore(totalRows)

	totalStart := time.Now()
	buildStart := time.Now()

	for ts := 0; ts < rowsPerPair; ts++ {
		for serviceIdx, service := range services {
			for hostIdx, host := range hosts {
				store.Ingest(makeLoadEntry(serviceIdx, service, hostIdx, host, ts))
			}
		}
	}
	buildElapsed := time.Since(buildStart)

	if got := len(store.entries); got != totalRows {
		panic(fmt.Sprintf("store rows: got %d, want %d", got, totalRows))
	}

	var indexElapsed time.Duration
	queryStart := time.Now()
	var stats QueryStats
	for range repeats {
		stats = accumulateStats(stats, runQueriesDetailed(store, services, hosts))
	}
	queryElapsed := time.Since(queryStart)
	totalElapsed := time.Since(totalStart)

	fmt.Printf(
		"store_only_bench: impl=go store_shards=1 rows=%d, queries_per_pass=%d, repeats=%d, build=%.3fs, index=%.3fs, query=%.3fs, total=%.3fs, total_hits=%d, checksum=%d\n",
		totalRows,
		queryCount,
		repeats,
		buildElapsed.Seconds(),
		indexElapsed.Seconds(),
		queryElapsed.Seconds(),
		totalElapsed.Seconds(),
		stats.TotalHits,
		stats.Checksum,
	)
	printQueryBreakdown("go", stats)
}

func accumulateBreakdown(current, delta QueryBreakdown) QueryBreakdown {
	current.Queries += delta.Queries
	current.Hits += delta.Hits
	current.Checksum += delta.Checksum
	current.Elapsed += delta.Elapsed
	return current
}

func accumulateStats(current, delta QueryStats) QueryStats {
	current.TotalHits += delta.TotalHits
	current.Checksum += delta.Checksum
	current.Service = accumulateBreakdown(current.Service, delta.Service)
	current.Host = accumulateBreakdown(current.Host, delta.Host)
	current.Pair = accumulateBreakdown(current.Pair, delta.Pair)
	return current
}

func printQueryBreakdown(implementation string, stats QueryStats) {
	fmt.Printf(
		"store_only_breakdown: impl=%s type=service queries=%d hits=%d checksum=%d time=%.3fs\n",
		implementation,
		stats.Service.Queries,
		stats.Service.Hits,
		stats.Service.Checksum,
		stats.Service.Elapsed.Seconds(),
	)
	fmt.Printf(
		"store_only_breakdown: impl=%s type=host queries=%d hits=%d checksum=%d time=%.3fs\n",
		implementation,
		stats.Host.Queries,
		stats.Host.Hits,
		stats.Host.Checksum,
		stats.Host.Elapsed.Seconds(),
	)
	fmt.Printf(
		"store_only_breakdown: impl=%s type=service_and_host queries=%d hits=%d checksum=%d time=%.3fs\n",
		implementation,
		stats.Pair.Queries,
		stats.Pair.Hits,
		stats.Pair.Checksum,
		stats.Pair.Elapsed.Seconds(),
	)
}
