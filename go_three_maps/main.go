package main

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// This benchmark keeps direct posting-list maps for service and host, then
// intersects those posting lists for service+host queries. It does not shard,
// but it does sort each query result to better mirror the Rust benchmark's
// returned ordering.
const (
	serviceCount = 100
	hostCount    = 100
	rowsPerPair  = 100
	totalRows    = serviceCount * hostCount * rowsPerPair
	queryCount   = 10_000

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
	Severity  uint8
}

type Store struct {
	entries   []Entry
	byService map[string][]int
	byHost    map[string][]int
}

func NewStore(capacity int) *Store {
	return &Store{
		entries:   make([]Entry, 0, capacity),
		byService: make(map[string][]int, serviceCount),
		byHost:    make(map[string][]int, hostCount),
	}
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
		Severity:  entry.Severity,
	}
}

func sortEntries(entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int {
		switch {
		case a.Timestamp != b.Timestamp:
			if a.Timestamp < b.Timestamp {
				return -1
			}
			return 1
		case a.Host != b.Host:
			if a.Host < b.Host {
				return -1
			}
			return 1
		case a.Service != b.Service:
			if a.Service < b.Service {
				return -1
			}
			return 1
		case a.Message != b.Message:
			if a.Message < b.Message {
				return -1
			}
			return 1
		default:
			return 0
		}
	})
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

func buildWorkload() ([]string, []string, []Entry) {
	services := make([]string, serviceCount)
	for idx := range services {
		services[idx] = fmt.Sprintf("svc-%03d", idx)
	}

	hosts := make([]string, hostCount)
	for idx := range hosts {
		hosts[idx] = fmt.Sprintf("host-%03d", idx)
	}

	entries := make([]Entry, 0, totalRows)
	for serviceIdx, service := range services {
		for hostIdx, host := range hosts {
			for ts := 0; ts < rowsPerPair; ts++ {
				msgIdx := (serviceIdx*31 + hostIdx*17 + ts) % len(errorMessages)
				entries = append(entries, Entry{
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
					Severity:  uint8(ts % 8),
				})
			}
		}
	}

	return services, hosts, entries
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
		checksum += uint64(entry.Severity)
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
			totalHits += len(result)
			expectedTotalHits += serviceFullHits
			checksum += consumeEntries(result)
		case 1:
			service := services[queryIdx%serviceCount]
			result := store.QueryByService(service, midRangeStart, midRangeEnd)
			mustEqual("service mid", len(result), serviceMidHits)
			totalHits += len(result)
			expectedTotalHits += serviceMidHits
			checksum += consumeEntries(result)
		case 2:
			host := hosts[queryIdx%hostCount]
			result := store.QueryByHost(host, fullRangeStart, fullRangeEnd)
			mustEqual("host full", len(result), hostFullHits)
			totalHits += len(result)
			expectedTotalHits += hostFullHits
			checksum += consumeEntries(result)
		case 3:
			host := hosts[queryIdx%hostCount]
			result := store.QueryByHost(host, midRangeStart, midRangeEnd)
			mustEqual("host mid", len(result), hostMidHits)
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
			totalHits += len(result)
			expectedTotalHits += pairNarrowHits
			checksum += consumeEntries(result)
		}
	}

	mustEqual("total hits", totalHits, expectedTotalHits)
	return totalHits, checksum
}

func main() {
	services, hosts, entries := buildWorkload()
	store := NewStore(totalRows)

	totalStart := time.Now()

	ingestStart := time.Now()
	for _, entry := range entries {
		store.Ingest(entry)
	}
	ingestElapsed := time.Since(ingestStart)

	queryStart := time.Now()
	totalHits, checksum := runQueries(store, services, hosts)
	queryElapsed := time.Since(queryStart)

	totalElapsed := time.Since(totalStart)

	fmt.Printf(
		"go_three_maps: rows=%d, queries=%d, ingest=%.3fs (%.0f rows/s), queries=%.3fs (%.0f q/s), total=%.3fs, total_hits=%d\n",
		totalRows,
		queryCount,
		ingestElapsed.Seconds(),
		float64(totalRows)/ingestElapsed.Seconds(),
		queryElapsed.Seconds(),
		float64(queryCount)/queryElapsed.Seconds(),
		totalElapsed.Seconds(),
		totalHits,
	)
	fmt.Printf("go_three_maps_checksum: %d\n", checksum)
}
