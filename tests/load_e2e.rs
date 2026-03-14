//! Load-style end-to-end integration test for the in-memory store.
//!
//! Runs 1M ingests, flushes the indexer, then executes 10k deterministic
//! queries and prints the timing breakdown.
//!
//! Run with:
//! cargo test --test load_e2e store_1m_ingest_and_10k_queries -- --ignored --nocapture

use std::time::Instant;

use chiron::chiron::ChironStore;
use chiron::log_entry::LogEntry;

const SERVICE_COUNT: usize = 100;
const HOST_COUNT: usize = 100;
const ROWS_PER_PAIR: usize = 100;
const TOTAL_ROWS: usize = SERVICE_COUNT * HOST_COUNT * ROWS_PER_PAIR;
const QUERY_COUNT: usize = 10_000;
const SHARD_COUNT: usize = 8;

const FULL_RANGE_START: i64 = 0;
const FULL_RANGE_END: i64 = ROWS_PER_PAIR as i64 - 1;
const MID_RANGE_START: i64 = 2;
const MID_RANGE_END: i64 = 6;
const MID_RANGE_COUNT: usize = (MID_RANGE_END - MID_RANGE_START + 1) as usize;
const NARROW_RANGE_START: i64 = 1;
const NARROW_RANGE_END: i64 = 3;
const NARROW_RANGE_COUNT: usize = (NARROW_RANGE_END - NARROW_RANGE_START + 1) as usize;

fn make_entry(ts: i64, service: &str, host: &str) -> LogEntry {
    LogEntry {
        timestamp: ts,
        service_name: service.to_string(),
        host_id: host.to_string(),
        message: "m".to_string(),
        severity: (ts % 8) as u8,
    }
}

#[test]
#[ignore = "expensive load-style test"]
fn store_1m_ingest_and_10k_queries() {
    let services: Vec<String> = (0..SERVICE_COUNT)
        .map(|idx| format!("svc-{idx:03}"))
        .collect();
    let hosts: Vec<String> = (0..HOST_COUNT)
        .map(|idx| format!("host-{idx:03}"))
        .collect();

    let mut dataset = Vec::with_capacity(TOTAL_ROWS);
    for service in &services {
        for host in &hosts {
            for ts in 0..ROWS_PER_PAIR {
                dataset.push(make_entry(ts as i64, service, host));
            }
        }
    }

    let mut store = ChironStore::with_shards(TOTAL_ROWS, SHARD_COUNT);

    let total_start = Instant::now();

    let ingest_start = Instant::now();
    for entry in dataset {
        store.ingest(entry);
    }
    let ingest_elapsed = ingest_start.elapsed();

    assert_eq!(store.len(), TOTAL_ROWS);

    let flush_start = Instant::now();
    store.flush_indexer();
    let flush_elapsed = flush_start.elapsed();

    assert_eq!(store.indexer_lag(), 0);

    let service_full_hits = HOST_COUNT * ROWS_PER_PAIR;
    let service_mid_hits = HOST_COUNT * MID_RANGE_COUNT;
    let host_full_hits = SERVICE_COUNT * ROWS_PER_PAIR;
    let host_mid_hits = SERVICE_COUNT * MID_RANGE_COUNT;
    let pair_full_hits = ROWS_PER_PAIR;
    let pair_narrow_hits = NARROW_RANGE_COUNT;

    let query_start = Instant::now();
    let mut total_hits = 0usize;
    let mut expected_total_hits = 0usize;

    for query_idx in 0..QUERY_COUNT {
        match query_idx % 6 {
            0 => {
                let service = &services[query_idx % SERVICE_COUNT];
                let result = store.query_by_service(service, FULL_RANGE_START, FULL_RANGE_END);
                assert_eq!(result.entries.len(), service_full_hits);
                total_hits += result.entries.len();
                expected_total_hits += service_full_hits;
            }
            1 => {
                let service = &services[query_idx % SERVICE_COUNT];
                let result = store.query_by_service(service, MID_RANGE_START, MID_RANGE_END);
                assert_eq!(result.entries.len(), service_mid_hits);
                total_hits += result.entries.len();
                expected_total_hits += service_mid_hits;
            }
            2 => {
                let host = &hosts[query_idx % HOST_COUNT];
                let result = store.query_by_host(host, FULL_RANGE_START, FULL_RANGE_END);
                assert_eq!(result.entries.len(), host_full_hits);
                total_hits += result.entries.len();
                expected_total_hits += host_full_hits;
            }
            3 => {
                let host = &hosts[query_idx % HOST_COUNT];
                let result = store.query_by_host(host, MID_RANGE_START, MID_RANGE_END);
                assert_eq!(result.entries.len(), host_mid_hits);
                total_hits += result.entries.len();
                expected_total_hits += host_mid_hits;
            }
            4 => {
                let service = &services[query_idx % SERVICE_COUNT];
                let host = &hosts[(query_idx * 7) % HOST_COUNT];
                let result = store.query_by_service_and_host(
                    service,
                    host,
                    FULL_RANGE_START,
                    FULL_RANGE_END,
                );
                assert_eq!(result.entries.len(), pair_full_hits);
                total_hits += result.entries.len();
                expected_total_hits += pair_full_hits;
            }
            _ => {
                let service = &services[query_idx % SERVICE_COUNT];
                let host = &hosts[(query_idx * 7) % HOST_COUNT];
                let result = store.query_by_service_and_host(
                    service,
                    host,
                    NARROW_RANGE_START,
                    NARROW_RANGE_END,
                );
                assert_eq!(result.entries.len(), pair_narrow_hits);
                total_hits += result.entries.len();
                expected_total_hits += pair_narrow_hits;
            }
        }
    }
    let query_elapsed = query_start.elapsed();
    let total_elapsed = total_start.elapsed();

    assert_eq!(total_hits, expected_total_hits);

    eprintln!(
        "load_e2e: rows={TOTAL_ROWS}, queries={QUERY_COUNT}, shards={SHARD_COUNT}, \
ingest={:.3}s ({:.0} rows/s), flush={:.3}s, queries={:.3}s ({:.0} q/s), total={:.3}s, total_hits={total_hits}",
        ingest_elapsed.as_secs_f64(),
        TOTAL_ROWS as f64 / ingest_elapsed.as_secs_f64(),
        flush_elapsed.as_secs_f64(),
        query_elapsed.as_secs_f64(),
        QUERY_COUNT as f64 / query_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64(),
    );
}
