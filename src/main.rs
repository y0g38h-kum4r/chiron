use std::path::Path;

use chiron::chiron::ChironStore;
use chiron::pipeline::{run_pipeline, PipelineConfig};

fn main() {
    println!("=== ChironVision Log Buffer — Real Kafka Pipeline Demo ===\n");

    // --- Configure the pipeline ---
    let services: Vec<String> = vec![
        "AuthService",
        "PaymentService",
        "OrderService",
        "NotifService",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let hosts: Vec<String> = (0..5).map(|i| format!("host-{}", i)).collect();

    let logs_per_producer = 500;
    let num_producers = services.len() * hosts.len();
    let total_expected = num_producers as u64 * logs_per_producer;

    let config = PipelineConfig::local(services.clone(), hosts.clone(), logs_per_producer);
    let num_partitions = config.num_partitions;

    println!("Configuration:");
    println!("  Brokers:           {}", config.brokers);
    println!("  Topic:             {}", config.topic);
    println!("  Services:          {:?}", config.services);
    println!("  Hosts:             {:?}", config.hosts);
    println!("  Kafka partitions:  {}", config.num_partitions);
    println!("  Consumer group:    {}", config.consumer_group);
    println!("  Producers:         {} (one per service×host)", num_producers);
    println!("  Logs per producer: {}", logs_per_producer);
    println!("  Total expected:    {}", total_expected);
    println!();

    // --- Run the concurrent pipeline ---
    println!("Starting pipeline (ensure Kafka is running: docker compose up -d)...");
    let (store, stats) = run_pipeline(config).expect("pipeline run failed");

    println!("Pipeline complete!");
    println!(
        "  Produced: {} entries in {:?}",
        stats.total_produced, stats.produce_duration
    );
    println!(
        "  Consumed: {} entries in {:?}",
        stats.total_consumed, stats.consume_duration
    );
    println!("  End-to-end pipeline time: {:?}", stats.pipeline_duration);
    println!(
        "  Background index flushes: {}, max observed lag: {}",
        stats.index_flushes, stats.max_indexer_lag
    );
    println!();

    // --- Show Kafka consumer offsets ---
    println!("Kafka consumer offsets:");
    let mut printed_offsets = false;
    let mut topics: Vec<_> = stats.kafka_offsets.inner().iter().collect();
    topics.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (topic, partitions) in topics {
        let mut ordered_partitions: Vec<_> = partitions.iter().collect();
        ordered_partitions.sort_by_key(|(partition, _)| **partition);
        for (partition, offset) in ordered_partitions {
            println!("  {topic}/partition-{partition}: offset {offset}");
            printed_offsets = true;
        }
    }
    if !printed_offsets {
        for p in 0..num_partitions {
            let off = stats.kafka_offsets.get("chiron-logs", p).unwrap_or(0);
            println!("  chiron-logs/partition-{}: offset {}", p, off);
        }
    }
    println!();

    // --- Query the store ---
    {
        let s = store.lock().unwrap();
        println!(
            "Buffer: {}/{} entries, indexer lag: {}\n",
            s.len(),
            s.capacity(),
            s.indexer_lag()
        );

        // Query (a): by service
        println!("--- Query (a): AuthService logs ---");
        let result = s.query_by_service("AuthService", 0, i64::MAX);
        println!("  Found {} entries", result.entries.len());
        for entry in result.entries.iter().take(3) {
            println!(
                "  [t={}] {} ({}): {}",
                entry.timestamp, entry.service_name, entry.host_id, entry.message
            );
        }
        if result.entries.len() > 3 {
            println!("  ... and {} more", result.entries.len() - 3);
        }

        // Query (b): by host
        println!("\n--- Query (b): host-2 logs ---");
        let result = s.query_by_host("host-2", 0, i64::MAX);
        println!("  Found {} entries", result.entries.len());
        for entry in result.entries.iter().take(3) {
            println!(
                "  [t={}] {} ({}): {}",
                entry.timestamp, entry.service_name, entry.host_id, entry.message
            );
        }
        if result.entries.len() > 3 {
            println!("  ... and {} more", result.entries.len() - 3);
        }

        // Query (c): by service + host
        println!("\n--- Query (c): AuthService on host-0 ---");
        let result = s.query_by_service_and_host("AuthService", "host-0", 0, i64::MAX);
        println!("  Found {} entries", result.entries.len());
        for entry in result.entries.iter().take(3) {
            println!(
                "  [t={}] {} ({}): {}",
                entry.timestamp, entry.service_name, entry.host_id, entry.message
            );
        }
        if result.entries.len() > 3 {
            println!("  ... and {} more", result.entries.len() - 3);
        }
    }

    // --- Snapshot with kafka offsets ---
    let snap_path = Path::new("/tmp/chiron_pipeline.snap");
    println!("\n--- Snapshot ---");
    {
        let s = store.lock().unwrap();
        s.save_snapshot(snap_path, &stats.kafka_offsets).unwrap();
    }
    println!("  Saved to {:?}", snap_path);

    // Restore and verify
    let (restored, restored_offsets) = ChironStore::from_snapshot(snap_path).unwrap();
    let result = restored.query_by_service("AuthService", 0, i64::MAX);
    println!(
        "  Restored: {}/{} entries, AuthService query: {} results",
        restored.len(),
        restored.capacity(),
        result.entries.len()
    );
    println!(
        "  Kafka offsets restored: chiron-logs/0={}, chiron-logs/1={}",
        restored_offsets.get("chiron-logs", 0).unwrap_or(0),
        restored_offsets.get("chiron-logs", 1).unwrap_or(0),
    );

    std::fs::remove_file(snap_path).ok();

    println!("\n=== Demo complete ===");
}
