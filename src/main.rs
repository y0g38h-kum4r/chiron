use chiron::chiron::ChironStore;
use chiron::log_entry::LogEntry;

fn main() {
    println!("=== ChironVision Log Buffer Demo ===\n");

    // 4 cores, time range [0, 1000), 10k entries per shard.
    let mut store = ChironStore::new(4, 1000, 10_000);

    // --- Ingest logs across services and hosts ---
    let services = ["AuthService", "PaymentService", "OrderService", "NotifService"];
    let hosts = ["host-0", "host-1", "host-2", "host-3", "host-4"];

    println!("Ingesting 5000 log entries...");
    for i in 0..5000 {
        let ts = i % 1000;
        let svc = services[i as usize % services.len()];
        let host = hosts[i as usize % hosts.len()];
        store.ingest(LogEntry {
            timestamp: ts,
            service_name: svc.to_string(),
            host_id: host.to_string(),
            message: format!("event-{}", i),
            severity: (i % 5) as u8,
        });
    }
    println!("Ingestion complete.\n");

    // --- Query (a): service in time range ---
    println!("--- Query (a): AuthService logs in [100, 200] ---");
    let result = store
        .query_by_service("AuthService", 100, 200, 1.0)
        .unwrap();
    println!(
        "  Found {} entries across {} shard(s)",
        result.entries.len(),
        result.shards_queried
    );
    for entry in result.entries.iter().take(5) {
        println!(
            "  [t={}] {} ({}): {}",
            entry.timestamp, entry.service_name, entry.host_id, entry.message
        );
    }
    if result.entries.len() > 5 {
        println!("  ... and {} more", result.entries.len() - 5);
    }

    // --- Query (b): host in time range ---
    println!("\n--- Query (b): host-1 logs in [200, 400] ---");
    let result = store
        .query_by_host("host-1", 200, 400, 2.0)
        .unwrap();
    println!(
        "  Found {} entries across {} shard(s)",
        result.entries.len(),
        result.shards_queried
    );
    for entry in result.entries.iter().take(5) {
        println!(
            "  [t={}] {} ({}): {}",
            entry.timestamp, entry.service_name, entry.host_id, entry.message
        );
    }
    if result.entries.len() > 5 {
        println!("  ... and {} more", result.entries.len() - 5);
    }

    // --- Query (c): service + host in time range ---
    // i%4==0 → AuthService, i%5==0 → host-0, so i%20==0 entries match both.
    println!("\n--- Query (c): AuthService on host-0 in [100, 300] ---");
    let result = store
        .query_by_service_and_host("AuthService", "host-0", 100, 300, 3.0)
        .unwrap();
    println!(
        "  Found {} entries across {} shard(s)",
        result.entries.len(),
        result.shards_queried
    );
    for entry in result.entries.iter().take(5) {
        println!(
            "  [t={}] {} ({}): {}",
            entry.timestamp, entry.service_name, entry.host_id, entry.message
        );
    }
    if result.entries.len() > 5 {
        println!("  ... and {} more", result.entries.len() - 5);
    }

    // --- Tick: maintenance cycle ---
    println!("Running maintenance tick...");
    store.tick(5.0);
    println!(
        "  Active shards: {}, vnodes: {}",
        store.ring.shards.len(),
        store.ring.vnodes.len()
    );

    println!("\n=== Demo complete ===");
}
