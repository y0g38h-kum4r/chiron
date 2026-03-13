use chiron::chiron::ChironStore;
use chiron::log_entry::LogEntry;

fn main() {
    println!("=== ChironVision Log Buffer Demo ===\n");

    let mut store = ChironStore::new(50_000);

    let services = ["AuthService", "PaymentService", "OrderService", "NotifService"];
    let hosts = ["host-0", "host-1", "host-2", "host-3", "host-4"];

    // --- Ingest ---
    println!("Ingesting 5000 log entries...");
    for i in 0..5000 {
        let ts = i % 1000;
        store.ingest(LogEntry {
            timestamp: ts,
            service_name: services[i as usize % services.len()].to_string(),
            host_id: hosts[i as usize % hosts.len()].to_string(),
            message: format!("event-{}", i),
            severity: (i % 5) as u8,
        });
    }
    println!("Ingestion complete. Indexer lag: {}\n", store.indexer_lag());

    // --- Flush indexer (in production, this runs on a dedicated thread) ---
    store.flush_indexer();
    println!("Indexer flushed. Lag: {}\n", store.indexer_lag());

    // --- Query (a): service in time range ---
    println!("--- Query (a): AuthService logs in [100, 200] ---");
    let result = store.query_by_service("AuthService", 100, 200);
    println!("  Found {} entries", result.entries.len());
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
    let result = store.query_by_host("host-1", 200, 400);
    println!("  Found {} entries", result.entries.len());
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
    println!("\n--- Query (c): AuthService on host-0 in [100, 300] ---");
    let result = store.query_by_service_and_host("AuthService", "host-0", 100, 300);
    println!("  Found {} entries", result.entries.len());
    for entry in result.entries.iter().take(5) {
        println!(
            "  [t={}] {} ({}): {}",
            entry.timestamp, entry.service_name, entry.host_id, entry.message
        );
    }
    if result.entries.len() > 5 {
        println!("  ... and {} more", result.entries.len() - 5);
    }

    // --- Maintenance tick ---
    println!("\nRunning maintenance tick...");
    store.tick();
    println!(
        "  Buffer: {}/{} entries",
        store.len(),
        store.capacity()
    );

    println!("\n=== Demo complete ===");
}
