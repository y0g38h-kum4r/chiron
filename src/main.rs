use chiron::pipeline::{PipelineConfig, run_pipeline};

fn main() {
    let services: Vec<String> = vec![
        "AuthService",
        "PaymentService",
        "OrderService",
        "NotifService",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let hosts: Vec<String> = (0..5).map(|i| format!("host-{i}")).collect();

    let config = PipelineConfig::local(services, hosts, 500);

    println!(
        "Starting Chiron pipeline: brokers={} topic={} partitions={} consumer_group={}",
        config.brokers, config.topic, config.num_partitions, config.consumer_group
    );

    match run_pipeline(config) {
        Ok((store, stats)) => {
            println!(
                "Pipeline complete: produced={} consumed={} store_len={} capacity={} pipeline_time_ms={}",
                stats.total_produced,
                stats.total_consumed,
                store.len(),
                store.capacity(),
                stats.pipeline_duration.as_millis()
            );
            println!(
                "Indexer stats: flushes={} max_lag={}",
                stats.index_flushes, stats.max_indexer_lag
            );
        }
        Err(err) => {
            eprintln!("Pipeline failed: {err}");
            std::process::exit(1);
        }
    }
}
