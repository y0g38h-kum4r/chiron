use chiron::pipeline::{PipelineConfig, run_pipeline};

fn main() {
    let config = PipelineConfig::from_env();

    println!(
        "Starting Chiron pipeline: brokers={} topic={} partitions={} capacity={} consumer_group={} consumer_threads={} batch_size={} poll_timeout_ms={} idle_timeout_ms={}",
        config.brokers,
        config.topic,
        config.num_partitions,
        config.capacity,
        config.consumer_group,
        config.consumer_threads,
        config.consumer_batch_size,
        config.consumer_poll_timeout.as_millis(),
        config.consumer_idle_timeout.as_millis()
    );

    match run_pipeline(config) {
        Ok((store, stats)) => {
            println!(
                "Pipeline complete: consumed={} store_len={} capacity={} consume_time_ms={} pipeline_time_ms={}",
                stats.total_consumed,
                store.len(),
                store.capacity(),
                stats.consume_duration.as_millis(),
                stats.pipeline_duration.as_millis()
            );

            let mut topics: Vec<_> = stats.kafka_offsets.inner().iter().collect();
            topics.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (topic, partitions) in topics {
                let mut ordered_partitions: Vec<_> = partitions.iter().collect();
                ordered_partitions.sort_by_key(|(partition, _)| **partition);
                for (partition, offset) in ordered_partitions {
                    println!("Committed offset: {topic}/partition-{partition} -> {offset}");
                }
            }
        }
        Err(err) => {
            eprintln!("Pipeline failed: {err}");
            std::process::exit(1);
        }
    }
}
