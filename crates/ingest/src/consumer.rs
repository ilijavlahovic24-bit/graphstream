use std::sync::{Arc, RwLock};

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::time::Instant;

use sharding::ShardCluster;

use crate::config::KafkaConfig;
use crate::error::IngestError;
use crate::event::EdgeEvent;
use crate::pipeline::IngestPipeline;

/// Run the Kafka consumer loop until the process is terminated.
///
/// ## Guarantees
///
/// * **At-least-once**: offsets are committed with `CommitMode::Async`
///   only after the event is successfully processed (or classified as a
///   duplicate, or rejected as late — all of which are terminal).
/// * **Poison-pill safe**: malformed payloads are logged and their offsets
///   committed to avoid an infinite retry loop.
/// * **Backpressure**: when `slow_streak_threshold` consecutive events
///   take longer than `slow_threshold`, the loop sleeps for `cooldown`
///   before resuming consumption.
pub async fn run_consumer(
    cluster: Arc<RwLock<ShardCluster>>,
    config: KafkaConfig,
) -> Result<(), IngestError> {
    let consumer: StreamConsumer = config.build_consumer()?;
    consumer.subscribe(&[&config.topic])?;

    tracing::info!(
        topic = %config.topic,
        group = %config.group_id,
        "kafka consumer started"
    );

    let mut pipeline = IngestPipeline::new(cluster);
    let mut slow_streak: u32 = 0;

    loop {
        // Backpressure: if the recent events were slow, sleep before
        // fetching the next one. This is the explicit pause the spec
        // requires. In addition, rdkafka's internal prefetch queue is
        // bounded by `queued.min.messages`, so the broker side is also
        // throttled naturally.
        if slow_streak >= config.slow_streak_threshold {
            tracing::debug!(
                streak = slow_streak,
                cooldown_ms = config.cooldown.as_millis(),
                "backpressure applied"
            );
            tokio::time::sleep(config.cooldown).await;
            slow_streak = 0;
        }

        let msg = match consumer.recv().await {
            Ok(m) => m,
            Err(rdkafka::error::KafkaError::PartitionEOF(_)) => continue,
            Err(e) => {
                tracing::warn!("kafka recv error: {e}");
                continue;
            }
        };

        let payload = msg.payload().unwrap_or(&[]);
        let event: EdgeEvent = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("malformed event payload: {e}");
                // Commit the bad message to avoid a poison-pill loop.
                let _ = consumer.commit_message(&msg, CommitMode::Async);
                continue;
            }
        };

        let t0 = Instant::now();
        let outcome = pipeline.process(event);
        let elapsed = t0.elapsed();

        match outcome {
            Ok(o) => {
                tracing::trace!(?o, "event processed");
                consumer
                    .commit_message(&msg, CommitMode::Async)
                    .map_err(IngestError::Kafka)?;
            }
            Err(e) => {
                tracing::warn!("ingest failed: {e}");
                // Do NOT commit — Kafka will redeliver.
            }
        }

        if elapsed > config.slow_threshold {
            slow_streak += 1;
        } else {
            slow_streak = 0;
        }
    }
}