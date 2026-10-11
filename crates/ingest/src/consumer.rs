use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{Message};
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
/// * **At-least-once**: offsets are committed only after the corresponding
///   events have been successfully processed (or classified as terminal:
///   duplicate / late / malformed).
/// * **Batched commits**: offsets are accumulated per partition and
///   committed when either `commit_batch_size` events have been processed
///   or `commit_interval` has elapsed since the last commit. On crash,
///   at most one batch is reprocessed — the dedup set handles it.
/// * **Dedup eviction**: periodically, sealed shards are identified and
///   their event IDs are evicted from the dedup set.
/// * **Poison-pill safe**: malformed payloads are logged and skipped.
/// * **Backpressure**: after `slow_streak_threshold` consecutive slow
///   events, the loop sleeps for `cooldown`.
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

    // Batched commit state.
    let mut pending_offsets: HashMap<(String, i32), i64> = HashMap::new();
    let mut pending_count: usize = 0;
    let mut last_commit = Instant::now();
    let mut last_evict = Instant::now();

    loop {
        if slow_streak >= config.slow_streak_threshold {
            tracing::debug!(streak = slow_streak, "backpressure applied");
            tokio::time::sleep(config.cooldown).await;
            slow_streak = 0;
        }

        // Commit in batches: either enough events, or enough time has passed.
        if pending_count >= config.commit_batch_size
            || last_commit.elapsed() >= config.commit_interval
        {
            if pending_count > 0 {
                commit_offsets(&consumer, &pending_offsets)?;
                pending_offsets.clear();
                pending_count = 0;
                last_commit = Instant::now();
            }
        }

        // Periodic dedup eviction.
        if last_evict.elapsed() >= config.evict_interval {
            let evicted = pipeline.evict_sealed();
            if evicted > 0 {
                tracing::info!(evicted, "evicted dedup entries for sealed shards");
            }
            last_evict = Instant::now();
        }

        let msg = match consumer.recv().await {
            Ok(m) => m,
            Err(rdkafka::error::KafkaError::PartitionEOF(_)) => continue,
            Err(e) => {
                tracing::warn!("kafka recv error: {e}");
                continue;
            }
        };

        let partition = msg.partition();
        let offset = msg.offset();
        let topic = msg.topic().to_string();

        let payload = msg.payload().unwrap_or(&[]);
        let event: EdgeEvent = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("malformed event payload: {e}");
                record_offset(&mut pending_offsets, &mut pending_count, &topic, partition, offset);
                continue;
            }
        };

        let t0 = Instant::now();
        let outcome = pipeline.process(event);
        let elapsed = t0.elapsed();

        match outcome {
            Ok(_) => {
                record_offset(&mut pending_offsets, &mut pending_count, &topic, partition, offset);
            }
            Err(e) => {
                // Do NOT record the offset; on next poll this message
                // may be redelivered. To avoid a tight loop on a
                // persistent error we still sleep briefly.
                tracing::warn!("ingest failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }

        if elapsed > config.slow_threshold {
            slow_streak += 1;
        } else {
            slow_streak = 0;
        }
    }
}

fn record_offset(
    pending: &mut HashMap<(String, i32), i64>,
    count: &mut usize,
    topic: &str,
    partition: i32,
    offset: i64,
) {
    // Keep the highest offset seen per partition. Offset is the offset of
    // the message just processed; the commit should be `offset + 1`
    // (Kafka semantics: commit the *next* offset to consume).
    let key = (topic.to_string(), partition);
    let entry = pending.entry(key).or_insert(offset + 1);
    if offset + 1 > *entry {
        *entry = offset + 1;
    }
    *count += 1;
}

fn commit_offsets(
    consumer: &StreamConsumer,
    pending: &HashMap<(String, i32), i64>,
) -> Result<(), IngestError> {
    let mut tpl = TopicPartitionList::new();
    for ((topic, partition), offset) in pending {
        tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset))
            .map_err(IngestError::Kafka)?;
    }
    consumer
        .commit(&tpl, CommitMode::Async)
        .map_err(IngestError::Kafka)?;
    Ok(())
}