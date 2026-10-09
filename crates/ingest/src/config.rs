use rdkafka::config::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use std::time::Duration;

/// Kafka consumer configuration.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Comma-separated broker addresses, e.g. `"localhost:9092"`.
    pub brokers: String,
    /// Topic to consume from.
    pub topic: String,
    /// Consumer group id.
    pub group_id: String,
    /// Maximum number of messages to prefetch. Bounds the consumer's
    /// internal queue; a smaller value tightens backpressure at the cost
    /// of throughput.
    pub fetch_batch_size: usize,
    /// If a single event takes longer than this to process, it counts
    /// toward the backpressure streak.
    pub slow_threshold: Duration,
    /// Number of consecutive slow events that triggers a cooldown.
    pub slow_streak_threshold: u32,
    /// How long the consumer sleeps when backpressure is applied.
    pub cooldown: Duration,
}

impl KafkaConfig {
    pub fn new(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            group_id: group_id.into(),
            fetch_batch_size: 500,
            slow_threshold: Duration::from_millis(50),
            slow_streak_threshold: 100,
            cooldown: Duration::from_millis(500),
        }
    }

    /// Build an `rdkafka` `StreamConsumer` from this configuration.
    ///
    /// Auto-commit is disabled — offsets are committed manually after
    /// successful processing.
    pub fn build_consumer(&self) -> Result<StreamConsumer, rdkafka::error::KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("queued.min.messages", self.fetch_batch_size.to_string())
            .create()?;
        Ok(consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let c = KafkaConfig::new("localhost:9092", "graphstream.events", "gs-consumer");
        assert_eq!(c.brokers, "localhost:9092");
        assert_eq!(c.topic, "graphstream.events");
        assert_eq!(c.group_id, "gs-consumer");
        assert!(c.slow_streak_threshold > 0);
    }
}