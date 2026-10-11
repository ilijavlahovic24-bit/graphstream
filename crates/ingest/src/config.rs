use rdkafka::config::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use std::time::Duration;

/// Kafka consumer configuration.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub fetch_batch_size: usize,
    pub slow_threshold: Duration,
    pub slow_streak_threshold: u32,
    pub cooldown: Duration,
    /// Number of events after which offsets are committed, regardless of
    /// elapsed time.
    pub commit_batch_size: usize,
    /// Maximum time between commits.
    pub commit_interval: Duration,
    /// How often to scan for sealed shards and evict their dedup entries.
    pub evict_interval: Duration,
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
            commit_batch_size: 500,
            commit_interval: Duration::from_secs(1),
            evict_interval: Duration::from_secs(30),
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