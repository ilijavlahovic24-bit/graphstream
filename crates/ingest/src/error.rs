use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("shard error: {0}")]
    Shard(#[from] sharding::ShardError),

    #[error("invalid event payload: {0}")]
    Payload(#[from] serde_json::Error),
}