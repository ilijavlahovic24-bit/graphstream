use chrono::NaiveDateTime;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShardError {
    /// The event's `start_time` does not belong to this shard's window.
    #[error("timestamp {0} is outside this shard's window")]
    NoShard(NaiveDateTime),

    /// The event is beyond the shard's allowed-lateness grace period.
    #[error("event too late: ts={ts}, watermark={watermark}, allowed={allowed:?}")]
    TooLate {
        ts: NaiveDateTime,
        watermark: NaiveDateTime,
        allowed: chrono::Duration,
    },

    /// A graph operation failed (e.g. unknown endpoint node).
    #[error("graph error: {0}")]
    Graph(#[from] temporal_graph::GraphError),

    /// Shard or cluster configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
}