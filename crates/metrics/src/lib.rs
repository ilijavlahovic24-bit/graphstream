//! # metrics
//!
//! Prometheus-compatible metrics for GraphStream.
//!
//! ## Exposed metrics
//!
//! | Metric |                                  Type    | Labels | Description |
//! |-------------------------------------------| ------|--------|-------------|
//! | `graphstream_shard_events_ingested_total` | gauge | `shard`   | Cumulative events per shard |
//! | `graphstream_shard_late_dropped_total`    | gauge | `shard`   | Cumulative late drops |
//! | `graphstream_shard_nodes`                 | gauge | `shard`   | Nodes currently stored |
//! | `graphstream_shard_edges`                 | gauge | `shard`   | Edges currently stored |
//! | `graphstream_shard_watermark_seconds`     | gauge | `shard`   | Per-shard watermark |
//! | `graphstream_shards_total`                | gauge | —         | Number of shards |
//! | `graphstream_events_ingested_total`       | gauge | —         | Total events |
//! | `graphstream_late_dropped_total`          | gauge | —         | Total late drops |
//! | `graphstream_nodes_total`                 | gauge | —         | Total nodes |
//! | `graphstream_edges_total`                 | gauge | —         | Total edges |
//! | `graphstream_global_watermark_seconds`    | gauge | —         | Global watermark |
//! | `graphstream_query_duration_seconds`      | histogram | —     | Query latency |
//!
//! Per-shard counters are exposed as gauges because the underlying
//! `ShardCluster` does not expose monotonic counters directly; the values
//! are monotonic in practice, and Prometheus derives rates from them.

pub mod collector;
pub mod registry;
pub mod server;

pub use collector::{collect_cluster, register_help, QUERY_LATENCY_BUCKETS};
pub use registry::{Counter, Gauge, Histogram, Registry};
pub use server::serve;