//! # sharding
//!
//! Time-windowed sharding for the temporal graph.
//!
//! A [`ShardCluster`] partitions temporal events into fixed-duration
//! [`TimeWindowShard`]s. Each shard owns its own [`TemporalGraph`] covering
//! one contiguous half-open time window `[start, end)`. Events are routed
//! to shards by their `start_time`.
//!
//! Out-of-order events are handled via a per-shard [`Watermark`]: events
//! with `start_time >= watermark` (where `watermark = max_seen -
//! allowed_lateness`) are accepted into their target shard. Older events
//! are rejected as "late" and counted.
//!
//! See `docs/adr/ADR-008-distributed-execution-and-deployment-plan.md`
//! for the overall v2/v3 plan and the rationale for time-window sharding.

pub mod cluster;
pub mod error;
pub mod shard;
pub mod watermark;

pub use cluster::ShardCluster;
pub use error::ShardError;
pub use shard::TimeWindowShard;
pub use watermark::Watermark;