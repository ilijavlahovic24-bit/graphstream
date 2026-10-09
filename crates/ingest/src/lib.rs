//! # ingest
//!
//! Kafka ingestion into the sharded temporal graph.
//!
//! ## Message format
//!
//! Each Kafka message is a JSON [`EdgeEvent`]:
//!
//! ```json
//! {
//!   "event_id": "evt-7f3a-...",
//!   "source": "host-42",
//!   "target": "host-100",
//!   "label": "Connection",
//!   "start_time": 1704067200000,
//!   "end_time":   1704067260000,
//!   "properties": { "correlation": 0.9 }
//! }
//! ```
//!
//! * `event_id` — unique per event, used for idempotent ingest
//! * `source` / `target` — external node keys; the pipeline auto-creates
//!   nodes on first sight and caches the mapping
//! * `start_time` / `end_time` — milliseconds since the Unix epoch,
//!   consistent with `TimeExpr::Timestamp` in the query engine
//! * `properties` — optional
//!
//! ## Delivery guarantees
//!
//! * **At-least-once**: offsets are committed only after the event is
//!   processed. On failure, the event is redelivered by Kafka.
//! * **Idempotent**: the pipeline deduplicates by `event_id`. Re-delivery
//!   of an already-processed event is a no-op.
//! * **Backpressure**: if per-event processing time exceeds
//!   `slow_threshold` for `slow_streak_threshold` consecutive events, the
//!   consumer sleeps for `cooldown` before resuming consumption.
//!
//! See `docs/adr/ADR-008-distributed-execution-and-deployment-plan.md`.

pub mod config;
pub mod consumer;
pub mod error;
pub mod event;
pub mod pipeline;

pub use config::KafkaConfig;
pub use consumer::run_consumer;
pub use error::IngestError;
pub use event::EdgeEvent;
pub use pipeline::{IngestPipeline, IngestStats, ProcessOutcome};