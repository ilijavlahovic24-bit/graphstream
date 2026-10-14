use std::collections::HashMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A time-sliced view of the graph: nodes and edges active at instant `t`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Timestamp of the slice (milliseconds since Unix epoch).
    pub time_ms: i64,
    /// Human-readable timestamp.
    pub time_display: String,
    pub nodes: Vec<SnapshotNode>,
    pub edges: Vec<SnapshotEdge>,
    /// Total node/edge counts in the underlying graph (not just the slice).
    pub total_nodes: usize,
    pub total_edges: usize,
    /// Shard metadata; populated when the snapshot comes from a cluster.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shards: Option<Vec<ShardInfo>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotNode {
    pub id: u64,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEdge {
    pub id: u64,
    pub source: u64,
    pub target: u64,
    pub label: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub properties: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub id: u32,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub events_ingested: u64,
    pub late_dropped: u64,
    pub node_count: usize,
    pub edge_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark_ms: Option<i64>,
}

/// Convert a `NaiveDateTime` to milliseconds since the Unix epoch.
pub fn ms(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_millis()
}

/// Unix epoch as `NaiveDateTime`.
pub fn epoch() -> NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}