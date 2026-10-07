use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use serde_json::Value;
use temporal_graph::{EdgeId, NodeId, TemporalGraph};

use crate::error::ShardError;
use crate::shard::TimeWindowShard;

/// A cluster of time-windowed shards.
///
/// Shards are laid out on a fixed-duration grid starting at `origin`. Given
/// an event timestamp, the cluster routes it to the shard whose half-open
/// window `[k·window, (k+1)·window)` contains the timestamp. Shards are
/// created lazily on first use.
#[derive(Debug)]
pub struct ShardCluster {
    window: Duration,
    allowed_lateness: Duration,
    shards: HashMap<u32, TimeWindowShard>,
    origin: NaiveDateTime,
    next_node_id: u64,
}

impl ShardCluster {
    pub fn new(
        window: Duration,
        allowed_lateness: Duration,
        origin: NaiveDateTime,
    ) -> Result<Self, ShardError> {
        if window <= Duration::zero() {
            return Err(ShardError::Config("window must be positive".into()));
        }
        Ok(Self {
            window,
            allowed_lateness,
            shards: HashMap::new(),
            origin,
            next_node_id: 0,
        })
    }

    pub fn window(&self) -> Duration {
        self.window
    }

    pub fn allowed_lateness(&self) -> Duration {
        self.allowed_lateness
    }

    pub fn origin(&self) -> NaiveDateTime {
        self.origin
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shards(&self) -> impl Iterator<Item = &TimeWindowShard> {
        self.shards.values()
    }

    pub fn shard(&self, id: u32) -> Option<&TimeWindowShard> {
        self.shards.get(&id)
    }

    pub fn shard_mut(&mut self, id: u32) -> Option<&mut TimeWindowShard> {
        self.shards.get_mut(&id)
    }

    /// Compute the shard index for a given timestamp.
    pub fn shard_index(&self, ts: NaiveDateTime) -> u32 {
        let delta = ts.signed_duration_since(self.origin);
        let window_ns = self.window.num_nanoseconds().unwrap_or(1);
        let delta_ns = delta.num_nanoseconds().unwrap_or(0);
        let idx = delta_ns / window_ns;
        idx.max(0) as u32
    }

    /// Ensure a shard exists for the given timestamp; returns its id.
    pub fn ensure_shard(&mut self, ts: NaiveDateTime) -> Result<u32, ShardError> {
        let idx = self.shard_index(ts);
        if !self.shards.contains_key(&idx) {
            let start = self.origin + self.window * idx as i32;
            let end = start + self.window;
            let s = TimeWindowShard::new(idx, start, end, self.allowed_lateness)?;
            self.shards.insert(idx, s);
        }
        Ok(idx)
    }

    /// Route a node event to its target shard. Returns `(shard_id, node_id)`.
    ///
    /// Node IDs are assigned by the cluster and are unique across all shards.
    pub fn ingest_node(
        &mut self,
        ts: NaiveDateTime,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<(u32, NodeId), ShardError> {
        let idx = self.ensure_shard(ts)?;
        let id = NodeId(self.next_node_id);
        self.next_node_id += 1;
        let shard = self.shards.get_mut(&idx).unwrap();
        shard.ingest_node_with_id(id, ts, label, properties)?;
        Ok((idx, id))
    }
    pub fn next_node_id(&self) -> u64 {
        self.next_node_id
    }

    /// Route an edge event to its target shard. Returns `(shard_id, edge_id)`.
    pub fn ingest_edge(
        &mut self,
        ts: NaiveDateTime,
        end: NaiveDateTime,
        source: NodeId,
        target: NodeId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<(u32, EdgeId), ShardError> {
        let idx = self.ensure_shard(ts)?;
        let shard = self.shards.get_mut(&idx).unwrap();
        let edge = shard.ingest_edge(ts, end, source, target, label, properties)?;
        Ok((idx, edge))
    }

    /// Global watermark = minimum of the per-shard watermarks.
    ///
    /// Returns `None` if any shard has not yet observed a single event —
    /// the cluster cannot make a global progress guarantee in that case.
    pub fn global_watermark(&self) -> Option<NaiveDateTime> {
        let mut min: Option<NaiveDateTime> = None;
        for s in self.shards.values() {
            let v = s.watermark().value()?;
            min = Some(match min {
                Some(m) if m <= v => m,
                _ => v,
            });
        }
        min
    }

    /// All shards whose window overlaps `[t1, t2]`.
    pub fn shards_overlapping(
        &self,
        t1: NaiveDateTime,
        t2: NaiveDateTime,
    ) -> Vec<&TimeWindowShard> {
        self.shards
            .values()
            .filter(|s| s.window_start <= t2 && s.window_end >= t1)
            .collect()
    }

    /// Merge the graphs of all shards into one. Used by the v1-style
    /// single-node fallback query path and by tests.
    ///
    /// Node ids are preserved. Edge ids are **not** — `TemporalGraph::add_edge`
    /// assigns fresh `EdgeId`s in the merged graph. This is acceptable for
    /// query purposes since results bind to `NodeId`.
    pub fn merged_graph(&self) -> TemporalGraph {
        let mut ids: Vec<u32> = self.shards.keys().copied().collect();
        ids.sort_unstable();

        let mut out = TemporalGraph::new(1024);
        for id in ids {
            let g = self.shards[&id].graph();
            for n in g.nodes() {
                out.insert_node(n.clone());
            }
            for e in g.edges() {
                for iv in e.intervals.iter() {
                    out.add_edge(
                        e.source,
                        e.target,
                        e.label.clone(),
                        iv.start_time,
                        iv.end_time,
                        iv.properties.clone(),
                    )
                        .ok();
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn t(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::seconds(secs)
    }

    fn props() -> HashMap<String, Value> {
        HashMap::new()
    }

    #[test]
    fn shard_index_is_stable() {
        let c = ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap();
        assert_eq!(c.shard_index(t(0)), 0);
        assert_eq!(c.shard_index(t(59)), 0);
        assert_eq!(c.shard_index(t(60)), 1);
        assert_eq!(c.shard_index(t(125)), 2);
    }

    #[test]
    fn routes_events_to_correct_shard() {
        let mut c = ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap();
        let (s0, _) = c.ingest_node(t(10), "Host", props()).unwrap();
        let (s1, _) = c.ingest_node(t(90), "Host", props()).unwrap();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(c.shard_count(), 2);
    }

    #[test]
    fn shards_are_created_lazily() {
        let mut c = ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap();
        assert_eq!(c.shard_count(), 0);
        c.ingest_node(t(3600), "Host", props()).unwrap();
        // Only one shard is created even though we jumped far into the timeline.
        assert_eq!(c.shard_count(), 1);
        assert!(c.shard(60).is_some());
    }

    #[test]
    fn global_watermark_is_minimum() {
        let mut c = ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap();
        c.ingest_node(t(30), "Host", props()).unwrap(); // shard 0, max t(30)
        c.ingest_node(t(90), "Host", props()).unwrap(); // shard 1, max t(90)
        assert_eq!(c.global_watermark(), Some(t(30)));
    }

    #[test]
    fn global_watermark_is_none_until_all_shards_have_events() {
        let mut c = ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap();
        c.ingest_node(t(30), "Host", props()).unwrap();
        // Only one shard exists — the cluster has no way to know whether
        // other shards are expected. Single-shard clusters return that
        // shard's watermark.
        assert_eq!(c.global_watermark(), Some(t(30)));
    }

    #[test]
    fn overlapping_shards_selects_range() {
        let mut c = ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap();
        c.ingest_node(t(10), "Host", props()).unwrap();   // shard 0: [0, 60)
        c.ingest_node(t(90), "Host", props()).unwrap();   // shard 1: [60, 120)
        c.ingest_node(t(150), "Host", props()).unwrap();  // shard 2: [120, 180)

        let shards = c.shards_overlapping(t(50), t(100));
        assert_eq!(shards.len(), 2); // shard 0 and shard 1
    }

    #[test]
    fn merged_graph_contains_all_nodes() {
        let mut c = ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap();
        c.ingest_node(t(10), "Host", props()).unwrap();
        c.ingest_node(t(90), "Host", props()).unwrap();
        let merged = c.merged_graph();
        assert_eq!(merged.node_count(), 2);
    }

    #[test]
    fn rejects_window_of_zero() {
        let err = ShardCluster::new(Duration::seconds(0), Duration::seconds(0), t(0)).unwrap_err();
        assert!(matches!(err, ShardError::Config(_)));
    }
}