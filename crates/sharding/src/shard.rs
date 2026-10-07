use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use serde_json::Value;
use temporal_graph::{EdgeId, NodeId, TemporalGraph};

use crate::error::ShardError;
use crate::watermark::Watermark;

/// One time-window shard: a [`TemporalGraph`] restricted to a fixed,
/// half-open window `[window_start, window_end)`.
///
/// Events whose `start_time` falls outside the window are rejected. Events
/// inside the window are additionally gated by the shard's [`Watermark`];
/// out-of-order events beyond the grace period are dropped and counted as
/// `late_dropped`.
#[derive(Debug)]
pub struct TimeWindowShard {
    pub id: u32,
    pub window_start: NaiveDateTime,
    pub window_end: NaiveDateTime,
    graph: TemporalGraph,
    watermark: Watermark,
    events_ingested: u64,
    late_dropped: u64,
}

impl TimeWindowShard {
    pub fn new(
        id: u32,
        window_start: NaiveDateTime,
        window_end: NaiveDateTime,
        allowed_lateness: Duration,
    ) -> Result<Self, ShardError> {
        if window_end <= window_start {
            return Err(ShardError::Config(
                "window_end must be strictly after window_start".into(),
            ));
        }
        Ok(Self {
            id,
            window_start,
            window_end,
            graph: TemporalGraph::new(256),
            watermark: Watermark::new(allowed_lateness),
            events_ingested: 0,
            late_dropped: 0,
        })
    }

    pub fn graph(&self) -> &TemporalGraph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut TemporalGraph {
        &mut self.graph
    }

    pub fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    pub fn events_ingested(&self) -> u64 {
        self.events_ingested
    }

    pub fn late_dropped(&self) -> u64 {
        self.late_dropped
    }

    /// Whether this shard's window contains `ts`.
    pub fn contains(&self, ts: NaiveDateTime) -> bool {
        ts >= self.window_start && ts < self.window_end
    }

    /// Ingest a node event at `ts`.
    pub fn ingest_node(
        &mut self,
        ts: NaiveDateTime,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<NodeId, ShardError> {
        self.check_time(ts)?;
        let id = self.graph.add_node(label, properties);
        self.watermark.advance(ts);
        self.events_ingested += 1;
        Ok(id)
    }

    /// Ingest an edge event `[ts, end]` with the given endpoints.
    pub fn ingest_edge(
        &mut self,
        ts: NaiveDateTime,
        end: NaiveDateTime,
        source: NodeId,
        target: NodeId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, ShardError> {
        self.check_time(ts)?;
        let id = self
            .graph
            .add_edge(source, target, label, ts, end, properties)?;
        self.watermark.advance(ts);
        self.events_ingested += 1;
        Ok(id)
    }

    fn check_time(&mut self, ts: NaiveDateTime) -> Result<(), ShardError> {
        if !self.contains(ts) {
            return Err(ShardError::NoShard(ts));
        }
        if !self.watermark.accepts(ts) {
            self.late_dropped += 1;
            return Err(ShardError::TooLate {
                ts,
                watermark: self.watermark.value().unwrap_or(self.window_start),
                allowed: self.watermark.allowed_lateness(),
            });
        }
        Ok(())
    }
    /// Ingest a node with a caller-assigned [`NodeId`].
    ///
    /// Used by [`ShardCluster`] to enforce cluster-wide unique node IDs
    /// across shards. The shard itself does not track a global counter —
    /// standalone use of `TimeWindowShard` should prefer [`ingest_node`].
    ///
    /// [`ShardCluster`]: crate::ShardCluster
    /// [`ingest_node`]: Self::ingest_node
    pub fn ingest_node_with_id(
        &mut self,
        id: NodeId,
        ts: NaiveDateTime,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<(), ShardError> {
        self.check_time(ts)?;
        let node = temporal_graph::Node::new(id, label).with_properties(properties);
        self.graph.insert_node(node);
        self.watermark.advance(ts);
        self.events_ingested += 1;
        Ok(())
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
    fn accepts_events_inside_window() {
        let mut s = TimeWindowShard::new(0, t(0), t(60), Duration::seconds(5)).unwrap();
        assert!(s.ingest_node(t(10), "Host", props()).is_ok());
        assert_eq!(s.events_ingested(), 1);
        assert_eq!(s.late_dropped(), 0);
    }

    #[test]
    fn rejects_events_outside_window() {
        let mut s = TimeWindowShard::new(0, t(0), t(60), Duration::seconds(5)).unwrap();
        assert!(matches!(
            s.ingest_node(t(90), "Host", props()),
            Err(ShardError::NoShard(_))
        ));
        assert_eq!(s.events_ingested(), 0);
    }

    #[test]
    fn window_is_half_open() {
        let s = TimeWindowShard::new(0, t(0), t(60), Duration::seconds(0)).unwrap();
        assert!(s.contains(t(0)));   // start inclusive
        assert!(s.contains(t(59)));
        assert!(!s.contains(t(60))); // end exclusive
    }

    #[test]
    fn rejects_late_events_beyond_grace() {
        let mut s = TimeWindowShard::new(0, t(0), t(600), Duration::seconds(5)).unwrap();
        s.ingest_node(t(100), "Host", props()).unwrap();
        // watermark now t(95); t(50) is too late
        assert!(matches!(
            s.ingest_node(t(50), "Host", props()),
            Err(ShardError::TooLate { .. })
        ));
        assert_eq!(s.late_dropped(), 1);
        assert_eq!(s.events_ingested(), 1);
    }

    #[test]
    fn accepts_out_of_order_within_grace() {
        let mut s = TimeWindowShard::new(0, t(0), t(600), Duration::seconds(10)).unwrap();
        s.ingest_node(t(100), "Host", props()).unwrap();
        // watermark now t(90); t(95) is on time
        assert!(s.ingest_node(t(95), "Host", props()).is_ok());
        assert_eq!(s.events_ingested(), 2);
    }
}