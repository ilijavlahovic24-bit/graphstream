use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use serde_json::Value;
use temporal_graph::NodeId;

use sharding::{ShardCluster, ShardError};

use crate::error::IngestError;
use crate::event::EdgeEvent;

/// Outcome of processing one event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// Event was new and successfully written.
    Ingested,
    /// Event was already seen (`event_id` in dedup set). No-op.
    Duplicate,
    /// Event was too late per the shard's watermark. Dropped and counted.
    LateDropped,
}

/// Running counters for observability.
#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    pub processed: u64,
    pub duplicates: u64,
    pub late_dropped: u64,
    pub failed: u64,
}

/// Core ingest pipeline: dedup + node auto-creation + shard routing.
///
/// This type is deliberately Kafka-agnostic. It exposes a synchronous
/// [`process`] method that takes an already-parsed [`EdgeEvent`]. The
/// Kafka consumer in [`crate::consumer`] is a thin wrapper that reads
/// messages, calls `process`, and commits offsets on success.
///
/// [`process`]: Self::process
pub struct IngestPipeline {
    cluster: Arc<RwLock<ShardCluster>>,
    dedup: HashSet<String>,
    external_ids: HashMap<String, NodeId>,
    next_node_id: u64,
    node_label: String,
    stats: IngestStats,
}

impl IngestPipeline {
    /// Create a pipeline over the given cluster.
    ///
    /// Auto-created nodes are given the label `"Node"` by default. Use
    /// [`with_node_label`] to override.
    ///
    /// [`with_node_label`]: Self::with_node_label
    pub fn new(cluster: Arc<RwLock<ShardCluster>>) -> Self {
        let next_node_id = cluster
            .read()
            .map(|c| c.next_node_id())
            .unwrap_or(0);
        Self {
            cluster,
            dedup: HashSet::new(),
            external_ids: HashMap::new(),
            next_node_id,
            node_label: "Node".to_string(),
            stats: IngestStats::default(),
        }
    }

    pub fn with_node_label(mut self, label: impl Into<String>) -> Self {
        self.node_label = label.into();
        self
    }

    pub fn stats(&self) -> IngestStats {
        self.stats
    }

    pub fn dedup_size(&self) -> usize {
        self.dedup.len()
    }

    pub fn known_external_keys(&self) -> usize {
        self.external_ids.len()
    }

    /// Process one event. See [`ProcessOutcome`] for possible results.
    /// Process one event. See [`ProcessOutcome`] for possible results.
    pub fn process(&mut self, event: EdgeEvent) -> Result<ProcessOutcome, IngestError> {
        // 1) Dedup by event_id.
        if self.dedup.contains(&event.event_id) {
            self.stats.duplicates += 1;
            return Ok(ProcessOutcome::Duplicate);
        }

        let start = event.start_dt();
        let end = event.end_dt();

        // 2) Clone the Arc first so the write guard does not borrow `self`.
        //    This lets us call `&mut self` methods while holding the guard.
        let cluster_arc = Arc::clone(&self.cluster);
        let mut cluster = cluster_arc
            .write()
            .map_err(|_| IngestError::Shard(ShardError::Config("cluster lock poisoned".into())))?;

        // 3) Resolve / create the source and target nodes in the shard
        //    that will hold the edge.
        let source_id = self.resolve_or_create(&mut cluster, &event.source, start)?;
        let target_id = self.resolve_or_create(&mut cluster, &event.target, start)?;

        // 4) Ingest the edge.
        let result = cluster.ingest_edge(
            start,
            end,
            source_id,
            target_id,
            &event.label,
            event.properties,
        );

        drop(cluster);

        match result {
            Ok(_) => {
                self.dedup.insert(event.event_id);
                self.stats.processed += 1;
                Ok(ProcessOutcome::Ingested)
            }
            Err(ShardError::TooLate { .. }) => {
                self.dedup.insert(event.event_id);
                self.stats.late_dropped += 1;
                Ok(ProcessOutcome::LateDropped)
            }
            Err(e) => {
                self.stats.failed += 1;
                Err(IngestError::Shard(e))
            }
        }
    }

    fn resolve_or_create(
        &mut self,
        cluster: &mut ShardCluster,
        key: &str,
        ts: chrono::NaiveDateTime,
    ) -> Result<NodeId, IngestError> {
        // Global node ID: cache hit or fresh allocation.
        let global_id = if let Some(&id) = self.external_ids.get(key) {
            id
        } else {
            let id = NodeId(self.next_node_id);
            self.next_node_id += 1;
            self.external_ids.insert(key.to_string(), id);
            id
        };

        // Ensure the node exists in the target shard.
        let idx = cluster.ensure_shard(ts)?;
        let shard = cluster.shard_mut(idx).unwrap();
        if shard.graph().node(global_id).is_none() {
            let props: HashMap<String, Value> = HashMap::new();
            shard.ingest_node_with_id(global_id, ts, &self.node_label, props)?;
        }

        Ok(global_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, NaiveDateTime};

    fn epoch_ms(dt: NaiveDateTime) -> i64 {
        (dt - ms_to_dt_epoch()).num_milliseconds()
    }

    fn ms_to_dt_epoch() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
            .and_hms_opt(0, 0, 0).unwrap()
    }

    fn t_2024(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
            .and_hms_opt(0, 0, 0).unwrap()
            + Duration::seconds(secs)
    }

    fn cluster() -> Arc<RwLock<ShardCluster>> {
        Arc::new(RwLock::new(
            ShardCluster::new(
                Duration::seconds(60),
                Duration::seconds(5),
                t_2024(0),
            )
                .unwrap(),
        ))
    }

    fn event(id: &str, src: &str, tgt: &str, start_s: i64, end_s: i64) -> EdgeEvent {
        EdgeEvent {
            event_id: id.into(),
            source: src.into(),
            target: tgt.into(),
            label: "Connection".into(),
            start_time: epoch_ms(t_2024(start_s)),
            end_time: epoch_ms(t_2024(end_s)),
            properties: HashMap::new(),
        }
    }

    #[test]
    fn ingests_a_simple_event() {
        let c = cluster();
        let mut p = IngestPipeline::new(c.clone());
        let out = p.process(event("e1", "a", "b", 10, 20)).unwrap();
        assert_eq!(out, ProcessOutcome::Ingested);
        assert_eq!(p.stats().processed, 1);

        let g = c.read().unwrap();
        assert_eq!(g.shard_count(), 1);
        assert_eq!(g.shard(0).unwrap().graph().node_count(), 2);
        assert_eq!(g.shard(0).unwrap().graph().edge_count(), 1);
    }

    #[test]
    fn duplicate_event_is_noop() {
        let c = cluster();
        let mut p = IngestPipeline::new(c.clone());
        assert_eq!(p.process(event("e1", "a", "b", 10, 20)).unwrap(), ProcessOutcome::Ingested);
        assert_eq!(p.process(event("e1", "a", "b", 10, 20)).unwrap(), ProcessOutcome::Duplicate);
        assert_eq!(p.stats().processed, 1);
        assert_eq!(p.stats().duplicates, 1);
    }

    #[test]
    fn same_external_key_reuses_node_id_across_shards() {
        let c = cluster();
        let mut p = IngestPipeline::new(c.clone());
        // Shard 0
        p.process(event("e1", "a", "b", 10, 20)).unwrap();
        // Shard 1 — same external key "a"
        p.process(event("e2", "a", "c", 90, 100)).unwrap();

        let g = c.read().unwrap();
        let a_id_in_s0 = g.shard(0).unwrap().graph()
            .nodes().find(|n| n.id.0 == 0).map(|n| n.id);
        let a_id_in_s1 = g.shard(1).unwrap().graph()
            .nodes().find(|n| n.id.0 == 0).map(|n| n.id);
        assert_eq!(a_id_in_s0, a_id_in_s1);
        assert_eq!(p.known_external_keys(), 3); // a, b, c
    }

    #[test]
    fn late_event_is_dropped() {
        let c = cluster();
        let mut p = IngestPipeline::new(c.clone());

        // Both events land in shard 0 (window [0, 60) seconds).
        // First event advances shard 0's watermark.
        p.process(event("e1", "a", "b", 30, 45)).unwrap();

        // Watermark is now ~25s (30 - 5s allowed lateness).
        // An event at t=10 is beyond the grace period.
        let out = p.process(event("e2", "a", "b", 10, 20)).unwrap();
        assert_eq!(out, ProcessOutcome::LateDropped);
        assert_eq!(p.stats().late_dropped, 1);
        assert_eq!(p.stats().processed, 1);
    }

    #[test]
    fn node_label_is_configurable() {
        let c = cluster();
        let mut p = IngestPipeline::new(c.clone()).with_node_label("Host");
        p.process(event("e1", "a", "b", 10, 20)).unwrap();
        let g = c.read().unwrap();
        for n in g.shard(0).unwrap().graph().nodes() {
            assert_eq!(n.label, "Host");
        }
    }
}