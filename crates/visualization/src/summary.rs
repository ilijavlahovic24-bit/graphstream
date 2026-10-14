use std::collections::HashSet;
use chrono::NaiveDateTime;
use sharding::ShardCluster;
use temporal_graph::TemporalGraph;
use crate::snapshot::{ms, ShardInfo, Snapshot, SnapshotEdge, SnapshotNode};

/// Build a snapshot of `graph` at instant `t`.
pub fn snapshot_graph(graph: &TemporalGraph, t: NaiveDateTime) -> Snapshot {
    // Edges active at t, expanded to per-interval rows (an edge may have
    // several overlapping intervals).
    let mut edges: Vec<SnapshotEdge> = Vec::new();
    for e in graph.edges_at(t) {
        for iv in e.activities_overlapping(t, t) {
            edges.push(SnapshotEdge {
                id: e.id.0,
                source: e.source.0,
                target: e.target.0,
                label: e.label.clone(),
                start_ms: ms(iv.start_time),
                end_ms: ms(iv.end_time),
                properties: iv.properties.clone(),
            });
        }
    }

    let mut node_ids: HashSet<u64> = HashSet::new();
    for e in &edges {
        node_ids.insert(e.source);
        node_ids.insert(e.target);
    }

    let nodes: Vec<SnapshotNode> = graph
        .nodes()
        .filter(|n| node_ids.contains(&n.id.0))
        .map(|n| SnapshotNode {
            id: n.id.0,
            label: n.label.clone(),
            properties: n.properties.clone(),
        })
        .collect();

    Snapshot {
        time_ms: ms(t),
        time_display: t.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        nodes,
        edges,
        total_nodes: graph.node_count(),
        total_edges: graph.edge_count(),
        shards: None,
    }
}

/// Build a snapshot from a shard cluster; adds per-shard metadata.
pub fn snapshot_cluster(
    cluster: &sharding::ShardCluster,
    t: NaiveDateTime,
) -> Snapshot {
    let merged = cluster.merged_graph();
    let mut snap = snapshot_graph(&merged, t);

    snap.shards = Some(
        cluster
            .shards()
            .map(|s| ShardInfo {
                id: s.id,
                window_start_ms: ms(s.window_start),
                window_end_ms: ms(s.window_end),
                events_ingested: s.events_ingested(),
                late_dropped: s.late_dropped(),
                node_count: s.graph().node_count(),
                edge_count: s.graph().edge_count(),
                watermark_ms: s.watermark().value().map(ms),
            })
            .collect(),
    );
    snap
}

/// Anything that can produce a [`Snapshot`] at a given instant.
pub trait SnapshotSource: Send + Sync + 'static {
    fn snapshot_at(&self, t: NaiveDateTime) -> Snapshot;

    /// Earliest and latest timestamps seen in the source, if any.
    fn bounds(&self) -> (Option<NaiveDateTime>, Option<NaiveDateTime>);

    fn total_nodes(&self) -> usize;
    fn total_edges(&self) -> usize;
}

impl SnapshotSource for TemporalGraph {
    fn snapshot_at(&self, t: NaiveDateTime) -> Snapshot {
        snapshot_graph(self, t)
    }

    fn bounds(&self) -> (Option<NaiveDateTime>, Option<NaiveDateTime>) {
        let mut lo: Option<NaiveDateTime> = None;
        let mut hi: Option<NaiveDateTime> = None;
        for e in self.edges() {
            for iv in e.intervals.iter() {
                lo = Some(match lo {
                    Some(prev) if prev <= iv.start_time => prev,
                    _ => iv.start_time,
                });
                hi = Some(match hi {
                    Some(prev) if prev >= iv.end_time => prev,
                    _ => iv.end_time,
                });
            }
        }
        (lo, hi)
    }

    fn total_nodes(&self) -> usize { self.node_count() }
    fn total_edges(&self) -> usize { self.edge_count() }
}

impl SnapshotSource for ShardCluster {
    fn snapshot_at(&self, t: NaiveDateTime) -> Snapshot {
        snapshot_cluster(self, t)
    }

    fn bounds(&self) -> (Option<NaiveDateTime>, Option<NaiveDateTime>) {
        let mut lo: Option<NaiveDateTime> = None;
        let mut hi: Option<NaiveDateTime> = None;
        for s in self.shards() {
            let s_lo = s.window_start;
            let s_hi = s.window_end;
            lo = Some(match lo { Some(prev) if prev <= s_lo => prev, _ => s_lo });
            hi = Some(match hi { Some(prev) if prev >= s_hi => prev, _ => s_hi });
        }
        (lo, hi)
    }

    fn total_nodes(&self) -> usize {
        self.shards().map(|s| s.graph().node_count()).sum()
    }
    fn total_edges(&self) -> usize {
        self.shards().map(|s| s.graph().edge_count()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use chrono::{Duration, NaiveDate};

    fn t(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::seconds(secs)
    }

    fn props() -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }

    #[test]
    fn empty_graph_yields_empty_snapshot() {
        let g = TemporalGraph::new(64);
        let s = snapshot_graph(&g, t(0));
        assert!(s.nodes.is_empty());
        assert!(s.edges.is_empty());
        assert_eq!(s.total_nodes, 0);
        assert_eq!(s.total_edges, 0);
    }

    #[test]
    fn snapshot_includes_active_edges_and_their_endpoints() {
        let mut g = TemporalGraph::new(64);
        let a = g.add_node("Host", props());
        let b = g.add_node("Host", props());
        let c = g.add_node("Host", props());
        g.add_edge(a, b, "Connection", t(0), t(10), props()).unwrap();
        g.add_edge(b, c, "Connection", t(20), t(30), props()).unwrap();

        let s = snapshot_graph(&g, t(5));
        assert_eq!(s.edges.len(), 1);
        assert_eq!(s.nodes.len(), 2); // a and b
        assert_eq!(s.total_nodes, 3);
        assert_eq!(s.total_edges, 2);
    }

    #[test]
    fn snapshot_at_gap_has_no_edges() {
        let mut g = TemporalGraph::new(64);
        let a = g.add_node("Host", props());
        let b = g.add_node("Host", props());
        g.add_edge(a, b, "Connection", t(0), t(10), props()).unwrap();
        g.add_edge(a, b, "Connection", t(20), t(30), props()).unwrap();

        let s = snapshot_graph(&g, t(15));
        assert!(s.edges.is_empty());
        assert!(s.nodes.is_empty());
    }
}