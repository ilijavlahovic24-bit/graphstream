use std::collections::HashMap;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use serde::Deserialize;
use serde_json::Value;

use temporal_graph::{Node, NodeId, TemporalGraph};

/// Dataset file format:
///
/// ```json
/// {
///   "nodes": [
///     { "id": 0, "label": "Host", "properties": { "critical": true } }
///   ],
///   "edges": [
///     {
///       "source": 0,
///       "target": 1,
///       "label": "Connection",
///       "intervals": [
///         { "start": 1000, "end": 2000, "properties": { "correlation": 0.9 } }
///       ]
///     }
///   ]
/// }
/// ```
///
/// Timestamps (`start`, `end`) are **milliseconds since the Unix epoch** and
/// are aligned with `TimeExpr::Timestamp(ms)` in the query engine.
#[derive(Debug, Deserialize)]
pub struct DatasetFile {
    #[serde(default)]
    pub nodes: Vec<NodeSpec>,
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
}

#[derive(Debug, Deserialize)]
pub struct NodeSpec {
    pub id: u64,
    pub label: String,
    #[serde(default)]
    pub properties: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct EdgeSpec {
    pub source: u64,
    pub target: u64,
    pub label: String,
    #[serde(default)]
    pub intervals: Vec<IntervalSpec>,
}

#[derive(Debug, Deserialize)]
pub struct IntervalSpec {
    pub start: i64,
    pub end: i64,
    #[serde(default)]
    pub properties: HashMap<String, Value>,
}

/// Read a JSON dataset file and populate the graph with its nodes and edges.
pub async fn load_dataset(graph: &mut TemporalGraph, path: &str) -> anyhow::Result<()> {
    let src = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read `{path}`: {e}"))?;
    load_from_json_str(graph, &src)
        .map_err(|e| anyhow::anyhow!("dataset `{path}`: {e}"))
}

/// Same as [`load_dataset`] but from an in-memory JSON string.
pub fn load_from_json_str(graph: &mut TemporalGraph, src: &str) -> anyhow::Result<()> {
    let ds: DatasetFile = serde_json::from_str(src)
        .map_err(|e| anyhow::anyhow!("invalid dataset JSON: {e}"))?;
    load_from_spec(graph, ds)
}

fn load_from_spec(graph: &mut TemporalGraph, ds: DatasetFile) -> anyhow::Result<()> {
    // Insert nodes first — edges reference them by id.
    for spec in ds.nodes {
        let node = Node::new(NodeId(spec.id), spec.label).with_properties(spec.properties);
        graph.insert_node(node);
    }

    // Then edges with their intervals.
    for spec in ds.edges {
        if spec.intervals.is_empty() {
            continue;
        }
        let source = NodeId(spec.source);
        let target = NodeId(spec.target);
        let label = spec.label.as_str();
        let mut iter = spec.intervals.into_iter();

        // The first interval goes through add_edge; the rest are appended.
        let first = iter.next().unwrap();
        let edge_id = graph.add_edge(
            source, target, label,
            ms_to_dt(first.start),
            ms_to_dt(first.end),
            first.properties,
        ).map_err(|e| anyhow::anyhow!("edge {} -> {}: {e}", spec.source, spec.target))?;

        for iv in iter {
            graph.add_activity(
                edge_id,
                ms_to_dt(iv.start),
                ms_to_dt(iv.end),
                iv.properties,
            ).map_err(|e| anyhow::anyhow!("edge {} -> {}: {e}", spec.source, spec.target))?;
        }
    }
    Ok(())
}

fn ms_to_dt(ms: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
        .and_hms_opt(0, 0, 0).unwrap()
        + Duration::milliseconds(ms)
}