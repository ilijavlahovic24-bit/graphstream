use std::collections::HashMap;
use chrono::NaiveDateTime;
use serde_json::Value;
use crate::{EdgeId, Node, NodeId, TemporalEdge};


/// In-memory temporal property graph.
///
/// v1 is single-node and append-heavy. Edges live in a flat `Vec`; node
/// lookup is a `HashMap`. The `max_memory_mb` limit is carried for
/// observability — enforcement is deferred (see ADR-003).
#[derive(Clone, Debug)]
pub struct TemporalGraph {
    nodes: HashMap<NodeId, Node>,
    edges: Vec<TemporalEdge>,
    next_node_id: u64,
    next_edge_id: u64,
    max_memory_mb: usize,
}

impl TemporalGraph {
    pub fn new(max_memory_mb: usize) -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
            next_node_id: 0,
            next_edge_id: 0,
            max_memory_mb,
        }
    }

    pub fn max_memory_mb(&self) -> usize {
        self.max_memory_mb
    }

    // ---- nodes ----------------------------------------------------------

    pub fn insert_node(&mut self, node: Node) {
        self.next_node_id = self.next_node_id.max(node.id.0 + 1);
        self.nodes.insert(node.id, node);
    }

    pub fn add_node(
        &mut self,
        label: impl Into<String>,
        properties: HashMap<String, Value>,
    ) -> NodeId {
        let id = NodeId(self.next_node_id);
        self.next_node_id += 1;
        let node = Node::new(id, label).with_properties(properties);
        self.nodes.insert(id, node);
        id
    }

    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    // ---- edges ----------------------------------------------------------

    pub fn add_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        label: impl Into<String>,
        start: NaiveDateTime,
        end: NaiveDateTime,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, GraphError> {
        if !self.nodes.contains_key(&source) {
            return Err(GraphError::UnknownNode(source));
        }
        if !self.nodes.contains_key(&target) {
            return Err(GraphError::UnknownNode(target));
        }

        let id = EdgeId(self.next_edge_id);
        self.next_edge_id += 1;

        let mut edge = TemporalEdge::new(id, source, target, label);
        edge.add_activity(start, end, properties);
        self.edges.push(edge);
        Ok(id)
    }

    pub fn add_activity(
        &mut self,
        edge: EdgeId,
        start: NaiveDateTime,
        end: NaiveDateTime,
        properties: HashMap<String, Value>,
    ) -> Result<(), GraphError> {
        let e = self
            .edges
            .iter_mut()
            .find(|e| e.id == edge)
            .ok_or(GraphError::UnknownEdge(edge))?;
        e.add_activity(start, end, properties);
        Ok(())
    }

    pub fn edges(&self) -> &[TemporalEdge] {
        &self.edges
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn edge(&self, id: EdgeId) -> Option<&TemporalEdge> {
        self.edges.iter().find(|e| e.id == id)
    }

    // ---- temporal queries ----------------------------------------------

    pub fn edges_at(&self, t: NaiveDateTime) -> Vec<&TemporalEdge> {
        self.edges.iter().filter(|e| e.active_at(t)).collect()
    }

    pub fn edges_during(&self, t1: NaiveDateTime, t2: NaiveDateTime) -> Vec<&TemporalEdge> {
        self.edges
            .iter()
            .filter(|e| e.active_during(t1, t2))
            .collect()
    }

    pub fn nodes_at(&self, t: NaiveDateTime) -> Vec<&Node> {
        let mut touched: Vec<NodeId> = Vec::new();
        for e in &self.edges {
            if e.active_at(t) {
                if !touched.contains(&e.source) {
                    touched.push(e.source);
                }
                if !touched.contains(&e.target) {
                    touched.push(e.target);
                }
            }
        }
        touched
            .into_iter()
            .filter_map(|id| self.nodes.get(&id))
            .collect()
    }

    pub fn nodes_during(&self, t1: NaiveDateTime, t2: NaiveDateTime) -> Vec<&Node> {
        let mut touched: Vec<NodeId> = Vec::new();
        for e in &self.edges {
            if e.active_during(t1, t2) {
                if !touched.contains(&e.source) {
                    touched.push(e.source);
                }
                if !touched.contains(&e.target) {
                    touched.push(e.target);
                }
            }
        }
        touched
            .into_iter()
            .filter_map(|id| self.nodes.get(&id))
            .collect()
    }

    pub fn outgoing_at(&self, from: NodeId, t: NaiveDateTime) -> Vec<&TemporalEdge> {
        self.edges
            .iter()
            .filter(|e| e.source == from && e.active_at(t))
            .collect()
    }

    pub fn incoming_at(&self, to: NodeId, t: NaiveDateTime) -> Vec<&TemporalEdge> {
        self.edges
            .iter()
            .filter(|e| e.target == to && e.active_at(t))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GraphError {
    UnknownNode(NodeId),
    UnknownEdge(EdgeId),
}

impl std::fmt::Display for GraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphError::UnknownNode(id) => write!(f, "unknown node {}", id),
            GraphError::UnknownEdge(id) => write!(f, "unknown edge e{}", id.0),
        }
    }
}

impl std::error::Error for GraphError {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate};

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

    fn graph_with_chain() -> (TemporalGraph, NodeId, NodeId, NodeId) {
        let mut g = TemporalGraph::new(64);
        let a = g.add_node("Host", props());
        let b = g.add_node("Host", props());
        let c = g.add_node("Host", props());
        g.add_edge(a, b, "Connection", t(0), t(10), props()).unwrap();
        g.add_edge(b, c, "Connection", t(5), t(20), props()).unwrap();
        (g, a, b, c)
    }

    #[test]
    fn add_and_lookup_nodes() {
        let mut g = TemporalGraph::new(64);
        let id = g.add_node("Host", props());
        assert_eq!(g.node_count(), 1);
        assert_eq!(g.node(id).unwrap().label, "Host");
    }

    #[test]
    fn add_edge_rejects_unknown_endpoint() {
        let mut g = TemporalGraph::new(64);
        let a = g.add_node("Host", props());
        let err = g
            .add_edge(a, NodeId(999), "Connection", t(0), t(10), props())
            .unwrap_err();
        assert!(matches!(err, GraphError::UnknownNode(_)));
    }

    #[test]
    fn edges_at_point() {
        let (g, _, _, _) = graph_with_chain();
        assert_eq!(g.edges_at(t(7)).len(), 2);
        assert_eq!(g.edges_at(t(3)).len(), 1);
        assert_eq!(g.edges_at(t(15)).len(), 1);
        assert_eq!(g.edges_at(t(30)).len(), 0);
    }

    #[test]
    fn edges_during_range() {
        let (g, _, _, _) = graph_with_chain();
        assert_eq!(g.edges_during(t(0), t(20)).len(), 2);
        assert_eq!(g.edges_during(t(11), t(20)).len(), 1);
        assert_eq!(g.edges_during(t(21), t(30)).len(), 0);
    }

    #[test]
    fn nodes_at_uses_incident_edges() {
        let (g, a, b, c) = graph_with_chain();

        let ns: Vec<NodeId> = g.nodes_at(t(3)).iter().map(|n| n.id).collect();
        assert!(ns.contains(&a) && ns.contains(&b) && !ns.contains(&c));

        let ns: Vec<NodeId> = g.nodes_at(t(15)).iter().map(|n| n.id).collect();
        assert!(ns.contains(&b) && ns.contains(&c) && !ns.contains(&a));

        assert!(g.nodes_at(t(30)).is_empty());
    }

    #[test]
    fn add_activity_extends_edge_lifetime() {
        let (mut g, _, _, _) = graph_with_chain();
        let e_id = g.edges()[0].id;
        g.add_activity(e_id, t(50), t(60), props()).unwrap();
        assert_eq!(g.edges_at(t(55)).len(), 1);
    }

    #[test]
    fn outgoing_and_incoming() {
        let (g, a, _, _) = graph_with_chain();
        assert_eq!(g.outgoing_at(a, t(3)).len(), 1);
        assert_eq!(g.incoming_at(a, t(3)).len(), 0);
    }
}