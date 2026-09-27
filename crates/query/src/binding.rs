use chrono::NaiveDateTime;
use std::collections::HashMap;
use temporal_graph::{EdgeId, NodeId};

/// Identifies one activity interval of an edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct IntervalKey {
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
}

/// What a TQL variable can be bound to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BoundValue {
    Node(NodeId),
    Edge(EdgeId, IntervalKey),
}

/// One row of the intermediate result set.
#[derive(Clone, Debug)]
pub(crate) struct Binding {
    pub vars: HashMap<String, BoundValue>,
    /// Representative time for windowing (first edge interval seen).
    pub time_key: Option<NaiveDateTime>,
    /// Window bucket index assigned by WINDOW.
    pub window_bucket: Option<i64>,
}

impl Binding {
    pub fn new() -> Self {
        Self { vars: HashMap::new(), time_key: None, window_bucket: None }
    }

    pub fn get(&self, name: &str) -> Option<BoundValue> {
        self.vars.get(name).copied()
    }
}