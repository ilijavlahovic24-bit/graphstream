use temporal_graph::TemporalGraph;

use std::sync::Arc;

pub struct QueryEngine {
    graph: Arc<TemporalGraph>,
}

impl QueryEngine {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        Self { graph }
    }

    pub fn graph(&self) -> &TemporalGraph {
        &self.graph
    }
}