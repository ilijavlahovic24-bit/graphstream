use std::sync::Arc;
use temporal_graph::TemporalGraph;

pub struct Sampler {
    graph: Arc<TemporalGraph>,
}

impl Sampler {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        Self { graph }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // TODO
        let _ = &self.graph;
        Ok(())
    }
}