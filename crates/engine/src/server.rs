use std::sync::Arc;
use query::QueryEngine;

pub struct Server {
    port: u16,
    query_engine: Arc<QueryEngine>,
}

impl Server {
    pub fn new(port: u16, query_engine: Arc<QueryEngine>) -> Self {
        Self { port, query_engine }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // TODO
        let _ = (&self.port, &self.query_engine);
        Ok(())
    }
}