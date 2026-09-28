use std::sync::Arc;

use temporal_graph::TemporalGraph;

use crate::engine::QueryEngine;
use crate::error::TqlError;
use crate::result::QueryResult;

/// Rust embedded API: `graph.query(tql_string) -> Result<QueryResult, TqlError>`.
///
/// This is the spec's primary entry point for programmatic TQL execution.
/// It builds a temporary [`QueryEngine`] around the graph, so it is best
/// suited for one-off queries. Hot query loops should hold a single
/// `QueryEngine` (constructed via `QueryEngine::new(Arc<graph>)`) instead.
///
///
/// example
/// use query::GraphQueryExt;
///
/// let result = graph.query("MATCH (a:Host) RETURN a")?;

pub trait GraphQueryExt {
    fn query(&self, tql: &str) -> Result<QueryResult, TqlError>;
}

impl GraphQueryExt for TemporalGraph {
    fn query(&self, tql: &str) -> Result<QueryResult, TqlError> {
        let engine = QueryEngine::new(Arc::new(self.clone()));
        engine.query(tql)
    }
}