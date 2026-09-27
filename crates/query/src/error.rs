use std::fmt;
use temporal_graph::{EdgeId, NodeId};

#[derive(Debug)]
pub enum TqlError {
    Parse(tql_parser::ParseError),
    UnknownVariable(String),
    UnknownNode(NodeId),
    UnknownEdge(EdgeId),
    TypeMismatch { context: String, detail: String },
    AggregateWithoutWindow,
    MissingWindowKey,
}

impl fmt::Display for TqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TqlError::Parse(e) => write!(f, "parse error: {e}"),
            TqlError::UnknownVariable(n) => write!(f, "unknown variable `{n}`"),
            TqlError::UnknownNode(id) => write!(f, "unknown node {id}"),
            TqlError::UnknownEdge(id) => write!(f, "unknown edge e{}", id.0),
            TqlError::TypeMismatch { context, detail } => write!(f, "type mismatch in {context}: {detail}"),
            TqlError::AggregateWithoutWindow => write!(f, "HAVING requires WINDOW"),
            TqlError::MissingWindowKey => write!(f, "window aggregation called without a window key"),
        }
    }
}

impl std::error::Error for TqlError {}
impl From<tql_parser::ParseError> for TqlError {
    fn from(e: tql_parser::ParseError) -> Self { TqlError::Parse(e) }
}