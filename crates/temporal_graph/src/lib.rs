//! # temporal_graph
//!
//! In-memory temporal property graph.
//!
//! A [`TemporalGraph`] holds:
//! * [`Node`]s — identified by a unique [`NodeId`], carrying a label and a
//!   property map. Nodes have no intrinsic time extent in v1.
//! * [`TemporalEdge`]s — directed (source, target) pairs carrying a label
//!   and an [`interval::IntervalTree`] of activity intervals. Each interval
//!   has its own property map.
//!
//! ## Node activity semantics (v1)
//!
//! The spec lists `nodes_at(t)` but does not attach time to nodes. In v1 a
//! node is considered *active* at instant `t` iff it has at least one
//! incident edge that is active at `t`. This keeps the temporal model in
//! one place (edges) and matches the use cases (a Host is "in the graph"
//! for the duration of its connections).
//!
//! See `docs/adr/ADR-003-single-node-execution-scope.md` for context.

pub mod edge;
pub mod node;
pub mod temporal_graph;

pub use edge::{EdgeId, TemporalEdge};
pub use node::{Node, NodeId};
pub use temporal_graph::TemporalGraph;