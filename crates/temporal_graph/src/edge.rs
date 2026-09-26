use crate::node::NodeId;
use chrono::NaiveDateTime;
use interval::{Interval, IntervalTree};
use serde_json::Value;
use std::collections::HashMap;

/// Stable identifier of an edge inside one [`TemporalGraph`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeId(pub u64);

/// A directed temporal edge. The interval tree stores one entry per
/// distinct time window in which the edge was active; each entry carries
/// its own property map (e.g. `correlation`, `bandwidth`, `energy`).
#[derive(Clone, Debug)]
pub struct TemporalEdge {
    pub id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub label: String,
    pub intervals: IntervalTree,
}

impl TemporalEdge {
    pub fn new(
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        label: impl Into<String>,
    ) -> Self {
        Self {
            id,
            source,
            target,
            label: label.into(),
            intervals: IntervalTree::new(),
        }
    }

    /// Add one activity interval with associated properties.
    pub fn add_activity(
        &mut self,
        start: NaiveDateTime,
        end: NaiveDateTime,
        properties: HashMap<String, Value>,
    ) {
        let iv = Interval::new(start, end).with_properties(properties);
        self.intervals.insert(iv);
    }

    /// True if this edge is active at instant `t`.
    pub fn active_at(&self, t: NaiveDateTime) -> bool {
        !self.intervals.at(t).is_empty()
    }

    /// True if this edge is active somewhere in `[t1, t2]`.
    pub fn active_during(&self, t1: NaiveDateTime, t2: NaiveDateTime) -> bool {
        !self.intervals.overlap(t1, t2).is_empty()
    }

    /// All activity intervals overlapping `[t1, t2]`.
    pub fn activities_overlapping(
        &self,
        t1: NaiveDateTime,
        t2: NaiveDateTime,
    ) -> Vec<&Interval> {
        self.intervals.overlap(t1, t2)
    }
}