use serde_json::Value;
use std::collections::HashMap;

/// Stable identifier of a node inside one [`TemporalGraph`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// A graph vertex with a label (e.g. `Host`, `Trade`, `Particle`) and
/// arbitrary properties.
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub id: NodeId,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

impl Node {
    pub fn new(id: NodeId, label: impl Into<String>) -> Self {
        Self {
            id,
            label: label.into(),
            properties: HashMap::new(),
        }
    }

    pub fn with_properties(mut self, properties: HashMap<String, Value>) -> Self {
        self.properties = properties;
        self
    }

    pub fn property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }
}