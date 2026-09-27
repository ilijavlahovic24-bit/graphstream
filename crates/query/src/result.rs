use chrono::NaiveDateTime;
use serde_json::Value;
use std::collections::HashMap;
use temporal_graph::{EdgeId, NodeId};

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub cells: Vec<ResultCell>,
}

#[derive(Debug, Clone)]
pub enum ResultCell {
    Node { id: NodeId, label: String, properties: HashMap<String, Value> },
    Edge {
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        label: String,
        start: NaiveDateTime,
        end: NaiveDateTime,
        properties: HashMap<String, Value>,
    },
    Value(Value),
    Null,
}

impl ResultCell {
    pub fn as_json(&self) -> Value {
        match self {
            ResultCell::Null => Value::Null,
            ResultCell::Value(v) => v.clone(),
            ResultCell::Node { id, label, properties } => {
                let mut m = serde_json::Map::new();
                m.insert("id".into(), Value::from(id.0));
                m.insert("label".into(), Value::from(label.clone()));
                m.insert("properties".into(), Value::Object(properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect()));
                Value::Object(m)
            }
            ResultCell::Edge { id, source, target, label, start, end, properties } => {
                let mut m = serde_json::Map::new();
                m.insert("id".into(), Value::from(id.0));
                m.insert("source".into(), Value::from(source.0));
                m.insert("target".into(), Value::from(target.0));
                m.insert("label".into(), Value::from(label.clone()));
                m.insert("start".into(), Value::from(start.to_string()));
                m.insert("end".into(), Value::from(end.to_string()));
                m.insert("properties".into(), Value::Object(properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect()));
                Value::Object(m)
            }
        }
    }
}