use std::collections::HashMap;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One Kafka message: a single edge activity interval.
///
/// See the crate-level docs for the JSON schema and semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeEvent {
    pub event_id: String,
    pub source: String,
    pub target: String,
    pub label: String,
    pub start_time: i64,
    pub end_time: i64,
    #[serde(default)]
    pub properties: HashMap<String, Value>,
}

impl EdgeEvent {
    /// Convert `start_time` (ms since epoch) to a timestamp.
    pub fn start_dt(&self) -> NaiveDateTime {
        ms_to_dt(self.start_time)
    }

    /// Convert `end_time` (ms since epoch) to a timestamp.
    pub fn end_dt(&self) -> NaiveDateTime {
        ms_to_dt(self.end_time)
    }
}

/// Milliseconds since the Unix epoch → `NaiveDateTime`.
pub fn ms_to_dt(ms: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        + Duration::milliseconds(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip() {
        let json = r#"{
            "event_id": "evt-1",
            "source": "host-a",
            "target": "host-b",
            "label": "Connection",
            "start_time": 1704067200000,
            "end_time":   1704067260000
        }"#;
        let ev: EdgeEvent = serde_json::from_str(json).unwrap();
        assert_eq!(ev.event_id, "evt-1");
        assert_eq!(ev.source, "host-a");
        assert_eq!(ev.label, "Connection");
        assert!(ev.properties.is_empty());

        let re = serde_json::to_string(&ev).unwrap();
        let ev2: EdgeEvent = serde_json::from_str(&re).unwrap();
        assert_eq!(ev, ev2);
    }

    #[test]
    fn json_with_properties() {
        let json = r#"{
            "event_id": "evt-2",
            "source": "h1",
            "target": "h2",
            "label": "CORRELATED",
            "start_time": 0,
            "end_time":   100,
            "properties": { "correlation": 0.9 }
        }"#;
        let ev: EdgeEvent = serde_json::from_str(json).unwrap();
        assert_eq!(ev.properties.get("correlation").unwrap().as_f64(), Some(0.9));
    }

    #[test]
    fn ms_to_dt_epoch() {
        assert_eq!(ms_to_dt(0).to_string(), "1970-01-01 00:00:00");
    }

    #[test]
    fn ms_to_dt_2024_01_01() {
        // 2024-01-01 00:00:00 UTC = 1704067200000 ms
        let dt = ms_to_dt(1_704_067_200_000);
        assert_eq!(dt.to_string(), "2024-01-01 00:00:00");
    }
}