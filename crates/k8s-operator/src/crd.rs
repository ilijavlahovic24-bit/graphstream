use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Kafka connection parameters that shard pods use to consume events.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KafkaSpec {
    /// Comma-separated broker addresses, e.g. `"kafka.default.svc:9092"`.
    pub brokers: String,
    /// Topic to consume from.
    pub topic: String,
    /// Consumer group id.
    pub group_id: String,
    /// Name of a Kubernetes `Secret` in the same namespace that holds
    /// `SASL_USERNAME` and `SASL_PASSWORD` keys. Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret: Option<String>,
}

/// Spec for a `GraphStreamCluster`.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "graphstream.io",
    version = "v1alpha1",
    kind = "GraphStreamCluster",
    plural = "graphstreamclusters",
    shortname = "gsc",
    namespaced,
    status = "GraphStreamClusterStatus",
    printcolumn = r#"{"name":"Shards","type":"integer","jsonPath":".spec.shards"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GraphStreamClusterSpec {
    /// Number of time-windowed shards. Each shard is a separate pod.
    #[schemars(range(min = 1, max = 256))]
    pub shards: u32,

    /// Replication factor per shard. `1` means no replication.
    #[schemars(range(min = 1, max = 5))]
    #[serde(default = "default_replication")]
    pub replication_factor: u32,

    /// Memory limit per shard pod, in MiB.
    #[schemars(range(min = 128))]
    #[serde(default = "default_memory_mb")]
    pub memory_limit_mb: u64,

    /// CPU request per shard pod, in millicores.
    #[serde(default = "default_cpu_millicores")]
    pub cpu_request_millicores: u32,

    /// Duration of one shard window, in seconds.
    #[schemars(range(min = 1))]
    pub shard_window_seconds: u64,

    /// Allowed lateness for out-of-order events, in milliseconds.
    #[serde(default = "default_lateness_ms")]
    pub allowed_lateness_ms: u64,

    /// Kafka connection parameters.
    pub kafka: KafkaSpec,

    /// Container image for shard pods.
    #[serde(default = "default_image")]
    pub image: String,
}

fn default_replication() -> u32 { 1 }
fn default_memory_mb() -> u64 { 1024 }
fn default_cpu_millicores() -> u32 { 500 }
fn default_lateness_ms() -> u64 { 5000 }
fn default_image() -> String { "graphstream/shard:latest".into() }

/// Lifecycle phase reported by the operator.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum ClusterPhase {
    Pending,
    Progressing,
    Ready,
    Failed,
}

/// Status observed by the operator.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GraphStreamClusterStatus {
    /// Generation of `.spec` last reconciled. Matches `.metadata.generation`
    /// when the observed state is up to date.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Number of shard pods currently ready.
    #[serde(default)]
    pub ready_replicas: u32,

    /// Phase of the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ClusterPhase>,

    /// Human-readable conditions, in the standard Kubernetes format.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ClusterCondition>,

    /// Message from the last reconcile, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCondition {
    pub r#type: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crd_schema_generates() {
        // The CRD YAML should be generatable from the Rust type alone.
        use kube::CustomResourceExt;
        let crd = GraphStreamCluster::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("graphstreamclusters"));
        assert!(yaml.contains("graphstream.io"));
    }

    #[test]
    fn spec_deserializes_from_camel_case_yaml() {
        let yaml = r#"
            shards: 4
            replicationFactor: 2
            memoryLimitMb: 512
            shardWindowSeconds: 3600
            allowedLatenessMs: 30000
            kafka:
              brokers: "kafka:9092"
              topic: "graphstream.events"
              groupId: "gs-prod"
        "#;
        let spec: GraphStreamClusterSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.shards, 4);
        assert_eq!(spec.replication_factor, 2);
        assert_eq!(spec.memory_limit_mb, 512);
        assert_eq!(spec.shard_window_seconds, 3600);
        assert_eq!(spec.kafka.topic, "graphstream.events");
    }
}