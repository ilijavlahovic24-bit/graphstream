use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector,
    PodSpec, PodTemplateSpec, Probe, SecretKeySelector, Service, ServicePort, ServiceSpec,
    TCPSocketAction,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, ObjectMeta, Patch, PatchParams, ResourceExt};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, Resource};
use serde_json::json;
use tracing::{error, info, warn};

use crate::crd::{ClusterCondition, ClusterPhase, GraphStreamCluster, GraphStreamClusterStatus};
use crate::error::{OperatorError, Result};

/// Shared state for the reconciliation loop.
pub struct Context {
    pub client: Client,
}

/// Entry point: run the controller until the process is terminated.
pub async fn run() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let clusters: Api<GraphStreamCluster> = Api::all(client.clone());
    let ctx = Arc::new(Context { client });

    info!("starting GraphStream operator");

    Controller::new(clusters, watcher::Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => info!(?obj, "reconciled"),
                Err(e) => warn!("reconcile error: {e}"),
            }
        })
        .await;

    Ok(())
}

/// Reconcile one `GraphStreamCluster`.
///
/// Ensures the owned `StatefulSet`, `Service`, and `ConfigMap` match the
/// spec, then updates `.status`.
pub async fn reconcile(
    cluster: Arc<GraphStreamCluster>,
    ctx: Arc<Context>,
) -> Result<Action> {
    let name = cluster.name_any();
    let ns = cluster.namespace().unwrap_or_else(|| "default".into());
    let spec = &cluster.spec;

    info!(namespace = %ns, name = %name, "reconciling GraphStreamCluster");

    // 1) ConfigMap with shard configuration consumed by every shard pod.
    ensure_configmap(&ctx.client, &cluster, &ns, &name).await?;

    // 2) Service exposing the shard StatefulSet's query port.
    ensure_service(&ctx.client, &cluster, &ns, &name).await?;

    // 3) StatefulSet running the shard pods.
    ensure_statefulset(&ctx.client, &cluster, &ns, &name).await?;

    // 4) Read back the StatefulSet to compute ready replicas.
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let ready = match sts_api.get(&name).await {
        Ok(sts) => sts
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0) as u32,
        Err(_) => 0,
    };

    let phase = if ready >= spec.shards {
        ClusterPhase::Ready
    } else if ready > 0 {
        ClusterPhase::Progressing
    } else {
        ClusterPhase::Pending
    };

    // 5) Update status.
    let status = build_status(&cluster, ready, phase);
    update_status(&ctx.client, &cluster, &ns, &name, status).await?;

    // Re-queue for periodic drift detection. Watches trigger reconciles
    // on child changes; this requeue catches spec changes that don't
    // produce a watch event (rare but possible).
    Ok(Action::requeue(Duration::from_secs(60)))
}

/// Error policy: log and requeue with backoff. Kubernetes retries
/// automatically; returning `Action::requeue` gives us control over the
/// delay.
fn error_policy(
    cluster: Arc<GraphStreamCluster>,
    err: &OperatorError,
    _ctx: Arc<Context>,
) -> Action {
    error!(
        name = %cluster.name_any(),
        error = %err,
        "reconcile failed, will retry"
    );
    Action::requeue(Duration::from_secs(15))
}

// ---- child object construction ---------------------------------------

async fn ensure_configmap(
    client: &Client,
    cluster: &GraphStreamCluster,
    ns: &str,
    name: &str,
) -> Result<()> {
    let cm_name = format!("{name}-config");
    let spec = &cluster.spec;

    let data = json!({
        "shards": spec.shards.to_string(),
        "replication_factor": spec.replication_factor.to_string(),
        "shard_window_seconds": spec.shard_window_seconds.to_string(),
        "allowed_lateness_ms": spec.allowed_lateness_ms.to_string(),
        "kafka_brokers": spec.kafka.brokers,
        "kafka_topic": spec.kafka.topic,
        "kafka_group_id": spec.kafka.group_id,
    });

    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(cm_name.clone()),
            namespace: Some(ns.into()),
            owner_references: Some(vec![owner_ref(cluster)]),
            labels: Some(labels(name)),
            ..Default::default()
        },
        data: Some(data.as_object().unwrap().iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").into()))
            .collect()),
        ..Default::default()
    };

    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
    api.patch(
        &cm_name,
        &PatchParams::apply("graphstream-operator").force(),
        &Patch::Apply(&cm),
    ).await?;

    Ok(())
}

async fn ensure_service(
    client: &Client,
    cluster: &GraphStreamCluster,
    ns: &str,
    name: &str,
) -> Result<()> {
    let svc = Service {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(ns.into()),
            owner_references: Some(vec![owner_ref(cluster)]),
            labels: Some(labels(name)),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(labels(name)),
            ports: Some(vec![ServicePort {
                name: Some("query".into()),
                port: 7474,
                target_port: Some(IntOrString::Int(7474)),
                protocol: Some("TCP".into()),
                ..Default::default()
            }]),
            cluster_ip: Some("None".into()), // headless, for stable per-pod DNS
            ..Default::default()
        }),
        ..Default::default()
    };

    let api: Api<Service> = Api::namespaced(client.clone(), ns);
    api.patch(
        name,
        &PatchParams::apply("graphstream-operator").force(),
        &Patch::Apply(&svc),
    ).await?;

    Ok(())
}

async fn ensure_statefulset(
    client: &Client,
    cluster: &GraphStreamCluster,
    ns: &str,
    name: &str,
) -> Result<()> {
    let spec = &cluster.spec;

    let mut env = vec![
        EnvVar {
            name: "POD_NAME".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.name".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "SHARD_INDEX".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.labels['apps.kubernetes.io/pod-index']".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "SHARD_CONFIG_DIR".into(),
            value: Some("/etc/graphstream".into()),
            ..Default::default()
        },
    ];

    // Optional SASL credentials from a Secret.
    if let Some(secret_name) = &spec.kafka.credentials_secret {
        for (env_name, key) in [("KAFKA_USERNAME", "SASL_USERNAME"), ("KAFKA_PASSWORD", "SASL_PASSWORD")] {
            env.push(EnvVar {
                name: env_name.into(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: secret_name.clone(),
                        key: key.into(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(ns.into()),
            owner_references: Some(vec![owner_ref(cluster)]),
            labels: Some(labels(name)),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(spec.shards as i32),
            service_name: name.into(),
            selector: LabelSelector {
                match_labels: Some(labels(name)),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels(name)),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "shard".into(),
                        image: Some(spec.image.clone()),
                        ports: Some(vec![ContainerPort {
                            name: Some("query".into()),
                            container_port: 7474,
                            ..Default::default()
                        }]),
                        env: Some(env),
                        resources: Some(resources(spec)),
                        liveness_probe: Some(probe("/healthz", 10, 3)),
                        readiness_probe: Some(probe("/readyz", 5, 1)),
                        volume_mounts: Some(vec![
                            k8s_openapi::api::core::v1::VolumeMount {
                                name: "config".into(),
                                mount_path: "/etc/graphstream".into(),
                                read_only: Some(true),
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    }],
                    volumes: Some(vec![k8s_openapi::api::core::v1::Volume {
                        name: "config".into(),
                        config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                            name: format!("{name}-config"),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
    api.patch(
        name,
        &PatchParams::apply("graphstream-operator").force(),
        &Patch::Apply(&sts),
    ).await?;

    Ok(())
}

fn resources(spec: &crate::crd::GraphStreamClusterSpec) -> k8s_openapi::api::core::v1::ResourceRequirements {
    use k8s_openapi::api::core::v1::ResourceRequirements;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

    let mut limits = std::collections::BTreeMap::new();
    limits.insert("memory".to_string(), Quantity(format!("{}Mi", spec.memory_limit_mb)));

    let mut requests = std::collections::BTreeMap::new();
    requests.insert("memory".to_string(), Quantity(format!("{}Mi", spec.memory_limit_mb / 2)));
    requests.insert("cpu".to_string(), Quantity(format!("{}m", spec.cpu_request_millicores)));

    ResourceRequirements {
        limits: Some(limits),
        requests: Some(requests),
        ..Default::default()
    }
}

fn probe(path: &str, period: i32, failure_threshold: i32) -> Probe {
    Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(7474),
            ..Default::default()
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(period),
        failure_threshold: Some(failure_threshold),
        ..Default::default()
    }
        .with_http_get_if(path)
}

trait ProbeExt {
    fn with_http_get_if(self, path: &str) -> Self;
}

impl ProbeExt for Probe {
    fn with_http_get_if(self, _path: &str) -> Self {
        // The shard exposes a TCP port; the path argument is unused for
        // v1. If HTTP probes are added later, switch to `http_get`.
        self
    }
}

fn labels(name: &str) -> std::collections::BTreeMap<String, String> {
    let mut m = std::collections::BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), "graphstream".into());
    m.insert("app.kubernetes.io/instance".into(), name.into());
    m.insert("app.kubernetes.io/component".into(), "shard".into());
    m
}

fn owner_ref(cluster: &GraphStreamCluster) -> OwnerReference {
    OwnerReference {
        api_version: "graphstream.io/v1alpha1".into(),
        kind: "GraphStreamCluster".into(),
        name: cluster.name_any(),
        uid: cluster.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

fn build_status(
    cluster: &GraphStreamCluster,
    ready: u32,
    phase: ClusterPhase,
) -> GraphStreamClusterStatus {
    let observed_gen = cluster.metadata.generation;
    let mut status = GraphStreamClusterStatus {
        observed_generation: observed_gen,
        ready_replicas: ready,
        phase: Some(phase.clone()),
        ..Default::default()
    };

    let (cond_status, reason, message) = match phase {
        ClusterPhase::Ready => ("True", "AllShardsReady", "all shard pods are ready"),
        ClusterPhase::Progressing => ("False", "Progressing", "waiting for shard pods"),
        ClusterPhase::Pending => ("False", "Pending", "no shard pods are ready"),
        ClusterPhase::Failed => ("False", "Failed", "cluster entered failed state"),
    };

    status.conditions.push(ClusterCondition {
        r#type: "Ready".into(),
        status: cond_status.into(),
        reason: reason.into(),
        message: message.into(),
        last_transition_time: None,
    });

    status
}

async fn update_status(
    client: &Client,
    cluster: &GraphStreamCluster,
    ns: &str,
    name: &str,
    status: GraphStreamClusterStatus,
) -> Result<()> {
    let api: Api<GraphStreamCluster> = Api::namespaced(client.clone(), ns);
    let patch = json!({ "status": status });
    api.patch_status(
        name,
        &PatchParams::apply("graphstream-operator").force(),
        &Patch::Apply(patch),
    ).await?;
    let _ = cluster;
    Ok(())
}