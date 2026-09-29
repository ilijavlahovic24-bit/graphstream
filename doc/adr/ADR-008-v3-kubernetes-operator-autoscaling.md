# ADR-009: v3 - Kubernetes operator and auto-scaling

## Status
Accepted

## Context
GraphStream v2 is a distributed temporal graph engine with Kafka ingestion. It can
scale beyond a single node and process live event streams, but operating it requires
manual configuration of shards, replicas, and Kafka parameters. In a production
environment, the workload is not static: cybersecurity monitoring has traffic spikes
during incidents, financial surveillance has peaks at market open/close, and physics
experiments have burst periods during beam runs.

Without automated orchestration and scaling, an operator must manually resize the
cluster in response to load changes, monitor shard health, and coordinate rolling
updates to avoid downtime. This is operationally expensive and error-prone.

v3 addresses this by making GraphStream a first-class Kubernetes workload with a
custom operator that manages its lifecycle, and a horizontal auto-scaling layer
driven by domain-specific metrics.

## Decision
v3 adds a Kubernetes operator and auto-scaling infrastructure on top of v2:

**Kubernetes operator:**
- Define a GraphStreamCluster CustomResourceDefinition (CRD) with configurable
  fields: number of shards, replication factor, memory limits per shard,
  Kafka broker addresses and topic configuration
- Implement a reconciliation loop that continuously compares the desired state
  (declared in the CRD) with the actual state of the cluster (running pods,
  shard assignments) and corrects any divergence
- Implement liveness and readiness probes for all GraphStream components so
  Kubernetes can detect and restart unhealthy instances
- Support rolling updates: when the CRD is updated (e.g. new image version,
  changed shard count), the operator updates instances one at a time while
  maintaining query availability

**Auto-scaling:**
- Implement a Prometheus metric exporter that exposes GraphStream-specific
  metrics: ingest rate (events/sec), query latency (p50, p99), shard memory
  utilization, watermark lag
- Configure Horizontal Pod Autoscaler (HPA) using these custom metrics as
  scaling signals: scale up on high ingest rate or high query latency,
  scale down when both are low
- Define cooldown periods for scale-up and scale-down decisions to prevent
  oscillation under variable load

## Rationale
- A Kubernetes operator is the standard pattern for managing stateful distributed
  systems on Kubernetes (etcd-operator, Kafka operator, CockroachDB operator
  all follow this model). Implementing one for GraphStream demonstrates familiarity
  with production-grade infrastructure patterns relevant to FAANG/CERN/ESA
  platform engineering roles.
- The CRD approach separates configuration (what the user declares) from
  implementation (what the operator does), which is the correct abstraction
  boundary for a production system. It also makes GraphStream deployable via
  standard Kubernetes tooling (kubectl, Helm, GitOps) without custom scripts.
- Custom metrics (ingest rate, query latency, watermark lag) are more meaningful
  scaling signals than generic CPU/memory for a temporal graph engine. CPU usage
  does not capture whether the system is keeping up with the event stream;
  watermark lag does.
- Rolling updates with zero downtime are a hard requirement for the target use
  cases: a cybersecurity monitoring system that goes dark during an update is
  operationally unacceptable.
- Prometheus is the de facto standard for metrics in Kubernetes environments.
  Exporting in Prometheus format makes GraphStream observable with existing
  tooling (Grafana dashboards, Alertmanager rules) without additional integration
  work.

## Consequences
- The operator must be written in a language with a mature Kubernetes client.
  The Rust Kubernetes client (kube-rs) is production-ready and consistent with
  the rest of the codebase. YAML manifests for CRD definitions and RBAC rules
  are the only non-Rust artifacts introduced in v3.
- Operating v3 requires a Kubernetes cluster. Local development uses kind or
  k3s. This raises the barrier for running the full system compared to v1
  (single binary) and v2 (binary + Kafka broker).
- The reconciliation loop must be idempotent: applying the same desired state
  twice must produce the same result. This is a correctness requirement for
  all Kubernetes operators and requires careful design of the shard assignment
  and rebalancing logic.
- Auto-scaling a stateful system (sharded graph) is more complex than scaling
  stateless services: adding a shard requires rebalancing existing interval
  data, which is a non-trivial operation that must be coordinated without
  interrupting ongoing queries. The initial v3 implementation scales by
  replication (more replicas of existing shards) before tackling shard
  rebalancing, which is deferred to a future version.
- v3 completes the project arc from single-node engine (v1) to distributed
  system (v2) to production-operated platform (v3). Each version is a complete,
  independently deployable system - v3 is not a prerequisite for using v1 or v2.
