//! # k8s-operator
//!
//! Kubernetes operator for GraphStream.
//!
//! ## Overview
//!
//! The operator watches `GraphStreamCluster` custom resources and reconciles
//! the actual cluster state to match the spec. For each CR it manages:
//!
//! * a `StatefulSet` running the shard pods (`graphstream-shards-<name>`)
//! * a `Service` exposing the query server (`graphstream-query-<name>`)
//! * a `ConfigMap` holding the shard window and Kafka configuration
//!
//! ## Reconciliation model
//!
//! Each `reconcile` call:
//!
//! 1. Fetches the CR.
//! 2. Ensures each owned child object exists with the desired spec
//!    (`apply` semantics — create if missing, patch if drifted).
//! 3. Updates `.status` with `observedGeneration`, `readyReplicas`,
//!    `conditions`, and the current phase.
//!
//! Failures return an error and are retried with backoff. Successes are
//! re-queued automatically when a watched child changes, so `kubectl edit
//! graphstreamcluster` triggers a reconcile within seconds.
//!
//! ## Rolling updates
//!
//! Shard pods are managed by a `StatefulSet` with
//! `updateStrategy.type = RollingUpdate`. The operator never deletes a
//! pod directly; it changes the StatefulSet spec and lets Kubernetes
//! drive the rolling update, which restarts shards one ordinal at a time
//! in reverse order — matching the `shard_window` layout so that the
//! oldest (lowest-ordinal) shard is restarted last.
//!
//! See `docs/adr/ADR-012-kubernetes-operator-design.md`.

pub mod controller;
pub mod crd;
pub mod error;

pub use controller::{reconcile, Context};
pub use crd::{GraphStreamCluster, GraphStreamClusterSpec, GraphStreamClusterStatus, KafkaSpec};
pub use error::OperatorError;