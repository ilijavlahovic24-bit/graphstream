use sharding::ShardCluster;
use crate::registry::Registry;

/// Latency histogram buckets, in seconds. Chosen to cover the range of
/// expected TQL query latencies.
pub const QUERY_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
];

/// Scrape the current state of a `ShardCluster` into the registry.
///
/// Called from the `/metrics` HTTP handler on every request, so gauges
/// reflect the state at scrape time.
pub fn collect_cluster(registry: &Registry, cluster: &ShardCluster) {
    let mut total_ingested = 0u64;
    let mut total_late = 0u64;
    let mut total_nodes = 0usize;
    let mut total_edges = 0usize;

    for shard in cluster.shards() {
        let id = shard.id.to_string();
        let labels: &[(&str, &str)] = &[("shard", id.as_str())];

        registry.set_gauge(
            "graphstream_shard_events_ingested_total",
            labels,
            shard.events_ingested() as f64,
        );
        registry.set_gauge(
            "graphstream_shard_late_dropped_total",
            labels,
            shard.late_dropped() as f64,
        );
        registry.set_gauge(
            "graphstream_shard_nodes",
            labels,
            shard.graph().node_count() as f64,
        );
        registry.set_gauge(
            "graphstream_shard_edges",
            labels,
            shard.graph().edge_count() as f64,
        );

        if let Some(wm) = shard.watermark().value() {
            let secs = wm.and_utc().timestamp_millis() as f64 / 1000.0;
            registry.set_gauge("graphstream_shard_watermark_seconds", labels, secs);
        }

        total_ingested += shard.events_ingested();
        total_late += shard.late_dropped();
        total_nodes += shard.graph().node_count();
        total_edges += shard.graph().edge_count();
    }

    registry.set_gauge("graphstream_shards_total", &[], cluster.shard_count() as f64);
    registry.set_gauge("graphstream_events_ingested_total", &[], total_ingested as f64);
    registry.set_gauge("graphstream_late_dropped_total", &[], total_late as f64);
    registry.set_gauge("graphstream_nodes_total", &[], total_nodes as f64);
    registry.set_gauge("graphstream_edges_total", &[], total_edges as f64);

    if let Some(wm) = cluster.global_watermark() {
        let secs = wm.and_utc().timestamp_millis() as f64 / 1000.0;
        registry.set_gauge("graphstream_global_watermark_seconds", &[], secs);
    }
}

/// Register HELP strings for all exposed metrics.
pub fn register_help(registry: &Registry) {
    registry.set_help(
        "graphstream_shard_events_ingested_total",
        "Cumulative number of events ingested by this shard.",
    );
    registry.set_help(
        "graphstream_shard_late_dropped_total",
        "Cumulative number of events dropped by this shard as too late.",
    );
    registry.set_help(
        "graphstream_shard_nodes",
        "Number of nodes currently stored in this shard.",
    );
    registry.set_help(
        "graphstream_shard_edges",
        "Number of edges currently stored in this shard.",
    );
    registry.set_help(
        "graphstream_shard_watermark_seconds",
        "Per-shard watermark (max_seen - allowed_lateness) in Unix seconds.",
    );
    registry.set_help(
        "graphstream_shards_total",
        "Number of active shards in the cluster.",
    );
    registry.set_help(
        "graphstream_events_ingested_total",
        "Cumulative number of events ingested across all shards.",
    );
    registry.set_help(
        "graphstream_late_dropped_total",
        "Cumulative number of events dropped as too late across all shards.",
    );
    registry.set_help(
        "graphstream_nodes_total",
        "Total number of nodes across all shards.",
    );
    registry.set_help(
        "graphstream_edges_total",
        "Total number of edges across all shards.",
    );
    registry.set_help(
        "graphstream_global_watermark_seconds",
        "Minimum per-shard watermark across the cluster, in Unix seconds.",
    );
    registry.set_help(
        "graphstream_query_duration_seconds",
        "Query execution latency, in seconds.",
    );
    registry.set_help(
        "graphstream_ingest_rate_events_per_second",
        "Rate of event ingestion over the last scrape interval, in events/second.",
    );
}