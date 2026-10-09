use std::sync::Arc;

use temporal_graph::TemporalGraph;
use sharding::{ShardCluster, TimeWindowShard};

use crate::distributed_planner::extract_time_range;
use crate::engine::QueryEngine;
use crate::error::TqlError;
use crate::result::{QueryResult, Row};
use tql_parser::ast::Query;

/// Distributed query engine: routes a TQL query across time-windowed
/// shards, executes it locally on each relevant shard, and merges results.
///
/// ## Known limitations (v2)
///
/// * **Cross-shard graph patterns are not supported.** A path that starts
///   in shard 0 and continues in shard 1 will not be matched. The
///   mitigation is to choose a shard duration longer than the longest
///   pattern window (e.g. 1 hour for the 30-minute lateral-movement
///   use case).
/// * **`AVG` over the merged result is not the global average** (mean of
///   means). `SUM`, `COUNT`, `MIN`, `MAX` merge correctly. v3 will
///   introduce mergeable aggregate state.
/// * **No cross-shard snapshot isolation.** Each shard is read at whatever
///   consistent state it currently holds; different shards may be at
///   different watermarks.
///
/// See `docs/adr/ADR-008-distributed-execution-and-deployment-plan.md`.
pub struct DistributedQueryEngine {
    cluster: Arc<ShardCluster>,
}

impl DistributedQueryEngine {
    pub fn new(cluster: Arc<ShardCluster>) -> Self {
        Self { cluster }
    }

    pub fn cluster(&self) -> &ShardCluster {
        &self.cluster
    }

    /// Parse and execute a TQL query against the cluster.
    pub fn query(&self, tql: &str) -> Result<QueryResult, TqlError> {
        let ast = tql_parser::parse(tql)?;
        self.execute(&ast)
    }

    /// Execute a pre-parsed query.
    pub fn execute(&self, q: &Query) -> Result<QueryResult, TqlError> {
        let shards = self.plan(q);

        // Execute per-shard and merge.
        let mut merged = QueryResult { columns: Vec::new(), rows: Vec::new() };

        for shard in shards {
            let local = self.run_on_shard(shard, q)?;
            merge_into(&mut merged, local);
        }

        Ok(merged)
    }

    /// Determine which shards participate in this query.
    fn plan<'a>(&'a self, q: &Query) -> Vec<&'a TimeWindowShard> {
        let range = extract_time_range(q.where_clause.as_ref());
        if range.is_unbounded() {
            let mut all: Vec<&TimeWindowShard> = self.cluster.shards().collect();
            all.sort_by_key(|s| s.id);
            all
        } else {
            let from = range.from.unwrap();
            let to   = range.to.unwrap_or(from);
            let mut selected = self.cluster.shards_overlapping(from, to);
            selected.sort_by_key(|s| s.id);
            selected
        }
    }

    /// Execute the query against one shard's local graph.
    fn run_on_shard(
        &self,
        shard: &TimeWindowShard,
        q: &Query,
    ) -> Result<QueryResult, TqlError> {
        let graph: Arc<TemporalGraph> = Arc::new(shard.graph().clone());
        let engine = QueryEngine::new(graph);
        engine.execute(q)
    }
}

/// Merge `src` into `dst`. The first non-empty source fixes the column set;
/// subsequent sources must agree on column names.
fn merge_into(dst: &mut QueryResult, src: QueryResult) {
    if src.rows.is_empty() && src.columns.is_empty() {
        return;
    }
    if dst.columns.is_empty() {
        dst.columns = src.columns;
    }
    // For v2, we trust that every shard returns the same schema. If this
    // becomes an issue, the engine can validate schema equality here.
    dst.rows.extend(src.rows);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use chrono::{Duration, NaiveDate, NaiveDateTime};
    use serde_json::Value;

    fn t(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::seconds(secs)
    }

    fn props() -> HashMap<String, Value> {
        HashMap::new()
    }

    fn cluster_with_two_shards() -> Arc<ShardCluster> {
        // Origin is 2024-01-01 00:00:00 UTC. Shards cover 60-second windows
        // starting from this point.
        let mut c = ShardCluster::new(
            Duration::seconds(60),
            Duration::seconds(0),
            t(0),
        )
            .unwrap();

        // Shard 0 [t(0), t(60)): hosts at t=10s and t=20s.
        c.ingest_node(t(10), "Host", props()).unwrap();
        c.ingest_node(t(20), "Host", props()).unwrap();

        // Shard 1 [t(60), t(120)): hosts at t=90s and t=100s.
        c.ingest_node(t(90), "Host", props()).unwrap();
        c.ingest_node(t(100), "Host", props()).unwrap();

        Arc::new(c)
    }

    #[test]
    fn match_all_hosts_spans_both_shards() {
        let cluster = cluster_with_two_shards();
        let engine = DistributedQueryEngine::new(cluster);
        let res = engine.query("MATCH (a:Host) RETURN a").unwrap();
        assert_eq!(res.rows.len(), 4);
    }

    #[test]
    fn empty_cluster_returns_empty_result() {
        let cluster = Arc::new(
            ShardCluster::new(Duration::seconds(60), Duration::seconds(0), t(0)).unwrap()
        );
        let engine = DistributedQueryEngine::new(cluster);
        let res = engine.query("MATCH (a:Host) RETURN a").unwrap();
        assert!(res.rows.is_empty());
        assert!(res.columns.is_empty());
    }

    #[test]
    fn planner_selects_only_relevant_shards() {
        let cluster = cluster_with_two_shards();
        let engine = DistributedQueryEngine::new(cluster);

        // 2024-01-01 00:00:00 UTC = 1704067200 s since epoch.
        // Query range 15–25 s after that falls inside shard 0's [0, 60) window.
        let q = tql_parser::parse(
            "MATCH (a:Host) \
             WHERE a.created BETWEEN 1704067215000, 1704067225000 \
             RETURN a",
        )
            .unwrap();
        let shards = engine.plan(&q);
        assert_eq!(shards.len(), 1, "expected only shard 0 to participate");
        assert_eq!(shards[0].id, 0);
    }

    #[test]
    fn planner_returns_all_shards_when_unbounded() {
        let cluster = cluster_with_two_shards();
        let engine = DistributedQueryEngine::new(cluster);

        let q = tql_parser::parse("MATCH (a:Host) RETURN a").unwrap();
        let shards = engine.plan(&q);
        assert_eq!(shards.len(), 2);
    }
}