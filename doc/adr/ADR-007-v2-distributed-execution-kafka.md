# ADR-008: v2 - Distributed execution and Kafka ingestion

## Status
Accepted

## Context
GraphStream v1 is a complete, single-node temporal graph query engine. The core
abstractions - Augmented Interval Tree, TemporalGraph, and TQL AST - were designed
in v1 with explicit forward-compatibility in mind (ADR-003, ADR-004): the execution
engine is decoupled from the parser, and the TQL language was intentionally scoped
to not require redesign when a distributed execution backend is added.

v1 has two fundamental limitations that v2 addresses:

1. **Scale** - a single-node TemporalGraph is bounded by the memory of one machine.
   Temporal datasets in the target use cases (network traffic for lateral movement
   detection, financial trade streams, physics experiment event logs) grow
   continuously and can exceed single-node capacity.

2. **Ingestion** - v1 loads datasets from static .tql or .json files. Real use cases
   produce data as live event streams, not static snapshots. There is no mechanism
   in v1 to ingest events as they arrive and make them immediately queryable.

Two concerns were deliberately separated in v2 scope: distributed execution (how the
graph is stored and queried across nodes) and stream ingestion (how new events enter
the system). These could have been implemented independently, but Kafka ingestion
without distributed execution still hits the single-node memory limit, and distributed
execution without a stream connector has no practical way to feed data at scale.
They are therefore developed together in v2.

## Decision
v2 adds two components on top of the complete v1:

**Distributed execution layer:**
- Shard the TemporalGraph by time window: each shard owns a contiguous time range
  and maintains its own Augmented Interval Tree
- Implement a distributed query planner that decomposes a TQL query into per-shard
  sub-queries and aggregates results
- Implement a watermark mechanism for tracking event-time progress and handling
  out-of-order events
- Guarantee exactly-once processing semantics: each event is written to exactly one
  shard, with no duplicates and no gaps

**Kafka ingestion connector:**
- Implement a Kafka consumer that reads an event stream and writes events into the
  distributed TemporalGraph in real time
- Define a documented JSON schema for Kafka messages (source, target, start_time,
  end_time, optional properties map)
- Implement idempotent writes: duplicate Kafka delivery does not create duplicate
  intervals in the graph
- Implement backpressure: the consumer pauses when ingest rate exceeds the
  execution engine's capacity

The TQL language and AST are unchanged from v1. The distributed execution engine
evaluates the same AST produced by the same parser.

## Rationale
- The v1 design decision to decouple parser from execution engine (ADR-004) makes
  this extension possible without any changes to the TQL layer. The distributed
  planner is a new execution backend, not a new language.
- Sharding by time window is the natural partition strategy for temporal data:
  most queries (AT, BETWEEN, DURING, WINDOW) have bounded time ranges that map
  cleanly onto one or a small number of shards, avoiding full scatter-gather.
- Kafka is the de facto standard for high-throughput event streaming in the
  target domains (cybersecurity SIEM pipelines, financial market data feeds,
  physics experiment DAQ systems). Using Kafka as the ingestion layer makes
  GraphStream directly connectable to existing infrastructure in those domains.
- Watermarking is necessary because temporal data in practice arrives out of order
  (network packet reordering, clock skew between sensors). Without watermarks,
  a query over a time window could return incomplete results because late-arriving
  events were not yet present.
- Exactly-once semantics is non-trivial but necessary for correctness in the
  target use cases: a lateral movement pattern detected twice, or a financial
  trade counted twice, produces incorrect analytical results.

## Consequences
- Distributed coordination introduces failure modes absent in v1: node failures,
  network partitions, shard imbalance. These require explicit handling that was
  not needed in the single-node implementation.
- The watermark mechanism adds latency: a query cannot return until the watermark
  has advanced past the query's time range, which means waiting for late events
  up to a configured maximum lateness bound.
- Exactly-once semantics requires either idempotent writes (preferred, via
  deterministic interval IDs) or distributed transactions (avoided due to
  complexity). The idempotent approach requires that each Kafka message carry
  a stable, content-derived ID.
- The Kafka dependency makes local development more complex - a Kafka broker
  must be running. A mock ingestion mode (feeding events via file or API,
  bypassing Kafka) should be supported for development and testing.
- TQL language and AST are strictly unchanged - any query valid in v1 is valid
  in v2. This is a hard constraint: v2 does not introduce new TQL syntax.
