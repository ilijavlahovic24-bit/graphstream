# GraphStream: A Temporal Graph Query Engine with TQL

## Status: v1 Complete - v2 In Progress

## Overview
GraphStream is a temporal graph query engine written in Rust, focused on expressing and evaluating complex time-evolving relationship patterns. It introduces the Temporal Query Language (TQL) — a declarative query language that combines Cypher-style graph traversal with SQL-style filtering and temporal operators as first-class constructs.

Unlike classical graph databases that store static relationships, GraphStream models edges and nodes as entities with time interval activity, enabling queries such as "which patterns occurred within this time window, in which order".

### Prerequisites
- Rust 1.75 or newer
- Linux or WSL

## Getting Started

```bash
# Run a .tql query file
cargo run -- run query.tql

# Start interactive REPL
cargo run -- repl
```

### Embedded API
```rust
let mut graph = TemporalGraph::new();
graph.add_node("host_a", "Host", props!{ "critical" => false });
graph.add_edge("host_a", "host_b", "Connection", 1000, 1500, props!{});

let results = graph.query("
    MATCH (a:Host)-[r:Connection]->(b:Host)
    AT t=1200
    RETURN a, b
")?;
```

## Project Structure

```
GraphStream
├── crates/
│   ├── interval/               # Augmented Interval Tree - core temporal data structure
│   ├── temporal_graph/         # TemporalGraph - node/edge model built on interval_tree
│   ├── query/                  # Query engine
│   ├── sharding/               # Sharding - Shards, Watermarks and ShardingCluster
│   ├── tql_parser/             # TQL lexer, parser, and AST definitions
│   ├── engine/                 # Execution engine - tree-walk evaluator over TemporalGraph
│   ├── visualizer/             # Visualzier- Realtime and Sampler
│   └── cli/                    # Rust API, .tql file execution, and REPL
├── docs/
│   ├── adr/                    # Architecture Decision Records
│   └── tql-grammar.md          # EBNF grammar specification
├── examples/
│   ├── lateral_movement.tql    # Cybersecurity use case
│   ├── spoofing.tql            # Finance use case
│   └── decay_chain.tql         # Physics use case
├── main.rs
└── Cargo.toml
```

## TQL - Temporal Query Language

TQL is a hybrid query language combining Cypher-style graph pattern matching with SQL-style filtering and temporal operators.

### Temporal Operators

| Operator | Semantics |
|---|---|
| `AT t` | All edges active at time t |
| `BETWEEN t1, t2` | Snapshot over range [t1, t2] |
| `DURING [t1, t2]` | Overlap with interval |
| `BEFORE` / `AFTER` | Temporal ordering between events |
| `WITHIN(t1, t2) < D` | Absolute temporal closeness |
| `DIFF(prop, t1, t2)` | State change between two moments |
| `WINDOW D` | Sliding/tumbling window aggregation |

### Example Queries

**Lateral movement detection (cybersecurity)**
```
MATCH (a:Host)-[r1:Connection]->(b:Host)-[r2:Connection]->(c:Host)
WHERE r1.time BEFORE r2.time
  AND WITHIN(r1.time, r2.time) < 30 MINUTES
  AND c.critical = true
RETURN a, b, c
```

**Coordinated trading detection (finance)**
```
MATCH (t1:Trade)-[c:CORRELATED]->(t2:Trade)
WHERE c.correlation > 0.8
  AND t1.exchange != t2.exchange
  AND WITHIN(t1.time, t2.time) < 100 MILLISECONDS
WINDOW 1 SECOND
HAVING COUNT(*) >= 5
RETURN t1, t2
```

**Particle decay chain analysis (physics)**
```
MATCH (parent:Particle)-[d:DECAYS_TO]->(child:Particle)
WHERE WITHIN(parent.time, child.time) < 1.5e-12 SECONDS
RETURN parent, SUM(child.energy) AS total_energy
```

## Progress

### v1 - Single-node engine (Complete)
- Augmented Interval Tree - iterative, arena-allocated (`interval_tree` crate)
- TemporalGraph - node/edge container built on Interval Tree (`graph` crate)
- TQL lexer, recursive descent parser, and AST (`tql` crate)
- Execution engine - tree-walk evaluator over TemporalGraph (`engine` crate)
- Rust API, .tql file execution, and REPL (`cli` crate)
- Use case demos - lateral movement, spoofing, particle decay (`examples/`)
- ADR documentation (ADR-001 through ADR-009)
- EBNF grammar specification (`docs/tql-grammar.md`)

### v2 - Distributed execution + Kafka ingestion (Planned)
- Shard TemporalGraph by time window across nodes
- Distributed query planner - decompose TQL queries per shard, aggregate results
- Watermark mechanism for out-of-order event handling
- Exactly-once processing semantics
- Kafka consumer with documented JSON schema and idempotent writes
- Backpressure mechanism for ingest rate control

### v3 - Production deployment (Planned)
- Kubernetes operator with GraphStreamCluster CRD
- Reconciliation loop for lifecycle management (scale, update, heal)
- Horizontal Pod Autoscaler driven by custom metrics (ingest rate, query latency, watermark lag)
- Prometheus metric exporter
- Zero-downtime rolling updates

## Architecture Decisions
All architectural decisions are documented in `docs/adr/`. Key decisions:

- **ADR-001** - Hybrid TQL syntax paradigm (Cypher + SQL)
- **ADR-002** - Temporal operator set
- **ADR-003** - Single-node execution scope for v1
- **ADR-004** - Use-case-driven language design
- **ADR-005** - Query input methods and implementation order
- **ADR-006** - Iterative arena-allocated Interval Tree (supersedes naive BST approach)
- **ADR-007** - v2 distributed execution and Kafka ingestion
- **ADR-008** - v3 Kubernetes operator and auto-scaling

## References and Documentation
1. [Granite: A distributed engine for scalable path queries over temporal property graphs - Cormode et al., 2021] - related work, distributed temporal graph queries
2. [Maintaining Knowledge about Temporal Intervals - Allen, 1983] - theoretical foundation for TQL temporal operators
3. [Computational Geometry: Algorithms and Applications - de Berg et al.] - Augmented Interval Tree (Chapter 10)
4. [The Rust Programming Language - https://doc.rust-lang.org/book/]
