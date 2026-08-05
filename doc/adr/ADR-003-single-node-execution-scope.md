# ADR-003: Single-node execution scope for v1

## Status
Accepted

## Context
GraphStream was originally conceived as distributed infrastructure for
temporal graph pattern detection (analogous in ambition to systems like
Granite, which targets distributed execution over large-scale temporal
property graphs). Distributed execution involves sharding, consistency
protocols, and watermarking for out-of-order events — each a substantial
project on its own.

The project must be completable as a single-developer effort alongside
ongoing coursework, while still producing a system that demonstrates the
target use cases meaningfully.

## Decision
v1 executes on a single node only. Distributed execution is explicitly
deferred to future work and is not a v1 requirement.

## Rationale
- Distributed consistency, sharding, and watermarking each represent
  significant independent scope; combining them with TQL grammar and
  parser design in a single timeframe is not realistic.
- The target use cases (e.g. lateral movement pattern detection) can be
  demonstrated meaningfully on a single-node synthetic dataset — the
  scale of execution does not change whether the query language can
  express the pattern.
- Single-node scope keeps the project's core contribution clear: the
  TQL language design and IntervalTree-based temporal execution, not
  distributed systems engineering (which is already a separate, deep
  area covered by other projects).

## Consequences
- No claims can be made about distributed throughput, fault tolerance,
  or consistency guarantees for v1.
- Synthetic benchmarks are scoped to single-node performance
  (e.g. interval tree query latency), not comparative claims against
  distributed systems.
- Architecture (TQL AST, query model) should avoid decisions that would
  preclude a distributed execution engine later, but should not be
  complicated in v1 to accommodate it preemptively.
