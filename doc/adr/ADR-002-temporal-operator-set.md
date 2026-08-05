# ADR-002: Temporal operator set for TQL

## Status
Accepted

## Context
TQL needs a defined, finite set of temporal operators rather than an
open-ended set added ad hoc as use cases arise. The operator set must
cover the temporal relations needed to express the target use cases
(e.g. attack propagation detection across a network over time) without
growing into a general-purpose temporal logic.

## Decision
Five temporal operators, each mapped to a distinct temporal relation:
- `AT` / `BETWEEN` — point-in-time and range snapshot queries
- `DURING` — interval overlap
- `EVOLVE` / `DIFF` — state change between two points in time
- `BEFORE` / `AFTER` — ordering between events/intervals
- `WINDOW` — sliding/tumbling window aggregation, primarily for stream mode

## Rationale
- These five cover the temporal relations required by the target use
  cases: snapshot inspection, overlap detection, change tracking,
  causal ordering, and windowed aggregation.
- Each operator maps to a distinct, well-understood temporal concept
  (closely related to Allen's interval algebra), keeping the language
  learnable rather than an arbitrary grab-bag.
- A small, fixed operator set keeps both the grammar and the execution
  engine scoped — each operator needs a defined evaluation strategy
  against the IntervalTree, and an open-ended set would make that
  intractable for a single-node v1.

## Consequences
- Temporal patterns not expressible via these five operators (e.g.
  complex multi-interval Allen relations beyond overlap/order) are out
  of scope for v1 and would require grammar extension later.
- `BEFORE`/`AFTER` and `DURING` have overlapping semantics in edge cases
  (e.g. adjacent intervals) that must be precisely defined during
  grammar/execution design, not left implicit.
- WINDOW's batch-mode semantics (vs. its natural stream-mode meaning)
  need explicit definition since the same clause must work in both
  execution modes.
