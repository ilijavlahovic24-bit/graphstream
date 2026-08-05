# ADR-001: Hybrid syntax paradigm for TQL

## Status
Accepted

## Context
TQL needs a query syntax for GraphStream. Options considered: SQL-like
(SELECT/FROM/WHERE), graph-native (Cypher/Gremlin-style MATCH and traversal),
or a hybrid of the two.

GraphStream must support both batch (snapshot/historical) and stream
(live event) queries sharing the same language, and must express both
graph traversal (pattern matching across nodes/edges) and temporal
reasoning (intervals, ordering, windows) without one concern distorting
the other.

## Decision
Hybrid syntax: Cypher-style `MATCH` for graph pattern/traversal, combined
with SQL-style `WHERE`/`RETURN` clauses for filtering and projection, with
temporal operators (`AT`, `BETWEEN`, `DURING`, `EVOLVE`/`DIFF`,
`BEFORE`/`AFTER`, `WINDOW`) as first-class clauses rather than functions
or WHERE-predicates.

## Rationale
- Graph traversal expressed as JOINs (pure SQL) becomes unreadable for
  multi-hop patterns; MATCH is the natural fit.
- Filtering, aggregation, and projection are well-solved by SQL-style
  clauses; reinventing this in a graph-native DSL adds no value.
- Temporal operators as first-class clauses (not WHERE-predicates) keep
  the temporal dimension visually and semantically distinct from regular
  property filters, which matters since temporal reasoning is the core
  differentiator of the language.
- Batch and stream queries can share a single AST under this model;
  only the execution engine differs by query mode.

## Consequences
- Grammar is more complex than either pure paradigm alone — more clause
  types to define and parse.
- No existing language to copy wholesale; grammar decisions require more
  original design work and testing against concrete query examples.
- Risk of clause ordering ambiguity (e.g. interaction between WINDOW and
  WHERE) that must be resolved explicitly in the grammar.
