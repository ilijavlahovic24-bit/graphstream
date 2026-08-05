# ADR-004: Use-case-driven language design, not execution-driven

## Status
Accepted

## Context
GraphStream's target use cases (e.g. network attack propagation
detection, correlated event analysis) imply requirements at two
different layers: the query language's expressiveness, and the
execution engine's performance/scale characteristics. Given the
single-node v1 scope (ADR-003), these two layers needed to be
explicitly decoupled to avoid use cases silently pulling in distributed
systems requirements through the back door.

## Decision
Target use cases drive the design of the TQL grammar and AST (which
patterns and temporal relations the language must be able to express),
not the requirements on the execution engine's performance or scale.
The execution engine remains a simple, single-node IntervalTree-backed
implementation regardless of use case complexity.

## Rationale
- Mirrors the approach taken in AIPlasma: domain use cases (there,
  plasma physics problems) shaped the `PhysicsProblem`/`PhysicsModel`
  interface design, while the solver implementation underneath stayed
  simple in v1 — the interface was designed to not require a redesign
  when a more capable solver is added later.
- This keeps the project's central deliverable demonstrable: a TQL
  query that expresses a realistic use case (e.g. a multi-hop temporal
  pattern with ordering constraints) can be written and shown to
  execute correctly, even though it runs on a small synthetic dataset
  rather than production-scale data.
- Prevents scope creep: without this separation, "support this use
  case" implicitly becomes "support this use case at the throughput/
  scale a real deployment would need," which reintroduces the
  distributed systems scope ruled out in ADR-003.

## Consequences
- Use case demos in v1 are necessarily synthetic and small-scale; they
  prove language expressiveness, not production readiness.
- The AST and grammar must be designed generally enough to not require
  a breaking redesign when a more capable execution engine (e.g.
  distributed, streaming at scale) is added later — this is a real
  design constraint even though that engine isn't built now.
- README/CV framing must clearly state that use cases are demonstrated
  at language level, not at scale, to avoid overstating v1's
  capabilities.
