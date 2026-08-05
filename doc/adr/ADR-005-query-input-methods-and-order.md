# ADR-005: Query input methods and implementation order

## Status
Accepted

## Context
Users (and Claude/the developer, during testing) need a way to submit
TQL queries to GraphStream. Three input methods are relevant: an
embedded Rust API, `.tql` script files, and an interactive REPL. These
could be built in any order or in parallel.

## Decision
Three input methods, built in this order:
1. Rust embedded API (`graph.query("...")`)
2. `.tql` file execution (`graphstream run query.tql`)
3. Interactive REPL

## Rationale
- The embedded API is the foundation — the parser and query execution
  path must exist here regardless of which other input methods are
  built, so building it first avoids redundant work.
- File execution is a thin wrapper once the API exists (read file,
  pass contents to the same `query()` call) — negligible additional
  design cost once step 1 is done.
- The REPL is the most polish-oriented of the three (line editing,
  history, interactive feedback) and depends on the same query path;
  building it last avoids investing in UX before the core engine is
  stable.

## Consequences
- Early development/testing of the language happens entirely through
  the Rust API (e.g. in tests), which is sufficient for grammar and
  execution validation but not for manual exploration.
- REPL-specific concerns (error message formatting for interactive use,
  multi-line query input) are deferred until the grammar has already
  stabilized, which should reduce rework.
- If development time runs short, file execution is the method most
  likely to ship since it requires the least additional work beyond
  the API.
