# ADR-007: Iterative implementation of Augmented Interval Tree with arena allocation

## Status
Accepted

## Context
GraphStream requires an Augmented Interval Tree as its core temporal data structure.
Two implementation approaches were considered:

**Recursive + Box<Node>**
The recursive approach maps directly onto the tree's definition and handles max_end
updates naturally on the call stack unwind. Box<T> is the closest Rust equivalent to
C++ heap pointers and is familiar from classical BST implementations.

**Iterative + Arena (Vec<Node> + usize indices)**
All nodes live in a flat Vec; edges between nodes are usize indices rather than
pointers. Traversal uses an explicit stack of indices instead of the call stack.

The primary concern with the recursive approach is stack overflow on degenerate trees.
Temporal event data in GraphStream arrives in chronological order by nature - lateral
movement detections, trade timestamps, and particle decay chains all produce
monotonically increasing start_time values. Inserting sorted intervals into an
unbalanced BST produces a degenerate linear chain, pushing recursion depth to O(n).
With Rust's default 8MB stack and ~150 bytes per frame, this overflows at roughly
55,000 nodes - a realistic dataset size for the cybersecurity use case (5,000 edges
per synthetic dataset, scaling toward production).

The recursive approach is salvageable only with balancing (AVL or Red-Black Tree),
which was already covered in coursework and adds significant implementation scope
without introducing new concepts.

## Decision
Iterative implementation with arena allocation: all IntervalTreeNode instances are
stored in a Vec<IntervalTreeNode> owned by IntervalTree, with left/right children
represented as Option<usize> indices into that Vec.

## Rationale
- Temporal data in GraphStream arrives sorted by start_time, making stack overflow
  a realistic runtime risk with recursive Box-based implementation, not a theoretical
  one. Arena eliminates this risk entirely without requiring tree balancing.
- Rust's borrow checker makes iterative traversal over Box<Node> require unsafe raw
  pointers to hold mutable references across loop iterations. Arena traversal uses
  usize indices - plain integer reads from Vec - which the borrow checker handles
  without unsafe.
- max_end updates on insert require walking back from the inserted node to the root.
  Recursive implementation gets this for free on stack unwind; iterative implementation
  requires explicitly recording the path during descent. This is additional code, but
  it is straightforward and well-understood.
- Vec<Node> is a contiguous memory allocation. Box<Node> heap-allocates each node
  individually, producing fragmented memory and poor cache behavior on traversal.
  For overlap queries that visit many nodes (O(log n + k)), cache locality matters.
- AVL balancing - the natural alternative to fix recursive stack overflow - was already
  implemented in coursework. Repeating a known solution adds no learning value and
  less differentiation on CV and in interviews than a novel allocator design choice.

## Consequences
- No stack overflow risk regardless of insertion order, including worst-case sorted
  temporal data.
- No unsafe code required for tree traversal or mutation.
- Node deletion is more complex: removing a node from Vec shifts indices and invalidates
  all existing references. This is addressed with a tombstone approach (mark deleted,
  compact periodically) and is acceptable for v1 where interval trees are append-heavy.
- Arena allocator pattern is less familiar than Box-based trees; the usize-as-pointer
  idiom requires a brief explanation for anyone reading the code. This is addressed
  by a module-level doc comment in interval_tree/src/lib.rs.
- If the project is later ported to a distributed setting (v2), arena-allocated nodes
  are straightforward to serialize and shard by time window - each shard is a
  sub-Vec with a local index space.