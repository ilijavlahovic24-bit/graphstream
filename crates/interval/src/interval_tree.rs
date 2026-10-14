//! Arena-allocated, iterative Augmented Interval Tree.
//!
//! All nodes live in a single `Vec<Node>` owned by [`IntervalTree`]; child
//! links are `Option<usize>` indices into that `Vec` rather than `Box`
//! pointers. Traversal uses an explicit stack of indices — no recursion
//! anywhere in this module.
//!
//! See `docs/adr/ADR-007-iterative-arena-interval-tree.md` for the rationale.

use chrono::NaiveDateTime;
use serde_json::Value;
use std::collections::HashMap;

/// A time interval `[start, end]` (closed) with associated properties.
#[derive(Clone, Debug, PartialEq)]
pub struct Interval {
    pub start_time: NaiveDateTime,
    pub end_time: NaiveDateTime,
    pub properties: HashMap<String, Value>,
}

impl Interval {
    pub fn new(start_time: NaiveDateTime, end_time: NaiveDateTime) -> Self {
        Self {
            start_time,
            end_time,
            properties: HashMap::new(),
        }
    }

    pub fn with_properties(mut self, properties: HashMap<String, Value>) -> Self {
        self.properties = properties;
        self
    }

    /// Closed-interval overlap test against `[t1, t2]`.
    #[inline]
    pub fn overlaps(&self, t1: NaiveDateTime, t2: NaiveDateTime) -> bool {
        self.start_time <= t2 && self.end_time >= t1
    }
}

#[derive(Clone, Debug)]
struct Node {
    interval: Interval,
    /// Augmented value: max `interval.end_time` in this node's subtree.
    max: NaiveDateTime,
    left: Option<usize>,
    right: Option<usize>,
    /// Tombstone flag. Deleted nodes remain in the arena to keep indices
    /// valid; they are skipped by queries and reclaimed by [`IntervalTree::compact`].
    deleted: bool,
}

/// Arena-allocated Augmented Interval Tree.
///
/// Invariants:
/// * `nodes` is the arena; `root` is an index into it (or `None` if empty).
/// * Every non-deleted node is reachable from `root` via BST-ordered
///   `left`/`right` links keyed on `interval.start_time`.
/// * `max` is the maximum `interval.end_time` over the subtree rooted at
///   that node (deleted nodes are conservatively included — the bound
///   stays valid, just possibly loose).
/// * `len` counts non-deleted nodes.
#[derive(Clone, Debug)]
pub struct IntervalTree {
    nodes: Vec<Node>,
    root: Option<usize>,
    len: usize,
}

impl Default for IntervalTree {
    fn default() -> Self {
        Self::new()
    }
}

impl IntervalTree {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            root: None,
            len: 0,
        }
    }

    /// Number of live (non-tombstoned) intervals.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Total slots occupied in the arena, including tombstones.
    pub fn capacity_used(&self) -> usize {
        self.nodes.len()
    }

    /// Iterate over all live intervals in arena order (not sorted).
    pub fn iter(&self) -> impl Iterator<Item = &Interval> {
        self.nodes.iter().filter(|n| !n.deleted).map(|n| &n.interval)
    }

    /// Insert a new interval.
    ///
    /// Iterative descent; `max` is augmented along the descent path — no
    /// recursion, no path recording needed.
    pub fn insert(&mut self, interval: Interval) {
        let end = interval.end_time;
        let idx = self.nodes.len();

        self.nodes.push(Node {
            interval,
            max: end,
            left: None,
            right: None,
            deleted: false,
        });
        self.len += 1;

        let Some(mut cur) = self.root else {
            self.root = Some(idx);
            return;
        };

        loop {
            // Augment `max` on the way down.
            if self.nodes[cur].max < end {
                self.nodes[cur].max = end;
            }

            let go_left =
                self.nodes[idx].interval.start_time < self.nodes[cur].interval.start_time;
            let next = if go_left {
                self.nodes[cur].left
            } else {
                self.nodes[cur].right
            };

            match next {
                Some(child) => cur = child,
                None => {
                    if go_left {
                        self.nodes[cur].left = Some(idx);
                    } else {
                        self.nodes[cur].right = Some(idx);
                    }
                    return;
                }
            }
        }
    }

    /// All intervals overlapping the closed range `[t1, t2]`.
    ///
    /// Uses `max` for left-subtree pruning and `interval.start_time <= t2`
    /// for right-subtree pruning. Explicit stack — no recursion.
    pub fn overlap(&self, t1: NaiveDateTime, t2: NaiveDateTime) -> Vec<&Interval> {
        let mut out = Vec::new();
        let Some(root) = self.root else {
            return out;
        };

        let mut stack: Vec<usize> = Vec::with_capacity(16);
        stack.push(root);

        while let Some(i) = stack.pop() {
            let n = &self.nodes[i];

            // Prune whole subtree: nothing here can reach t1.
            if n.max < t1 {
                continue;
            }

            if !n.deleted && n.interval.overlaps(t1, t2) {
                out.push(&n.interval);
            }

            // Left subtree may still contain overlaps (max >= t1).
            if let Some(l) = n.left {
                stack.push(l);
            }
            // Right subtree only if this node's start is within range.
            if n.interval.start_time <= t2 {
                if let Some(r) = n.right {
                    stack.push(r);
                }
            }
        }
        out
    }

    /// Point query: all intervals active at instant `t`.
    pub fn at(&self, t: NaiveDateTime) -> Vec<&Interval> {
        self.overlap(t, t)
    }

    /// Range query: all intervals active anywhere in `[t1, t2]`.
    pub fn between(&self, t1: NaiveDateTime, t2: NaiveDateTime) -> Vec<&Interval> {
        self.overlap(t1, t2)
    }

    /// Set difference between active intervals at `t1` and at `t2`.
    ///
    /// Returns `(only_at_t1, only_at_t2)`. Identity is by pointer into the
    /// arena, so the same stored interval appearing on both sides is not
    /// reported.
    pub fn diff(
        &self,
        t1: NaiveDateTime,
        t2: NaiveDateTime,
    ) -> (Vec<&Interval>, Vec<&Interval>) {
        let a = self.at(t1);
        let b = self.at(t2);

        let only_t1: Vec<&Interval> = a
            .iter()
            .copied()
            .filter(|iv| !b.iter().any(|x| std::ptr::eq(*x, *iv)))
            .collect();

        let only_t2: Vec<&Interval> = b
            .iter()
            .copied()
            .filter(|iv| !a.iter().any(|x| std::ptr::eq(*x, *iv)))
            .collect();

        (only_t1, only_t2)
    }

    /// Tombstone delete: mark the first live interval matching `(start, end)`
    /// as deleted. Returns `true` if found.
    ///
    /// O(n) — acceptable for v1 which is append-heavy; call [`compact`] to
    /// reclaim tombstoned slots in bulk.
    ///
    /// [`compact`]: Self::compact
    pub fn delete(&mut self, target: &Interval) -> bool {
        for n in &mut self.nodes {
            if !n.deleted
                && n.interval.start_time == target.start_time
                && n.interval.end_time == target.end_time
            {
                n.deleted = true;
                self.len -= 1;
                return true;
            }
        }
        false
    }

    /// Rebuild the arena, dropping tombstoned nodes and restoring tight
    /// `max` bounds. O(n log n).
    pub fn compact(&mut self) {
        let old = std::mem::take(&mut self.nodes);
        self.root = None;
        self.len = 0;
        for node in old {
            if !node.deleted {
                self.insert(node.interval);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate};

    fn t(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::seconds(secs)
    }

    fn iv(a: i64, b: i64) -> Interval {
        Interval::new(t(a), t(b))
    }

    #[test]
    fn empty_tree() {
        let tree = IntervalTree::new();
        assert!(tree.is_empty());
        assert!(tree.at(t(0)).is_empty());
        assert!(tree.overlap(t(0), t(10)).is_empty());
        assert!(tree.between(t(0), t(10)).is_empty());
    }

    #[test]
    fn single_interval() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(5, 10));
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.at(t(5)).len(), 1);  // boundary start
        assert_eq!(tree.at(t(7)).len(), 1);
        assert_eq!(tree.at(t(10)).len(), 1); // boundary end
        assert_eq!(tree.at(t(4)).len(), 0);
        assert_eq!(tree.at(t(11)).len(), 0);
    }

    #[test]
    fn full_overlap() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(0, 100));
        assert_eq!(tree.overlap(t(10), t(20)).len(), 1);
        assert_eq!(tree.overlap(t(-10), t(200)).len(), 1);
        assert_eq!(tree.overlap(t(0), t(0)).len(), 1);
    }

    #[test]
    fn no_overlap() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(0, 10));
        tree.insert(iv(20, 30));
        assert_eq!(tree.overlap(t(11), t(19)).len(), 0);
        assert_eq!(tree.at(t(15)).len(), 0);
    }

    #[test]
    fn adjacent_intervals_closed() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(0, 10));
        tree.insert(iv(10, 20));
        // Closed intervals share the endpoint t=10.
        assert_eq!(tree.at(t(10)).len(), 2);
        assert_eq!(tree.at(t(9)).len(), 1);
        assert_eq!(tree.at(t(11)).len(), 1);
    }

    #[test]
    fn diff_reports_change() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(0, 10));
        tree.insert(iv(20, 30));
        let (only_t1, only_t2) = tree.diff(t(5), t(25));
        assert_eq!(only_t1.len(), 1);
        assert_eq!(only_t2.len(), 1);
    }

    #[test]
    fn diff_no_change() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(0, 100));
        let (only_t1, only_t2) = tree.diff(t(10), t(20));
        assert!(only_t1.is_empty());
        assert!(only_t2.is_empty());
    }

    #[test]
    fn delete_and_compact() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(0, 10));   // A
        tree.insert(iv(5, 15));   // B
        tree.insert(iv(20, 30));  // C

        // Before deletion: at t=7 both A and B are active.
        assert_eq!(tree.at(t(7)).len(), 2);
        assert_eq!(tree.len(), 3);

        assert!(tree.delete(&iv(5, 15))); // B is tombstoned
        assert_eq!(tree.len(), 2);

        // After deletion: only A remains at t=7.
        assert_eq!(tree.at(t(7)).len(), 1);

        let used_before = tree.capacity_used();
        assert_eq!(used_before, 3); // B is still in the arena

        tree.compact();
        assert_eq!(tree.len(), 2);
        assert_eq!(tree.capacity_used(), 2); // B reclaimed
        assert!(tree.capacity_used() <= used_before);

        // C is still active at t=25.
        assert_eq!(tree.at(t(25)).len(), 1);
    }

    #[test]
    fn delete_missing_returns_false() {
        let mut tree = IntervalTree::new();
        tree.insert(iv(0, 10));
        assert!(!tree.delete(&iv(1, 2)));
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn degenerate_sorted_insertions_do_not_overflow() {
        // The exact scenario ADR-007 calls out: chronological inserts.
        // 5,000 is enough to exercise the iterative path — the absence
        // of recursion (not the count) is what prevents overflow.
        let mut tree = IntervalTree::new();
        for i in 0..5_000 {
            tree.insert(iv(i, i + 1));
        }
        assert_eq!(tree.len(), 5_000);

        // Query strictly inside one interval to avoid the closed-interval
        // boundary (adjacent intervals share endpoints).
        let mid = t(2_500) + Duration::milliseconds(500);
        assert_eq!(tree.at(mid).len(), 1);
    }

    #[test]
    #[ignore] // Run with: cargo test -p interval --release -- --ignored
    fn degenerate_50k_stress() {
        // Full-scale stress test. Kept ignored so the default test suite
        // completes quickly; run it manually before a release or when
        // benchmarking.
        let mut tree = IntervalTree::new();
        for i in 0..50_000 {
            tree.insert(iv(i, i + 1));
        }
        assert_eq!(tree.len(), 50_000);
    }
}