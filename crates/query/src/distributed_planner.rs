use chrono::{Duration, NaiveDate, NaiveDateTime};
use tql_parser::ast::*;

/// Time range extracted from a WHERE clause.
///
/// `None` bounds mean "unbounded" on that side. The planner uses this to
/// prune shards — only shards whose window overlaps `[from, to]` participate
/// in query execution.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeRange {
    pub from: Option<NaiveDateTime>,
    pub to: Option<NaiveDateTime>,
}

impl TimeRange {
    pub fn unbounded() -> Self {
        Self { from: None, to: None }
    }

    pub fn is_unbounded(&self) -> bool {
        self.from.is_none() && self.to.is_none()
    }

    /// Intersect with another range. Missing bounds are treated as ±∞.
    pub fn intersect(&self, other: &TimeRange) -> TimeRange {
        TimeRange {
            from: match (self.from, other.from) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None)    => Some(a),
                (None,    Some(b)) => Some(b),
                (None,    None)    => None,
            },
            to: match (self.to, other.to) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None)    => Some(a),
                (None,    Some(b)) => Some(b),
                (None,    None)    => None,
            },
        }
    }

    /// Whether `[window_from, window_to)` overlaps this range.
    pub fn overlaps(&self, window_from: NaiveDateTime, window_to: NaiveDateTime) -> bool {
        if let Some(f) = self.from {
            if f >= window_to { return false; }
        }
        if let Some(t) = self.to {
            if t < window_from { return false; }
        }
        true
    }
}

/// Extract a conservative time range from a WHERE clause.
///
/// Only literal timestamps (`TimeExpr::Timestamp`) produce bounds. Predicates
/// that reference property values (`r.time`, `parent.time`) cannot be
/// evaluated at plan time — they widen the range to unbounded on that side.
///
/// The extraction is conservative in the safe direction: if it cannot prove
/// a bound, it returns a wider range (never narrower), so no shard is
/// incorrectly pruned.
pub fn extract_time_range(where_clause: Option<&WhereClause>) -> TimeRange {
    let Some(w) = where_clause else { return TimeRange::unbounded() };

    let mut range = TimeRange::unbounded();
    for c in &w.conditions {
        let r = condition_range(c);
        range = range.intersect(&r);
    }
    range
}

fn condition_range(c: &Condition) -> TimeRange {
    match c {
        Condition::At(ac) => {
            if let Some(t) = literal_dt(&ac.instant) {
                TimeRange { from: Some(t), to: Some(t) }
            } else {
                TimeRange::unbounded()
            }
        }
        Condition::Between(bc) => TimeRange {
            from: literal_dt(&bc.t1),
            to:   literal_dt(&bc.t2),
        },
        Condition::During(dc) => TimeRange {
            from: literal_dt(&dc.t1),
            to:   literal_dt(&dc.t2),
        },
        Condition::Ordering(oc) => {
            let l = literal_dt(&oc.left);
            let r = literal_dt(&oc.right);
            match (l, r, oc.order) {
                (Some(l), Some(r), Order::Before) => TimeRange { from: Some(l), to: Some(r) },
                (Some(l), Some(r), Order::After)  => TimeRange { from: Some(r), to: Some(l) },
                _ => TimeRange::unbounded(),
            }
        }
        // Property-dependent predicates — cannot bound without data.
        Condition::Within(_) | Condition::Diff(_) | Condition::Property(_) => {
            TimeRange::unbounded()
        }
    }
}

fn literal_dt(e: &TimeExpr) -> Option<NaiveDateTime> {
    match e {
        TimeExpr::Timestamp(ms) => Some(epoch() + Duration::milliseconds(*ms)),
        _ => None,
    }
}

fn epoch() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tql_parser::ast::Query;

    fn t(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::seconds(secs)
    }

    fn range_of(tql: &str) -> TimeRange {
        let q: Query = tql_parser::parse(tql).unwrap();
        extract_time_range(q.where_clause.as_ref())
    }

    #[test]
    fn no_where_is_unbounded() {
        let r = range_of("MATCH (a:Host) RETURN a");
        assert!(r.is_unbounded());
    }

    #[test]
    fn between_literals_narrows() {
        let r = range_of(
            "MATCH (a:Host) WHERE a.created BETWEEN 1000, 2000 RETURN a"
        );
        // 1000 ms and 2000 ms after epoch
        assert_eq!(r.from, Some(epoch() + Duration::milliseconds(1000)));
        assert_eq!(r.to,   Some(epoch() + Duration::milliseconds(2000)));
    }

    #[test]
    fn intersect_keeps_tighter_bounds() {
        let a = TimeRange { from: Some(t(0)),   to: Some(t(100)) };
        let b = TimeRange { from: Some(t(50)),  to: Some(t(200)) };
        let c = a.intersect(&b);
        assert_eq!(c.from, Some(t(50)));
        assert_eq!(c.to,   Some(t(100)));
    }

    #[test]
    fn property_predicate_widens() {
        let r = range_of(
            "MATCH (a:Host) WHERE WITHIN(a.time, a.time) < 30 MINUTES RETURN a"
        );
        assert!(r.is_unbounded());
    }
}