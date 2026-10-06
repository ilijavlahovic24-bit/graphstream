use chrono::{Duration, NaiveDateTime};

/// Tracks the maximum observed event time for one shard, with an
/// `allowed_lateness` grace period for out-of-order arrivals.
///
/// Semantics:
/// * `watermark = max_seen - allowed_lateness`
/// * An event with `start_time >= watermark` is **on time** (accepted).
/// * An event with `start_time < watermark` is **late** (rejected).
///
/// `Duration::zero()` means strictly ordered input — any older event is
/// late.
#[derive(Debug, Clone)]
pub struct Watermark {
    max_seen: Option<NaiveDateTime>,
    allowed_lateness: Duration,
}

impl Watermark {
    pub fn new(allowed_lateness: Duration) -> Self {
        Self { max_seen: None, allowed_lateness }
    }

    /// Strictly ordered watermark: any event older than the latest is late.
    pub fn strict() -> Self {
        Self::new(Duration::zero())
    }

    pub fn allowed_lateness(&self) -> Duration {
        self.allowed_lateness
    }

    pub fn max_seen(&self) -> Option<NaiveDateTime> {
        self.max_seen
    }

    /// Current watermark value. `None` until the first event is observed.
    pub fn value(&self) -> Option<NaiveDateTime> {
        self.max_seen.map(|t| t - self.allowed_lateness)
    }

    /// Whether an event at `ts` is on time. Does not advance the watermark —
    /// call [`advance`] separately after ingestion succeeds.
    ///
    /// [`advance`]: Self::advance
    pub fn accepts(&self, ts: NaiveDateTime) -> bool {
        match self.value() {
            None => true,
            Some(w) => ts >= w,
        }
    }

    /// Advance the watermark by observing an event time. No-op if the event
    /// is older than the current `max_seen`.
    pub fn advance(&mut self, ts: NaiveDateTime) {
        self.max_seen = Some(match self.max_seen {
            Some(prev) if prev >= ts => prev,
            _ => ts,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn t(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::seconds(secs)
    }

    #[test]
    fn empty_watermark_accepts_everything() {
        let wm = Watermark::strict();
        assert!(wm.accepts(t(-1_000_000)));
        assert!(wm.accepts(t(0)));
        assert!(wm.accepts(t(1_000_000)));
        assert!(wm.value().is_none());
    }

    #[test]
    fn strict_watermark_rejects_older() {
        let mut wm = Watermark::strict();
        wm.advance(t(10));
        assert!(wm.accepts(t(10)));
        assert!(!wm.accepts(t(9)));
    }

    #[test]
    fn lateness_allows_grace_period() {
        let mut wm = Watermark::new(Duration::seconds(5));
        wm.advance(t(10));
        // watermark = t(5); events >= t(5) accepted
        assert!(wm.accepts(t(5)));
        assert!(wm.accepts(t(8)));
        assert!(!wm.accepts(t(4)));
    }

    #[test]
    fn advance_is_monotonic() {
        let mut wm = Watermark::strict();
        wm.advance(t(10));
        wm.advance(t(5)); // older -> ignored
        assert_eq!(wm.max_seen(), Some(t(10)));
    }
}