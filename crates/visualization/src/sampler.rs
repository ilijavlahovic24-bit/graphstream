use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDateTime};
use tokio::sync::broadcast;

use crate::control::{ControlAction, Mode, SamplerState};
use crate::snapshot::{epoch, ms, Snapshot};
use crate::summary::SnapshotSource;

const BROADCAST_CAPACITY: usize = 32;

/// Drives snapshot generation and pushes them to subscribers.
///
/// The sampler owns the *virtual clock* used in playback mode. Live mode
/// ignores the virtual clock and uses wall time. Both modes build a
/// snapshot every `tick_interval` and publish it on the broadcast channel
/// returned by [`subscribe`].
pub struct Sampler<S: SnapshotSource> {
    source: Arc<S>,
    tick_interval: Duration,
    default_speed: f64,
    state: Arc<RwLock<SamplerState>>,
    tx: broadcast::Sender<Arc<Snapshot>>,
    total_ticks: Arc<AtomicU64>,
    subscriber_count: Arc<AtomicUsize>,
}

impl<S: SnapshotSource> Sampler<S> {
    pub fn new(source: Arc<S>) -> Self {
        let (from, to) = source.bounds();
        let from = from.unwrap_or_else(|| epoch());
        let to = to.unwrap_or_else(|| from + ChronoDuration::seconds(60));

        let initial = SamplerState {
            mode: Mode::Playback,
            playing: false,
            speed: 1.0,
            loop_: true,
            virtual_time_ms: ms(from),
            from_ms: ms(from),
            to_ms: ms(to),
            subscriber_count: 0,
        };

        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);

        Self {
            source,
            tick_interval: Duration::from_millis(200),
            default_speed: 1.0,
            state: Arc::new(RwLock::new(initial)),
            tx,
            total_ticks: Arc::new(AtomicU64::new(0)),
            subscriber_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    pub fn with_default_speed(mut self, speed: f64) -> Self {
        self.default_speed = speed;
        if let Ok(mut s) = self.state.write() {
            s.speed = speed;
        }
        self
    }

    /// Subscribe to the snapshot stream.
    ///
    /// Returns a receiver and a guard that decrements the subscriber
    /// counter when dropped. Wrap the guard in a variable so the count
    /// stays accurate for the duration of the connection.
    pub fn subscribe(&self) -> (broadcast::Receiver<Arc<Snapshot>>, SubscriberGuard) {
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut s) = self.state.write() {
            s.subscriber_count = self.subscriber_count.load(Ordering::Relaxed);
        }
        let guard = SubscriberGuard {
            count: self.subscriber_count.clone(),
        };
        (self.tx.subscribe(), guard)
    }

    pub fn state(&self) -> SamplerState {
        self.state.read().map(|s| s.clone()).unwrap_or(SamplerState {
            mode: Mode::Playback,
            playing: false,
            speed: self.default_speed,
            loop_: false,
            virtual_time_ms: 0,
            from_ms: 0,
            to_ms: 0,
            subscriber_count: 0,
        })
    }

    pub fn source(&self) -> &S {
        &self.source
    }

    /// Apply a control action.
    pub fn apply(&self, action: ControlAction) -> Result<(), String> {
        let mut s = self.state.write().map_err(|_| "state lock poisoned".to_string())?;
        match action {
            ControlAction::SetMode { mode } => {
                s.mode = mode;
                // Switching to playback resets the virtual clock to the
                // current state's start; switching to live snaps to now.
                if mode == Mode::Live {
                    s.playing = false;
                }
            }
            ControlAction::Play => {
                if s.mode == Mode::Playback {
                    s.playing = true;
                }
            }
            ControlAction::Pause => {
                s.playing = false;
            }
            ControlAction::Seek { t_ms } => {
                let t = t_ms.clamp(s.from_ms, s.to_ms);
                s.virtual_time_ms = t;
            }
            ControlAction::SetSpeed { value } => {
                if value <= 0.0 || !value.is_finite() {
                    return Err("speed must be a positive finite number".into());
                }
                s.speed = value;
            }
            ControlAction::SetLoop { value } => {
                s.loop_ = value;
            }
            ControlAction::SetRange { from_ms, to_ms } => {
                if to_ms <= from_ms {
                    return Err("to_ms must be greater than from_ms".into());
                }
                s.from_ms = from_ms;
                s.to_ms = to_ms;
                s.virtual_time_ms = s.virtual_time_ms.clamp(from_ms, to_ms);
            }
        }
        Ok(())
    }

    /// Run the sampler loop until the process is terminated.
    ///
    /// Each tick:
    /// 1. Advances the virtual clock (playback) or reads wall clock (live).
    /// 2. Builds a snapshot at that instant.
    /// 3. Publishes it to subscribers.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(self.tick_interval);
        let dt_secs = self.tick_interval.as_secs_f64();

        loop {
            ticker.tick().await;

            let t = self.advance(dt_secs);
            let snap = Arc::new(self.source.snapshot_at(t));

            self.total_ticks.fetch_add(1, Ordering::Relaxed);

            // Broadcast failure just means no subscribers are listening.
            let _ = self.tx.send(snap);
        }
    }

    /// Compute the timestamp for the current tick.
    fn advance(&self, dt_secs: f64) -> NaiveDateTime {
        let mut s = match self.state.write() {
            Ok(s) => s,
            Err(_) => return chrono::Utc::now().naive_utc(),
        };

        match s.mode {
            Mode::Live => chrono::Utc::now().naive_utc(),

            Mode::Playback => {
                let mut t_ms = s.virtual_time_ms;
                if s.playing {
                    let step_ms = (dt_secs * s.speed * 1000.0).round() as i64;
                    t_ms += step_ms.max(1);

                    if t_ms > s.to_ms {
                        if s.loop_ {
                            t_ms = s.from_ms;
                        } else {
                            t_ms = s.to_ms;
                            s.playing = false;
                        }
                    }
                    s.virtual_time_ms = t_ms;
                }
                epoch() + ChronoDuration::milliseconds(t_ms)
            }
        }
    }

    pub fn total_ticks(&self) -> u64 {
        self.total_ticks.load(Ordering::Relaxed)
    }
}

/// Decrements the subscriber count when dropped.
pub struct SubscriberGuard {
    count: Arc<AtomicUsize>,
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use chrono::NaiveDate;
    use temporal_graph::TemporalGraph;

    fn t(secs: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + ChronoDuration::seconds(secs)
    }

    fn props() -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }

    fn make_graph() -> TemporalGraph {
        let mut g = TemporalGraph::new(64);
        let a = g.add_node("Host", props());
        let b = g.add_node("Host", props());
        g.add_edge(a, b, "Connection", t(0), t(10), props()).unwrap();
        g.add_edge(a, b, "Connection", t(20), t(30), props()).unwrap();
        g
    }

    #[test]
    fn initial_state_reflects_bounds() {
        let s = Sampler::new(Arc::new(make_graph()));
        let st = s.state();
        assert_eq!(st.mode, Mode::Playback);
        assert!(!st.playing);
        assert_eq!(st.from_ms, ms(t(0)));
        assert_eq!(st.to_ms, ms(t(30)));
    }

    #[test]
    fn seek_clamps_to_bounds() {
        let s = Sampler::new(Arc::new(make_graph()));
        s.apply(ControlAction::Seek { t_ms: ms(t(100)) }).unwrap();
        assert_eq!(s.state().virtual_time_ms, ms(t(30)));
        s.apply(ControlAction::Seek { t_ms: ms(t(-100)) }).unwrap();
        assert_eq!(s.state().virtual_time_ms, ms(t(0)));
    }

    #[test]
    fn set_speed_rejects_zero_and_negative() {
        let s = Sampler::new(Arc::new(make_graph()));
        assert!(s.apply(ControlAction::SetSpeed { value: 0.0 }).is_err());
        assert!(s.apply(ControlAction::SetSpeed { value: -1.0 }).is_err());
        assert!(s.apply(ControlAction::SetSpeed { value: 10.0 }).is_ok());
    }

    #[test]
    fn playback_advances_and_loops() {
        let s = Sampler::new(Arc::new(make_graph())).with_default_speed(1000.0);
        s.apply(ControlAction::SetMode { mode: Mode::Playback }).unwrap();
        s.apply(ControlAction::SetLoop { value: true }).unwrap();
        s.apply(ControlAction::Play).unwrap();

        // Tick with dt=0.2s at speed 1000 → +200s per tick. Since window
        // is 30s, first tick should wrap back to from_ms.
        let _ = s.advance(0.2);
        let st = s.state();
        assert_eq!(st.virtual_time_ms, ms(t(0)));
        assert!(st.playing, "loop should keep playing");
    }
}