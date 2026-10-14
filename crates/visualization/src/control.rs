use serde::{Deserialize, Serialize};

/// Runtime mode of the sampler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Advance with wall-clock time. Snapshots reflect "now".
    Live,
    /// Advance a virtual clock at `speed` multiplier.
    Playback,
}

/// Client → server control messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ControlAction {
    /// Switch between live and playback.
    SetMode { mode: Mode },
    /// Play/pause playback. No-op in live mode.
    Play,
    Pause,
    /// Jump playback to a specific instant (ms since epoch).
    Seek { t_ms: i64 },
    /// Set the playback speed multiplier.
    SetSpeed { value: f64 },
    /// Enable/disable looping in playback.
    SetLoop { value: bool },
    /// Set the playback window bounds.
    SetRange { from_ms: i64, to_ms: i64 },
}

/// Server → client state broadcast on every change and on `/meta`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplerState {
    pub mode: Mode,
    pub playing: bool,
    pub speed: f64,
    pub loop_: bool,
    pub virtual_time_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    /// Number of clients currently subscribed to the SSE stream.
    pub subscriber_count: usize,
}