use thiserror::Error;

#[derive(Debug, Error)]
pub enum OperatorError {
    #[error("kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("invalid spec: {0}")]
    Spec(String),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Outcome type used by the reconcile loop.
pub type Result<T, E = OperatorError> = std::result::Result<T, E>;