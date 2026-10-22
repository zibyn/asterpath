//! Facade error sum. Each layer keeps its own enum; this only adds them up.

use asterpath_core::{ExecutionError, LifecycleError, StorageError};
use asterpath_protocol::{AgentError, ProviderError};

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum AsterError {
    #[error("configuration error: {message}")]
    Config { message: String },
    #[error(transparent)]
    Agent(#[from] AgentError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
}
